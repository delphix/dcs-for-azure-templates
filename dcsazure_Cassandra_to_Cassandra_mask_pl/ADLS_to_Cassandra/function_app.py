import ctypes
import gc
import json
import logging
import time
import uuid
from collections import deque
from contextlib import contextmanager
from datetime import datetime
from io import StringIO
from typing import Dict, Iterator, List, Optional, Tuple

import azure.durable_functions as df
import azure.functions as func
import pandas as pd
from azure.core.exceptions import ClientAuthenticationError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.filedatalake import DataLakeServiceClient
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, RoundRobinPolicy

# ============================================================================
# CONSTANTS
# ============================================================================

DEFAULT_BATCH_SIZE           = 50000
ARRAY_PROCESSING_BATCH_SIZE  = 5000
DEFAULT_MAX_RETRIES          = 5
DEFAULT_BASE_DELAY           = 0.5
DEFAULT_MAX_DELAY            = 60.0
PIPE_DELIMITER               = "|"
ESCAPE_CHARACTER             = "\\"

# ============================================================================
# AZURE FUNCTIONS APP
# ============================================================================

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# ============================================================================
# MEMORY MANAGEMENT
# ============================================================================

def force_gc() -> None:
    gc.collect(2)
    gc.collect(2)
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass


def release_dataframe(df_obj: Optional[pd.DataFrame]) -> None:
    if df_obj is None:
        return
    try:
        df_obj._mgr = None
    except AttributeError:
        try:
            df_obj._data = None
        except AttributeError:
            pass
    del df_obj
    force_gc()


@contextmanager
def memory_managed_batch(batch_label: str = "batch"):
    try:
        yield
    finally:
        logging.debug(f"[memory] flushing after {batch_label}")
        force_gc()


# ============================================================================
# KEY VAULT
# ============================================================================

def get_secret(key_vault_name: str, secret_name: str) -> str:
    kv_uri     = f"https://{key_vault_name}.vault.azure.net"
    credential = DefaultAzureCredential()
    client     = SecretClient(vault_url=kv_uri, credential=credential)
    return client.get_secret(secret_name).value


# ============================================================================
# CASSANDRA CONNECTION
# ============================================================================

def build_cassandra_cluster(
    contact_points: List[str],
    port: int,
    username: str,
    password: str,
    preferred_node: Optional[str] = None,
) -> Cluster:
    auth_provider = PlainTextAuthProvider(username=username, password=password)
    lb_policy = (
        DCAwareRoundRobinPolicy(local_dc=preferred_node)
        if preferred_node
        else RoundRobinPolicy()
    )
    profile = ExecutionProfile(load_balancing_policy=lb_policy)
    return Cluster(
        contact_points=contact_points,
        port=port,
        auth_provider=auth_provider,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        connect_timeout=30,
        control_connection_timeout=30,
        compression=True,
        protocol_version=4,
        executor_threads=4,
    )


def validate_cassandra_connection(
    contact_points, port, username, password, keyspace, table, preferred_node=None
) -> Tuple:
    try:
        cluster = build_cassandra_cluster(contact_points, port, username, password, preferred_node)
        session = cluster.connect(keyspace)
        rows = session.execute(
            "SELECT table_name FROM system_schema.tables "
            "WHERE keyspace_name=%s AND table_name=%s ALLOW FILTERING",
            (keyspace, table),
        )
        if not rows.one():
            raise ValueError(f"Table '{table}' not found in keyspace '{keyspace}'.")
        return cluster, session
    except ValueError:
        raise
    except Exception as e:
        raise ValueError(f"Cassandra connection failed: {str(e)}")


def shutdown_cassandra(cluster, session) -> None:
    try:
        if session:
            session.shutdown()
    except Exception:
        pass
    try:
        if cluster:
            cluster.shutdown()
    except Exception:
        pass
    force_gc()


# ============================================================================
# ADLS CONNECTION
# ============================================================================

def validate_adls_connection(adls_account_name: str, adls_file_system: str):
    try:
        credential     = DefaultAzureCredential()
        service_client = DataLakeServiceClient(
            account_url=f"https://{adls_account_name}.dfs.core.windows.net",
            credential=credential,
        )
        fs_client = service_client.get_file_system_client(adls_file_system)
        list(fs_client.get_paths(path="", max_results=1))
        return service_client
    except ClientAuthenticationError:
        raise ValueError("ADLS authentication failed.")
    except ResourceNotFoundError:
        raise ValueError(f"ADLS filesystem '{adls_file_system}' does not exist.")
    except Exception as e:
        raise ValueError(f"ADLS connection failed: {str(e)}")


# ============================================================================
# CASSANDRA SCHEMA UTILITIES
# ============================================================================

def get_column_type(session, keyspace: str, table: str, column: str) -> str:
    try:
        row = session.execute(
            "SELECT type FROM system_schema.columns "
            "WHERE keyspace_name=%s AND table_name=%s AND column_name=%s ALLOW FILTERING",
            (keyspace, table, column),
        ).one()
        return row.type if row else "text"
    except Exception as e:
        logging.warning(f"[get_column_type] Could not fetch type for {column!r}: {e}")
        return "text"


def _parse_cassandra_timestamp(value: str):
    from datetime import datetime as _dt
    if not value or not isinstance(value, str): return value
    v = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try: return _dt.strptime(v, fmt)
        except ValueError: continue
    logging.warning(f"[_parse_cassandra_timestamp] Cannot parse {v!r}.")
    return v

def _parse_cassandra_date(value: str):
    from datetime import date as _date, datetime as _dt
    if not value or not isinstance(value, str): return value
    v = value.strip()
    try: return _date.fromisoformat(v)
    except ValueError: pass
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try: return _dt.strptime(v, fmt).date()
        except ValueError: continue
    logging.warning(f"[_parse_cassandra_date] Cannot parse {v!r}.")
    return v

def _parse_cassandra_time(value: str):
    from datetime import datetime as _dt
    if not value or not isinstance(value, str): return value
    v = value.strip()
    for fmt in ("%H:%M:%S.%f", "%H:%M:%S", "%H:%M"):
        try: return _dt.strptime(v, fmt).time()
        except ValueError: continue
    logging.warning(f"[_parse_cassandra_time] Cannot parse {v!r}.")
    return v

def coerce_to_cql_type(value: str, cql_type: str):
    t = (cql_type or "text").lower().strip()
    try:
        if t in ("int", "smallint", "tinyint", "varint"):  return int(value)
        if t in ("bigint", "counter"):                     return int(value)
        if t in ("float",):                                return float(value)
        if t in ("double", "decimal"):                     return float(value)
        if t in ("boolean",):                              return value.strip().lower() in ("true", "1", "yes")
        if t in ("uuid", "timeuuid"):                      return uuid.UUID(value)
        if t in ("timestamp",):                            return _parse_cassandra_timestamp(value)
        if t in ("date",):                                 return _parse_cassandra_date(value)
        if t in ("time",):                                 return _parse_cassandra_time(value)
        return value
    except Exception as e:
        logging.warning(f"[coerce_to_cql_type] Cannot coerce {value!r} → {cql_type!r}: {e}")
        return value


class CassandraMetadata:

    @staticmethod
    def get_column_metadata(session, keyspace: str, table: str) -> List[Dict]:
        rows   = session.execute(
            "SELECT column_name, type FROM system_schema.columns "
            "WHERE keyspace_name=%s AND table_name=%s ALLOW FILTERING",
            (keyspace, table),
        )
        result = [{"column_name": r.column_name, "type": r.type} for r in rows]
        del rows
        force_gc()
        return result

    @staticmethod
    def classify_columns(column_metadata: List[Dict]) -> Tuple[List[str], List[str]]:
        relational, json_candidates = [], []
        for col in column_metadata:
            cname, ctype = col["column_name"], col["type"].lower()
            if ctype in ("text", "varchar"):
                json_candidates.append(cname)
            else:
                relational.append(cname)
        return relational, json_candidates

    @staticmethod
    def get_clustering_keys(session, keyspace: str, table: str) -> List[str]:
        """Fetch clustering key column names from Cassandra system schema."""
        rows = session.execute(
            "SELECT column_name FROM system_schema.columns "
            "WHERE keyspace_name=%s AND table_name=%s AND kind='clustering' ALLOW FILTERING",
            (keyspace, table),
        )
        result = [r.column_name for r in rows]
        del rows
        force_gc()
        return result

    @staticmethod
    def detect_json_columns(
        session, keyspace, table, candidate_columns,
        partition_key=None, partition_key_value=None, sample_size=2,
        clustering_key=None, clustering_key_value=None,
    ) -> List[str]:
        if not candidate_columns:
            return []
        cols_expr = ", ".join(f'"{c}"' for c in candidate_columns)
        logging.info(f"{partition_key}partition_key_value {partition_key_value}")
        if partition_key and partition_key_value:

            pk_type = get_column_type(session, keyspace, table, partition_key)

            # Ensure value is list for IN
            if isinstance(partition_key_value, str):
                partition_key_value = [v.strip() for v in partition_key_value.split(",") if v.strip()]
            elif not isinstance(partition_key_value, list):
                partition_key_value = [partition_key_value]

            coerced_values = [
                coerce_to_cql_type(v, pk_type) for v in partition_key_value
            ]

            placeholders = ",".join(["%s"] * len(coerced_values))

            where_clause = f'WHERE "{partition_key}" IN ({placeholders})'
            bind_values = coerced_values

            logging.info(f"{clustering_key}clustering_key_value {clustering_key_value}")

            if clustering_key and clustering_key_value:

                ck_type = get_column_type(session, keyspace, table, clustering_key)

                if isinstance(clustering_key_value, str):
                    clustering_key_value = [v.strip() for v in clustering_key_value.split(",") if v.strip()]
                elif not isinstance(clustering_key_value, list):
                    clustering_key_value = [clustering_key_value]

                coerced_ck_values = [
                    coerce_to_cql_type(v, ck_type) for v in clustering_key_value
                ]

                ck_placeholders = ",".join(["%s"] * len(coerced_ck_values))

                where_clause += f' AND "{clustering_key}" IN ({ck_placeholders})'

                bind_values.extend(coerced_ck_values)

            cql = (
                f'SELECT {cols_expr} FROM "{keyspace}"."{table}" '
                f'{where_clause} ALLOW FILTERING'
            )
            logging.info(f"Fitering Check this : {cql}")
            rows = list(session.execute(cql, tuple(bind_values)))
        else:
            cql  = f'SELECT {cols_expr} FROM "{keyspace}"."{table}" ALLOW FILTERING'
            rows = list(session.execute(cql))

        confirmed_json: set = set()
        for row in rows:
            for col in candidate_columns:
                val = getattr(row, col, None)
                if val and isinstance(val, str):
                    stripped = val.strip()
                    if stripped.startswith(("{", "[")):
                        try:
                            json.loads(val)
                            confirmed_json.add(col)
                        except (json.JSONDecodeError, ValueError):
                            pass
        del rows
        force_gc()
        return list(confirmed_json)


# ============================================================================
# RETRY STRATEGY
# ============================================================================

class CassandraRetryStrategy:

    def __init__(self, max_retries=DEFAULT_MAX_RETRIES, base_delay=DEFAULT_BASE_DELAY, max_delay=DEFAULT_MAX_DELAY):
        self.max_retries = max_retries
        self.base_delay  = base_delay
        self.max_delay   = max_delay
        self.retry_count_by_error: Dict[str, int] = {}

    def execute_with_retry(self, operation, operation_name: str = "operation"):
        import random
        for attempt in range(self.max_retries + 1):
            try:
                return operation()
            except Exception as e:
                error_type = self._classify_error(str(e))
                self.retry_count_by_error[error_type] = self.retry_count_by_error.get(error_type, 0) + 1
                if attempt < self.max_retries:
                    wait = min(self.base_delay * (2 ** attempt), self.max_delay)
                    wait += random.uniform(0, 0.1 * wait)
                    logging.warning(f"Retryable error in {operation_name} ({error_type}) attempt {attempt+1}. Waiting {wait:.2f}s.")
                    time.sleep(wait)
                else:
                    logging.error(f"{operation_name} failed after {self.max_retries+1} attempts: {e}")
                    raise

    def _classify_error(self, msg: str) -> str:
        m = msg.lower()
        if "unavailable" in m:                 return "UnavailableException"
        if "timeout" in m or "timed out" in m: return "Timeout"
        if "overloaded" in m:                  return "Overloaded"
        if "connection" in m:                  return "ConnectionError"
        return "Other"

    def get_stats(self) -> Dict:
        return {"total_retries": sum(self.retry_count_by_error.values()), "retries_by_error_type": self.retry_count_by_error.copy()}


# ============================================================================
# STREAMING CASSANDRA READER
# ============================================================================

class StreamingCassandraReader:

    def __init__(self, session, keyspace, table, batch_size,
                 partition_key=None, partition_key_value=None, retry_strategy=None,
                 clustering_key=None, clustering_key_value=None):
        self.session               = session
        self.keyspace              = keyspace
        self.table                 = table
        self.batch_size            = batch_size
        self.partition_key         = (partition_key         or "").strip() or None
        self.partition_key_value   = partition_key_value
        self.clustering_key        = (clustering_key        or "").strip() or None
        self.clustering_key_value  = clustering_key_value
        self.retry_strategy        = retry_strategy or CassandraRetryStrategy()
        self._rows_read            = 0

    def stream_rows(self) -> Iterator[List]:
        if self.partition_key and self.partition_key_value:
            logging.info(f"[stream_rows] PARTITION mode — key={self.partition_key!r} value={self.partition_key_value!r}")
            yield from self._stream_partition()
        else:
            if self.partition_key or self.partition_key_value:
                logging.warning("[stream_rows] Both partition key AND value required. Falling back to FULL TABLE SCAN.")
            else:
                logging.info("[stream_rows] FULL TABLE SCAN mode")
            yield from self._stream_all()

    def _stream_partition(self) -> Iterator[List]:
        pk_type       = get_column_type(self.session, self.keyspace, self.table, self.partition_key)
        
        # Ensure values are lists for IN operator
        if isinstance(self.partition_key_value, str):
            pkv_list = [v.strip() for v in self.partition_key_value.split(",") if v.strip()]
        elif isinstance(self.partition_key_value, list):
            pkv_list = self.partition_key_value
        else:
            pkv_list = [self.partition_key_value]

        coerced_values = [coerce_to_cql_type(v, pk_type) for v in pkv_list]
        placeholders = ",".join(["?"] * len(coerced_values))
        bind_values  = coerced_values
        where_clause = f'WHERE "{self.partition_key}" IN ({placeholders})'

        if self.clustering_key and self.clustering_key_value:
            ck_type = get_column_type(self.session, self.keyspace, self.table, self.clustering_key)
            
            if isinstance(self.clustering_key_value, str):
                ckv_list = [v.strip() for v in self.clustering_key_value.split(",") if v.strip()]
            elif isinstance(self.clustering_key_value, list):
                ckv_list = self.clustering_key_value
            else:
                ckv_list = [self.clustering_key_value]

            coerced_ck_values = [coerce_to_cql_type(v, ck_type) for v in ckv_list]
            ck_placeholders = ",".join(["?"] * len(coerced_ck_values))
            where_clause    += f' AND "{self.clustering_key}" IN ({ck_placeholders})'
            bind_values.extend(coerced_ck_values)
            logging.info(f"[stream_rows] CLUSTERING filter — key={self.clustering_key!r} value={ckv_list!r}")

        cql = (
            f'SELECT * FROM "{self.keyspace}"."{self.table}" '
            f'{where_clause} ALLOW FILTERING'
        )
        logging.info(f"filter query: {cql}")
                     
        def _execute():
            stmt = self.session.prepare(cql)
            stmt.fetch_size = self.batch_size
            return self.session.execute(stmt, tuple(bind_values))
        try:
            result_set = self.retry_strategy.execute_with_retry(_execute, "stream_partition")
        except Exception as e:
            logging.error(f"[_stream_partition] Query failed: {e}")
            return
        yield from self._paginate(result_set)

    def _stream_all(self) -> Iterator[List]:
        cql = f'SELECT * FROM "{self.keyspace}"."{self.table}" ALLOW FILTERING'
        def _execute():
            stmt = self.session.prepare(cql)
            stmt.fetch_size = self.batch_size
            return self.session.execute(stmt)
        try:
            result_set = self.retry_strategy.execute_with_retry(_execute, "stream_all")
        except Exception as e:
            logging.error(f"[_stream_all] Query failed: {e}")
            return
        yield from self._paginate(result_set)

    def _paginate(self, result_set) -> Iterator[List]:
        batch: List = []
        batch_count = 0
        for row in result_set:
            batch.append(row)
            self._rows_read += 1
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
                batch_count += 1
                if batch_count % 5 == 0:
                    force_gc()
        if batch:
            yield batch
            force_gc()

    def get_stats(self) -> Dict:
        return {"rows_read": self._rows_read}


# ============================================================================
# DATA TRANSFORMATION  — OPTIMISED
# ============================================================================

def flatten_no_arrays(d: Dict, parent: str = "") -> Dict:
    """Iterative flatten (avoids recursion overhead on deep docs)."""
    flat: Dict = {}
    stack = [(d, parent)]
    while stack:
        current_dict, current_parent = stack.pop()
        for k, v in current_dict.items():
            key = f"{current_parent}.{k}" if current_parent else k
            if isinstance(v, list):
                flat[key] = v
            elif isinstance(v, dict):
                stack.append((v, key))
            else:
                flat[key] = v
    return flat


def _process_flat_fields(
    flat: Dict,
    row_rid: str,
    base_table: str,
    parent_fields: Dict,
    child_queue: deque,
):
    """
    Split a flat dict into scalar parent fields and array children.
    Mutates parent_fields in-place. Pushes children onto child_queue.
    Avoids creating intermediate dicts.
    """
    for key, value in flat.items():
        if not isinstance(value, list):
            parent_fields[key] = value
            continue
        primitives = [v for v in value if not isinstance(v, dict)]
        dicts      = [v for v in value if isinstance(v, dict)]
        if primitives and not dicts:
            parent_fields[key] = value
            continue
        parent_fields[f"_has_array_{key}"] = True
        child_table = f"{base_table}.{key}" if base_table else key
        for item in dicts:
            child_queue.append((item, child_table, row_rid))


def extract_arrays_streaming(
    doc: Dict,
    root_rid: str,
    base_table: str = "",
) -> Tuple[Dict, Dict[str, List[Dict]]]:
    """
    OPTIMISED replacement for extract_arrays_from_json_doc.

    Key changes vs original:
    - Single iterative pass with a deque (no nested pd.DataFrame creation mid-row)
    - Returns raw row-dicts per child table, NOT DataFrames
      → caller accumulates across many source rows before calling pd.DataFrame once
    - base_table is the JSON column name (e.g. "orders"), so child array folders
      are named after the column — never the "_root_" sentinel.
    """
    doc["_rid"] = root_rid
    parent_fields: Dict = {}
    child_rows: Dict[str, List[Dict]] = {}

    flat_root = flatten_no_arrays(doc)
    child_queue: deque = deque()
    _process_flat_fields(flat_root, root_rid, base_table, parent_fields, child_queue)
    del flat_root

    while child_queue:
        item, table_name, parent_rid = child_queue.popleft()
        child_rid  = str(uuid.uuid4())
        flat_child = flatten_no_arrays(item)
        row: Dict  = {"_rid": child_rid, "_parent_rid": parent_rid}

        _process_flat_fields(flat_child, child_rid, table_name, row, child_queue)
        del flat_child

        data_keys = {k for k in row if k not in ("_rid", "_parent_rid")}
        if data_keys:
            child_rows.setdefault(table_name, []).append(row)

    return parent_fields, child_rows


# ============================================================================
# ROW-LEVEL PROCESSING
# ============================================================================

def _process_single_row(
    row,
    column_names: List[str],
    relational_cols: List[str],
    json_cols: List[str],
) -> Tuple[Dict, Dict[str, Dict]]:
    """
    Process ONE Cassandra row → (parent_record_dict, per_json_col child rows).

    Returns raw dicts, NOT DataFrames — DataFrame construction is deferred to
    batch-flush time so we only call pd.DataFrame(records) ONCE per batch.
    """
    row_rid       = str(uuid.uuid4())
    parent_record = {"_row_rid": row_rid}

    for col in relational_cols:
        parent_record[col] = getattr(row, col, None)

    json_col_output: Dict[str, Dict] = {}

    for jcol in json_cols:
        raw = getattr(row, jcol, None)
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            continue

        try:
            parsed = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, ValueError):
            json_col_output.setdefault(jcol, {"parent": [], "children": {}})
            json_col_output[jcol]["parent"].append({"_row_rid": row_rid, jcol: raw})
            continue

        if isinstance(parsed, dict):
            doc = parsed
        elif isinstance(parsed, list):
            doc = {jcol: parsed}
        else:
            json_col_output.setdefault(jcol, {"parent": [], "children": {}})
            json_col_output[jcol]["parent"].append({"_row_rid": row_rid, jcol: parsed})
            continue

        parent_fields, child_rows = extract_arrays_streaming(doc, row_rid, base_table=jcol)
        parent_fields["_row_rid"] = row_rid
        parent_fields.pop("_rid", None)

        entry = json_col_output.setdefault(jcol, {"parent": [], "children": {}})
        if parent_fields:
            entry["parent"].append(parent_fields)
        for arr_name, rows_list in child_rows.items():
            entry["children"].setdefault(arr_name, []).extend(rows_list)

        del child_rows, parent_fields, parsed, doc

    return parent_record, json_col_output


def process_cassandra_batch_streaming(
    rows: List,
    column_names: List[str],
    relational_cols: List[str],
    json_cols: List[str],
) -> Tuple[pd.DataFrame, Dict[str, Dict[str, pd.DataFrame]]]:
    """
    OPTIMISED batch processor.

    Strategy:
    1. Accumulate raw dicts for ALL rows in the batch (no per-row DataFrame).
    2. Build DataFrames ONCE at the end of the batch with pd.DataFrame(records).
    3. This cuts DataFrame construction overhead from O(N) to O(1) per batch.
    4. JSON child tables are also built from accumulated dicts → one concat call.

    Returns:
        parent_df          — relational columns, one row per Cassandra row
        json_col_results   — { jcol: { "parent": DataFrame, "children": { arr: DataFrame } } }
    """
    parent_records: List[Dict] = []
    accumulator: Dict[str, Dict] = {jcol: {"parent": [], "children": {}} for jcol in json_cols}

    for row in rows:
        parent_record, json_col_output = _process_single_row(row, column_names, relational_cols, json_cols)
        parent_records.append(parent_record)

        for jcol, data in json_col_output.items():
            accumulator[jcol]["parent"].extend(data["parent"])
            for arr_name, child_list in data["children"].items():
                accumulator[jcol]["children"].setdefault(arr_name, []).extend(child_list)

        del json_col_output

    parent_df = pd.DataFrame(parent_records) if parent_records else pd.DataFrame()
    del parent_records
    force_gc()

    json_col_results: Dict[str, Dict[str, pd.DataFrame]] = {}

    for jcol, data in accumulator.items():
        parent_df_jcol = pd.DataFrame(data["parent"]) if data["parent"] else pd.DataFrame()

        children_dfs: Dict[str, pd.DataFrame] = {}
        for arr_name, row_list in data["children"].items():
            if row_list:
                children_dfs[arr_name] = pd.DataFrame(row_list)
            del row_list

        json_col_results[jcol] = {"parent": parent_df_jcol, "children": children_dfs}
        del parent_df_jcol, children_dfs, data

    del accumulator
    force_gc()

    return parent_df, json_col_results


# ============================================================================
# DATA CLEANING
# ============================================================================

def clean_dataframe(df_obj: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df_obj is None or df_obj.empty:
        return pd.DataFrame()

    def fix_value(val):
        if val is None:                                                  return None
        if isinstance(val, (str, int, float, bool)):                    return val
        if isinstance(val, list) and all(not isinstance(i, dict) for i in val): return val
        if isinstance(val, (dict, list)):                               return None
        try:                                                             return str(val)
        except Exception:                                               return None

    for col in df_obj.columns:
        if df_obj[col].dtype == object:
            df_obj[col] = df_obj[col].apply(fix_value)

    meta_cols = {"_rid", "_parent_rid", "_row_rid"}
    data_cols = [c for c in df_obj.columns if c not in meta_cols]
    if data_cols:
        df_obj = df_obj.dropna(subset=data_cols, how="all")

    return df_obj.reset_index(drop=True)


def cassandra_row_to_dict(row, column_names: List[str]) -> Dict:
    return {col: getattr(row, col, None) for col in column_names}


# ============================================================================
# ADLS UPLOAD  (unchanged logic, kept intact)
# ============================================================================

def _serialize_df(df_obj: pd.DataFrame, include_header: bool = True) -> bytes:
    buf = StringIO()
    df_obj.fillna("").to_csv(
        buf, index=False, sep=PIPE_DELIMITER,
        header=include_header, quoting=0, escapechar=ESCAPE_CHARACTER,
    )
    return buf.getvalue().encode("utf-8")


def _ensure_adls_directory(fs_client, directory: str) -> None:
    """Create directory hierarchy in ADLS, swallow errors if already exists."""
    dir_segments = directory.strip("/").split("/") if directory else []
    curr = ""
    for seg in dir_segments:
        curr = f"{curr}/{seg}" if curr else seg
        try:
            fs_client.get_directory_client(curr).create_directory()
        except Exception:
            pass


def _delete_adls_file_if_exists(file_client) -> None:
    """Silently delete an ADLS file — no-op if it does not exist."""
    try:
        file_client.delete_file()
    except Exception:
        pass


def _get_committed_file_size(file_client) -> int:
    """
    Re-fetch the ACTUAL committed byte length from ADLS every time.

    Never track size locally across calls — the local estimate goes stale if
    a previous upload partially failed, the SDK cached an old value, or a
    schema-evolution rewrite replaced the file mid-stream.
    Fetching fresh properties is one cheap API call but eliminates
    InvalidFlushPosition entirely.
    """
    try:
        return file_client.get_file_properties().size
    except Exception:
        return 0


def _write_fresh_file(dir_client, file_name: str, content_bytes: bytes) -> None:
    """Create a brand-new ADLS file and write content_bytes starting at offset 0."""
    fc = dir_client.create_file(file_name)
    fc.append_data(content_bytes, offset=0, length=len(content_bytes))
    fc.flush_data(len(content_bytes))


def upload_csv_to_adls(
    service_client,
    file_system: str,
    directory: str,
    file_name: str,
    df_input: pd.DataFrame,
    mode: str = "write",
    known_columns: Optional[set] = None,
) -> set:
    """
    Upload a DataFrame as a pipe-delimited CSV to ADLS Gen2.

    mode="write"  → delete any existing file, write fresh (batch 1 of a job run)
    mode="append" → append to existing file; include header only if file is new.

    Correctness rules:
    - NEVER track offset locally across calls. Always call _get_committed_file_size()
      immediately before each append so the offset is always fresh.
    - Schema evolution (new columns mid-stream): download existing, merge, rewrite
      the entire file from offset 0 on a brand-new file handle.
    - flush_data(offset + len) where offset = freshly-fetched committed size.
    """
    df_input = clean_dataframe(df_input)
    if df_input is None or df_input.empty:
        return known_columns or set()

    fs_client       = service_client.get_file_system_client(file_system)
    _ensure_adls_directory(fs_client, directory)

    dir_client      = fs_client.get_directory_client(directory)
    file_client     = dir_client.get_file_client(file_name)
    current_columns = set(df_input.columns)

    if known_columns is None:
        known_columns = current_columns

    new_columns = current_columns - known_columns

    # ── Schema evolution: new columns appeared — download, merge, rewrite ─────
    if mode == "append" and new_columns:
        try:
            committed_size = _get_committed_file_size(file_client)
            if committed_size > 0:
                existing_content = file_client.download_file().readall().decode("utf-8")
                existing_df      = pd.read_csv(
                    StringIO(existing_content), sep=PIPE_DELIMITER, keep_default_na=False
                )
                del existing_content

                known_columns = known_columns | new_columns
                for col in known_columns:
                    if col not in existing_df.columns: existing_df[col] = None
                    if col not in df_input.columns:    df_input[col]    = None

                column_order = sorted(known_columns)
                combined_df  = pd.concat(
                    [existing_df[column_order], df_input[column_order]], ignore_index=True
                )
                release_dataframe(existing_df)

                content_bytes = _serialize_df(combined_df, include_header=True)
                release_dataframe(combined_df)

                _delete_adls_file_if_exists(file_client)
                _write_fresh_file(dir_client, file_name, content_bytes)
                del content_bytes
                force_gc()
                return known_columns
        except Exception as exc:
            logging.warning(
                f"[upload] Schema-evolution rewrite failed for {file_name}: {exc}. "
                "Falling through to standard append."
            )

    # ── Align columns to known schema ─────────────────────────────────────────
    for col in known_columns:
        if col not in df_input.columns:
            df_input[col] = None
    if known_columns:
        df_input = df_input[sorted(known_columns)]

    try:
        if mode == "write":
            _delete_adls_file_if_exists(file_client)
            content_bytes = _serialize_df(df_input, include_header=True)
            release_dataframe(df_input)
            _write_fresh_file(dir_client, file_name, content_bytes)
            del content_bytes

        else:
            committed_size = _get_committed_file_size(file_client)
            file_is_new    = committed_size == 0

            content_bytes = _serialize_df(df_input, include_header=file_is_new)
            release_dataframe(df_input)

            if file_is_new:
                file_client = dir_client.create_file(file_name)

            file_client.append_data(content_bytes, offset=committed_size, length=len(content_bytes))
            file_client.flush_data(committed_size + len(content_bytes))
            del content_bytes

    except Exception as e:
        logging.error(f"[upload] Failed for {file_name}: {e}")
        raise
    finally:
        force_gc()

    return known_columns | current_columns


def upload_json_col_child_tables(
    service_client,
    adls_file_system: str,
    export_dir: str,
    json_col_name: str,
    children: Dict[str, pd.DataFrame],
    child_schemas: Dict,
    mode: str,
) -> Dict:
    for arr_name, child_df in children.items():
        cleaned_df = clean_dataframe(child_df)
        release_dataframe(child_df)

        if cleaned_df is None or cleaned_df.empty:
            release_dataframe(cleaned_df)
            continue

        path_parts = arr_name.split(".")
        final_dir  = f"{export_dir}/{json_col_name}/{'/'.join(path_parts)}"
        file_name  = f"{path_parts[-1]}.csv"
        schema_key = f"{json_col_name}.{arr_name}"

        child_schemas[schema_key] = upload_csv_to_adls(
            service_client, adls_file_system, final_dir,
            file_name, cleaned_df, mode, child_schemas.get(schema_key),
        )
        release_dataframe(cleaned_df)

    force_gc()
    return child_schemas


# ============================================================================
# PARAMETER VALIDATION
# ============================================================================

def validate_and_extract_params(params: dict) -> dict:
    required = {
        "adls_account_name":               params.get("adls_account_name"),
        "adls_file_system":                params.get("adls_file_system"),
        "cassandra_contact_points":        params.get("cassandra_contact_points"),
        "cassandra_username":              params.get("cassandra_username"),
        "cassandra_keyspace":              params.get("cassandra_keyspace"),
        "cassandra_table":                 params.get("cassandra_table"),
        "Cassandra_key_vault_name":        params.get("Cassandra_key_vault_name"),
        "Cassandra_key_vault_secret_name": params.get("Cassandra_key_vault_secret_name"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise ValueError(f"Missing required parameters: {missing}")

    raw_cp = params["cassandra_contact_points"]
    contact_points = (
        [cp.strip() for cp in raw_cp.split(",") if cp.strip()]
        if isinstance(raw_cp, str) else list(raw_cp)
    )

    try:    port = int(params.get("cassandra_port", 9042))
    except: port = 9042

    try:    preferred_port = int(params.get("cassandra_preferred_port", port))
    except: preferred_port = port

    raw_batch = params.get("adls_batch_size") or params.get("cassandra_batch_size") or DEFAULT_BATCH_SIZE
    try:    batch_size = max(1, int(raw_batch))
    except: batch_size = DEFAULT_BATCH_SIZE

    try:    array_batch = max(1, int(params.get("array_processing_batch_size", ARRAY_PROCESSING_BATCH_SIZE)))
    except: array_batch = ARRAY_PROCESSING_BATCH_SIZE

    raw_pk  = (params.get("cassandra_partition_key")       or "").strip()
    raw_pkv = params.get("cassandra_partition_key_value")

    if bool(raw_pk) != (raw_pkv is not None):
        logging.warning("[validate_params] Both partition key and value must be set. Falling back to full-table scan.")
        raw_pk, raw_pkv = "", None

    raw_ck  = (params.get("cassandra_clustering_key")       or "").strip()
    raw_ckv = params.get("cassandra_clustering_key_value")

    if bool(raw_ck) != (raw_ckv is not None):
        logging.warning("[validate_params] Both clustering key and value must be set. Ignoring clustering key filter.")
        raw_ck, raw_ckv = "", None

    if raw_ck and not raw_pk:
        logging.warning("[validate_params] Clustering key filter requires a partition key. Ignoring clustering key filter.")
        raw_ck, raw_ckv = "", None

    raw_trunc = params.get("truncate_sink_before_write", False)
    truncate  = (raw_trunc.strip().lower() in ("true", "1", "yes") if isinstance(raw_trunc, str) else bool(raw_trunc))

    extracted = {
        "adls_account_name":              params["adls_account_name"],
        "adls_file_system":               params["adls_file_system"],
        "adls_directory":                 (params.get("adls_directory") or "").strip(),
        "cassandra_contact_points":       contact_points,
        "cassandra_port":                 port,
        "cassandra_preferred_node":       (params.get("cassandra_preferred_node") or "").strip() or None,
        "cassandra_preferred_port":       preferred_port,
        "cassandra_username":             params["cassandra_username"],
        "cassandra_keyspace":             params["cassandra_keyspace"],
        "cassandra_table":                params["cassandra_table"],
        "cassandra_partition_key":        raw_pk  or None,
        "cassandra_partition_key_value":  raw_pkv,
        "cassandra_clustering_key":       raw_ck  or None,
        "cassandra_clustering_key_value": raw_ckv,
        "key_vault_name":                 params["Cassandra_key_vault_name"],
        "key_vault_secret_name":          params["Cassandra_key_vault_secret_name"],
        "batch_size":                     batch_size,
        "array_processing_batch_size":    array_batch,
        "truncate_sink_before_write":     truncate,
    }

    logging.info(
        f"[validate_params] "
        f"table={extracted['cassandra_keyspace']}.{extracted['cassandra_table']} | "
        f"partition_key={extracted['cassandra_partition_key']!r} | "
        f"partition_value={extracted['cassandra_partition_key_value']!r} | "
        f"clustering_key={extracted['cassandra_clustering_key']!r} | "
        f"clustering_value={extracted['cassandra_clustering_key_value']!r} | "
        f"batch_size={extracted['batch_size']} | "
        f"truncate={extracted['truncate_sink_before_write']}"
    )
    return extracted


# ============================================================================
# MAIN ACTIVITY FUNCTION — OPTIMISED BATCH LOOP
# ============================================================================

def _run_cassandra_to_adls(params: dict):
    """
    Optimised pipeline:
      • process_cassandra_batch_streaming: accumulates raw dicts, builds ONE
        DataFrame per batch (not per row)
      • ADLS upload happens immediately after each batch is built → memory freed
        before next batch begins
      • No second-pass consolidation step — child table dicts flushed inline
    """
    start_time = datetime.utcnow()
    cluster    = None
    session    = None

    try:
        if isinstance(params, str):
            try:
                params = json.loads(params)
                logging.info("[activity] params was a JSON string — deserialised OK")
            except Exception as e:
                logging.warning(f"[activity] params is a string but not valid JSON: {e}")

        params = validate_and_extract_params(params)

        cassandra_password = get_secret(params["key_vault_name"], params["key_vault_secret_name"])

        cluster, session = validate_cassandra_connection(
            contact_points=params["cassandra_contact_points"],
            port=params["cassandra_port"],
            username=params["cassandra_username"],
            password=cassandra_password,
            keyspace=params["cassandra_keyspace"],
            table=params["cassandra_table"],
            preferred_node=params["cassandra_preferred_node"],
        )
        del cassandra_password
        force_gc()

        service_client = validate_adls_connection(params["adls_account_name"], params["adls_file_system"])

        keyspace = params["cassandra_keyspace"]
        table    = params["cassandra_table"]

        column_metadata  = CassandraMetadata.get_column_metadata(session, keyspace, table)
        relational_cols, json_candidates = CassandraMetadata.classify_columns(column_metadata)
        column_names     = [c["column_name"] for c in column_metadata]
        del column_metadata
        force_gc()

        # ── Auto-detect clustering keys from Cassandra system schema ──────────
        clustering_key       = params.get("cassandra_clustering_key")
        clustering_key_value = params.get("cassandra_clustering_key_value")

        detected_ck = CassandraMetadata.get_clustering_keys(session, keyspace, table)
        if detected_ck:
            logging.info(f"[activity] Detected clustering keys for table '{table}': {detected_ck}")
            if not clustering_key:
                logging.info(
                    f"[activity] No cassandra_clustering_key supplied in params. "
                    f"Available clustering keys: {detected_ck}. "
                    "Pass cassandra_clustering_key + cassandra_clustering_key_value to filter."
                )
        else:
            logging.info(f"[activity] No clustering keys detected for table '{table}'.")

        json_cols = CassandraMetadata.detect_json_columns(
            session=session, keyspace=keyspace, table=table,
            candidate_columns=json_candidates,
            partition_key=params["cassandra_partition_key"],
            partition_key_value=params["cassandra_partition_key_value"],
            clustering_key=clustering_key,
            clustering_key_value=clustering_key_value,
        )

        non_json_text   = [c for c in json_candidates if c not in json_cols]
        relational_cols = relational_cols + non_json_text
        del json_candidates, non_json_text
        force_gc()

        logging.info(f"Schema — relational: {relational_cols} | json: {json_cols}")

        retry_strategy = CassandraRetryStrategy()
        reader = StreamingCassandraReader(
            session=session, keyspace=keyspace, table=table,
            batch_size=params["batch_size"],
            partition_key=params["cassandra_partition_key"],
            partition_key_value=params["cassandra_partition_key_value"],
            clustering_key=clustering_key,
            clustering_key_value=clustering_key_value,
            retry_strategy=retry_strategy,
        )

        export_dir = (
            f"{params['adls_directory']}/{table}" if params["adls_directory"] else table
        )

        # ── Always delete the output directory before starting ───────────────
        fs_c = service_client.get_file_system_client(params["adls_file_system"])
        try:
            fs_c.get_directory_client(export_dir).delete_directory()
            logging.info(f"[init] Cleared existing output directory: {export_dir}")
        except Exception:
            logging.info(f"[init] Output directory did not exist yet: {export_dir}")

        # ── State that persists across batches (schema tracking only) ─────────
        rows_processed    = 0
        total_parent_rows = 0
        batch_num         = 0
        parent_schema: Optional[set]         = None
        json_col_parent_schemas: Dict[str, Optional[set]] = {jc: None for jc in json_cols}
        json_col_child_schemas:  Dict[str, Dict]          = {jc: {}   for jc in json_cols}

        # ── Main batch loop ───────────────────────────────────────────────────
        for batch_rows in reader.stream_rows():
            batch_num  += 1
            mode        = "append" if batch_num > 1 else "write"
            batch_label = f"batch_{batch_num:04d}"

            logging.info(f"[{batch_label}] Processing {len(batch_rows)} rows …")

            with memory_managed_batch(batch_label):

                # ── STEP 1: Transform batch → DataFrames ──────────────────────
                parent_df, json_col_results = process_cassandra_batch_streaming(
                    rows=batch_rows,
                    column_names=column_names,
                    relational_cols=relational_cols,
                    json_cols=json_cols,
                )
                del batch_rows
                force_gc()

                # ── STEP 2: Upload parent table ───────────────────────────────
                if not parent_df.empty:
                    total_parent_rows += len(parent_df)
                    parent_schema = upload_csv_to_adls(
                        service_client, params["adls_file_system"],
                        export_dir, f"{table}.csv",
                        parent_df, mode, parent_schema,
                    )
                release_dataframe(parent_df)
                force_gc()

                # ── STEP 3: Upload each JSON column's parent + children ────────
                for jcol, data in json_col_results.items():
                    jcol_dir       = f"{export_dir}/{jcol}"
                    jcol_parent_df = data["parent"]

                    if not jcol_parent_df.empty:
                        json_col_parent_schemas[jcol] = upload_csv_to_adls(
                            service_client, params["adls_file_system"],
                            jcol_dir, f"{jcol}.csv",
                            jcol_parent_df, mode, json_col_parent_schemas[jcol],
                        )
                    release_dataframe(jcol_parent_df)
                    force_gc()

                    json_col_child_schemas[jcol] = upload_json_col_child_tables(
                        service_client=service_client,
                        adls_file_system=params["adls_file_system"],
                        export_dir=export_dir,
                        json_col_name=jcol,
                        children=data["children"],
                        child_schemas=json_col_child_schemas[jcol],
                        mode=mode,
                    )
                    data["children"].clear()

                del json_col_results
                force_gc()

            rows_processed = reader.get_stats().get("rows_read", 0)
            logging.info(f"[{batch_label}] ✓ uploaded — cumulative rows: {rows_processed}")

        # ── Shutdown ──────────────────────────────────────────────────────────
        shutdown_cassandra(cluster, session)
        cluster = session = None

        if rows_processed == 0:
            return {"status": "success", "message": "No rows found.", "rows_processed": 0}

        retry_stats = retry_strategy.get_stats()
        duration    = (datetime.utcnow() - start_time).total_seconds()

        return {
            "status":                "success",
            "rows_processed":        rows_processed,
            "parent_rows_written":   total_parent_rows,
            "json_columns_detected": json_cols,
            "relational_columns":    relational_cols,
            "batches_processed":     batch_num,
            "batch_size":            params["batch_size"],
            "duration_seconds":      round(duration, 2),
            "total_retries":         retry_stats.get("total_retries", 0),
            "retries_by_error_type": retry_stats.get("retries_by_error_type", {}),
        }

    except Exception as e:
        logging.error(f"Activity failed: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}

    finally:
        if cluster is not None or session is not None:
            shutdown_cassandra(cluster, session)
        force_gc()


# ============================================================================
# ADLS -> CASSANDRA: HELPERS
# ============================================================================

def read_csv_from_adls(service_client, file_system: str, full_path: str) -> pd.DataFrame:
    try:
        fs_client   = service_client.get_file_system_client(file_system)
        file_client = fs_client.get_file_client(full_path)
        content     = file_client.download_file().readall().decode("utf-8")
        df          = pd.read_csv(StringIO(content), sep=PIPE_DELIMITER, keep_default_na=False, dtype=str)
        del content
        force_gc()
        return df
    except Exception as e:
        logging.warning(f"[read_csv_from_adls] Cannot read '{full_path}': {e}")
        return pd.DataFrame()


def discover_json_cols_from_adls(service_client, file_system, export_dir, schema_text_cols) -> List[str]:
    """
    Identify JSON columns by checking {export_dir}/{col}/{col}.csv exists in ADLS.
    Uses ADLS structure as ground truth -- works even when the Cassandra table is
    empty (e.g. after truncate_sink_before_write from a previous run).
    """
    fs_client = service_client.get_file_system_client(file_system)
    json_cols = []
    for col in schema_text_cols:
        try:
            fs_client.get_file_client(f"{export_dir}/{col}/{col}.csv").get_file_properties()
            json_cols.append(col)
            logging.info(f"[discover_json_cols_from_adls] '{col}' -> JSON column.")
        except Exception:
            logging.debug(f"[discover_json_cols_from_adls] '{col}' -> plain text.")
    return json_cols


def discover_json_col_csvs(service_client, file_system, export_dir, json_col_name) -> Dict[str, str]:
    """
    List every CSV under {export_dir}/{json_col_name}/ -> { arr_name: full_path }.
    Mirrors upload_json_col_child_tables path formula:
        arr_name -> dir  : arr_name.replace('.', '/')
        dir -> arr_name  : dir_part.replace('/', '.')
    """
    fs_client = service_client.get_file_system_client(file_system)
    prefix    = f"{export_dir}/{json_col_name}"
    try:
        all_paths = list(fs_client.get_paths(path=prefix, recursive=True))
    except Exception as e:
        logging.warning(f"[discover_json_col_csvs] Cannot list '{prefix}': {e}")
        return {}
    result   = {}
    root_csv = f"{prefix}/{json_col_name}.csv"
    for path_item in all_paths:
        if getattr(path_item, "is_directory", False):
            continue
        full_path = path_item.name
        if not full_path.endswith(".csv"):
            continue
        if full_path == root_csv:
            result[json_col_name] = full_path
            continue
        rel      = full_path[len(f"{prefix}/"):]
        dir_part = rel.rsplit("/", 1)[0] if "/" in rel else ""
        if dir_part:
            result[dir_part.replace("/", ".")] = full_path
    logging.info(f"[discover_json_col_csvs] '{json_col_name}' -> {len(result)} CSV(s): {list(result.keys())}")
    return result


def _set_nested(d: Dict, dotted_key: str, value) -> None:
    parts = dotted_key.split(".")
    for part in parts[:-1]:
        node = d.get(part)
        if not isinstance(node, dict):
            d[part] = {}
        d = d[part]
    d[parts[-1]] = value


def _infer_csv_value(val):
    import math as _math
    if val is None: return None
    if isinstance(val, float) and _math.isnan(val): return None
    if not isinstance(val, str): return val
    s = val.strip()
    if s == "": return None
    lo = s.lower()
    if lo == "true":  return True
    if lo == "false": return False
    try:
        if "." not in s and "e" not in lo: return int(s)
    except ValueError: pass
    try:    return float(s)
    except ValueError: pass
    return s


def reconstruct_json_for_row(row: Dict, arr_name: str, children_index: Dict[str, Dict[str, List[Dict]]]) -> Dict:
    """
    Unflatten one CSV row back to a nested dict.

    RID resolution:
      Root JSON-column CSV (e.g. address.csv): _rid is stripped during forward
      export; _row_rid is present and matches _parent_rid in child CSVs.
      Child CSV rows: _rid present, used as _parent_rid by their children.
      Fix: rid = row.get('_rid') or row.get('_row_rid', '')

    _has_array_<X>: X may be dotted (e.g. 'nestedArrays.departments') for
    plain-dict-wrapped arrays; child_arr_name = arr_name + '.' + X.
    """
    result: Dict = {}
    rid = row.get("_rid") or row.get("_row_rid", "")
    for key, value in row.items():
        if key in ("_rid", "_parent_rid", "_row_rid"):
            continue
        if key.startswith("_has_array_"):
            if not value or str(value).strip().lower() in ("", "false", "0", "nan", "none"):
                continue
            array_key      = key[len("_has_array_"):]
            child_arr_name = f"{arr_name}.{array_key}"
            child_rows     = children_index.get(child_arr_name, {}).get(rid, [])
            _set_nested(result, array_key, [
                reconstruct_json_for_row(cr, child_arr_name, children_index)
                for cr in child_rows
            ])
            continue
        _set_nested(result, key, _infer_csv_value(value))
    return result


def build_children_index(table_dfs: Dict[str, pd.DataFrame], json_col_name: str) -> Dict[str, Dict[str, List[Dict]]]:
    children_index: Dict[str, Dict[str, List[Dict]]] = {}
    for arr_name, df in table_dfs.items():
        if arr_name == json_col_name or df.empty:
            continue
        if "_parent_rid" not in df.columns:
            logging.warning(f"[build_children_index] '{arr_name}' has no _parent_rid -- skipped.")
            continue
        index: Dict[str, List[Dict]] = {}
        for row_dict in df.to_dict(orient="records"):
            p_rid = row_dict.get("_parent_rid", "")
            if p_rid:
                index.setdefault(p_rid, []).append(row_dict)
        children_index[arr_name] = index
    return children_index


def reconstruct_json_column(jcol_df: pd.DataFrame, children_index: Dict[str, Dict[str, List[Dict]]], json_col_name: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for row_dict in jcol_df.to_dict(orient="records"):
        row_rid = row_dict.get("_row_rid", "")
        if not row_rid:
            logging.warning("[reconstruct_json_column] Row missing _row_rid -- skipped.")
            continue
        result[row_rid] = json.dumps(
            reconstruct_json_for_row(row_dict, json_col_name, children_index),
            ensure_ascii=False,
        )
    return result


# ============================================================================
# ADLS -> CASSANDRA: WRITER
# ============================================================================

def _group_rows_by_columns(rows: List[Dict]) -> Dict[frozenset, List[Dict]]:
    groups: Dict[frozenset, List[Dict]] = {}
    for row in rows:
        groups.setdefault(frozenset(row.keys()), []).append(row)
    return groups


def insert_rows_to_cassandra(session, keyspace, table, rows, column_types, retry_strategy) -> int:
    if not rows:
        return 0
    schema_cols = set(column_types.keys())
    cleaned     = [r for r in ({col: val for col, val in row.items() if col in schema_cols} for row in rows) if r]
    if not cleaned:
        return 0
    written = 0
    for col_set, group in _group_rows_by_columns(cleaned).items():
        col_names    = sorted(col_set)
        cols_cql     = ", ".join(f'"{c}"' for c in col_names)
        placeholders = ", ".join(["?"] * len(col_names))
        cql          = f'INSERT INTO "{keyspace}"."{table}" ({cols_cql}) VALUES ({placeholders})'
        try:
            stmt = session.prepare(cql)
        except Exception as e:
            logging.error(f"[insert_rows] Prepare failed: {e}  CQL: {cql}")
            raise
        for row in group:
            values = [
                None if (row.get(col) is None or row.get(col) == "")
                else coerce_to_cql_type(str(row[col]), column_types.get(col, "text"))
                for col in col_names
            ]
            def _do_insert(v=tuple(values)):
                session.execute(stmt, v)
            retry_strategy.execute_with_retry(_do_insert, f"insert_{table}")
            written += 1
    return written


# ============================================================================
# ADLS CLEANUP
# ============================================================================

def delete_adls_export_directory(service_client, file_system: str, export_dir: str) -> None:
    """
    Recursively delete the entire export directory from ADLS.
    Called when P_Delete_ADLS_Records=true after a successful ADLS->Cassandra load.
    Removes the parent CSV, all JSON-column sub-folders, and all child array CSVs.
    """
    try:
        fs_client  = service_client.get_file_system_client(file_system)
        dir_client = fs_client.get_directory_client(export_dir)
        dir_client.delete_directory()
        logging.info(f"[delete_adls_export_directory] Deleted '{export_dir}' and all contents.")
    except Exception as e:
        logging.warning(f"[delete_adls_export_directory] Could not delete '{export_dir}': {e}")


# ============================================================================
# ADLS -> CASSANDRA: PARAMETER VALIDATION
# ============================================================================

def validate_adls_to_cassandra_params(params: dict) -> dict:
    required = {
        "adls_account_name":               params.get("adls_account_name"),
        "adls_file_system":                params.get("adls_file_system"),
        "cassandra_contact_points":        params.get("cassandra_contact_points"),
        "cassandra_username":              params.get("cassandra_username"),
        "cassandra_keyspace":              params.get("cassandra_keyspace"),
        "cassandra_table":                 params.get("cassandra_table"),
        "Cassandra_key_vault_name":        params.get("Cassandra_key_vault_name"),
        "Cassandra_key_vault_secret_name": params.get("Cassandra_key_vault_secret_name"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise ValueError(f"Missing required parameters: {missing}")
    raw_cp = params["cassandra_contact_points"]
    contact_points = ([cp.strip() for cp in raw_cp.split(",") if cp.strip()] if isinstance(raw_cp, str) else list(raw_cp))
    try:    port = int(params.get("cassandra_port", 9042))
    except: port = 9042
    raw_batch = params.get("adls_batch_size") or params.get("cassandra_batch_size") or DEFAULT_BATCH_SIZE
    try:    batch_size = max(1, int(raw_batch))
    except: batch_size = DEFAULT_BATCH_SIZE
    raw_trunc = params.get("truncate_sink_before_write", False)
    truncate  = (raw_trunc.strip().lower() in ("true", "1", "yes") if isinstance(raw_trunc, str) else bool(raw_trunc))
    raw_del   = params.get("P_Delete_ADLS_Records", False)
    delete_adls = (raw_del.strip().lower() in ("true", "1", "yes") if isinstance(raw_del, str) else bool(raw_del))
    extracted = {
        "adls_account_name":               params["adls_account_name"],
        "adls_file_system":                params["adls_file_system"],
        "adls_directory":                  (params.get("adls_directory") or "").strip(),
        "cassandra_contact_points":        contact_points,
        "cassandra_port":                  port,
        "cassandra_preferred_node":        (params.get("cassandra_preferred_node") or "").strip() or None,
        "cassandra_username":              params["cassandra_username"],
        "cassandra_keyspace":              params["cassandra_keyspace"],
        "cassandra_table":                 params["cassandra_table"],
        "key_vault_name":                  params["Cassandra_key_vault_name"],
        "key_vault_secret_name":           params["Cassandra_key_vault_secret_name"],
        "batch_size":                      batch_size,
        "truncate_sink_before_write":      truncate,
        "delete_adls_records":             delete_adls,
    }
    logging.info(
        f"[adls_to_cassandra_params] "
        f"table={extracted['cassandra_keyspace']}.{extracted['cassandra_table']} | "
        f"adls_dir={extracted['adls_directory']!r} | "
        f"truncate={extracted['truncate_sink_before_write']} | "
        f"delete_adls={extracted['delete_adls_records']}"
    )
    return extracted


# ============================================================================
# ADLS -> CASSANDRA: INTERNAL PIPELINE
# ============================================================================

def _run_adls_to_cassandra(params: dict):
    """
    Internal ADLS->Cassandra pipeline called by pipeline_activity.
    On success with P_Delete_ADLS_Records=true, deletes the entire ADLS
    export directory (parent CSV + all JSON-column sub-folders + child CSVs).
    """
    start_time = datetime.utcnow()
    cluster    = None
    session    = None
    try:
        params = validate_adls_to_cassandra_params(params)

        cassandra_password = get_secret(params["key_vault_name"], params["key_vault_secret_name"])
        cluster, session   = validate_cassandra_connection(
            contact_points=params["cassandra_contact_points"],
            port=params["cassandra_port"],
            username=params["cassandra_username"],
            password=cassandra_password,
            keyspace=params["cassandra_keyspace"],
            table=params["cassandra_table"],
            preferred_node=params["cassandra_preferred_node"],
        )
        del cassandra_password
        force_gc()

        service_client = validate_adls_connection(params["adls_account_name"], params["adls_file_system"])
        keyspace       = params["cassandra_keyspace"]
        table          = params["cassandra_table"]

        col_meta        = CassandraMetadata.get_column_metadata(session, keyspace, table)
        column_types    = {c["column_name"]: c["type"] for c in col_meta}
        relational_cols, text_cols = CassandraMetadata.classify_columns(col_meta)
        del col_meta
        force_gc()

        export_dir = f"{params['adls_directory']}/{table}" if params["adls_directory"] else table

        json_cols       = discover_json_cols_from_adls(service_client, params["adls_file_system"], export_dir, text_cols)
        non_json_text   = [c for c in text_cols if c not in json_cols]
        relational_cols = relational_cols + non_json_text
        del non_json_text
        force_gc()

        logging.info(f"[adls_to_cassandra] relational: {relational_cols} | json: {json_cols}")

        main_csv_path = f"{export_dir}/{table}.csv"
        main_df = read_csv_from_adls(service_client, params["adls_file_system"], main_csv_path)
        if main_df.empty:
            return {"status": "success", "message": "No data found in ADLS.", "rows_processed": 0}

        json_col_data: Dict[str, Dict[str, str]] = {}
        for jcol in json_cols:
            logging.info(f"[adls_to_cassandra] Reconstructing '{jcol}' ...")
            csv_paths = discover_json_col_csvs(service_client, params["adls_file_system"], export_dir, jcol)
            if jcol not in csv_paths:
                logging.warning(f"[adls_to_cassandra] Root CSV for '{jcol}' not found -- skipped.")
                continue
            table_dfs: Dict[str, pd.DataFrame] = {}
            for arr_name, adls_path in csv_paths.items():
                df = read_csv_from_adls(service_client, params["adls_file_system"], adls_path)
                if not df.empty:
                    table_dfs[arr_name] = df
                    logging.info(f"[adls_to_cassandra]   '{arr_name}' -> {len(df)} rows")
                del df
            if jcol not in table_dfs:
                logging.warning(f"[adls_to_cassandra] Root CSV for '{jcol}' was empty -- skipped.")
                continue
            children_index = build_children_index(table_dfs, jcol)
            json_col_data[jcol] = reconstruct_json_column(table_dfs[jcol], children_index, jcol)
            logging.info(f"[adls_to_cassandra] '{jcol}' -> {len(json_col_data[jcol])} JSON objects.")
            del table_dfs, children_index
            force_gc()

        if params["truncate_sink_before_write"]:
            session.execute(f'TRUNCATE "{keyspace}"."{table}"')
            logging.info(f"[adls_to_cassandra] Truncated {keyspace}.{table}")

        retry_strategy = CassandraRetryStrategy()
        batch_size     = params["batch_size"]
        rows_written   = 0
        batch_num      = 0
        all_main_rows  = main_df.to_dict(orient="records")
        release_dataframe(main_df)

        for batch_start in range(0, len(all_main_rows), batch_size):
            batch_num += 1
            batch      = all_main_rows[batch_start: batch_start + batch_size]
            cassandra_rows: List[Dict] = []
            for row in batch:
                row_rid  = row.get("_row_rid", "")
                cass_row: Dict = {}
                for col in relational_cols:
                    if col in row and col != "_row_rid":
                        cass_row[col] = row[col]
                for jcol in json_cols:
                    json_str = json_col_data.get(jcol, {}).get(row_rid, "")
                    if json_str:
                        cass_row[jcol] = json_str
                if cass_row:
                    cassandra_rows.append(cass_row)
            written = insert_rows_to_cassandra(session, keyspace, table, cassandra_rows, column_types, retry_strategy)
            rows_written += written
            with memory_managed_batch(f"write_batch_{batch_num:04d}"):
                pass
            logging.info(f"[adls_to_cassandra] batch {batch_num:04d} -> {written} rows (cumulative: {rows_written})")

        shutdown_cassandra(cluster, session)
        cluster = session = None

        # Delete ADLS directory only after a fully successful Cassandra write
        if params.get("delete_adls_records"):
            logging.info(f"[adls_to_cassandra] P_Delete_ADLS_Records=true -> deleting '{export_dir}'")
            delete_adls_export_directory(service_client, params["adls_file_system"], export_dir)

        retry_stats = retry_strategy.get_stats()
        duration    = (datetime.utcnow() - start_time).total_seconds()
        return {
            "status":                       "success",
            "rows_written":                 rows_written,
            "json_columns_reconstructed":   json_cols,
            "relational_columns":           relational_cols,
            "batches_processed":            batch_num,
            "batch_size":                   batch_size,
            "duration_seconds":             round(duration, 2),
            "adls_records_deleted":         params.get("delete_adls_records", False),
            "total_retries":                retry_stats.get("total_retries", 0),
            "retries_by_error_type":        retry_stats.get("retries_by_error_type", {}),
        }

    except Exception as e:
        logging.error(f"[adls_to_cassandra] Failed: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}
    finally:
        if cluster is not None or session is not None:
            shutdown_cassandra(cluster, session)
        force_gc()


# ============================================================================
# UNIFIED PIPELINE  --  3 functions total
#
#   pipeline_http_start   (HTTP trigger)
#   pipeline_orchestrator (Orchestrator)
#   pipeline_activity     (Activity)
#
# Pass "direction": "cassandra_to_adls"  or  "direction": "adls_to_cassandra"
# ============================================================================

@app.activity_trigger(input_name="params")
def pipeline_activity(params: dict):
    """
    Single activity that dispatches to the correct internal pipeline:
        'cassandra_to_adls'  ->  export Cassandra rows to ADLS CSVs
        'adls_to_cassandra'  ->  import ADLS CSVs back into Cassandra
                                 P_Delete_ADLS_Records=true deletes the
                                 export directory after a successful load.
    """
    if isinstance(params, str):
        try:   params = json.loads(params)
        except Exception as e:
            logging.warning(f"[pipeline_activity] params not valid JSON: {e}")

    direction = "adls_to_cassandra"
    logging.info(f"[pipeline_activity] direction='{direction}'")

    if direction == "adls_to_cassandra":
        return _run_adls_to_cassandra(params)
    elif direction == "cassandra_to_adls":
        return _run_cassandra_to_adls(params)
    else:
        msg = (
            f"Unknown or missing 'direction': '{direction}'. "
            "Must be 'cassandra_to_adls' or 'adls_to_cassandra'."
        )
        logging.error(f"[pipeline_activity] {msg}")
        return {"status": "error", "message": msg}


@app.orchestration_trigger(context_name="context")
def pipeline_orchestrator(context: df.DurableOrchestrationContext):
    params = context.get_input()
    if isinstance(params, str):
        try:   params = json.loads(params)
        except Exception: pass
    result = yield context.call_activity("pipeline_activity", params)
    return result


@app.route(route="Pipeline_V1", methods=["POST"])
@app.durable_client_input(client_name="client")
async def pipeline_http_start(req: func.HttpRequest, client) -> func.HttpResponse:
    """
    Single HTTP trigger for both pipeline directions.

    Body must include:
        direction  : 'cassandra_to_adls'  or  'adls_to_cassandra'
        ... all other existing parameters unchanged ...

    adls_to_cassandra only:
        P_Delete_ADLS_Records : true  ->  delete ADLS export folder after load
    """
    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse("Invalid JSON body", status_code=400)

    direction = "adls_to_cassandra"
    
    try:
        instance_id = await client.start_new("pipeline_orchestrator", None, body)
        logging.info(f"[pipeline_http_start] instance={instance_id} direction={direction}")
        return client.create_check_status_response(req, instance_id)
    except Exception as e:
        logging.error(f"[pipeline_http_start] Failed: {e}", exc_info=True)
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)