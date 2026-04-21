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
from enum import Enum
from typing import Callable, Dict, Iterator, List, Optional, Tuple

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

# Parameter key names — defined once here so they are never hard-coded as
# repeated string literals elsewhere in the file.
PARAM_ADLS_ACCOUNT_NAME               = "adls_account_name"
PARAM_ADLS_FILE_SYSTEM                = "adls_file_system"
PARAM_CASSANDRA_CONTACT_POINTS        = "cassandra_contact_points"
PARAM_CASSANDRA_USERNAME              = "cassandra_username"
PARAM_CASSANDRA_KEYSPACE              = "cassandra_keyspace"
PARAM_CASSANDRA_TABLE                 = "cassandra_table"
PARAM_CASSANDRA_KEY_VAULT_NAME        = "cassandra_key_vault_name"
PARAM_CASSANDRA_KEY_VAULT_SECRET_NAME = "cassandra_key_vault_secret_name"

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


def coerce_to_cql_type(value: str, cql_type: str):
    """
    Coerce a raw string value to the appropriate Python type that matches the
    column's CQL type.  Used to ensure IN-clause bind parameters have consistent
    internal types before being passed to the Cassandra driver.
    """
    t = (cql_type or "text").lower().strip()
    try:
        if t in ("bigint", "counter", "int", "smallint", "tinyint", "varint",):
            return int(value)
        elif t in ("decimal", "double", "float",):
            return float(value)
        elif t in ("boolean",):
            return value.strip().lower() in ("true", "1", "yes")
        elif t in ("timeuuid", "uuid",):
            return uuid.UUID(value)
        else:
            return value
    except Exception as e:
        logging.warning(f"[coerce_to_cql_type] Cannot coerce {value!r} → {cql_type!r}: {e}")
        return value


def _normalise_to_list(value) -> list:
    """
    Normalise a partition/clustering key value into a plain list suitable for
    a CQL IN clause.  Handles three input shapes:
      - str  → split on commas, strip whitespace, drop blanks
      - list → returned as-is
      - anything else (e.g. int/UUID) → wrapped in a single-element list
    """
    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()]
    if isinstance(value, list):
        return value
    return [value]


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
    def _is_json_candidate(cql_type: str) -> bool:
        """
        Return True when a column's CQL type means its values *might* contain
        JSON.  Only text-like types are candidates.

        Note: ``get_column_type`` falls back to ``"text"`` when the type cannot
        be resolved from the system schema.  That means an unknown-type column
        will be treated as a JSON candidate and go through JSON detection.  In
        practice this is the safer default — a false-positive candidate is just
        inspected and ruled out at runtime, whereas a false-negative would cause
        real JSON to be treated as a plain string.
        """
        return cql_type in ("text", "varchar")

    @staticmethod
    def classify_columns(column_metadata: List[Dict]) -> Tuple[List[str], List[str]]:
        relational, json_candidates = [], []
        for col in column_metadata:
            cname, ctype = col["column_name"], col["type"].lower()
            if CassandraMetadata._is_json_candidate(ctype):
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

            partition_key_value = _normalise_to_list(partition_key_value)

            bind_values = [
                coerce_to_cql_type(v, pk_type) for v in partition_key_value
            ]

            placeholders = ",".join(["%s"] * len(bind_values))

            where_clause = f'WHERE "{partition_key}" IN ({placeholders})'

            logging.info(f"{clustering_key}clustering_key_value {clustering_key_value}")

            if clustering_key and clustering_key_value:

                ck_type = get_column_type(session, keyspace, table, clustering_key)

                clustering_key_value = _normalise_to_list(clustering_key_value)

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
        # Assumption: a column is either consistently JSON-valued or consistently
        # not — we do not expect mixed columns (e.g. sometimes "{}", sometimes plain
        # text).  Given that assumption we only need ONE confirming row per column,
        # so we skip a column once it is confirmed and short-circuit the whole loop
        # once every candidate has been confirmed.
        for row in rows:
            remaining = [col for col in candidate_columns if col not in confirmed_json]
            if not remaining:
                # All candidates confirmed — no need to examine further rows.
                break
            for col in remaining:
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

class CassandraErrorType(str, Enum):
    """Categorises Cassandra errors for retry-stat tracking and log messages."""
    UNAVAILABLE      = "UnavailableException"
    TIMEOUT          = "Timeout"
    OVERLOADED       = "Overloaded"
    CONNECTION_ERROR = "ConnectionError"
    OTHER            = "Other"


class CassandraRetryStrategy:

    def __init__(self, max_retries=DEFAULT_MAX_RETRIES, base_delay=DEFAULT_BASE_DELAY, max_delay=DEFAULT_MAX_DELAY):
        self.max_retries = max_retries
        self.base_delay  = base_delay
        self.max_delay   = max_delay
        self.retry_count_by_error: Dict[str, int] = {}

    def execute_with_retry(self, operation: Callable, operation_name: str):
        # operation_name is used only in log/warning messages — it is not
        # passed to the operation itself.
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

    def _classify_error(self, msg: str) -> CassandraErrorType:
        m = msg.lower()
        if "unavailable" in m:                 return CassandraErrorType.UNAVAILABLE
        if "timeout" in m or "timed out" in m: return CassandraErrorType.TIMEOUT
        if "overloaded" in m:                  return CassandraErrorType.OVERLOADED
        if "connection" in m:                  return CassandraErrorType.CONNECTION_ERROR
        return CassandraErrorType.OTHER

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
        self.partition_key         = (partition_key or "").strip() or None
        self.partition_key_value   = partition_key_value
        self.clustering_key        = (clustering_key or "").strip() or None
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
        pk_type  = get_column_type(self.session, self.keyspace, self.table, self.partition_key)

        pkv_list = _normalise_to_list(self.partition_key_value)

        bind_values  = [coerce_to_cql_type(v, pk_type) for v in pkv_list]
        placeholders = ",".join(["?"] * len(bind_values))
        where_clause = f'WHERE "{self.partition_key}" IN ({placeholders})'

        if self.clustering_key and self.clustering_key_value:
            ck_type  = get_column_type(self.session, self.keyspace, self.table, self.clustering_key)

            ckv_list = _normalise_to_list(self.clustering_key_value)

            coerced_ck_values = [coerce_to_cql_type(v, ck_type) for v in ckv_list]
            ck_placeholders   = ",".join(["?"] * len(coerced_ck_values))
            where_clause     += f' AND "{self.clustering_key}" IN ({ck_placeholders})'
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

def _append_raw_to_json_col_output(
    json_col_output: Dict,
    jcol: str,
    row_rid: str,
    value,
) -> None:
    """
    Write a single un-parseable / scalar value for *jcol* into the output dict
    as a plain parent record.  Extracted to avoid duplicating the
    setdefault + append pattern which appears for both the JSON-parse-failure
    path and the non-dict/non-list scalar path.
    """
    json_col_output.setdefault(jcol, {"parent": [], "children": {}})
    json_col_output[jcol]["parent"].append({"_row_rid": row_rid, jcol: value})


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
            _append_raw_to_json_col_output(json_col_output, jcol, row_rid, raw)
            continue

        if isinstance(parsed, dict):
            doc = parsed
        elif isinstance(parsed, list):
            doc = {jcol: parsed}
        else:
            _append_raw_to_json_col_output(json_col_output, jcol, row_rid, parsed)
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
        # Return the value as-is for scalar types and primitive-only lists.
        if isinstance(val, (str, int, float, bool)):
            return val
        if isinstance(val, list) and all(not isinstance(i, dict) for i in val):
            return val
        # Return None for explicit null, or for complex types that can't be
        # flattened to a scalar cell value (dicts, mixed lists).
        if val is None or isinstance(val, (dict, list)):
            return None
        # Last resort: stringify anything else (e.g. UUID, Decimal).
        try:
            return str(val)
        except Exception:
            return None

    for col in df_obj.columns:
        if df_obj[col].dtype == object:
            df_obj[col] = df_obj[col].apply(fix_value)

    meta_cols = {"_rid", "_parent_rid", "_row_rid"}
    data_cols = [c for c in df_obj.columns if c not in meta_cols]
    if data_cols:
        df_obj.dropna(subset=data_cols, how="all", inplace=True)

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
    file_client = dir_client.create_file(file_name)
    file_client.append_data(content_bytes, offset=0, length=len(content_bytes))
    file_client.flush_data(len(content_bytes))


class _AdlsAppendClient:
    """
    Thin wrapper around an ADLS DataLake file client that enforces the project's
    correctness rule: the committed file size MUST be re-fetched from the service
    immediately before every append — never tracked locally across calls.

    Using this wrapper makes it structurally impossible to pass a stale offset to
    append_data / flush_data, because callers never touch the raw offset at all.
    """

    def __init__(self, file_client):
        self._client = file_client

    # ── Delegate attribute access so callers can still reach the underlying ──
    # client for operations like delete_file, get_file_properties, etc.
    def __getattr__(self, name):
        return getattr(self._client, name)

    def safe_append(self, content_bytes: bytes) -> None:
        """
        Append *content_bytes* to the file, always using a freshly fetched
        committed offset.  Flushes immediately after appending.
        """
        offset = _get_committed_file_size(self._client)
        self._client.append_data(content_bytes, offset=offset, length=len(content_bytes))
        self._client.flush_data(offset + len(content_bytes))


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
    # Wrap in _AdlsAppendClient so all appends go through safe_append(), which
    # always re-fetches the committed offset — never uses a locally tracked value.
    file_client     = _AdlsAppendClient(dir_client.get_file_client(file_name))
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
            file_is_new = _get_committed_file_size(file_client) == 0

            content_bytes = _serialize_df(df_input, include_header=file_is_new)
            release_dataframe(df_input)

            if file_is_new:
                # Re-wrap the newly created file client so safe_append still applies.
                file_client = _AdlsAppendClient(dir_client.create_file(file_name))

            file_client.safe_append(content_bytes)
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
        PARAM_ADLS_ACCOUNT_NAME:               params.get(PARAM_ADLS_ACCOUNT_NAME),
        PARAM_ADLS_FILE_SYSTEM:                params.get(PARAM_ADLS_FILE_SYSTEM),
        PARAM_CASSANDRA_CONTACT_POINTS:        params.get(PARAM_CASSANDRA_CONTACT_POINTS),
        PARAM_CASSANDRA_USERNAME:              params.get(PARAM_CASSANDRA_USERNAME),
        PARAM_CASSANDRA_KEYSPACE:              params.get(PARAM_CASSANDRA_KEYSPACE),
        PARAM_CASSANDRA_TABLE:                 params.get(PARAM_CASSANDRA_TABLE),
        PARAM_CASSANDRA_KEY_VAULT_NAME:        params.get(PARAM_CASSANDRA_KEY_VAULT_NAME),
        PARAM_CASSANDRA_KEY_VAULT_SECRET_NAME: params.get(PARAM_CASSANDRA_KEY_VAULT_SECRET_NAME),
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

    raw_pk  = (params.get("cassandra_partition_key") or "").strip()
    raw_pkv = params.get("cassandra_partition_key_value")

    if bool(raw_pk) != (raw_pkv is not None):
        logging.warning("[validate_params] Both partition key and value must be set. Falling back to full-table scan.")
        raw_pk, raw_pkv = "", None

    raw_ck  = (params.get("cassandra_clustering_key") or "").strip()
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
        "adls_account_name":              params[PARAM_ADLS_ACCOUNT_NAME],
        "adls_file_system":               params[PARAM_ADLS_FILE_SYSTEM],
        "adls_directory":                 (params.get("adls_directory") or "").strip(),
        "cassandra_contact_points":       contact_points,
        "cassandra_port":                 port,
        "cassandra_preferred_node":       (params.get("cassandra_preferred_node") or "").strip() or None,
        "cassandra_preferred_port":       preferred_port,
        "cassandra_username":             params[PARAM_CASSANDRA_USERNAME],
        "cassandra_keyspace":             params[PARAM_CASSANDRA_KEYSPACE],
        "cassandra_table":                params[PARAM_CASSANDRA_TABLE],
        "cassandra_partition_key":        raw_pk  or None,
        "cassandra_partition_key_value":  raw_pkv,
        "cassandra_clustering_key":       raw_ck  or None,
        "cassandra_clustering_key_value": raw_ckv,
        "key_vault_name":                 params[PARAM_CASSANDRA_KEY_VAULT_NAME],
        "key_vault_secret_name":          params[PARAM_CASSANDRA_KEY_VAULT_SECRET_NAME],
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

@app.activity_trigger(input_name="params")
def process_cassandra_to_adls_activity(params: dict):
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
        fs_client = service_client.get_file_system_client(params["adls_file_system"])
        try:
            fs_client.get_directory_client(export_dir).delete_directory()
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
# ORCHESTRATOR AND HTTP TRIGGER
# ============================================================================

@app.orchestration_trigger(context_name="context")
def cassandra_to_adls_orchestrator(context: df.DurableOrchestrationContext):
    params = context.get_input()
    if isinstance(params, str):
        try:
            params = json.loads(params)
        except Exception:
            pass
    result = yield context.call_activity("process_cassandra_to_adls_activity", params)
    return result


@app.route(route="Cassandra_to_ADLS_V1", methods=["POST"])
@app.durable_client_input(client_name="client")
async def cassandra_to_adls_http_start(req: func.HttpRequest, client) -> func.HttpResponse:
    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse("Invalid JSON body", status_code=400)

    try:
        instance_id = await client.start_new("cassandra_to_adls_orchestrator", None, body)
        return client.create_check_status_response(req, instance_id)
    except Exception as e:
        logging.error(f"HTTP start failed: {e}", exc_info=True)
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)