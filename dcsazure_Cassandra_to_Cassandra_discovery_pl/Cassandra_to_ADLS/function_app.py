import ctypes
import gc
import json
import logging
import time
import uuid
from collections import deque
from contextlib import contextmanager
from datetime import datetime
from io import BytesIO, StringIO
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
ADLS_DOWNLOAD_CHUNK_BYTES    = 4 * 1024 * 1024   # 4 MiB per streaming download chunk
CSV_PARSE_CHUNK_ROWS         = 50_000

GC_BATCH_INTERVAL            = 5
META_COLS                    = {"_rid", "_parent_rid", "_row_rid"}
PARENT_KEY                   = "parent"
CHILDREN_KEY                 = "children"

# ============================================================================
# AZURE FUNCTIONS APP
# ============================================================================

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# ============================================================================
# MEMORY MANAGEMENT
# ============================================================================

def force_gc() -> None:
    """
    Frees up memory immediately by running Python's garbage collector twice,
    then asking the OS to reclaim any unused heap pages (Linux only).
    Input  : none
    Output : none — side effect is reduced memory usage
    """
    gc.collect(2)
    gc.collect(2)
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception as exc:
        logging.debug(f"[force_gc] malloc_trim not available on this platform: {exc}")


def release_dataframe(df_obj: Optional[pd.DataFrame]) -> None:
    """
    Safely destroys a DataFrame and frees its memory right away.
    Passing None is safe — the function simply does nothing.
    Input  : df_obj — the DataFrame to destroy (or None)
    Output : none
    """
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
    """
    A helper that automatically cleans up memory after each batch finishes,
    even if the batch fails. Use it as a "with" block around batch work.
    Input  : batch_label — a name shown in debug logs (e.g. "batch_0001")
    Output : none — side effect is memory cleanup after the block exits
    """
    try:
        yield
    finally:
        logging.debug(f"[memory] flushing after {batch_label}")
        force_gc()


# ============================================================================
# KEY VAULT
# ============================================================================

def get_secret(key_vault_name: str, secret_name: str) -> str:
    """
    Fetches a password or secret from Azure Key Vault.
    Input  : key_vault_name — name of the Key Vault; secret_name — name of the secret
    Output : the secret value as a plain string
    Raises : exception if authentication fails or the secret does not exist
    """
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
    preferred_port: Optional[int] = None,
) -> Cluster:
    """
    Builds a Cassandra connection configuration (does not connect yet).
    If preferred_node is given, traffic is routed to that data centre first.
    If preferred_port is given, it overrides the base port for the connection.
    Input  : contact_points — list of Cassandra host IPs; port — default port;
             username/password — credentials; preferred_node — optional DC name;
             preferred_port — optional port override
    Output : a configured Cassandra Cluster object ready to connect
    """
    auth_provider = PlainTextAuthProvider(username=username, password=password)
    lb_policy = (
        DCAwareRoundRobinPolicy(local_dc=preferred_node)
        if preferred_node
        else RoundRobinPolicy()
    )
    profile = ExecutionProfile(load_balancing_policy=lb_policy)
    return Cluster(
        contact_points=contact_points,
        port=preferred_port if preferred_port is not None else port,
        auth_provider=auth_provider,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        connect_timeout=30,
        control_connection_timeout=30,
        compression=True,
        protocol_version=4,
        executor_threads=4,
    )


def validate_cassandra_connection(
    contact_points, port, username, password, keyspace, table, preferred_node=None, preferred_port=None
) -> Tuple:
    """
    Connects to Cassandra and checks that the target table actually exists.
    Input  : contact_points, port, username, password — connection details;
             keyspace/table — where to look; preferred_node/preferred_port — optional overrides
    Output : (cluster, session) tuple ready for querying
    Raises : ValueError if the table is missing or connection fails
    """
    try:
        cluster = build_cassandra_cluster(contact_points, port, username, password, preferred_node, preferred_port)
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
    """
    Closes the Cassandra session and cluster connection safely.
    Any errors during shutdown are ignored so they don't hide the real error.
    Input  : cluster, session — the active Cassandra connection objects
    Output : none — frees connection resources and memory
    """
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
    """
    Connects to Azure Data Lake Storage and verifies the filesystem is reachable.
    Input  : adls_account_name — storage account name; adls_file_system — container name
    Output : DataLakeServiceClient — an active ADLS client ready for file operations
    Raises : ValueError with a clear message if auth fails or container not found
    """
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
    """
    Looks up what data type a Cassandra column holds (e.g. int, uuid, text).
    Input  : session — active Cassandra session; keyspace/table/column — location of the column
    Output : data type string (e.g. "int", "uuid"); defaults to "text" if not found
    """
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
                "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try: return _dt.strptime(v, fmt)
        except ValueError: continue
    logging.warning(f"[_parse_cassandra_timestamp] Cannot parse {v!r}.")
    return value

def _parse_cassandra_date(value: str):
    from datetime import date as _date, datetime as _dt
    if not value or not isinstance(value, str): return value
    v = value.strip()
    try: return _date.fromisoformat(v)
    except ValueError: pass
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d"):
        try: return _dt.strptime(v, fmt).date()
        except ValueError: continue
    logging.warning(f"[_parse_cassandra_date] Cannot parse {v!r}.")
    return value

def _parse_cassandra_time(value: str):
    from datetime import datetime as _dt
    if not value or not isinstance(value, str): return value
    v = value.strip()
    for fmt in ("%H:%M:%S.%f", "%H:%M:%S", "%H:%M"):
        try: return _dt.strptime(v, fmt).time()
        except ValueError: continue
    logging.warning(f"[_parse_cassandra_time] Cannot parse {v!r}.")
    return value

def coerce_to_cql_type(value: str, cql_type: str):
    t = (cql_type or "text").lower().strip()
    try:
        if t in ("int", "smallint", "tinyint", "varint", "bigint", "counter"): return int(value)
        if t in ("float", "double", "decimal"):                                 return float(value)
        if t in ("boolean",):                              return value.strip().lower() in ("true", "1", "yes")
        if t in ("uuid", "timeuuid"):                      return uuid.UUID(value)
        if t in ("timestamp",):                            return _parse_cassandra_timestamp(value)
        if t in ("date",):                                 return _parse_cassandra_date(value)
        if t in ("time",):                                 return _parse_cassandra_time(value)
        return value
    except Exception as e:
        logging.warning(f"[coerce_to_cql_type] Cannot coerce {value!r} → {cql_type!r}: {e}")
        return value


def _parse_key_value_list(raw_value, delimiter=",") -> list:
    """Normalises a partition/clustering key value to a list, handling str, list, or scalar."""
    if isinstance(raw_value, str):
        return [v.strip() for v in raw_value.split(delimiter) if v.strip()]
    elif isinstance(raw_value, list):
        return raw_value
    return [raw_value]


class CassandraMetadata:
    """
    A toolkit for reading a Cassandra table's structure (schema).
    Tells us what columns exist, what types they are, which are JSON columns,
    and what the clustering keys are — without touching the actual data rows.
    """

    @staticmethod
    def get_column_metadata(session, keyspace: str, table: str) -> List[Dict]:
        """
        Fetches a list of all columns and their data types for a given table.
        Input  : session — active connection; keyspace/table — target table location
        Output : list of dicts, each with "column_name" and "type"
        """
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
        """
        Splits columns into two groups: plain data columns vs text columns
        that might store JSON (these need special handling during export).
        Input  : column_metadata — list of column dicts from get_column_metadata
        Output : (relational_cols, json_candidate_cols) — two lists of column names
        """
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
        """
        Returns the clustering key column names for a table.
        These are the columns used to sort rows within a partition in Cassandra.
        Input  : session — active connection; keyspace/table — target table
        Output : list of clustering key column name strings
        """
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
        partition_key=None, partition_key_value=None,
        clustering_key=None, clustering_key_value=None,
    ) -> List[str]:
        """
        Checks which text columns actually contain JSON by reading sample rows.
        Optionally filters by partition/clustering key to limit how much data is scanned.
        Input  : session — active connection; keyspace/table — target table;
                 candidate_columns — text columns to check;
                 partition_key/value and clustering_key/value — optional row filters
        Output : list of column names confirmed to contain JSON data
        """
        if not candidate_columns:
            return []
        cols_expr = ", ".join(f'"{c}"' for c in candidate_columns)
        logging.info(
            f"[detect_json_columns] Detecting JSON columns — "
            f"partition_key={partition_key!r} partition_key_value={partition_key_value!r}"
        )
        if partition_key and partition_key_value:

            pk_type = get_column_type(session, keyspace, table, partition_key)

            pkv_list = _parse_key_value_list(partition_key_value)
            coerced_pk_values = [coerce_to_cql_type(v, pk_type) for v in pkv_list]
            placeholders = ",".join(["%s"] * len(coerced_pk_values))
            where_clause = f'WHERE "{partition_key}" IN ({placeholders})'
            bind_values  = list(coerced_pk_values)

            logging.info(
                f"[detect_json_columns] Applying clustering filter — "
                f"clustering_key={clustering_key!r} clustering_key_value={clustering_key_value!r}"
            )

            if clustering_key and clustering_key_value:

                ck_type = get_column_type(session, keyspace, table, clustering_key)

                ckv_list = _parse_key_value_list(clustering_key_value)
                coerced_ck_values = [coerce_to_cql_type(v, ck_type) for v in ckv_list]
                ck_placeholders = ",".join(["%s"] * len(coerced_ck_values))
                where_clause += f' AND "{clustering_key}" IN ({ck_placeholders})'
                bind_values.extend(coerced_ck_values)

            cql = (
                f'SELECT {cols_expr} FROM "{keyspace}"."{table}" '
                f'{where_clause} ALLOW FILTERING'
            )
            logging.info(f"[detect_json_columns] Executing filtered query: {cql}")
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
    """
    Automatically retries a Cassandra operation when it fails, waiting longer
    between each attempt (exponential backoff). Tracks how many retries happened
    and what kinds of errors caused them, for reporting at the end of the job.
    """

    def __init__(self, max_retries=DEFAULT_MAX_RETRIES, base_delay=DEFAULT_BASE_DELAY, max_delay=DEFAULT_MAX_DELAY):
        """
        Sets up the retry policy.
        Input  : max_retries — max attempts before giving up (default 5);
                 base_delay — initial wait in seconds (default 0.5);
                 max_delay — maximum wait cap in seconds (default 60)
        """
        self.max_retries = max_retries
        self.base_delay  = base_delay
        self.max_delay   = max_delay
        self.retry_count_by_error: Dict[str, int] = {}

    def execute_with_retry(self, operation, operation_name: str = "operation"):
        """
        Runs the given operation and retries if it fails, with increasing wait times.
        Input  : operation — a callable (function) to run; operation_name — label for logs
        Output : the return value of the operation if it succeeds
        Raises : the last exception if all retries are exhausted
        """
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
        """
        Converts an error message into a short category label for tracking.
        Input  : msg — the exception message string
        Output : one of "UnavailableException", "Timeout", "Overloaded", "ConnectionError", "Other"
        """
        m = msg.lower()
        if "unavailable" in m:                 return "UnavailableException"
        if "timeout" in m or "timed out" in m: return "Timeout"
        if "overloaded" in m:                  return "Overloaded"
        if "connection" in m:                  return "ConnectionError"
        return "Other"

    def get_stats(self) -> Dict:
        """
        Returns a summary of how many retries happened and why.
        Output : dict with "total_retries" count and "retries_by_error_type" breakdown
        """
        return {"total_retries": sum(self.retry_count_by_error.values()), "retries_by_error_type": self.retry_count_by_error.copy()}


# ============================================================================
# STREAMING CASSANDRA READER
# ============================================================================

class StreamingCassandraReader:
    """
    Reads rows from a Cassandra table in manageable batches so the entire
    table is never loaded into memory at once. Supports optional filters to
    read only a specific partition or clustering key range.
    """

    def __init__(self, session, keyspace, table, batch_size,
                 partition_key=None, partition_key_value=None, retry_strategy=None,
                 clustering_key=None, clustering_key_value=None):
        """
        Sets up the reader for a specific table.
        Input  : session — active Cassandra connection; keyspace/table — what to read;
                 batch_size — how many rows per batch;
                 partition_key/value — optional filter to read specific partitions only;
                 retry_strategy — retry policy (a default one is used if not provided);
                 clustering_key/value — optional secondary filter (requires partition filter)
        """
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
        """
        Yields one batch of rows at a time from Cassandra.
        Uses partition-key mode if filters are set, otherwise reads the full table.
        Output : yields List of Cassandra row objects, batch_size rows at a time
        """
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
        """
        Reads rows matching the given partition key (and optional clustering key).
        Values are automatically cast to the correct Cassandra type before querying.
        Output : yields batches of matching Cassandra rows
        """
        pk_type  = get_column_type(self.session, self.keyspace, self.table, self.partition_key)
        pkv_list = _parse_key_value_list(self.partition_key_value)
        coerced_values = [coerce_to_cql_type(v, pk_type) for v in pkv_list]
        placeholders   = ",".join(["?"] * len(coerced_values))
        bind_values    = list(coerced_values)
        where_clause   = f'WHERE "{self.partition_key}" IN ({placeholders})'

        if self.clustering_key and self.clustering_key_value:
            ck_type  = get_column_type(self.session, self.keyspace, self.table, self.clustering_key)
            ckv_list = _parse_key_value_list(self.clustering_key_value)
            coerced_ck_values = [coerce_to_cql_type(v, ck_type) for v in ckv_list]
            ck_placeholders   = ",".join(["?"] * len(coerced_ck_values))
            where_clause      += f' AND "{self.clustering_key}" IN ({ck_placeholders})'
            bind_values.extend(coerced_ck_values)
            logging.info(f"[stream_rows] CLUSTERING filter — key={self.clustering_key!r} value={ckv_list!r}")

        cql = (
            f'SELECT * FROM "{self.keyspace}"."{self.table}" '
            f'{where_clause} ALLOW FILTERING'
        )
        logging.info(f"[_stream_partition] Executing partition-filtered query: {cql}")

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
        """
        Reads every row in the table with no filters (full table scan).
        Output : yields all rows in batches
        """
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
        """
        Groups rows from a Cassandra result set into fixed-size batches and yields each one.
        Cleans up memory every GC_BATCH_INTERVAL batches to keep usage bounded.
        Input  : result_set — a Cassandra query result
        Output : yields List of rows, each list up to batch_size long
        """
        batch: List = []
        batch_count = 0
        for row in result_set:
            batch.append(row)
            self._rows_read += 1
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
                batch_count += 1
                if batch_count % GC_BATCH_INTERVAL == 0:
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
    """
    Flattens a nested dictionary into a single level using dotted key names.
    Example: {"a": {"b": 1}} becomes {"a.b": 1}. Lists are kept as-is.
    Input  : d — the nested dict to flatten; parent — key prefix (used internally)
    Output : flat dict with dotted keys
    """
    flattened: Dict = {}
    stack = [(d, parent)]
    while stack:
        current_dict, current_parent = stack.pop()
        for k, v in current_dict.items():
            key = f"{current_parent}.{k}" if current_parent else k
            if isinstance(v, list):
                flattened[key] = v
            elif isinstance(v, dict):
                stack.append((v, key))
            else:
                flattened[key] = v
    return flattened


def _process_flat_fields(
    flat: Dict,
    row_rid: str,
    base_table: str,
    parent_fields: Dict,
    child_queue: deque,
):
    """
    Separates a flat dict into plain fields (kept on the parent row)
    and array fields (pushed onto a queue for child-table processing).
    Input  : flat — flattened dict; row_rid — unique ID of this row;
             base_table — name context for child tables; parent_fields — dict to populate;
             child_queue — queue to push child items onto
    Output : none — mutates parent_fields and child_queue in place
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
    Breaks a JSON document apart into a parent row (scalar fields) and
    child rows (one per item in each nested array), ready for CSV export.
    Input  : doc — the JSON object to process; root_rid — unique ID linking parent to children;
             base_table — the JSON column name (e.g. "address")
    Output : (parent_fields dict, {child_table_name: [row dicts]})
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
    relational_cols: List[str],
    json_cols: List[str],
) -> Tuple[Dict, Dict[str, Dict]]:
    """
    Converts one Cassandra row into exportable dicts — plain fields go into a
    parent dict, and any JSON column values are parsed and split into parent/child dicts.
    Input  : row — a single Cassandra row object;
             relational_cols — plain column names; json_cols — JSON column names
    Output : (parent_record dict, {json_col: {parent: [...], children: {...}}})
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
            json_col_output.setdefault(jcol, {PARENT_KEY: [], CHILDREN_KEY: {}})
            json_col_output[jcol][PARENT_KEY].append({"_row_rid": row_rid, jcol: raw})
            continue

        if isinstance(parsed, dict):
            doc = parsed
        elif isinstance(parsed, list):
            doc = {jcol: parsed}
        else:
            json_col_output.setdefault(jcol, {PARENT_KEY: [], CHILDREN_KEY: {}})
            json_col_output[jcol][PARENT_KEY].append({"_row_rid": row_rid, jcol: parsed})
            continue

        parent_fields, child_rows = extract_arrays_streaming(doc, row_rid, base_table=jcol)
        parent_fields["_row_rid"] = row_rid
        parent_fields.pop("_rid", None)

        entry = json_col_output.setdefault(jcol, {PARENT_KEY: [], CHILDREN_KEY: {}})
        if parent_fields:
            entry[PARENT_KEY].append(parent_fields)
        for arr_name, rows_list in child_rows.items():
            entry[CHILDREN_KEY].setdefault(arr_name, []).extend(rows_list)

        del child_rows, parent_fields, parsed, doc

    return parent_record, json_col_output


def process_cassandra_batch_streaming(
    rows: List,
    relational_cols: List[str],
    json_cols: List[str],
) -> Tuple[pd.DataFrame, Dict[str, Dict[str, pd.DataFrame]]]:
    """
    Transforms a batch of Cassandra rows into DataFrames ready for CSV upload.
    Collects all rows first, then builds DataFrames in one go (much faster than
    building one DataFrame per row).
    Input  : rows — list of Cassandra row objects;
             relational_cols — plain columns; json_cols — JSON columns
    Output : (parent DataFrame, {json_col: {parent: DataFrame, children: {name: DataFrame}}})
    """
    parent_records: List[Dict] = []
    accumulator: Dict[str, Dict] = {jcol: {PARENT_KEY: [], CHILDREN_KEY: {}} for jcol in json_cols}

    for row in rows:
        parent_record, json_col_output = _process_single_row(row, relational_cols, json_cols)
        parent_records.append(parent_record)

        for jcol, data in json_col_output.items():
            accumulator[jcol][PARENT_KEY].extend(data[PARENT_KEY])
            for arr_name, child_list in data[CHILDREN_KEY].items():
                accumulator[jcol][CHILDREN_KEY].setdefault(arr_name, []).extend(child_list)

        del json_col_output

    parent_df = pd.DataFrame(parent_records) if parent_records else pd.DataFrame()
    del parent_records
    force_gc()

    json_col_results: Dict[str, Dict[str, pd.DataFrame]] = {}

    for jcol, data in accumulator.items():
        parent_df_jcol = pd.DataFrame(data[PARENT_KEY]) if data[PARENT_KEY] else pd.DataFrame()

        children_dfs: Dict[str, pd.DataFrame] = {}
        for arr_name, row_list in data[CHILDREN_KEY].items():
            if row_list:
                children_dfs[arr_name] = pd.DataFrame(row_list)
            del row_list

        json_col_results[jcol] = {PARENT_KEY: parent_df_jcol, CHILDREN_KEY: children_dfs}
        del parent_df_jcol, children_dfs, data

    del accumulator
    force_gc()

    return parent_df, json_col_results


# ============================================================================
# DATA CLEANING
# ============================================================================

def clean_dataframe(df_obj: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    Cleans up a DataFrame so it can be safely written to a CSV file.
    Removes unsupported types (dicts inside cells), drops fully-empty rows,
    and leaves metadata columns (_rid, _row_rid) untouched.
    Input  : df_obj — the DataFrame to clean (None is safe)
    Output : cleaned DataFrame, or an empty DataFrame if input is None/empty
    """

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

    data_cols = [c for c in df_obj.columns if c not in META_COLS]
    if data_cols:
        df_obj = df_obj.dropna(subset=data_cols, how="all")

    return df_obj.reset_index(drop=True)


# ============================================================================
# ADLS UPLOAD  (unchanged logic, kept intact)
# ============================================================================

def _serialize_df(df_obj: pd.DataFrame, include_header: bool = True) -> bytes:
    """
    Converts a DataFrame into pipe-delimited CSV bytes ready to write to ADLS.
    Empty cells become empty strings. Header row is included only when requested.
    Input  : df_obj — DataFrame to serialise; include_header — whether to write the header line
    Output : UTF-8 encoded CSV bytes
    """
    buf = StringIO()
    df_obj.fillna("").to_csv(
        buf, index=False, sep=PIPE_DELIMITER,
        header=include_header, quoting=0, escapechar=ESCAPE_CHARACTER,
    )
    return buf.getvalue().encode("utf-8")


def _ensure_adls_directory(fs_client, directory: str) -> None:
    """
    Creates every folder in the given path if it doesn't already exist.
    Safe to call even if the folders are already there — no error is raised.
    Input  : fs_client — ADLS filesystem client; directory — full folder path to ensure
    Output : none
    """
    dir_segments = directory.strip("/").split("/") if directory else []
    curr = ""
    for seg in dir_segments:
        curr = f"{curr}/{seg}" if curr else seg
        try:
            fs_client.get_directory_client(curr).create_directory()
        except Exception:
            pass


def _delete_adls_file_if_exists(file_client) -> None:
    """
    Deletes an ADLS file if it exists; does nothing if it doesn't.
    Used to clear the way before writing a fresh copy of a file.
    Input  : file_client — ADLS file client pointing to the file
    Output : none
    """
    try:
        file_client.delete_file()
    except Exception:
        pass


def _get_committed_file_size(file_client) -> int:
    """
    Returns the current size of an ADLS file in bytes by asking ADLS directly.
    Always fetches live — never uses a cached value — to avoid append offset errors.
    Input  : file_client — ADLS file client
    Output : file size in bytes, or 0 if the file doesn't exist or an error occurs
    """
    try:
        return file_client.get_file_properties().size
    except Exception:
        return 0


def _write_fresh_file(dir_client, file_name: str, content_bytes: bytes) -> None:
    """
    Creates a new ADLS file and writes all content to it in one operation.
    The old file must be deleted first (use _delete_adls_file_if_exists).
    Input  : dir_client — ADLS directory client; file_name — name of the new file;
             content_bytes — the data to write
    Output : none
    """
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
    Uploads a DataFrame as a pipe-delimited CSV file to ADLS Gen2.
    mode="write"  → overwrites the file from scratch (used for the first batch).
    mode="append" → adds to the existing file (used for subsequent batches).
    If new columns appear mid-stream, the existing file is downloaded, merged, and rewritten.
    Input  : service_client — ADLS client; file_system — container; directory/file_name — path;
             df_input — data to upload; mode — "write" or "append";
             known_columns — column set from previous batches (for schema tracking)
    Output : updated set of all known column names
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
    """
    Uploads each nested array table (child CSV) for a JSON column to ADLS.
    Each array in the JSON becomes its own CSV file in a matching sub-folder.
    Input  : service_client — ADLS client; adls_file_system — container;
             export_dir — base export path; json_col_name — parent JSON column;
             children — {array_name: DataFrame}; child_schemas — schema tracker;
             mode — "write" or "append"
    Output : updated child_schemas dict
    """
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
    """
    Validates and cleans up the input parameters for the Cassandra→ADLS export job.
    Checks that all required fields are present, fills in defaults for optional ones,
    and logs a summary of what will be used.
    Input  : params — raw input dict from the HTTP request
    Output : cleaned params dict ready for use by the pipeline
    Raises : ValueError listing any missing required fields
    """
    required = {
        "adls_account_name":               params.get("adls_account_name"),
        "adls_file_system":                params.get("adls_file_system"),
        "cassandra_contact_points":        params.get("cassandra_contact_points"),
        "cassandra_username":              params.get("cassandra_username"),
        "cassandra_keyspace":              params.get("cassandra_keyspace"),
        "cassandra_table":                 params.get("cassandra_table"),
        "cassandra_key_vault_name":        params.get("cassandra_key_vault_name"),
        "cassandra_key_vault_secret_name": params.get("cassandra_key_vault_secret_name"),
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
        "key_vault_name":                 params["cassandra_key_vault_name"],
        "key_vault_secret_name":          params["cassandra_key_vault_secret_name"],
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
    Exports a Cassandra table to pipe-delimited CSV files in ADLS Gen2.
    JSON columns are flattened — scalar fields go to the main CSV,
    nested arrays become separate child CSVs in sub-folders.
    Processes data in batches so memory stays bounded throughout.

    Input  : params — validated pipeline config dict
    Output : result dict with status, rows_processed, batch count, duration, retry stats
             or {"status": "error", "message": ...} on failure
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
            preferred_port=params.get("cassandra_preferred_port"),
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
                    jcol_parent_df = data[PARENT_KEY]

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
                        children=data[CHILDREN_KEY],
                        child_schemas=json_col_child_schemas[jcol],
                        mode=mode,
                    )
                    data[CHILDREN_KEY].clear()

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
    """
    Downloads a pipe-delimited CSV file from ADLS and returns it as a DataFrame.
    Reads the file in chunks to avoid loading everything into memory at once.
    Input  : service_client — ADLS client; file_system — container name;
             full_path — full path to the CSV file inside the container
    Output : DataFrame containing all rows from the file,
             or an empty DataFrame if the file is missing or unreadable
    """
    try:
        fs_client   = service_client.get_file_system_client(file_system)
        file_client = fs_client.get_file_client(full_path)

        # Stream the raw bytes in chunks to avoid a single large allocation
        buffer   = BytesIO()
        download = file_client.download_file()
        for chunk in download.chunks():
            buffer.write(chunk)
        buffer.seek(0)

        # Parse in row-batches so the CSV parser never materialises the full file
        chunk_iter = pd.read_csv(
            buffer,
            sep=PIPE_DELIMITER,
            keep_default_na=False,
            dtype=str,
            chunksize=CSV_PARSE_CHUNK_ROWS,
        )
        chunks: List[pd.DataFrame] = []
        for chunk_df in chunk_iter:
            chunks.append(chunk_df)

        buffer.close()
        del buffer

        if not chunks:
            return pd.DataFrame()

        df = pd.concat(chunks, ignore_index=True)
        del chunks
        force_gc()
        return df

    except Exception as e:
        logging.error(f"[read_csv_from_adls] Cannot read '{full_path}': {e}", exc_info=True)
        return pd.DataFrame()


def discover_json_cols_from_adls(service_client, file_system, export_dir, schema_text_cols) -> List[str]:
    """
    Finds which text columns were exported as JSON by checking the ADLS folder structure.
    A column is JSON if a matching sub-folder CSV exists (e.g. address/address.csv).
    Input  : service_client — ADLS client; file_system — container; export_dir — base export path;
             schema_text_cols — list of text column names to check
    Output : list of column names confirmed to be JSON columns
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
    Lists all CSV files under a JSON column's export folder, including nested child arrays.
    Input  : service_client — ADLS client; file_system — container; export_dir — base path;
             json_col_name — name of the JSON column (e.g. "address")
    Output : dict mapping array name → full ADLS path (e.g. {"address": ".../address.csv"})
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
    """
    Sets a value deep inside a nested dict using a dotted key path.
    Example: _set_nested(d, "address.city", "London") sets d["address"]["city"] = "London".
    Input  : d — the target dict; dotted_key — dot-separated path; value — value to set
    Output : none — mutates d in place
    """
    parts = dotted_key.split(".")
    for part in parts[:-1]:
        node = d.get(part)
        if not isinstance(node, dict):
            d[part] = {}
        d = d[part]
    d[parts[-1]] = value


def _infer_csv_value(val):
    """
    Converts a CSV cell string back to its original Python type.
    Empty → None, "true"/"false" → bool, numeric strings → int or float, others unchanged.
    Input  : val — a value read from a CSV cell (usually a string)
    Output : the value cast to the most appropriate Python type
    """
    import math as _math
    if val is None: return None
    if isinstance(val, float) and _math.isnan(val): return None
    if not isinstance(val, str): return val
    s     = val.strip()
    lower = s.lower()
    if s == "": return None
    if lower == "true":  return True
    if lower == "false": return False
    try:
        if "." not in s and "e" not in lower: return int(s)
    except ValueError: pass
    try:    return float(s)
    except ValueError: pass
    return s


def _is_falsy_string(value) -> bool:
    """Returns True if value is empty, "false", "0", "nan", or "none"."""
    return not value or str(value).strip().lower() in ("", "false", "0", "nan", "none")


def reconstruct_json_for_row(row: Dict, arr_name: str, children_index: Dict[str, Dict[str, List[Dict]]]) -> Dict:
    """
    Rebuilds one original JSON object from a flat CSV row and its child rows.
    Reverses the flattening done during export — dotted keys become nested dicts,
    and _has_array_ markers are expanded back into arrays using the children index.
    Input  : row — flat dict from CSV; arr_name — JSON column name;
             children_index — lookup of child rows keyed by parent _rid
    Output : reconstructed nested dict (the original JSON structure)
    """
    result: Dict = {}
    rid = row.get("_rid") or row.get("_row_rid", "")
    for key, value in row.items():
        if key in META_COLS:
            continue
        if key.startswith("_has_array_"):
            if _is_falsy_string(value):
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
    """
    Builds a fast lookup so each child row can be matched back to its parent row.
    Groups child rows by their _parent_rid so reconstruction is O(1) per parent.
    Input  : table_dfs — {array_name: DataFrame} for all CSVs of a JSON column;
             json_col_name — root table name to skip
    Output : {array_name: {parent_rid: [child row dicts]}}
    """
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
    """
    Rebuilds the JSON string for every row in a JSON column's CSV.
    Each row is reconstructed into its original nested JSON and serialised to a string.
    Input  : jcol_df — the root CSV DataFrame for this JSON column;
             children_index — output of build_children_index; json_col_name — column name
    Output : {_row_rid: json_string} — maps each row ID to its reconstructed JSON
    """
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
    """
    Groups rows by their column set so we can prepare one INSERT statement
    per unique column combination, rather than one per row.
    Input  : rows — list of dicts to group
    Output : {frozenset(column_names): [rows with that exact column set]}
    """
    groups: Dict[frozenset, List[Dict]] = {}
    for row in rows:
        groups.setdefault(frozenset(row.keys()), []).append(row)
    return groups


def insert_rows_to_cassandra(session, keyspace, table, rows, column_types, retry_strategy) -> int:
    """
    Inserts a list of row dicts into a Cassandra table using prepared statements.
    Automatically strips unknown columns, casts values to the right types,
    and retries each insert on failure.
    Input  : session — active Cassandra session; keyspace/table — destination;
             rows — list of row dicts; column_types — schema type map;
             retry_strategy — retry policy
    Output : number of rows successfully inserted
    """
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
    Deletes the entire export directory and all its contents from ADLS.
    Used as a final cleanup step when explicitly requested.
    Input  : service_client — ADLS client; file_system — container name;
             export_dir — folder path to delete
    Output : none
    """
    try:
        fs_client  = service_client.get_file_system_client(file_system)
        dir_client = fs_client.get_directory_client(export_dir)
        dir_client.delete_directory()
        logging.info(f"[delete_adls_export_directory] Deleted '{export_dir}' and all contents.")
    except Exception as e:
        logging.warning(f"[delete_adls_export_directory] Could not delete '{export_dir}': {e}")


# ============================================================================
# ADLS -> CASSANDRA: BATCH-WISE ROW DELETION
# ============================================================================

def _delete_batch_rows_from_adls(
    service_client,
    file_system: str,
    main_csv_path: str,
    export_dir: str,
    processed_row_rids: set,
    remaining_rows: List[Dict],
    json_cols: List[str],
    batch_num: int,
    all_columns: List[str],
) -> None:
    """
    After each batch is inserted into Cassandra, removes those rows from the ADLS CSV files.
    Files and folders are NEVER deleted — only the row content is updated.
    When all rows are consumed, the file is kept with just its header row intact.
    If this step fails, it logs a warning but does NOT undo the Cassandra insert.

    Input  : service_client — ADLS client; file_system — container;
             main_csv_path — path to the main table CSV;
             export_dir — base folder for this table's export;
             processed_row_rids — set of _row_rid values just inserted into Cassandra;
             remaining_rows — rows not yet processed (used to rewrite the main CSV);
             json_cols — JSON column names (their CSVs are also updated);
             batch_num — current batch number (for logging);
             all_columns — full column list used to write header-only when empty
    Output : none — rewrites ADLS CSV files in place
    """
    try:
        fs_client = service_client.get_file_system_client(file_system)

        # ── Rewrite main CSV with remaining unprocessed rows (never delete) ───
        main_dir  = main_csv_path.rsplit("/", 1)[0]
        main_file = main_csv_path.rsplit("/", 1)[1]
        main_fc   = fs_client.get_file_client(main_csv_path)
        if remaining_rows:
            remaining_df = pd.DataFrame(remaining_rows)
        else:
            # All rows consumed — preserve header using original column names
            remaining_df = pd.DataFrame(columns=all_columns)
        content_bytes = _serialize_df(remaining_df, include_header=True)
        release_dataframe(remaining_df)
        _delete_adls_file_if_exists(main_fc)
        _write_fresh_file(fs_client.get_directory_client(main_dir), main_file, content_bytes)
        del content_bytes
        logging.info(
            f"[delete_batch_rows] batch {batch_num:04d} — main CSV updated: "
            f"{len(remaining_rows)} rows remaining."
        )

        # ── Filter processed _row_rids from each JSON column parent CSV ───────
        for jcol in json_cols:
            jcol_csv_path = f"{export_dir}/{jcol}/{jcol}.csv"
            try:
                jcol_fc = fs_client.get_file_client(jcol_csv_path)
                buffer  = BytesIO()
                for chunk in jcol_fc.download_file().chunks():
                    buffer.write(chunk)
                buffer.seek(0)
                jcol_df = pd.read_csv(buffer, sep=PIPE_DELIMITER, keep_default_na=False, dtype=str)
                buffer.close()
                del buffer

                jcol_columns = list(jcol_df.columns)
                if "_row_rid" in jcol_df.columns:
                    jcol_df = jcol_df[~jcol_df["_row_rid"].isin(processed_row_rids)].reset_index(drop=True)

                # Always rewrite — preserve header even when all rows are filtered out
                if jcol_df.empty:
                    jcol_df = pd.DataFrame(columns=jcol_columns)
                content_bytes = _serialize_df(jcol_df, include_header=True)
                release_dataframe(jcol_df)
                _delete_adls_file_if_exists(jcol_fc)
                _write_fresh_file(
                    fs_client.get_directory_client(f"{export_dir}/{jcol}"),
                    f"{jcol}.csv",
                    content_bytes,
                )
                del content_bytes
                logging.info(
                    f"[delete_batch_rows] batch {batch_num:04d} — JSON col CSV '{jcol}' updated."
                )
            except Exception as je:
                logging.warning(
                    f"[delete_batch_rows] batch {batch_num:04d} — could not update JSON col CSV '{jcol}': {je}"
                )

        force_gc()

    except Exception as e:
        logging.warning(
            f"[delete_batch_rows] batch {batch_num:04d} — failed to update batch rows in ADLS: {e}"
        )


# ============================================================================
# ADLS -> CASSANDRA: PARAMETER VALIDATION
# ============================================================================

def validate_adls_to_cassandra_params(params: dict) -> dict:
    """
    Validates and cleans up the input parameters for the ADLS→Cassandra import job.
    Checks required fields, fills in defaults, and handles optional settings like
    truncate-before-write and per-batch record deletion.
    Input  : params — raw input dict from the HTTP request
    Output : cleaned params dict ready for use by the pipeline
    Raises : ValueError listing any missing required fields
    """
    required = {
        "adls_account_name":               params.get("adls_account_name"),
        "adls_file_system":                params.get("adls_file_system"),
        "cassandra_contact_points":        params.get("cassandra_contact_points"),
        "cassandra_username":              params.get("cassandra_username"),
        "cassandra_keyspace":              params.get("cassandra_keyspace"),
        "cassandra_table":                 params.get("cassandra_table"),
        "cassandra_key_vault_name":        params.get("cassandra_key_vault_name"),
        "cassandra_key_vault_secret_name": params.get("cassandra_key_vault_secret_name"),
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
    # Accept any casing of this key (e.g. P_DELETE_ADLS_RECORDS, P_Delete_ADLS_Records)
    _del_key  = next((k for k in params if k.lower() == "p_delete_adls_records"), None)
    raw_del   = params.get(_del_key, False) if _del_key else False
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
        "key_vault_name":                  params["cassandra_key_vault_name"],
        "key_vault_secret_name":           params["cassandra_key_vault_secret_name"],
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
    Imports CSV files from ADLS back into a Cassandra table.
    Reads the main CSV in batches, reconstructs JSON columns from their child CSVs,
    then inserts each batch into Cassandra with retry logic.
    If P_Delete_ADLS_Records is true, processed rows are removed from the CSV
    after each successful batch insert (files are never deleted, only rows).

    Input  : params — validated pipeline config dict
    Output : result dict with status, rows_written, rows_failed, batch count,
             duration, retry stats — or {"status": "error", "message": ...} on failure
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
            preferred_port=params.get("cassandra_preferred_port"),
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
        rows_attempted = 0
        batch_num      = 0
        all_main_rows  = main_df.to_dict(orient="records")
        release_dataframe(main_df)

        for batch_start in range(0, len(all_main_rows), batch_size):
            batch_num += 1
            batch      = all_main_rows[batch_start: batch_start + batch_size]
            cassandra_rows: List[Dict] = []
            batch_row_rids: set        = set()
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
                    if row_rid:
                        batch_row_rids.add(row_rid)
            rows_attempted += len(cassandra_rows)
            written = insert_rows_to_cassandra(session, keyspace, table, cassandra_rows, column_types, retry_strategy)
            rows_written += written
            with memory_managed_batch(f"write_batch_{batch_num:04d}"):
                pass
            logging.info(f"[adls_to_cassandra] batch {batch_num:04d} -> {written} rows (cumulative: {rows_written})")

            # ── Batch-wise ADLS deletion: remove just-inserted rows from CSV files ──
            if params.get("delete_adls_records") and written > 0:
                remaining_rows = all_main_rows[batch_start + batch_size:]
                _delete_batch_rows_from_adls(
                    service_client      = service_client,
                    file_system         = params["adls_file_system"],
                    main_csv_path       = main_csv_path,
                    export_dir          = export_dir,
                    processed_row_rids  = batch_row_rids,
                    remaining_rows      = remaining_rows,
                    json_cols           = json_cols,
                    batch_num           = batch_num,
                    all_columns         = list(all_main_rows[0].keys()) if all_main_rows else [],
                )

        shutdown_cassandra(cluster, session)
        cluster = session = None

        retry_stats = retry_strategy.get_stats()
        duration    = (datetime.utcnow() - start_time).total_seconds()
        return {
            "status":                         "success",
            "rows_written":                   rows_written,
            "rows_failed":                    rows_attempted - rows_written,
            "rows_attempted":                 rows_attempted,
            "json_columns_reconstructed":     json_cols,
            "relational_columns":             relational_cols,
            "batches_processed":              batch_num,
            "batch_size":                     batch_size,
            "duration_seconds":               round(duration, 2),
            "adls_records_deleted_per_batch": params.get("delete_adls_records", False),
            "total_retries":                  retry_stats.get("total_retries", 0),
            "retries_by_error_type":          retry_stats.get("retries_by_error_type", {}),
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
#   adls_to_cassandra_http_start   (HTTP trigger)
#   adls_to_cassandra_orchestrator (Orchestrator)
#   process_adls_to_cassandra_activity     (Activity)
#
# Pass "direction": "cassandra_to_adls"  or  "direction": "adls_to_cassandra"
# ============================================================================

@app.activity_trigger(input_name="params")
def process_adls_to_cassandra_activity(params: dict):
    """
    The main Durable Functions activity. Routes the job to the correct pipeline
    based on the direction field in params.
    Input  : params — dict (or JSON string) with all pipeline parameters including
             direction: "adls_to_cassandra" or "cassandra_to_adls"
    Output : result dict from the underlying pipeline (status, row counts, etc.)
    """
    if isinstance(params, str):
        try:   params = json.loads(params)
        except Exception as e:
            logging.warning(f"[process_adls_to_cassandra_activity] params not valid JSON: {e}")

    direction = "adls_to_cassandra"
    logging.info(f"[process_adls_to_cassandra_activity] direction='{direction}'")

    if direction == "adls_to_cassandra":
        return _run_adls_to_cassandra(params)
    elif direction == "cassandra_to_adls":
        return _run_cassandra_to_adls(params)
    else:
        msg = (
            f"Unknown or missing 'direction': '{direction}'. "
            "Must be 'cassandra_to_adls' or 'adls_to_cassandra'."
        )
        logging.error(f"[process_adls_to_cassandra_activity] {msg}")
        return {"status": "error", "message": msg}


@app.orchestration_trigger(context_name="context")
def adls_to_cassandra_orchestrator(context: df.DurableOrchestrationContext):
    """
    Orchestrates the pipeline by calling the activity function and returning its result.
    Kept intentionally thin — all real work happens inside the activity.
    Input  : context — Durable Functions orchestration context carrying the input params
    Output : the result dict returned by process_adls_to_cassandra_activity
    """
    params = context.get_input()
    if isinstance(params, str):
        try:   params = json.loads(params)
        except Exception: pass
    result = yield context.call_activity("process_adls_to_cassandra_activity", params)
    return result


@app.route(route="ADLS_to_Cassandra_V1", methods=["POST"])
@app.durable_client_input(client_name="client")
async def adls_to_cassandra_http_start(req: func.HttpRequest, client) -> func.HttpResponse:
    """
    HTTP entry point — receives a POST request and kicks off the pipeline.
    Immediately returns a 202 response with URLs to check the job status.
    Input  : req — HTTP POST with a JSON body containing all pipeline parameters;
             required: adls_account_name, adls_file_system, cassandra_contact_points,
             cassandra_username, cassandra_keyspace, cassandra_table,
             cassandra_key_vault_name, cassandra_key_vault_secret_name
    Output : 202 with status-check URLs on success; 400 for bad JSON; 500 on start failure
    """
    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse("Invalid JSON body", status_code=400)

    direction = "adls_to_cassandra"

    try:
        instance_id = await client.start_new("adls_to_cassandra_orchestrator", None, body)
        logging.info(f"[adls_to_cassandra_http_start] instance={instance_id} direction={direction}")
        return client.create_check_status_response(req, instance_id)
    except Exception as e:
        logging.error(f"[adls_to_cassandra_http_start] Failed: {e}", exc_info=True)
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)
