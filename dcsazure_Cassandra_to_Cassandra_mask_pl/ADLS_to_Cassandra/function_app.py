"""
ADLS_to_Cassandra.py
====================
Azure Durable Function — reverse of Cassandra_to_ADLS.py.

Reads pipe-delimited CSVs written by Cassandra_to_ADLS, reconstructs
(un-flattens) the original Cassandra rows in batch-wise chunks, and
inserts them back into a Cassandra table.

All known bugs fixed, including the critical linking of nested arrays
(servers, teams, employees, tasks).  The unflattening logic now mirrors
the working ADLS‑to‑Cosmos implementation.
"""

import ast
import ctypes
import gc
import json
import logging
import time
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

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
from cassandra.query import BatchStatement, BatchType

# ============================================================================
# CONSTANTS
# ============================================================================

DEFAULT_BATCH_SIZE = 10_000
DEFAULT_CHILD_CHUNK = 10_000
DEFAULT_INSERT_CHUNK = 10
DEFAULT_MAX_RETRIES = 5
DEFAULT_BASE_DELAY = 0.5
DEFAULT_MAX_DELAY = 60.0
DEFAULT_CONCURRENT_INSERTS = 64

PIPE_DELIMITER = "|"
ESCAPE_CHAR = "\\"

SYSTEM_FIELDS: Set[str] = {"_rid", "_parent_rid", "_row_rid"}
_CQL_JSON_TEXT_TYPES: Set[str] = {"text", "varchar", "ascii"}

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
def memory_managed_batch(label: str = "batch"):
    try:
        yield
    finally:
        logging.debug(f"[memory] flush after {label}")
        force_gc()


# ============================================================================
# KEY VAULT
# ============================================================================

def get_secret(key_vault_name: str, secret_name: str) -> str:
    kv_uri = f"https://{key_vault_name}.vault.azure.net"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=kv_uri, credential=credential)
    return client.get_secret(secret_name).value


# ============================================================================
# BOOLEAN PARAMETER HELPER
# ============================================================================

def _bool_param(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes")
    return bool(value)


# ============================================================================
# CASSANDRA
# ============================================================================

def build_cassandra_cluster(
    contact_points: List[str],
    port: int,
    username: str,
    password: str,
    preferred_node: Optional[str] = None,
) -> Cluster:
    auth = PlainTextAuthProvider(username=username, password=password)
    lb = (
        DCAwareRoundRobinPolicy(local_dc=preferred_node)
        if preferred_node else RoundRobinPolicy()
    )
    return Cluster(
        contact_points=contact_points,
        port=port,
        auth_provider=auth,
        execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=lb)},
        connect_timeout=30,
        control_connection_timeout=30,
    )


def validate_cassandra_connection(
    contact_points, port, username, password, keyspace, table, preferred_node=None
):
    try:
        cluster = build_cassandra_cluster(contact_points, port, username, password, preferred_node)
        session = cluster.connect(keyspace)
        rows = session.execute(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s AND table_name=%s",
            (keyspace, table),
        )
        if not rows.one():
            raise ValueError(f"Table '{table}' not found in keyspace '{keyspace}'.")
        del rows
        return cluster, session
    except ValueError:
        raise
    except Exception as e:
        raise ValueError(f"Cassandra connection failed: {str(e)}")


def shutdown_cassandra(cluster, session) -> None:
    for obj in (session, cluster):
        try:
            if obj:
                obj.shutdown()
        except Exception:
            pass
    force_gc()


def get_cassandra_columns(session, keyspace: str, table: str) -> List[str]:
    rows = session.execute(
        "SELECT column_name FROM system_schema.columns WHERE keyspace_name=%s AND table_name=%s",
        (keyspace, table),
    )
    cols = [r.column_name for r in rows]
    del rows
    force_gc()
    return cols


def get_cassandra_column_types(session, keyspace: str, table: str) -> Dict[str, str]:
    rows = session.execute(
        "SELECT column_name, type FROM system_schema.columns WHERE keyspace_name=%s AND table_name=%s",
        (keyspace, table),
    )
    types = {r.column_name: r.type for r in rows}
    del rows
    force_gc()
    return types


def get_cassandra_key_columns(session, keyspace: str, table: str) -> Tuple[List[str], List[str]]:
    rows = session.execute(
        """
        SELECT column_name, kind, position
        FROM system_schema.columns
        WHERE keyspace_name=%s AND table_name=%s
        """,
        (keyspace, table),
    )
    partition_keys: List[Tuple[int, str]] = []
    clustering_keys: List[Tuple[int, str]] = []
    for r in rows:
        if r.kind == "partition_key":
            partition_keys.append((r.position, r.column_name))
        elif r.kind == "clustering":
            clustering_keys.append((r.position, r.column_name))
    partition_keys.sort()
    clustering_keys.sort()
    pk = [c for _, c in partition_keys]
    ck = [c for _, c in clustering_keys]
    logging.info(f"Schema auto-discovered — partition keys: {pk}  clustering keys: {ck}")
    del rows
    force_gc()
    return pk, ck


# Common datetime formats
_DATETIME_FORMATS = [
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
]

_CQL_TIMESTAMP_TYPES = {"timestamp", "date", "time"}
_CQL_UUID_TYPES = {"uuid", "timeuuid"}
_CQL_INT_TYPES = {"int", "tinyint", "smallint", "bigint", "varint", "counter"}
_CQL_FLOAT_TYPES = {"float", "double", "decimal"}
_CQL_BOOL_TYPES = {"boolean"}
_CQL_INET_TYPES = {"inet"}


def coerce_value_for_cassandra(value: Any, cql_type: str) -> Any:
    import uuid as _uuid
    import ipaddress

    if value is None:
        return None

    base_type = cql_type.strip().lower()
    for wrapper in ("frozen<", "list<", "set<", "tuple<"):
        if base_type.startswith(wrapper):
            base_type = base_type[len(wrapper):].rstrip(">").strip()
            break
    if base_type.startswith("map<"):
        inner = base_type[4:].rstrip(">")
        parts = inner.split(",", 1)
        base_type = parts[1].strip() if len(parts) == 2 else parts[0].strip()

    if not isinstance(value, str):
        return value

    v = value.strip()
    if v == "":
        return None

    if base_type in _CQL_TIMESTAMP_TYPES:
        for fmt in _DATETIME_FORMATS:
            try:
                return datetime.strptime(v, fmt)
            except ValueError:
                continue
        try:
            return pd.Timestamp(v).to_pydatetime()
        except Exception:
            pass
        logging.warning(f"Cannot parse datetime '{v}' — passing as-is.")
        return value

    if base_type in _CQL_UUID_TYPES:
        try:
            return _uuid.UUID(v)
        except (ValueError, AttributeError):
            logging.warning(f"Cannot parse UUID '{v}' — passing as-is.")
            return value

    if base_type in _CQL_INT_TYPES:
        try:
            return int(float(v)) if "." in v else int(v)
        except (ValueError, TypeError):
            logging.warning(f"Cannot parse int '{v}' — passing as-is.")
            return value

    if base_type in _CQL_FLOAT_TYPES:
        try:
            return float(v)
        except (ValueError, TypeError):
            logging.warning(f"Cannot parse float '{v}' — passing as-is.")
            return value

    if base_type in _CQL_BOOL_TYPES:
        lower_v = v.lower()
        if lower_v in ("true", "1", "yes"):
            return True
        if lower_v in ("false", "0", "no"):
            return False
        logging.warning(f"Cannot parse boolean '{v}' — passing as-is.")
        return value

    if base_type in _CQL_INET_TYPES:
        try:
            ipaddress.ip_address(v)
        except ValueError:
            logging.warning(f"Invalid inet address '{v}' — passing as-is.")
        return v

    return value


# ============================================================================
# ADLS
# ============================================================================

def validate_adls_connection(adls_account_name: str, adls_file_system: str) -> DataLakeServiceClient:
    try:
        credential = DefaultAzureCredential()
        service_client = DataLakeServiceClient(
            account_url=f"https://{adls_account_name}.dfs.core.windows.net",
            credential=credential,
        )
        fs_client = service_client.get_file_system_client(adls_file_system)
        list(fs_client.get_paths(path="", max_results=1))
        return service_client
    except ClientAuthenticationError:
        raise ValueError("ADLS authentication failed. Check managed identity permissions.")
    except ResourceNotFoundError:
        raise ValueError(f"ADLS filesystem '{adls_file_system}' does not exist.")
    except Exception as e:
        raise ValueError(f"ADLS connection failed: {str(e)}")


def get_csv_row_count(service_client, file_system, full_path, chunk_size=50_000) -> int:
    try:
        fs_client = service_client.get_file_system_client(file_system)
        download = fs_client.get_file_client(full_path).download_file()
        total = 0
        for chunk in pd.read_csv(
            download, sep=PIPE_DELIMITER, chunksize=chunk_size,
            iterator=True, keep_default_na=False, escapechar=ESCAPE_CHAR,
        ):
            total += len(chunk)
            del chunk
        return total
    except Exception as e:
        logging.error(f"Error counting rows in '{full_path}': {str(e)}")
        raise


def read_csv_from_adls_batched(
    service_client, file_system, full_path, skip_rows=0, nrows=None
) -> pd.DataFrame:
    try:
        fs_client = service_client.get_file_system_client(file_system)
        download = fs_client.get_file_client(full_path).download_file()

        read_params: Dict[str, Any] = {
            "sep": PIPE_DELIMITER, "keep_default_na": False,
            "escapechar": ESCAPE_CHAR, "dtype": str,
        }
        if skip_rows > 0:
            read_params["skiprows"] = range(1, skip_rows + 1)
        if nrows:
            read_params["nrows"] = nrows

        df_out = pd.read_csv(download, **read_params)
        df_out = df_out.where(df_out != "", other=None)
        return df_out
    except Exception as e:
        logging.error(f"Error reading CSV '{full_path}' (skip={skip_rows}, nrows={nrows}): {e}")
        raise


def discover_all_csv_paths(service_client, file_system, export_dir) -> List[str]:
    fs_client = service_client.get_file_system_client(file_system)
    paths: List[str] = []
    try:
        for item in fs_client.get_paths(path=export_dir, recursive=True):
            if not item.is_directory and item.name.endswith(".csv"):
                paths.append(item.name)
    except Exception as e:
        logging.warning(f"Could not list paths under '{export_dir}': {e}")
    return paths


# ============================================================================
# CSV PATH CLASSIFICATION (adapted from Cosmos working version)
# ============================================================================

def get_json_col_scalar_paths(csv_paths, export_dir, table) -> Dict[str, str]:
    parent_csv = f"{export_dir}/{table}.csv"
    scalar_map: Dict[str, str] = {}
    for full_path in csv_paths:
        if full_path == parent_csv:
            continue
        rel = full_path[len(export_dir):].lstrip("/")
        parts = [p for p in rel.split("/") if p]
        if len(parts) == 2 and parts[-1].rsplit(".", 1)[0] == parts[0]:
            scalar_map[parts[0]] = full_path
    return scalar_map


def organize_csv_paths_by_depth(
    csv_paths,
    export_dir,
    table,
    cassandra_col_types: Optional[Dict[str, str]] = None,
) -> Dict[int, List[Dict]]:
    parent_csv = f"{export_dir}/{table}.csv"

    # Pass 1: known jcol names from scalar CSVs
    known_jcols: Set[str] = set()
    for full_path in csv_paths:
        if full_path == parent_csv:
            continue
        rel = full_path[len(export_dir):].lstrip("/")
        parts = [p for p in rel.split("/") if p]
        if len(parts) == 2 and parts[-1].rsplit(".", 1)[0] == parts[0]:
            known_jcols.add(parts[0])

    # Pass 1.5: detect array-only JSON columns using schema
    if cassandra_col_types:
        for full_path in csv_paths:
            if full_path == parent_csv:
                continue
            rel = full_path[len(export_dir):].lstrip("/")
            parts = [p for p in rel.split("/") if p]
            if not parts:
                continue
            candidate = parts[0]
            if candidate in known_jcols:
                continue
            cql_type = cassandra_col_types.get(candidate, "").strip().lower()
            if cql_type in _CQL_JSON_TEXT_TYPES:
                known_jcols.add(candidate)
                logging.info(f"[path-classify] '{candidate}' added via schema (array-only)")

    result: Dict[int, List[Dict]] = {}

    # Pass 2: classify arrays based on directory structure (always include)
    for full_path in sorted(csv_paths):
        if full_path == parent_csv:
            continue
        rel = full_path[len(export_dir):].lstrip("/")
        parts = [p for p in rel.split("/") if p]
        if not parts:
            continue

        filename_stem = parts[-1].rsplit(".", 1)[0]

        # Skip scalar JSON-column CSVs
        if len(parts) == 2 and filename_stem == parts[0]:
            continue

        if parts[0] not in known_jcols:
            # Top-level array
            arr_dirs = parts[:-1]
            arr_name = ".".join(arr_dirs)
            depth = len(arr_dirs) - 1
            jcol = None
            result.setdefault(depth, []).append({
                "full_path": full_path,
                "arr_name": arr_name,
                "jcol": jcol,
                "depth": depth,
            })
        else:
            # JSON-column array
            jcol = parts[0]
            start_idx = 2 if len(parts) > 2 and parts[1] == jcol else 1
            arr_dirs = parts[start_idx:-1]
            if arr_dirs:
                arr_name = ".".join(arr_dirs)
                depth = len(arr_dirs) - 1
            else:
                arr_name = filename_stem
                depth = 0

            result.setdefault(depth, []).append({
                "full_path": full_path,
                "arr_name": arr_name,
                "jcol": jcol,
                "depth": depth,
            })

    return result


# ============================================================================
# UNFLATTEN HELPERS (from Cosmos, with boolean fix)
# ============================================================================

def parse_json_string(value: str) -> Any:
    v = value.strip()
    if v.lower() == "true":
        return True
    if v.lower() == "false":
        return False
    if (v.startswith("[") and v.endswith("]")) or (v.startswith("{") and v.endswith("}")):
        try:
            return json.loads(v)
        except Exception:
            try:
                return ast.literal_eval(v)
            except Exception:
                pass
    return value


def unflatten_dict(flat: Dict[str, Any], sep: str = ".") -> Dict:
    result: Dict = {}
    for key, value in flat.items():
        if isinstance(value, str):
            value = parse_json_string(value)
        parts = key.split(sep)
        current = result
        for i, part in enumerate(parts):
            if i == len(parts) - 1:
                current[part] = value
            else:
                if part not in current or not isinstance(current[part], dict):
                    current[part] = {}
                current = current[part]
    return result


def strip_system_fields(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {
            k: strip_system_fields(v)
            for k, v in obj.items()
            if k not in SYSTEM_FIELDS and not k.startswith("_has_array_")
        }
    if isinstance(obj, list):
        return [strip_system_fields(v) for v in obj]
    return obj


def cassandra_safe(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): cassandra_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [cassandra_safe(v) for v in obj]
    try:
        import numpy as np
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, (np.bool_,)):
            return bool(obj)
    except ImportError:
        pass
    try:
        if isinstance(obj, float) and pd.isna(obj):
            return None
        if obj is pd.NaT:
            return None
    except Exception:
        pass
    return obj


# ============================================================================
# PREPARE BATCH PARENTS (unchanged)
# ============================================================================

def prepare_batch_parents(parent_df: pd.DataFrame) -> List[Dict]:
    parents: List[Dict] = []
    for _, row in parent_df.iterrows():
        flat = {k: v for k, v in row.items() if v is not None and v == v}

        has_array_fields = {k: v for k, v in flat.items() if k.startswith("_has_array_")}
        regular_fields = {
            k: v for k, v in flat.items()
            if not k.startswith("_has_array_") and k != "_parent_rid"
        }

        nested = unflatten_dict(regular_fields)
        nested.update(has_array_fields)
        parents.append(nested)
    return parents


# ============================================================================
# CHILD OBJECT BUILDER (from Cosmos, with boolean fix)
# ============================================================================

def _is_boolean_string(v: Any) -> bool:
    return isinstance(v, str) and v.strip().lower() in ("true", "false")


def _build_child_object(row: pd.Series) -> Dict:
    rid = str(row.get("_rid", "") or "") if pd.notna(row.get("_rid")) else None
    parent_rid = str(row.get("_parent_rid", "") or "") if pd.notna(row.get("_parent_rid")) else None

    obj = {}
    obj["_rid"] = rid
    obj["_parent_rid"] = parent_rid

    for k, v in row.items():
        if k in ("_rid", "_parent_rid"):
            continue
        if pd.notna(v):
            obj[k] = v

    has_array_fields = {k: v for k, v in obj.items() if k.startswith("_has_array_")}
    regular_fields = {
        k: v
        for k, v in obj.items()
        if k not in ("_rid", "_parent_rid") and not k.startswith("_has_array_")
    }

    nested_obj = unflatten_dict(regular_fields)
    nested_obj.update(has_array_fields)
    nested_obj["_rid"] = obj["_rid"]
    nested_obj["_parent_rid"] = obj["_parent_rid"]

    return nested_obj


# ============================================================================
# STREAM CHILD CSV (from Cosmos)
# ============================================================================

def process_child_csv_streaming(
    service_client,
    file_system: str,
    full_path: str,
    parent_rids: Set[str],
    arr_name: str,
    all_objects_by_rid: Dict,
    child_objects_by_table: Dict,
) -> Tuple[int, Set[str]]:
    try:
        logging.debug(f"  Reading child array from: {full_path} for {arr_name}")
        fs_client = service_client.get_file_system_client(file_system)
        download = fs_client.get_file_client(full_path).download_file()

        child_rids: Set[str] = set()
        row_count = 0

        if arr_name not in child_objects_by_table:
            child_objects_by_table[arr_name] = []

        chunk_iter = pd.read_csv(
            download,
            sep=PIPE_DELIMITER, chunksize=DEFAULT_CHILD_CHUNK,
            iterator=True, keep_default_na=False, escapechar=ESCAPE_CHAR, dtype=str,
        )

        for chunk in chunk_iter:
            if "_parent_rid" not in chunk.columns:
                del chunk
                continue

            chunk["_parent_rid"] = chunk["_parent_rid"].astype(str)
            filtered_chunk = chunk[chunk["_parent_rid"].isin(parent_rids)]
            del chunk

            if filtered_chunk.empty:
                release_dataframe(filtered_chunk)
                continue

            if "_rid" in filtered_chunk.columns:
                filtered_chunk["_rid"] = filtered_chunk["_rid"].astype(str)

            for _, r in filtered_chunk.iterrows():
                obj = _build_child_object(r)
                child_objects_by_table[arr_name].append(obj)

                if obj["_rid"]:
                    child_rids.add(str(obj["_rid"]))
                    all_objects_by_rid[str(obj["_rid"])] = obj

                row_count += 1

            release_dataframe(filtered_chunk)

        logging.debug(f"  [{arr_name}] {row_count} rows, {len(child_rids)} child RIDs")
        return row_count, child_rids

    except Exception as e:
        logging.error(f"Error streaming child CSV '{full_path}': {e}")
        raise


# ============================================================================
# ARRAY MARKER HANDLING (from Cosmos)
# ============================================================================

def _record_array_markers(all_objects_by_rid: Dict, arrays_with_markers: Dict,
                          row_rid_to_parent: Optional[Dict] = None) -> None:
    for rid, obj in all_objects_by_rid.items():
        for key in list(obj.keys()):
            if key.startswith("_has_array_"):
                arr_name = key[len("_has_array_"):]
                arrays_with_markers.setdefault(arr_name, set()).add(rid)

    if row_rid_to_parent:
        for row_rid, obj in row_rid_to_parent.items():
            for key in list(obj.keys()):
                if key.startswith("_has_array_"):
                    arr_name = key[len("_has_array_"):]
                    arrays_with_markers.setdefault(arr_name, set()).add(row_rid)


def _initialize_arrays_from_markers(all_objects_by_rid: Dict) -> None:
    for _rid, obj in all_objects_by_rid.items():
        markers = [k for k in list(obj.keys()) if k.startswith("_has_array_")]
        for marker in markers:
            arr_name = marker[len("_has_array_"):]
            path_parts = arr_name.split(".")
            target = obj
            ok = True
            for part in path_parts[:-1]:
                if part not in target:
                    target[part] = {}
                elif not isinstance(target[part], dict):
                    ok = False
                    break
                target = target[part]
            if ok:
                final = path_parts[-1]
                if final not in target:
                    target[final] = []
                elif not isinstance(target[final], list):
                    target[final] = [target[final]] if target[final] is not None else []
            if marker in obj:
                del obj[marker]


def _determine_filter_rids(
    arr_name: str,
    current_depth: int,
    parent_rids: Set[str],
    rids_by_depth: Dict[int, Set[str]],
    all_objects_by_rid: Dict,
) -> Set[str]:
    arr_parts = arr_name.split(".")
    possible_markers = [".".join(arr_parts[i:]) for i in range(len(arr_parts))]

    for marker_suffix in possible_markers:
        marker_key = f"_has_array_{marker_suffix}"
        rids_with_marker = {
            rid for rid, obj in all_objects_by_rid.items() if marker_key in obj
        }

        if rids_with_marker:
            if current_depth == 0:
                valid_rids = rids_with_marker & parent_rids
                if valid_rids:
                    return valid_rids
            else:
                valid_rids = set()
                for depth in range(current_depth + 1):
                    if depth in rids_by_depth:
                        valid_rids.update(rids_with_marker & rids_by_depth[depth])
                if valid_rids:
                    return valid_rids

    # Fallback
    if current_depth == 0:
        return parent_rids

    for d in range(current_depth, -1, -1):
        if d in rids_by_depth and rids_by_depth[d]:
            return rids_by_depth[d]

    return set()


def _assign_children_to_parents(
    child_objects_by_table: Dict,
    all_objects_by_rid: Dict,
    rid_to_parent: Dict,
    arrays_with_markers: Dict,
) -> None:
    sorted_tables = sorted(child_objects_by_table.items(), key=lambda x: x[0].count("."))

    for arr_name, objects in sorted_tables:
        if not objects:
            continue

        grouped: Dict[str, List[Dict]] = defaultdict(list)
        for obj in objects:
            p_rid = str(obj.get("_parent_rid", "") or "")
            if p_rid and p_rid != "None":
                grouped[p_rid].append(obj)

        for p_rid, children in grouped.items():
            parent_obj = all_objects_by_rid.get(p_rid) or rid_to_parent.get(p_rid)
            if parent_obj is None:
                continue

            clean_children = _clean_child_objects(children, all_objects_by_rid)
            _add_children_to_array(
                parent_obj,
                arr_name,
                clean_children,
                p_rid,
                arrays_with_markers,
                rid_to_parent,
            )


def _clean_child_objects(children: List[Dict], all_objects_by_rid: Dict) -> List[Dict]:
    clean_children = []
    for child in children:
        child_rid = str(child.get("_rid", "") or "")
        clean = {
            k: v
            for k, v in child.items()
            if k not in ("_rid", "_parent_rid") and not k.startswith("_has_array_")
        }
        clean_children.append(clean)

        if child_rid and child_rid in all_objects_by_rid:
            all_objects_by_rid[child_rid] = clean

    return clean_children


def _add_children_to_array(
    parent_obj: Dict,
    arr_name: str,
    clean_children: List[Dict],
    parent_rid: str,
    arrays_with_markers: Dict,
    rid_to_parent: Dict,
) -> None:
    has_marker_in_parent = False
    actual_marker_name = None

    if parent_rid in arrays_with_markers.get(arr_name, set()):
        has_marker_in_parent = True
        actual_marker_name = arr_name
    else:
        parent_markers = [
            marker for marker, rids in arrays_with_markers.items() if parent_rid in rids
        ]

        if parent_markers:
            for marker in parent_markers:
                if arr_name.endswith(marker) or arr_name == marker:
                    has_marker_in_parent = True
                    actual_marker_name = marker
                    break

    if has_marker_in_parent:
        target = parent_obj
        path_segments = actual_marker_name.split(".")

        for part in path_segments[:-1]:
            if part not in target:
                target[part] = {}
            elif not isinstance(target[part], dict):
                return
            target = target[part]

        final_array_key = path_segments[-1]
        if final_array_key not in target:
            target[final_array_key] = []

        if isinstance(target[final_array_key], list):
            target[final_array_key].extend(clean_children)
        else:
            target[final_array_key] = clean_children
    else:
        path_segments = arr_name.split(".")
        final_array_key = path_segments[-1]

        if final_array_key not in parent_obj:
            parent_obj[final_array_key] = []

        if isinstance(parent_obj[final_array_key], list):
            parent_obj[final_array_key].extend(clean_children)
        else:
            parent_obj[final_array_key] = clean_children


def _clean_documents(documents: List[Dict]) -> None:
    def clean_recursive(obj):
        if isinstance(obj, dict):
            return {
                k: clean_recursive(v)
                for k, v in obj.items()
                if k not in ("_rid", "_parent_rid") and not k.startswith("_has_array_")
            }
        elif isinstance(obj, list):
            return [clean_recursive(item) for item in obj]
        else:
            return obj

    for doc in documents:
        for key in list(doc.keys()):
            if key not in ("_rid", "_parent_rid") and not key.startswith("_has_array_"):
                doc[key] = clean_recursive(doc[key])

        doc.pop("_rid", None)
        doc.pop("_parent_rid", None)


# ============================================================================
# JSON COLUMN SCALAR CSV (unchanged, with boolean fix)
# ============================================================================

def process_json_col_scalar_csv(
    service_client,
    file_system: str,
    scalar_path: str,
    jcol: str,
    parent_row_rids: Set[str],
    row_rid_to_parent: Dict,
) -> None:
    try:
        fs_client = service_client.get_file_system_client(file_system)
        download = fs_client.get_file_client(scalar_path).download_file()

        chunk_iter = pd.read_csv(
            download, sep=PIPE_DELIMITER, chunksize=DEFAULT_CHILD_CHUNK,
            iterator=True, keep_default_na=False, escapechar=ESCAPE_CHAR, dtype=str,
        )

        for chunk in chunk_iter:
            if "_row_rid" not in chunk.columns:
                del chunk
                continue

            chunk["_row_rid"] = chunk["_row_rid"].fillna("").astype(str)
            filtered = chunk[chunk["_row_rid"].isin(parent_row_rids)].copy()
            del chunk

            if filtered.empty:
                release_dataframe(filtered)
                continue

            for _, row in filtered.iterrows():
                row_rid = str(row.get("_row_rid", "") or "")
                parent = row_rid_to_parent.get(row_rid)
                if parent is None:
                    continue

                scalar_flat: Dict[str, Any] = {}
                for k, v in row.items():
                    if k in SYSTEM_FIELDS or k.startswith("_has_array_"):
                        continue
                    if _is_boolean_string(v):
                        scalar_flat[k] = v
                        continue
                    if pd.notna(v) and v != "":
                        scalar_flat[k] = v

                if not scalar_flat:
                    continue

                nested_scalars = unflatten_dict(scalar_flat)

                existing = parent.get(jcol)
                if isinstance(existing, dict):
                    existing.update(nested_scalars)
                else:
                    parent[jcol] = nested_scalars

            release_dataframe(filtered)

    except Exception as e:
        logging.error(f"Error in scalar CSV '{scalar_path}' for column '{jcol}': {e}")
        raise


# ============================================================================
# CORE BATCH PROCESSING (adapted from Cosmos)
# ============================================================================

def process_batch(
    batch_parents: List[Dict],
    service_client,
    file_system: str,
    export_dir: str,
    table: str,
    json_col_scalar_paths: Dict[str, str],
    csv_info_by_depth: Dict[int, List[Dict]],
) -> Tuple[List[Dict], int]:
    # Build lookups
    rid_to_parent: Dict[str, Dict] = {}
    row_rid_to_parent: Dict[str, Dict] = {}

    for parent in batch_parents:
        rrid = str(parent.get("_row_rid", "") or "")
        rid = str(parent.get("_rid", "") or "")
        if rrid:
            row_rid_to_parent[rrid] = parent
        if rid:
            rid_to_parent[rid] = parent

    all_objects_by_rid: Dict[str, Dict] = dict(rid_to_parent)
    parent_rids = set(rid_to_parent.keys())
    total_child_rows = 0

    # Process JSON scalar CSVs
    for jcol, scalar_path in json_col_scalar_paths.items():
        try:
            process_json_col_scalar_csv(
                service_client=service_client,
                file_system=file_system,
                scalar_path=scalar_path,
                jcol=jcol,
                parent_row_rids=set(row_rid_to_parent.keys()),
                row_rid_to_parent=row_rid_to_parent,
            )
        except Exception as e:
            logging.error(f"Scalar CSV error for '{jcol}': {e} — continuing.")

    # Prepare RID sets per depth
    rids_by_depth: Dict[int, Set[str]] = {0: parent_rids}
    child_objects_by_table: Dict[str, List[Dict]] = {}
    arrays_with_markers: Dict[str, Set[str]] = {}

    max_depth = max(csv_info_by_depth.keys()) if csv_info_by_depth else -1

    # Process depths in increasing order
    for depth in range(max_depth + 1):
        if depth not in csv_info_by_depth:
            continue

        if depth + 1 not in rids_by_depth:
            rids_by_depth[depth + 1] = set()

        for csv_info in csv_info_by_depth[depth]:
            filter_rids = _determine_filter_rids(
                arr_name=csv_info["arr_name"],
                current_depth=depth,
                parent_rids=parent_rids,
                rids_by_depth=rids_by_depth,
                all_objects_by_rid=all_objects_by_rid,
            )

            if not filter_rids:
                logging.debug(f"    No filter RIDs for '{csv_info['arr_name']}' depth={depth}")
                continue

            try:
                child_count, child_rids = process_child_csv_streaming(
                    service_client=service_client,
                    file_system=file_system,
                    full_path=csv_info["full_path"],
                    parent_rids=filter_rids,
                    arr_name=csv_info["arr_name"],
                    all_objects_by_rid=all_objects_by_rid,
                    child_objects_by_table=child_objects_by_table,
                )

                total_child_rows += child_count
                if child_rids:
                    rids_by_depth[depth + 1].update(child_rids)

            except Exception as e:
                logging.error(f"Error processing '{csv_info['full_path']}': {e}")
                continue

    # Assign children into parents
    _record_array_markers(all_objects_by_rid, arrays_with_markers,
                          row_rid_to_parent=row_rid_to_parent)
    _initialize_arrays_from_markers(all_objects_by_rid)
    _initialize_arrays_from_markers(row_rid_to_parent)

    _assign_children_to_parents(
        child_objects_by_table,
        all_objects_by_rid,
        rid_to_parent,
        arrays_with_markers,
    )

    # Clean metadata
    _clean_documents(batch_parents)
    documents = [cassandra_safe(strip_system_fields(doc)) for doc in batch_parents]

    # Cleanup
    del rid_to_parent, row_rid_to_parent, all_objects_by_rid
    del child_objects_by_table, arrays_with_markers, rids_by_depth
    force_gc()

    return documents, total_child_rows


# ============================================================================
# RETRY STRATEGY (unchanged)
# ============================================================================

class CassandraRetryStrategy:
    def __init__(self, max_retries=DEFAULT_MAX_RETRIES, base_delay=DEFAULT_BASE_DELAY, max_delay=DEFAULT_MAX_DELAY):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.retry_counts: Dict[str, int] = {}

    def execute_with_retry(self, operation, name="op"):
        import random
        for attempt in range(self.max_retries + 1):
            try:
                return operation()
            except Exception as e:
                etype = self._classify(str(e))
                self.retry_counts[etype] = self.retry_counts.get(etype, 0) + 1
                if attempt < self.max_retries:
                    wait = min(self.base_delay * (2 ** attempt), self.max_delay)
                    wait += random.uniform(0, 0.1 * wait)
                    logging.warning(f"[retry] {name} ({etype}) attempt {attempt+1}/{self.max_retries+1} wait={wait:.2f}s")
                    time.sleep(wait)
                else:
                    logging.error(f"[retry] {name} failed: {e}")
                    raise

    def _classify(self, msg):
        m = msg.lower()
        if "unavailable" in m:
            return "Unavailable"
        if "timeout" in m:
            return "Timeout"
        if "overloaded" in m:
            return "Overloaded"
        if "connection" in m:
            return "Connection"
        return "Other"

    def get_stats(self):
        return {"total_retries": sum(self.retry_counts.values()), "retries_by_error_type": dict(self.retry_counts)}


# ============================================================================
# CASSANDRA INSERT (unchanged)
# ============================================================================

def insert_rows_into_cassandra(
    session, keyspace, table, rows, cassandra_columns, retry_strategy, batch_size=DEFAULT_BATCH_SIZE,
    col_types: Optional[Dict[str, str]] = None,
    concurrency: int = DEFAULT_CONCURRENT_INSERTS,
    total_inserted_so_far: int = 0,
) -> Tuple[int, int]:
    if not rows:
        return 0, 0

    cassandra_col_set = set(cassandra_columns)
    all_keys = [c for c in cassandra_columns if any(c in row for row in rows) and c in cassandra_col_set]

    if not all_keys:
        logging.warning("No matching Cassandra columns in rows — skipping insert.")
        return 0, len(rows)

    col_list = ", ".join(f'"{c}"' for c in all_keys)
    val_list = ", ".join("?" * len(all_keys))
    cql = f'INSERT INTO "{keyspace}"."{table}" ({col_list}) VALUES ({val_list})'

    try:
        prepared = session.prepare(cql)
    except Exception as e:
        logging.error(f"Failed to prepare INSERT: {e}")
        raise

    inserted = failed = 0
    total_rows = len(rows)

    for batch_start in range(0, total_rows, batch_size):
        sub_rows = rows[batch_start: batch_start + batch_size]

        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        for row in sub_rows:
            values = []
            for c in all_keys:
                val = cassandra_safe(row.get(c))
                if isinstance(val, (dict, list)):
                    val = json.dumps(val, ensure_ascii=False, default=str)
                if col_types and c in col_types:
                    val = coerce_value_for_cassandra(val, col_types[c])
                values.append(val)
            batch.add(prepared, tuple(values))

        def _execute(b=batch):
            session.execute(b)

        try:
            retry_strategy.execute_with_retry(_execute, f"insert_batch_{batch_start}")
            inserted += len(sub_rows)
        except Exception as e:
            logging.error(f"Batch insert failed start={batch_start}: {e}. Skipping {len(sub_rows)} rows.")
            failed += len(sub_rows)

        batch.clear()
        del batch, sub_rows

        running_total = total_inserted_so_far + inserted
        logging.info(
            f"  [INSERT PROGRESS] sub-batch {batch_start // batch_size + 1}"
            f" | this_chunk={len(rows[batch_start: batch_start + batch_size])} failed={failed}"
            f" | rows transferred so far: {running_total:,}"
        )

    force_gc()
    return inserted, failed


# ============================================================================
# ADLS FOLDER DELETION (unchanged)
# ============================================================================

def delete_adls_table_folder(
    adls_account_name: str,
    table: str,
    source_keyspace: str,
    target_keyspace: str,
) -> Dict[str, Any]:
    credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(
        account_url=f"https://{adls_account_name}.dfs.core.windows.net",
        credential=credential,
    )

    results: Dict[str, str] = {}

    for keyspace_dir, label in [
        (source_keyspace, "source"),
        (target_keyspace, "target"),
    ]:
        if not keyspace_dir:
            logging.warning(f"[ADLS DELETE] {label} keyspace directory not provided — skipping.")
            results[label] = "skipped (no directory provided)"
            continue

        folder_path = f"{keyspace_dir}/{table}"

        try:
            try:
                fs_client = service_client.get_file_system_client(keyspace_dir)
                dir_client = fs_client.get_directory_client(table)
            except Exception:
                fs_client = service_client.get_file_system_client(file_system="$root")
                dir_client = fs_client.get_directory_client(folder_path)

            dir_client.delete_directory()
            logging.info(f"[ADLS DELETE] {label} folder deleted: '{folder_path}'")
            results[label] = f"deleted: {folder_path}"

        except ResourceNotFoundError:
            logging.warning(f"[ADLS DELETE] {label} folder not found (already deleted?): '{folder_path}'")
            results[label] = f"not found: {folder_path}"
        except Exception as e:
            logging.error(f"[ADLS DELETE] Failed to delete {label} folder '{folder_path}': {e}")
            results[label] = f"error: {str(e)}"

    return results


# ============================================================================
# ADLS CSV RECORD TRUNCATION (unchanged)
# ============================================================================

def truncate_adls_csv_records(
    service_client: DataLakeServiceClient,
    file_system: str,
    all_csv_paths: List[str],
) -> Dict[str, Any]:
    truncated: List[str] = []
    skipped: List[str] = []
    failed: List[Dict[str, str]] = []

    fs_client = service_client.get_file_system_client(file_system)

    for full_path in all_csv_paths:
        try:
            raw_bytes: bytes = fs_client.get_file_client(full_path).download_file().readall()

            if not raw_bytes:
                logging.info(f"[TRUNCATE] '{full_path}' is already empty — skipping.")
                skipped.append(full_path)
                continue

            pos_crlf = raw_bytes.find(b"\r\n")
            pos_lf = raw_bytes.find(b"\n")

            if pos_crlf != -1 and (pos_lf == -1 or pos_crlf <= pos_lf):
                header_bytes = raw_bytes[: pos_crlf + 2]
            elif pos_lf != -1:
                header_bytes = raw_bytes[: pos_lf + 1]
            else:
                logging.info(f"[TRUNCATE] '{full_path}' has no data rows — skipping.")
                skipped.append(full_path)
                continue

            file_client = fs_client.get_file_client(full_path)
            file_client.create_file()
            file_client.append_data(header_bytes, offset=0, length=len(header_bytes))
            file_client.flush_data(len(header_bytes))

            logging.info(f"[TRUNCATE] '{full_path}' → header only ({len(header_bytes)} bytes).")
            truncated.append(full_path)

        except Exception as e:
            logging.error(f"[TRUNCATE] Failed on '{full_path}': {e}")
            failed.append({"path": full_path, "error": str(e)})

    logging.info(
        f"[TRUNCATE] Done — truncated={len(truncated)} "
        f"skipped={len(skipped)} failed={len(failed)}"
    )
    return {"truncated": truncated, "skipped": skipped, "failed": failed}


# ============================================================================
# PARAMETER VALIDATION (unchanged)
# ============================================================================

def validate_and_extract_params(params: dict) -> dict:
    required = {
        "ADLS account name": params.get("adls_account_name"),
        "ADLS file system": params.get("adls_file_system"),
        "Cassandra contact points": params.get("cassandra_contact_points"),
        "Cassandra username": params.get("cassandra_username"),
        "Cassandra keyspace": params.get("cassandra_keyspace"),
        "Cassandra table": params.get("cassandra_table"),
        "Cassandra Key Vault name": params.get("Cassandra_key_vault_name"),
        "Cassandra Key Vault secret name": params.get("Cassandra_key_vault_secret_name"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise ValueError(f"Missing required parameters: {missing}")
    if params.get("truncate_sink_before_write") is None:
        raise ValueError("Missing required parameter: truncate_sink_before_write")

    raw_cp = params["cassandra_contact_points"]
    contact_points = (
        [cp.strip() for cp in raw_cp.split(",") if cp.strip()]
        if isinstance(raw_cp, str) else list(raw_cp)
    )

    def _int(key, default):
        try:
            v = int(params.get(key, default))
            return v if v > 0 else default
        except (ValueError, TypeError):
            return default

    return {
        "adls_account_name": params["adls_account_name"],
        "adls_file_system": params["adls_file_system"],
        "adls_directory": params.get("adls_directory", "").rstrip("/"),
        "cassandra_contact_points": contact_points,
        "cassandra_port": _int("cassandra_port", 9042),
        "cassandra_preferred_node": params.get("cassandra_preferred_node") or None,
        "cassandra_preferred_port": _int("cassandra_preferred_port", 9042),
        "cassandra_username": params["cassandra_username"],
        "cassandra_keyspace": params["cassandra_keyspace"],
        "cassandra_table": params["cassandra_table"],
        "cassandra_batch_size": _int("cassandra_batch_size", DEFAULT_BATCH_SIZE),
        "insert_chunk_size": _int("insert_chunk_size", DEFAULT_INSERT_CHUNK),
        "concurrent_inserts": _int("concurrent_inserts", DEFAULT_CONCURRENT_INSERTS),
        "key_vault_name": params["Cassandra_key_vault_name"],
        "key_vault_secret_name": params["Cassandra_key_vault_secret_name"],

        "truncate_sink_before_write": _bool_param(params.get("truncate_sink_before_write"), False),
        "delete_adls_after_load": _bool_param(params.get("delete_adls_after_load"), False),
        "P_Delete_ADLS_Records": _bool_param(params.get("P_Delete_ADLS_Records"), False),

        "P_CASSANDRA_SOURCE_KEYSPACE": params.get("P_CASSANDRA_SOURCE_KEYSPACE", "").rstrip("/"),
        "P_CASSANDRA_TARGET_KEYSPACE": params.get("P_CASSANDRA_TARGET_KEYSPACE", "").rstrip("/"),
        "P_CASSANDRA_TABLE": params.get("P_CASSANDRA_TABLE", ""),
    }


# ============================================================================
# MAIN ACTIVITY FUNCTION (unchanged except for the new process_batch)
# ============================================================================

@app.activity_trigger(input_name="params")
def process_adls_to_cassandra_activity(params: dict):
    start_time = datetime.utcnow()
    cluster = None
    session = None

    try:
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

        keyspace = params["cassandra_keyspace"]
        table = params["cassandra_table"]

        cassandra_columns = get_cassandra_columns(session, keyspace, table)
        cassandra_col_types = get_cassandra_column_types(session, keyspace, table)
        partition_keys, clustering_keys = get_cassandra_key_columns(session, keyspace, table)
        logging.info(f"Cassandra columns ({len(cassandra_columns)}): {cassandra_columns}")
        logging.info(f"Auto-discovered partition keys: {partition_keys}  clustering keys: {clustering_keys}")

        if params["truncate_sink_before_write"]:
            logging.info(f"Truncating {keyspace}.{table} …")
            session.execute(f'TRUNCATE "{keyspace}"."{table}"')
            logging.info("TRUNCATE complete.")

        service_client = validate_adls_connection(params["adls_account_name"], params["adls_file_system"])
        file_system = params["adls_file_system"]
        export_dir = (
            f"{params['adls_directory']}/{table}" if params["adls_directory"] else table
        )
        parent_full_path = f"{export_dir}/{table}.csv"

        total_parent_rows = get_csv_row_count(
            service_client, file_system, parent_full_path,
            chunk_size=params["cassandra_batch_size"],
        )
        logging.info(f"Total parent rows: {total_parent_rows}")

        if total_parent_rows == 0:
            return {"status": "success", "message": f"No rows at '{parent_full_path}'.", "rows_inserted": 0}

        all_csv_paths = discover_all_csv_paths(service_client, file_system, export_dir)
        logging.info(f"CSV paths discovered: {len(all_csv_paths)}")
        for p in all_csv_paths:
            logging.info(f"  {p}")

        json_col_scalar_paths = get_json_col_scalar_paths(all_csv_paths, export_dir, table)
        csv_info_by_depth = organize_csv_paths_by_depth(
            all_csv_paths,
            export_dir,
            table,
            cassandra_col_types=cassandra_col_types,
        )

        json_col_names: Set[str] = set(json_col_scalar_paths.keys())
        for csv_info in csv_info_by_depth.get(0, []):
            if csv_info["jcol"] is not None:
                json_col_names.add(csv_info["jcol"])

        jcol_root_arrays: Dict[str, Set[str]] = {}
        for csv_info in csv_info_by_depth.get(0, []):
            if csv_info["jcol"] is not None:
                jcol_root_arrays.setdefault(csv_info["jcol"], set()).add(csv_info["arr_name"])

        top_level_array_cols: Set[str] = set()
        for csv_info in csv_info_by_depth.get(0, []):
            if csv_info["jcol"] is None:
                top_level_array_cols.add(csv_info["arr_name"])

        logging.info(f"JSON column scalar CSVs:         {list(json_col_scalar_paths.keys())}")
        logging.info(f"json_col_names (all JSON cols):  {json_col_names}")
        logging.info(f"jcol_root_arrays:                { {k: list(v) for k, v in jcol_root_arrays.items()} }")
        logging.info(f"top_level_array_cols:            {top_level_array_cols}")
        logging.info(f"csv_info_by_depth:               { {d: [{'arr_name': x['arr_name'], 'jcol': x.get('jcol')} for x in v] for d, v in csv_info_by_depth.items()} }")

        retry_strategy = CassandraRetryStrategy()
        batch_size = params["cassandra_batch_size"]
        concurrent_inserts = params["concurrent_inserts"]
        num_batches = (total_parent_rows + batch_size - 1) // batch_size
        total_inserted = 0
        total_failed = 0
        total_child_rows = 0

        logging.info(f"Processing {num_batches} batches (concurrent_inserts={concurrent_inserts})")

        for batch_num in range(num_batches):
            skip_rows = batch_num * batch_size
            current_batch_size = min(batch_size, total_parent_rows - skip_rows)
            batch_label = f"batch_{batch_num + 1:04d}_of_{num_batches:04d}"
            batch_start_time = datetime.utcnow()
            logging.info(
                f"[{batch_label}] START {batch_start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC | "
                f"rows {skip_rows}–{skip_rows + current_batch_size - 1}"
            )

            with memory_managed_batch(batch_label):

                parent_df = read_csv_from_adls_batched(
                    service_client, file_system, parent_full_path,
                    skip_rows=skip_rows, nrows=current_batch_size,
                )

                batch_parents = prepare_batch_parents(parent_df)
                release_dataframe(parent_df)

                if not batch_parents:
                    logging.warning(f"[{batch_label}] No parents prepared — skipping.")
                    continue

                logging.info(f"[{batch_label}] {len(batch_parents)} parents prepared")

                documents, child_rows = process_batch(
                    batch_parents=batch_parents,
                    service_client=service_client,
                    file_system=file_system,
                    export_dir=export_dir,
                    table=table,
                    json_col_scalar_paths=json_col_scalar_paths,
                    csv_info_by_depth=csv_info_by_depth,
                )
                total_child_rows += child_rows
                del batch_parents
                force_gc()

                logging.info(f"[{batch_label}] {len(documents)} docs reconstructed, {child_rows} child rows")

                inserted = failed = 0
                insert_chunk_size = params["insert_chunk_size"]
                num_docs = len(documents)

                for chunk_start in range(0, num_docs, insert_chunk_size):
                    chunk_docs = documents[chunk_start: chunk_start + insert_chunk_size]
                    insert_rows: List[Dict] = []

                    for doc in chunk_docs:
                        for jcol, root_arr_names in jcol_root_arrays.items():
                            jcol_dict = doc.get(jcol)
                            if jcol_dict is None:
                                jcol_dict = {}
                                doc[jcol] = jcol_dict
                            if not isinstance(jcol_dict, dict):
                                continue
                            for arr_name in root_arr_names:
                                arr_val = doc.pop(arr_name, None)
                                if arr_val is not None:
                                    jcol_dict[arr_name] = arr_val

                        row: Dict[str, Any] = {}
                        for col in cassandra_columns:
                            val = doc.get(col)
                            if val is None:
                                continue
                            if col in json_col_names or col in top_level_array_cols:
                                if isinstance(val, (dict, list)):
                                    row[col] = json.dumps(val, ensure_ascii=False, default=str)
                                elif val is not None:
                                    row[col] = str(val)
                            else:
                                row[col] = val
                        if row:
                            insert_rows.append(row)

                    chunk_inserted, chunk_failed = insert_rows_into_cassandra(
                        session=session, keyspace=keyspace, table=table,
                        rows=insert_rows, cassandra_columns=cassandra_columns,
                        retry_strategy=retry_strategy, batch_size=insert_chunk_size,
                        col_types=cassandra_col_types,
                        concurrency=concurrent_inserts,
                        total_inserted_so_far=total_inserted + inserted,
                    )
                    inserted += chunk_inserted
                    failed += chunk_failed

                    logging.info(
                        f"[{batch_label}] chunk {chunk_start // insert_chunk_size + 1}: "
                        f"inserted={chunk_inserted} failed={chunk_failed} "
                        f"({chunk_start + len(chunk_docs)}/{num_docs} docs)"
                    )

                    del insert_rows, chunk_docs
                    force_gc()

                del documents

            total_inserted += inserted
            total_failed += failed
            batch_end_time = datetime.utcnow()
            batch_duration = (batch_end_time - batch_start_time).total_seconds()
            logging.info(
                f"[{batch_label}] COMPLETE | "
                f"start={batch_start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC  "
                f"end={batch_end_time.strftime('%Y-%m-%d %H:%M:%S')} UTC  "
                f"duration={batch_duration:.2f}s | "
                f"inserted={inserted} failed={failed} child_rows={child_rows} | "
                f"TOTAL ROWS TRANSFERRED SO FAR: {total_inserted:,} / ~{total_parent_rows:,}"
            )

        shutdown_cassandra(cluster, session)
        cluster = session = None

        retry_stats = retry_strategy.get_stats()
        duration = (datetime.utcnow() - start_time).total_seconds()

        adls_delete_results: Dict[str, str] = {}
        if params["delete_adls_after_load"] and total_failed == 0:
            logging.info(
                f"[ADLS DELETE] rows_failed=0 and delete_adls_after_load=true — "
                f"deleting table folder '{params['P_CASSANDRA_TABLE']}' from source and target directories."
            )
            adls_delete_results = delete_adls_table_folder(
                adls_account_name=params["adls_account_name"],
                table=params["P_CASSANDRA_TABLE"],
                source_keyspace=params["P_CASSANDRA_SOURCE_KEYSPACE"],
                target_keyspace=params["P_CASSANDRA_TARGET_KEYSPACE"],
            )
        elif params["delete_adls_after_load"] and total_failed > 0:
            logging.warning(
                f"[ADLS DELETE] Skipped — delete_adls_after_load=true but rows_failed={total_failed}. "
                f"ADLS folders will NOT be deleted until all rows are successfully inserted."
            )
            adls_delete_results = {
                "source": "skipped (rows_failed > 0)",
                "target": "skipped (rows_failed > 0)",
            }

        adls_truncate_results: Dict[str, Any] = {}
        if params["P_Delete_ADLS_Records"]:
            if total_failed == 0:
                logging.info(
                    f"[ADLS TRUNCATE] rows_failed=0 and P_Delete_ADLS_Records=true — "
                    f"truncating {len(all_csv_paths)} CSV file(s) under '{export_dir}' to headers-only."
                )
                adls_truncate_results = truncate_adls_csv_records(
                    service_client=service_client,
                    file_system=file_system,
                    all_csv_paths=all_csv_paths,
                )
            else:
                logging.warning(
                    f"[ADLS TRUNCATE] Skipped — P_Delete_ADLS_Records=true but "
                    f"rows_failed={total_failed}.  CSVs will NOT be truncated."
                )
                adls_truncate_results = {
                    "truncated": [],
                    "skipped": [],
                    "failed": [],
                    "reason": f"skipped (rows_failed={total_failed} > 0)",
                }

        return {
            "status": "success" if total_failed == 0 else "completed_with_errors",
            "rows_inserted": total_inserted,
            "rows_failed": total_failed,
            "message": total_failed,
            "total_child_rows": total_child_rows,
            "json_columns_processed": list(json_col_names),
            "batches_processed": num_batches,
            "batch_size": batch_size,
            "truncated_before_write": params["truncate_sink_before_write"],
            "duration_seconds": round(duration, 2),
            "total_retries": retry_stats.get("total_retries", 0),
            "retries_by_error_type": retry_stats.get("retries_by_error_type", {}),
            "delete_adls_after_load": params["delete_adls_after_load"],
            "adls_delete_results": adls_delete_results,
            "P_Delete_ADLS_Records": params["P_Delete_ADLS_Records"],
            "adls_truncate_results": adls_truncate_results,
        }

    except Exception as e:
        logging.error(f"Activity failed: {str(e)}", exc_info=True)
        return {"status": "error", "message": str(e)}

    finally:
        if cluster is not None or session is not None:
            shutdown_cassandra(cluster, session)
        force_gc()


# ============================================================================
# ORCHESTRATOR AND HTTP TRIGGER (unchanged)
# ============================================================================

@app.orchestration_trigger(context_name="context")
def adls_to_cassandra_orchestrator(context: df.DurableOrchestrationContext):
    params = context.get_input()
    result = yield context.call_activity("process_adls_to_cassandra_activity", params)
    return result


@app.route(route="ADLS_to_Cassandra_V1", methods=["POST"])
@app.durable_client_input(client_name="client")
async def adls_to_cassandra_http_start(req: func.HttpRequest, client) -> func.HttpResponse:
    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse("Invalid JSON body", status_code=400)

    try:
        instance_id = await client.start_new("adls_to_cassandra_orchestrator", None, body)
        return client.create_check_status_response(req, instance_id)
    except Exception as e:
        logging.error(f"HTTP start failed: {str(e)}", exc_info=True)
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)