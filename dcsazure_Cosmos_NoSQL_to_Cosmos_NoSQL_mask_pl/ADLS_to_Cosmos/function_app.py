"""
Azure Durable Function for importing data from Azure Data Lake Storage (ADLS) to Cosmos DB.

This module provides functionality to read hierarchical CSV data from ADLS and reconstruct
nested document structures for upsert into Cosmos DB, with adaptive throttling and 
performance optimization.
"""

import azure.functions as func
import azure.durable_functions as df
import logging
import json
import pandas as pd
import asyncio
import time
from azure.cosmos.aio import CosmosClient
from azure.cosmos import exceptions, PartitionKey, ThroughputProperties
from azure.storage.filedatalake import DataLakeServiceClient
from typing import Dict, List, Any, Tuple, Set
import numpy as np
import ast
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# System fields to exclude from documents
SYSTEM_FIELDS = {"_rid", "_self", "_etag", "_attachments", "_ts"}

# Autoscale throughput settings
AUTOSCALE_RU_SAFETY_MARGIN = 0.95
AUTOSCALE_MIN_THROTTLES_BEFORE_BACKOFF = 5
AUTOSCALE_SCALE_UP_WAIT_MS = 100

# Manual/Shared throughput settings
MANUAL_RU_SAFETY_MARGIN = 0.75  
MANUAL_MIN_THROTTLES_BEFORE_BACKOFF = 2 

# Concurrency limits
MIN_CONCURRENT_OPERATIONS = 5
MAX_CONCURRENT_OPERATIONS_ABSOLUTE = 500

# Retry configuration
RETRY_MAX_ATTEMPTS = 10
RETRY_BASE_DELAY = 1
BACKOFF_MULTIPLIER = 2

# RU estimation
ESTIMATED_RU_PER_UPSERT = 10
ESTIMATED_RU_PER_KB = 5

# Logging interval
PROGRESS_LOG_INTERVAL = 100


def get_secret(key_vault_name: str, secretname: str) -> str:
    """
    Retrieve a secret from Azure Key Vault.
    
    Uses DefaultAzureCredential for authentication and connects to the specified
    Key Vault to retrieve the named secret.
    
    Args:
        key_vault_name: Name of the Key Vault (without .vault.azure.net suffix)
        secretname: Name of the secret to retrieve
        
    Returns:
        The secret value as a string
        
    Raises:
        Exception: If secret retrieval fails due to authentication issues,
                  missing secret, or network problems
    """
    try:
        kv_uri = f"https://{key_vault_name}.vault.azure.net"
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=kv_uri, credential=credential)
        secret = client.get_secret(secretname)
        return secret.value
    except Exception as e:
        logging.error(f"Failed to retrieve secret '{secretname}' from Key Vault '{key_vault_name}': {str(e)}")
        raise


def strip_system_fields(doc: Any) -> Any:
    """
    Recursively remove Cosmos DB system fields from documents.
    
    System fields like _rid, _self, _etag, _attachments, and _ts are internal
    to Cosmos DB and should not be included when upserting documents.
    
    Args:
        doc: Document or value to clean. Can be a dict, list, or primitive value
        
    Returns:
        Cleaned document without system fields. Returns the same type as input
    """
    if isinstance(doc, dict):
        return {k: strip_system_fields(v) for k, v in doc.items() if k not in SYSTEM_FIELDS}
    if isinstance(doc, list):
        return [strip_system_fields(v) for v in doc]
    return doc


def cosmos_safe(obj: Any) -> Any:
    """
    Convert objects to Cosmos DB-compatible types.
    
    Handles numpy types, NaN values, and ensures all dict keys are strings.
    This is necessary because Cosmos DB doesn't accept numpy types and has
    specific requirements for JSON serialization.
    
    Args:
        obj: Object to convert. Can be dict, list, numpy type, or primitive
        
    Returns:
        Cosmos DB-compatible object with all types converted appropriately:
        - numpy integers -> Python int
        - numpy floats -> Python float
        - numpy bool -> Python bool
        - NaN/NaT -> None
        - dict keys -> strings
    """
    if isinstance(obj, dict):
        return {str(k): cosmos_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [cosmos_safe(v) for v in obj]
    if isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    if isinstance(obj, (np.floating, np.float64)):
        return float(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if (isinstance(obj, float) and pd.isna(obj)) or obj is pd.NaT:
        return None
    return obj


def parse_json_string(value: str) -> Any:
    """
    Attempt to parse a string as JSON or Python literal.
    
    Tries to parse strings that look like JSON arrays or objects. Falls back
    to Python's ast.literal_eval for Python-style literals.
    
    Args:
        value: String value to parse
        
    Returns:
        Parsed value (dict, list, etc.) or original string if parsing fails
    """
    v = value.strip()
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
    """
    Convert flat dictionary with dot-separated keys to nested structure.
    
    Takes a flat dictionary where nested paths are represented by dot-separated
    keys (e.g., "address.city") and converts it to a nested dictionary structure.
    
    Args:
        flat: Flat dictionary with keys like "address.city", "address.zip"
        sep: Separator used in keys (default: ".")
        
    Returns:
        Nested dictionary structure
    """
    result = {}
    for key, value in flat.items():
        if isinstance(value, str):
            value = parse_json_string(value)

        parts = key.split(sep)
        cur = result
        for i, part in enumerate(parts):
            if i == len(parts) - 1:
                cur[part] = value
            else:
                if part not in cur or not isinstance(cur[part], dict):
                    cur[part] = {}
                cur = cur[part]
    return result


def get_csv_row_count(service_client: DataLakeServiceClient, file_system: str, 
                      full_path: str, chunk_size: int = 50000) -> int:
    """
    Get total row count from CSV by streaming with proper CSV parsing.
    
    Streams the CSV file in chunks to avoid loading the entire file into memory,
    which is important for large files.
    
    Args:
        service_client: ADLS service client for connecting to storage
        file_system: ADLS file system (container) name
        full_path: Full path to CSV file within the file system
        chunk_size: Number of rows to process per chunk (default: 50000)
        
    Returns:
        Total number of rows in the CSV file (excluding header)
        
    Raises:
        Exception: If reading the CSV fails due to access issues, invalid format,
                  or network problems
    """
    try:
        fs_client = service_client.get_file_system_client(file_system=file_system)
        file_client = fs_client.get_file_client(full_path)
        download = file_client.download_file()
        
        total_rows = 0
        chunk_iterator = pd.read_csv(
            download,
            sep="|",
            chunksize=chunk_size,
            iterator=True,
            quoting=1,
            keep_default_na=False,
            escapechar='\\'
        )
        
        for chunk in chunk_iterator:
            total_rows += len(chunk)
            del chunk
            
        return total_rows
        
    except Exception as e:
        logging.error(f"Error counting rows in CSV '{full_path}': {str(e)}")
        raise


def read_csv_from_adls_batched(service_client: DataLakeServiceClient, file_system: str, 
                                full_path: str, skip_rows: int = 0, nrows: int = None) -> pd.DataFrame:
    """
    Read CSV from ADLS in batches using streaming.
    
    Efficiently reads a portion of a CSV file from ADLS by skipping rows and
    limiting the number of rows read. Uses pipe (|) as delimiter.
    
    Args:
        service_client: ADLS service client for connecting to storage
        file_system: ADLS file system (container) name
        full_path: Full path to CSV file within the file system
        skip_rows: Number of rows to skip after header (default: 0)
        nrows: Maximum number of rows to read (default: None for all rows)
        
    Returns:
        DataFrame containing the requested rows
        
    Raises:
        Exception: If reading the CSV fails due to access issues, invalid format,
                  or network problems
    """
    try:
        fs_client = service_client.get_file_system_client(file_system=file_system)
        file_client = fs_client.get_file_client(full_path)
        download = file_client.download_file()
        
        read_params = {
            "sep": "|",
            "iterator": False,
            "quoting": 1,
            "keep_default_na": False,
            "escapechar": '\\'
        }
        
        if skip_rows > 0:
            read_params["skiprows"] = range(1, skip_rows + 1)
        if nrows:
            read_params["nrows"] = nrows
        
        df = pd.read_csv(download, **read_params)
        return df
        
    except Exception as e:
        logging.error(f"Error reading CSV '{full_path}' (skip_rows={skip_rows}, nrows={nrows}): {str(e)}")
        raise


def process_child_csv_streaming(
    service_client: DataLakeServiceClient, 
    file_system: str, 
    full_path: str, 
    parent_rids: Set[str],
    arr_name: str, 
    all_objects_by_rid: Dict,
    child_objects_by_table: Dict
) -> Tuple[int, Set[str]]:
    """
    Process child CSV in streaming mode without accumulating large DataFrames.
    
    Streams child records from a CSV file, filters them by parent RID, and builds
    nested objects. This approach minimizes memory usage for large child tables.
    
    Args:
        service_client: ADLS service client for connecting to storage
        file_system: ADLS file system (container) name
        full_path: Full path to child CSV file
        parent_rids: Set of parent RIDs to filter by (only children of these parents
                    will be included)
        arr_name: Name of the array/table (e.g., "orders" or "orders.items")
        all_objects_by_rid: Dictionary to store all objects by their RID for lookup
        child_objects_by_table: Dictionary to store child objects grouped by table name
        
    Returns:
        Tuple of (row_count, set_of_child_rids):
        - row_count: Number of child rows processed
        - set_of_child_rids: Set of RIDs for the child records (used for nested children)
        
    Raises:
        Exception: If streaming the CSV fails
    """
    try:
        fs_client = service_client.get_file_system_client(file_system=file_system)
        file_client = fs_client.get_file_client(full_path)
        download = file_client.download_file()
        
        chunk_size = 10000
        child_rids = set()
        row_count = 0
        
        if arr_name not in child_objects_by_table:
            child_objects_by_table[arr_name] = []
        
        chunk_iterator = pd.read_csv(
            download, 
            sep="|", 
            chunksize=chunk_size, 
            iterator=True,
            quoting=1, 
            keep_default_na=False,
            escapechar='\\'
        )
        
        for chunk in chunk_iterator:
            if "_parent_rid" not in chunk.columns:
                del chunk
                continue
            
            chunk["_parent_rid"] = chunk["_parent_rid"].astype(str)
            filtered_chunk = chunk[chunk["_parent_rid"].isin(parent_rids)]
            del chunk
            
            if filtered_chunk.empty:
                del filtered_chunk
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
            
            del filtered_chunk
        
        logging.info(f"Collected {row_count} child records from {arr_name}, found {len(child_rids)} unique child RIDs")
        return row_count, child_rids
        
    except Exception as e:
        logging.error(f"Error streaming CSV '{full_path}': {str(e)}")
        raise


def _build_child_object(row: pd.Series) -> Dict:
    """
    Build child object from DataFrame row, separating array markers from regular fields.
    
    Converts a flat DataFrame row into a nested object structure while preserving
    special fields like _rid, _parent_rid, and array markers (_has_array_*).
    
    Args:
        row: DataFrame row containing flattened child record data
        
    Returns:
        Nested object with the following structure:
        - _rid: Child's resource ID
        - _parent_rid: Parent's resource ID
        - _has_array_*: Array markers indicating nested children
        - other fields: Nested according to dot notation
    """
    obj = {}
    obj["_rid"] = str(row.get("_rid", "")) if pd.notna(row.get("_rid")) else None
    obj["_parent_rid"] = str(row.get("_parent_rid", "")) if pd.notna(row.get("_parent_rid")) else None
    
    for k, v in row.items():
        if k not in ("_rid", "_parent_rid") and pd.notna(v):
            obj[k] = v
    
    has_array_fields = {k: v for k, v in obj.items() if k.startswith("_has_array_")}
    regular_fields = {k: v for k, v in obj.items() 
                     if k not in ("_rid", "_parent_rid") and not k.startswith("_has_array_")}
    
    nested_obj = unflatten_dict(regular_fields)
    nested_obj.update(has_array_fields)
    nested_obj["_rid"] = obj["_rid"]
    nested_obj["_parent_rid"] = obj["_parent_rid"]
    
    return nested_obj


def estimate_document_size_kb(doc: Dict) -> float:
    """
    Estimate document size in kilobytes.
    
    Serializes the document to JSON and calculates its size in KB. This is used
    for estimating RU consumption and optimizing batch sizes.
    
    Args:
        doc: Document to estimate (must be JSON-serializable)
        
    Returns:
        Estimated size in KB (kilobytes)
    """
    try:
        doc_json = json.dumps(doc)
        return len(doc_json.encode("utf-8")) / 1024
    except Exception:
        return 1


async def discover_container_configuration(
    database, 
    container, 
    container_name: str
) -> Dict[str, Any]:
    """
    Discover Cosmos DB container configuration including throughput settings.
    
    Queries the container and database to determine partition key, throughput type
    (autoscale, manual, shared, or serverless), and provisioned RU settings. This
    information is critical for optimizing concurrency and batch sizes.
    
    Args:
        database: Cosmos database client (async)
        container: Cosmos container client (async)
        container_name: Name of the container (used for logging)
        
    Returns:
        Dictionary containing configuration details:
        - partition_key_path: Partition key path (e.g., "/id")
        - provisioned_rus: Current RU/s provisioned
        - min_rus: Minimum RU/s (autoscale only)
        - max_rus: Maximum RU/s (autoscale only)
        - throughput_type: One of "autoscale", "manual", "autoscale_shared",
                          "manual_shared", or "serverless"
        - is_autoscale: Boolean indicating autoscale mode
        - is_serverless: Boolean indicating serverless mode
        - uses_shared_throughput: Boolean indicating database-level throughput
    """
    config = {
        "partition_key_path": None,
        "provisioned_rus": None,
        "min_rus": None,
        "max_rus": None,
        "throughput_type": "unknown",
        "is_autoscale": False,
        "is_serverless": False,
        "uses_shared_throughput": False
    }

    config["partition_key_path"] = await _get_partition_key_path(container)
    throughput_config = await _get_throughput_configuration(database, container)
    config.update(throughput_config)
    
    return config


async def _get_partition_key_path(container) -> str:
    """
    Get partition key path from container properties.
    
    Reads the container definition to extract the partition key path. Falls back
    to "/id" if the partition key cannot be determined.
    
    Args:
        container: Cosmos container client (async)
        
    Returns:
        Partition key path (e.g., "/id", "/userId", etc.)
    """
    try:
        container_props = await container.read()
        partition_key_def = container_props.get("partitionKey", {})
        paths = partition_key_def.get("paths", [])
        return paths[0] if paths else "/id"
    except Exception as e:
        logging.warning(f"Failed to read partition key, defaulting to '/id': {str(e)}")
        return "/id"


async def _get_throughput_configuration(database, container) -> Dict[str, Any]:
    """
    Get throughput configuration from container or database level.
    
    Attempts to read throughput settings first at the container level, then at
    the database level (for shared throughput). If neither is found, assumes
    serverless configuration.
    
    Args:
        database: Cosmos database client (async)
        container: Cosmos container client (async)
        
    Returns:
        Dictionary containing throughput configuration fields:
        - is_autoscale: Boolean indicating autoscale mode
        - is_serverless: Boolean indicating serverless mode
        - uses_shared_throughput: Boolean indicating database-level throughput
        - throughput_type: String describing the throughput type
        - provisioned_rus: Current RU/s provisioned
        - min_rus: Minimum RU/s (autoscale only)
        - max_rus: Maximum RU/s (autoscale only)
    """
    config = {}
    
    # Try container-level throughput first
    try:
        container_offer = await container.get_throughput()
        if container_offer:
            return _parse_offer_properties(container_offer.properties, is_shared=False)
    except Exception as e:
        logging.info(f"No container-level throughput found: {str(e)}")
    
    # Try database-level (shared) throughput
    try:
        db_offer = await database.get_throughput()
        if db_offer:
            return _parse_offer_properties(db_offer.properties, is_shared=True)
    except Exception as e:
        logging.info(f"No database-level throughput found: {str(e)}")
    
    # Assume serverless if no throughput found
    logging.info("Assuming serverless configuration")
    return {
        "is_serverless": True,
        "throughput_type": "serverless",
        "provisioned_rus": 1000,
        "min_rus": 100,
        "max_rus": 5000
    }


def _parse_offer_properties(offer_props: Dict, is_shared: bool) -> Dict[str, Any]:
    """
    Parse throughput offer properties from Cosmos DB.
    
    Extracts and interprets the throughput offer to determine whether it's
    autoscale or manual, and what the RU limits are.
    
    Args:
        offer_props: Offer properties dictionary from Cosmos DB
        is_shared: Whether this is database-level (shared) throughput
        
    Returns:
        Dictionary containing parsed throughput configuration:
        - is_autoscale: Boolean indicating autoscale mode
        - throughput_type: String describing the throughput type
        - provisioned_rus: Current RU/s provisioned
        - min_rus: Minimum RU/s
        - max_rus: Maximum RU/s
        - uses_shared_throughput: Boolean passed from is_shared parameter
    """
    content = offer_props.get("content", {})
    autoscale_settings = content.get("offerAutopilotSettings")
    
    config = {"uses_shared_throughput": is_shared}
    
    if autoscale_settings:
        max_ru = autoscale_settings.get("maxThroughput", 4000)
        config.update({
            "is_autoscale": True,
            "throughput_type": "autoscale_shared" if is_shared else "autoscale",
            "max_rus": max_ru,
            "min_rus": max_ru // 10,
            "provisioned_rus": max_ru
        })
    else:
        manual_ru = content.get("offerThroughput", 400)
        config.update({
            "throughput_type": "manual_shared" if is_shared else "manual",
            "provisioned_rus": manual_ru,
            "min_rus": 400,
            "max_rus": manual_ru
        })
    
    return config


def calculate_optimal_concurrency(
    config: Dict[str, Any], 
    total_docs: int, 
    avg_doc_size_kb: float
) -> Dict[str, int]:
    """
    Calculate optimal concurrency settings based on throughput configuration.
    
    Uses the container's throughput settings, document count, and average document
    size to calculate optimal batch sizes and concurrency limits. Applies different
    strategies for autoscale, manual, and serverless configurations.
    
    Args:
        config: Container configuration from discover_container_configuration()
        total_docs: Total number of documents to process
        avg_doc_size_kb: Average document size in KB
        
    Returns:
        Dictionary with concurrency settings:
        - max_concurrent_operations: Maximum number of simultaneous operations
        - initial_batch_size: Starting batch size for processing
        - min_batch_size: Minimum batch size (for throttle backoff)
        - max_batch_size: Maximum batch size (for scaling up)
        - estimated_ru_per_doc: Estimated RU cost per document
        - available_rus_per_second: Available RUs per second after safety margin
        - is_autoscale: Whether autoscale is enabled
    """
    provisioned_rus = config["provisioned_rus"]
    is_autoscale = config["is_autoscale"]
    is_serverless = config["is_serverless"]

    estimated_ru_per_doc = ESTIMATED_RU_PER_UPSERT + (avg_doc_size_kb * ESTIMATED_RU_PER_KB)
    
    safety_margin = AUTOSCALE_RU_SAFETY_MARGIN if is_autoscale else MANUAL_RU_SAFETY_MARGIN
    ru_multiplier = 1.5 if is_autoscale else 1.0
    available_rus = provisioned_rus * safety_margin

    if is_serverless:
        max_concurrent = min(int(available_rus / estimated_ru_per_doc * 2), MAX_CONCURRENT_OPERATIONS_ABSOLUTE)
        initial_batch = min(100, total_docs // 10)
    elif is_autoscale:
        max_concurrent = min(int(available_rus / estimated_ru_per_doc * ru_multiplier), MAX_CONCURRENT_OPERATIONS_ABSOLUTE)
        initial_batch = min(100, total_docs // 10)
    else:
        max_concurrent = min(int(available_rus / estimated_ru_per_doc * ru_multiplier), MAX_CONCURRENT_OPERATIONS_ABSOLUTE)
        initial_batch = min(30, total_docs // 30)

    max_concurrent = max(max_concurrent, MIN_CONCURRENT_OPERATIONS)
    initial_batch = max(initial_batch, MIN_CONCURRENT_OPERATIONS)

    return {
        "max_concurrent_operations": max_concurrent,
        "initial_batch_size": initial_batch,
        "min_batch_size": max(5, initial_batch // 10),
        "max_batch_size": min(300, initial_batch * 5) if is_autoscale else min(200, initial_batch * 4),
        "estimated_ru_per_doc": estimated_ru_per_doc,
        "available_rus_per_second": available_rus,
        "is_autoscale": is_autoscale
    }


class AdaptiveThrottleManager:
    """
    Manages adaptive throttling and batch sizing based on Cosmos DB throughput type.
    
    This class dynamically adjusts batch sizes and tracks performance metrics to
    optimize throughput while minimizing throttling. It implements different strategies
    for autoscale vs manual throughput provisioning.
    
    For autoscale configurations:
    - More aggressive scaling with higher tolerance for throttles
    - Implements warmup period to allow autoscale to ramp up
    - Tracks when maximum throughput is reached
    
    For manual configurations:
    - Conservative scaling to avoid throttles
    - Faster backoff on throttling
    - Lower tolerance for consecutive throttles
    
    Attributes:
        is_autoscale: Whether autoscale mode is enabled
        current_batch_size: Current dynamic batch size
        min_batch_size: Minimum allowed batch size
        max_batch_size: Maximum allowed batch size
        max_concurrent: Maximum concurrent operations
        consecutive_successes: Count of consecutive successful operations
        consecutive_throttles: Count of consecutive throttled operations
        total_throttles: Total number of throttles encountered
        total_rus_consumed: Total RUs consumed across all operations
        total_operations: Total number of operations performed
        avg_ru_per_operation: Running average of RU per operation
    """
    
    def __init__(self, concurrency_config: Dict[str, int]):
        """
        Initialize throttle manager with concurrency configuration.
        
        Sets up initial batch sizes, thresholds, and tracking variables based on
        the provided configuration.
        
        Args:
            concurrency_config: Configuration dictionary containing:
                - initial_batch_size: Starting batch size
                - min_batch_size: Minimum batch size
                - max_batch_size: Maximum batch size
                - max_concurrent_operations: Maximum concurrent operations
                - is_autoscale: Whether autoscale is enabled
                - estimated_ru_per_doc: Estimated RU per document
        """
        self.is_autoscale = concurrency_config.get("is_autoscale", False)
        self.current_batch_size = concurrency_config["initial_batch_size"]
        self.min_batch_size = concurrency_config["min_batch_size"]
        self.max_batch_size = concurrency_config["max_batch_size"]
        self.max_concurrent = concurrency_config["max_concurrent_operations"]
        self.consecutive_successes = 0
        self.consecutive_throttles = 0
        self.total_throttles = 0
        self.lock = asyncio.Lock()
        self.total_rus_consumed = 0
        self.total_operations = 0
        self.avg_ru_per_operation = concurrency_config["estimated_ru_per_doc"]
        
        self.autoscale_warmup_period = True
        self.operations_since_last_throttle = 0
        self.autoscale_max_reached = False
        self.high_throttle_count_at_max = 0
        
        self.throttle_tolerance = (
            AUTOSCALE_MIN_THROTTLES_BEFORE_BACKOFF if self.is_autoscale 
            else MANUAL_MIN_THROTTLES_BEFORE_BACKOFF
        )

    async def report_success(self, ru_charge: float = 0):
        """
        Report successful operation and adjust batch size if needed.
        
        Records a successful operation, updates RU consumption statistics, and
        potentially scales up the batch size based on consecutive successes.
        
        Args:
            ru_charge: RU charge for the operation (default: 0)
        """
        async with self.lock:
            self.consecutive_successes += 1
            self.consecutive_throttles = 0
            self.operations_since_last_throttle += 1
    
            if ru_charge > 0:
                self.total_rus_consumed += ru_charge
                self.total_operations += 1
                self.avg_ru_per_operation = self.total_rus_consumed / self.total_operations
    
            self._scale_up_if_needed()

    def _scale_up_if_needed(self):
        """
        Scale up batch size based on consecutive successes.
        
        Implements different scaling strategies for autoscale and manual throughput:
        - Autoscale: More aggressive scaling during warmup, conservative after max reached
        - Manual: Moderate scaling with lower thresholds
        
        This is an internal method called by report_success().
        """
        if self.is_autoscale:
            if self.autoscale_warmup_period:
                if self.consecutive_successes >= 20 and self.current_batch_size < self.max_batch_size:
                    self.current_batch_size = min(int(self.current_batch_size * 1.5), self.max_batch_size)
                    self.consecutive_successes = 0
            else:
                if self.consecutive_successes >= 30 and self.current_batch_size < self.max_batch_size:
                    self.current_batch_size = min(int(self.current_batch_size * 1.1), self.max_batch_size)
                    self.consecutive_successes = 0
            
            if self.operations_since_last_throttle > 100:
                self.autoscale_warmup_period = False
                
            if self.operations_since_last_throttle > 200:
                self.autoscale_max_reached = True
        else:
            if self.consecutive_successes >= 10 and self.current_batch_size < self.max_batch_size:
                self.current_batch_size = min(int(self.current_batch_size * 1.2), self.max_batch_size)
                self.consecutive_successes = 0

    async def report_throttle(self):
        """
        Report throttled operation and adjust batch size if needed.
        
        Records a throttled request (HTTP 429) and potentially scales down the
        batch size to reduce load on the Cosmos DB account.
        """
        async with self.lock:
            self.consecutive_throttles += 1
            self.total_throttles += 1
            self.consecutive_successes = 0
            self.operations_since_last_throttle = 0
            
            self._scale_down_if_needed()

    def _scale_down_if_needed(self):
        """
        Scale down batch size based on consecutive throttles.
        
        Implements different backoff strategies for autoscale and manual throughput:
        - Autoscale: Tolerates more throttles, moderate backoff
        - Manual: Aggressive backoff to quickly reduce load
        
        This is an internal method called by report_throttle().
        """
        if self.is_autoscale:
            if self.autoscale_max_reached:
                self.high_throttle_count_at_max += 1
            
            if self.consecutive_throttles >= self.throttle_tolerance:
                if self.autoscale_max_reached and self.high_throttle_count_at_max > 3:
                    self.current_batch_size = max(int(self.current_batch_size * 0.5), self.min_batch_size)
                elif self.autoscale_warmup_period:
                    self.current_batch_size = max(int(self.current_batch_size * 0.8), self.min_batch_size)
                else:
                    self.current_batch_size = max(int(self.current_batch_size * 0.6), self.min_batch_size)
                self.consecutive_throttles = 0
        else:
            if self.consecutive_throttles >= self.throttle_tolerance:
                self.current_batch_size = max(int(self.current_batch_size * 0.5), self.min_batch_size)
                self.consecutive_throttles = 0

    async def get_batch_size(self) -> int:
        """
        Get current batch size.
        
        Returns:
            Current dynamic batch size
        """
        async with self.lock:
            return self.current_batch_size

    async def get_stats(self) -> Dict[str, Any]:
        """
        Get current throttle manager statistics.
        
        Returns:
            Dictionary containing performance metrics:
            - total_rus_consumed: Total RUs consumed
            - total_operations: Total operations performed
            - avg_ru_per_operation: Average RU per operation
            - current_batch_size: Current batch size
            - total_throttles: Total number of throttles
            - is_autoscale: Whether autoscale is enabled
        """
        async with self.lock:
            return {
                "total_rus_consumed": self.total_rus_consumed,
                "total_operations": self.total_operations,
                "avg_ru_per_operation": self.avg_ru_per_operation,
                "current_batch_size": self.current_batch_size,
                "total_throttles": self.total_throttles,
                "is_autoscale": self.is_autoscale
            }


async def upsert_item_with_retry(
    container, 
    doc: Dict, 
    partition_key: str,
    throttle_manager: AdaptiveThrottleManager,
    semaphore: asyncio.Semaphore,
    is_autoscale: bool
) -> Tuple[bool, str, Dict, float]:
    """
    Upsert a single document with exponential backoff retry logic.
    
    Attempts to upsert a document to Cosmos DB with automatic retry on throttling
    (HTTP 429), timeouts, and transient errors. Uses exponential backoff with jitter
    and respects Cosmos DB's retry-after headers.
    
    Args:
        container: Cosmos container client (async)
        doc: Document to upsert (must be JSON-serializable)
        partition_key: Partition key value for this document
        throttle_manager: Throttle manager for reporting successes/throttles
        semaphore: Asyncio semaphore for concurrency control
        is_autoscale: Whether autoscale is enabled (affects retry timing)
        
    Returns:
        Tuple of (success, error_message, document, ru_charge):
        - success: Boolean indicating if upsert succeeded
        - error_message: Error description if failed, None if succeeded
        - document: The original document
        - ru_charge: RU cost of the operation
    """
    async with semaphore:
        for attempt in range(RETRY_MAX_ATTEMPTS):
            try:
                response = await container.upsert_item(body=doc)
                ru_charge = _extract_ru_charge(response)
                await throttle_manager.report_success(ru_charge)
                return True, None, doc, ru_charge

            except exceptions.CosmosHttpResponseError as e:
                if e.status_code == 429:
                    await throttle_manager.report_throttle()
                    wait_time = _calculate_throttle_wait_time(e, attempt, is_autoscale)
                    
                    if attempt < RETRY_MAX_ATTEMPTS - 1:
                        await asyncio.sleep(wait_time)
                        continue

                elif e.status_code in [408, 503, 500]:
                    wait_time = RETRY_BASE_DELAY * (BACKOFF_MULTIPLIER ** attempt)
                    if attempt < RETRY_MAX_ATTEMPTS - 1:
                        await asyncio.sleep(wait_time)
                        continue

                error_msg = f"HTTP {e.status_code}: {str(e)[:100]}"
                logging.error(f"Cosmos upsert failed: {error_msg}")
                return False, error_msg, doc, 0

            except Exception as e:
                error_msg = f"Unexpected error: {str(e)[:100]}"
                logging.error(f"Unexpected error during upsert: {str(e)}")
                return False, error_msg, doc, 0

        error_msg = f"Max retries ({RETRY_MAX_ATTEMPTS}) exceeded"
        logging.error(error_msg)
        return False, error_msg, doc, 0


def _extract_ru_charge(response) -> float:
    """
    Extract RU charge from Cosmos DB response.
    
    Reads the x-ms-request-charge header from a Cosmos DB response to determine
    how many Request Units (RUs) were consumed.
    
    Args:
        response: Response object from Cosmos DB operation
        
    Returns:
        RU charge as a float, or 0 if not found
    """
    try:
        headers = response.get_response_headers()
        return float(headers.get("x-ms-request-charge", 0))
    except Exception:
        return 0


def _calculate_throttle_wait_time(error: exceptions.CosmosHttpResponseError, 
                                  attempt: int, 
                                  is_autoscale: bool) -> float:
    """
    Calculate wait time for throttled requests.
    
    Determines how long to wait before retrying a throttled request, respecting
    Cosmos DB's retry-after-ms header and applying different strategies for
    autoscale vs manual throughput.
    
    Args:
        error: CosmosHttpResponseError with status code 429
        attempt: Current retry attempt number (0-indexed)
        is_autoscale: Whether autoscale is enabled
        
    Returns:
        Wait time in seconds (including jitter)
    """
    retry_after_ms = error.headers.get("x-ms-retry-after-ms")
    
    if is_autoscale:
        if retry_after_ms:
            wait_time = float(retry_after_ms) / 1000 * 0.5
        else:
            wait_time = max(AUTOSCALE_SCALE_UP_WAIT_MS / 1000, 
                          RETRY_BASE_DELAY * (1.5 ** attempt))
        jitter = np.random.uniform(0, 0.05 * wait_time)
    else:
        if retry_after_ms:
            wait_time = float(retry_after_ms) / 1000
        else:
            wait_time = RETRY_BASE_DELAY * (BACKOFF_MULTIPLIER ** attempt)
        jitter = np.random.uniform(0, 0.1 * wait_time)
    
    return wait_time + jitter


async def batch_upsert_documents(
    container, 
    documents: List[Dict],
    partition_key_path: str,
    throttle_manager: AdaptiveThrottleManager,
    max_concurrent: int,
    is_autoscale: bool
) -> Dict:
    """
    Upsert multiple documents with concurrency control.
    
    Processes a batch of documents in parallel with controlled concurrency,
    tracking successes, failures, RU consumption, and performance metrics.
    
    Args:
        container: Cosmos container client (async)
        documents: List of documents to upsert
        partition_key_path: Partition key path (e.g., "/id")
        throttle_manager: Throttle manager instance
        max_concurrent: Maximum number of simultaneous operations
        is_autoscale: Whether autoscale is enabled
        
    Returns:
        Dictionary with upsert results and statistics:
        - total: Total number of documents processed
        - successful: Number of successful upserts
        - failed: Number of failed upserts
        - failed_docs: List of documents that failed
        - elapsed_seconds: Time taken for the batch
        - document_rate_per_second: Throughput in documents/second
        - total_rus_consumed: Total RUs consumed
        - ru_rate_per_second: RU consumption rate
        - avg_ru_per_document: Average RU per document
        - total_throttles: Number of throttles encountered
    """
    total_docs = len(documents)
    semaphore = asyncio.Semaphore(max_concurrent)
    
    tasks = [
        upsert_item_with_retry(
            container, 
            doc, 
            _extract_partition_key(doc, partition_key_path),
            throttle_manager, 
            semaphore, 
            is_autoscale
        )
        for doc in documents
    ]

    results = []
    total_rus = 0
    start_time = time.time()

    for i in range(0, len(tasks), PROGRESS_LOG_INTERVAL):
        chunk = tasks[i:i + PROGRESS_LOG_INTERVAL]
        chunk_results = await asyncio.gather(*chunk)
        results.extend(chunk_results)
        total_rus += sum(ru for _, _, _, ru in chunk_results)

    successful = sum(1 for success, _, _, _ in results if success)
    failed = total_docs - successful
    failed_docs = [doc for success, _, doc, _ in results if not success]

    elapsed_total = time.time() - start_time
    stats = await throttle_manager.get_stats()

    return {
        "total": total_docs,
        "successful": successful,
        "failed": failed,
        "failed_docs": failed_docs,
        "elapsed_seconds": elapsed_total,
        "document_rate_per_second": total_docs / elapsed_total if elapsed_total > 0 else 0,
        "total_rus_consumed": total_rus,
        "ru_rate_per_second": total_rus / elapsed_total if elapsed_total > 0 else 0,
        "avg_ru_per_document": total_rus / total_docs if total_docs > 0 else 0,
        "total_throttles": stats["total_throttles"]
    }


def _extract_partition_key(doc: Dict, partition_key_path: str) -> Any:
    """
    Extract partition key value from document.
    
    Navigates the document structure following the partition key path to extract
    the partition key value. Falls back to doc["id"] if navigation fails.
    
    Args:
        doc: Document dictionary
        partition_key_path: Partition key path (e.g., "/userId" or "/user/id")
        
    Returns:
        Partition key value
    """
    pk_path_parts = partition_key_path.strip("/").split("/")
    pk_value = doc

    for part in pk_path_parts:
        pk_value = pk_value.get(part, doc.get("id"))
        if not isinstance(pk_value, dict):
            break

    return pk_value


async def process_batch(
    batch_parents: List[Dict],
    service_client: DataLakeServiceClient,
    adls_file_system: str, 
    csv_paths: List[str], 
    export_dir: str,
    cosmos_container: str, 
    container, 
    config: Dict, 
    throttle_manager: AdaptiveThrottleManager,
    concurrency_config: Dict
) -> Tuple[Dict, int]:
    """
    Process a batch of parent documents with their nested children.
    
    This is the core processing function that:
    1. Builds a mapping of parent RIDs
    2. Processes child CSVs level by level (depth-first)
    3. Reconstructs the nested document structure
    4. Upserts the complete documents to Cosmos DB
    
    Args:
        batch_parents: List of parent documents to process
        service_client: ADLS service client
        adls_file_system: ADLS file system (container) name
        csv_paths: List of all CSV file paths for child tables
        export_dir: Base export directory path
        cosmos_container: Cosmos container name
        container: Cosmos container client (async)
        config: Container configuration from discover_container_configuration()
        throttle_manager: Throttle manager instance
        concurrency_config: Concurrency configuration
        
    Returns:
        Tuple of (upsert_result, total_child_rows_processed):
        - upsert_result: Dictionary with upsert results (from batch_upsert_documents)
        - total_child_rows_processed: Total number of child rows processed
    """
    rid_to_parent = _build_rid_to_parent_mapping(batch_parents)
    all_objects_by_rid = dict(rid_to_parent)
    parent_rids = set(rid_to_parent.keys())
    
    rids_by_depth = {0: parent_rids}
    csv_info_by_depth = _organize_csv_paths_by_depth(csv_paths, export_dir, cosmos_container)
    
    child_objects_by_table = {}
    arrays_with_markers = {}
    total_child_rows_in_batch = 0
    max_depth = max(csv_info_by_depth.keys()) if csv_info_by_depth else -1
    
    # Process child CSVs level by level
    for current_depth in range(max_depth + 1):
        if current_depth not in csv_info_by_depth:
            continue
        
        if current_depth + 1 not in rids_by_depth:
            rids_by_depth[current_depth + 1] = set()
        
        for csv_info in csv_info_by_depth[current_depth]:
            filter_rids = _determine_filter_rids(
                csv_info["arr_name"], 
                current_depth, 
                parent_rids, 
                rids_by_depth, 
                all_objects_by_rid
            )
            
            if not filter_rids:
                continue
            
            try:
                child_count, child_rids = process_child_csv_streaming(
                    service_client, adls_file_system, csv_info["full_path"], 
                    filter_rids, csv_info["arr_name"], all_objects_by_rid, child_objects_by_table
                )
                
                total_child_rows_in_batch += child_count
                rids_by_depth[current_depth + 1].update(child_rids)
                
            except Exception as e:
                logging.error(f"Error processing {csv_info['full_path']}: {str(e)}")
                continue
    
    _record_array_markers(all_objects_by_rid, arrays_with_markers)
    _initialize_arrays_from_markers(all_objects_by_rid)
    _assign_children_to_parents(child_objects_by_table, all_objects_by_rid, 
                                rid_to_parent, arrays_with_markers)
    _clean_documents(batch_parents)
    
    documents = [cosmos_safe(strip_system_fields(doc)) for doc in batch_parents]
    
    result = await batch_upsert_documents(
        container,
        documents,
        config["partition_key_path"],
        throttle_manager,
        concurrency_config["max_concurrent_operations"],
        config["is_autoscale"]
    )
    
    return result, total_child_rows_in_batch


def _build_rid_to_parent_mapping(batch_parents: List[Dict]) -> Dict[str, Dict]:
    """
    Build mapping of RID to parent object.
    
    Creates a lookup dictionary for quick parent object access by RID.
    
    Args:
        batch_parents: List of parent documents
        
    Returns:
        Dictionary mapping RID strings to parent objects
    """
    rid_to_parent = {}
    for parent in batch_parents:
        rid = parent.get("_rid")
        if rid:
            rid_to_parent[str(rid)] = parent
    return rid_to_parent


def _organize_csv_paths_by_depth(csv_paths: List[str], export_dir: str, 
                                 cosmos_container: str) -> Dict[int, List[Dict]]:
    """
    Organize CSV paths by their depth in the hierarchy.
    
    Parses CSV file paths to determine nesting depth based on directory structure
    and groups them for level-by-level processing.
    
    Args:
        csv_paths: List of full CSV file paths
        export_dir: Base export directory path
        cosmos_container: Cosmos container name (parent table)
        
    Returns:
        Dictionary mapping depth (int) to list of CSV info dictionaries:
        - full_path: Full path to the CSV file
        - arr_name: Array name (e.g., "orders" or "orders.items")
        - depth: Nesting depth (0 = direct children, 1 = grandchildren, etc.)
    """
    csv_info_by_depth = {}
    
    for full_path in sorted(csv_paths):
        if full_path.endswith(f"/{cosmos_container}.csv"):
            continue

        parts = [s for s in full_path.split("/") if s]
        rel = parts[len(export_dir.split("/")):] if full_path.startswith(export_dir) else parts

        if not rel:
            continue

        arr_dirs = rel[:-1]
        arr_name = ".".join(arr_dirs) if arr_dirs else rel[-1].rsplit(".", 1)[0]
        depth = arr_name.count(".")
        
        if depth not in csv_info_by_depth:
            csv_info_by_depth[depth] = []
        
        csv_info_by_depth[depth].append({
            "full_path": full_path,
            "arr_name": arr_name,
            "depth": depth
        })
    
    return csv_info_by_depth


def _determine_filter_rids(arr_name: str, current_depth: int, parent_rids: Set[str],
                           rids_by_depth: Dict[int, Set[str]], 
                           all_objects_by_rid: Dict) -> Set[str]:
    """
    Determine which RIDs to filter by for a given array.
    
    Uses array markers (_has_array_*) to intelligently determine which parent
    objects should have this particular child array, avoiding unnecessary processing.
    
    Args:
        arr_name: Name of the array (e.g., "orders.items")
        current_depth: Current processing depth
        parent_rids: Set of parent document RIDs
        rids_by_depth: Dictionary mapping depth to sets of RIDs
        all_objects_by_rid: Dictionary of all objects indexed by RID
        
    Returns:
        Set of RIDs that should be used to filter this array's CSV
    """
    arr_parts = arr_name.split(".")
    possible_markers = [".".join(arr_parts[i:]) for i in range(len(arr_parts))]
    
    for marker_suffix in possible_markers:
        marker_key = f"_has_array_{marker_suffix}"
        rids_with_marker = {rid for rid, obj in all_objects_by_rid.items() if marker_key in obj}
        
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


def _record_array_markers(all_objects_by_rid: Dict, arrays_with_markers: Dict):
    """
    Record which objects have which array markers.
    
    Scans all objects for _has_array_* fields and builds a reverse index mapping
    array names to the RIDs of objects that contain them.
    
    Args:
        all_objects_by_rid: Dictionary of all objects indexed by RID
        arrays_with_markers: Dictionary to populate with array markers
                            (maps array_name -> set of RIDs)
    """
    for rid, obj in all_objects_by_rid.items():
        array_markers = [k for k in obj.keys() if k.startswith("_has_array_")]
        for marker in array_markers:
            array_name = marker.replace("_has_array_", "")
            if array_name not in arrays_with_markers:
                arrays_with_markers[array_name] = set()
            arrays_with_markers[array_name].add(rid)


def _initialize_arrays_from_markers(all_objects_by_rid: Dict):
    """
    Initialize empty arrays based on markers in objects.
    
    For each _has_array_* marker found, creates the corresponding empty array
    in the nested structure and removes the marker field.
    
    Args:
        all_objects_by_rid: Dictionary of all objects indexed by RID (modified in place)
        
    Raises:
        Exception: If array initialization fails
    """
    try:
        for rid, obj in all_objects_by_rid.items():
            array_markers = [k for k in list(obj.keys()) if k.startswith("_has_array_")]
            
            for marker in array_markers:
                array_name = marker.replace("_has_array_", "")
                path_parts = array_name.split(".")
                
                target = obj
                navigation_success = True
                
                for i, part in enumerate(path_parts[:-1]):
                    if part not in target:
                        target[part] = {}
                    elif not isinstance(target[part], dict):
                        navigation_success = False
                        break
                    target = target[part]
                
                if navigation_success:
                    final_key = path_parts[-1]
                    if final_key not in target:
                        target[final_key] = []
                
                if marker in obj:
                    del obj[marker]
    except Exception as e:
      logging.error(f"Error initializing arrays from markers: {str(e)}")
      raise


def _assign_children_to_parents(child_objects_by_table: Dict, all_objects_by_rid: Dict,
                                rid_to_parent: Dict, arrays_with_markers: Dict):
    """
    Assign child objects to their parent arrays.
    
    Processes child objects table by table, grouping them by parent RID and
    adding them to the appropriate array in the parent object. Handles nested
    arrays and marker-based placement.
    
    Args:
        child_objects_by_table: Dictionary mapping table names to lists of child objects
        all_objects_by_rid: Dictionary of all objects indexed by RID
        rid_to_parent: Dictionary mapping parent RIDs to parent objects
        arrays_with_markers: Dictionary mapping array names to sets of RIDs
    """
    sorted_tables = sorted(child_objects_by_table.items(), key=lambda x: x[0].count("."))
    
    for arr_name, objects in sorted_tables:
        if not objects:
            continue
        
        grouped = {}
        for obj in objects:
            parent_rid = str(obj.get("_parent_rid", ""))
            if not parent_rid or parent_rid == "None":
                continue
            
            if parent_rid not in grouped:
                grouped[parent_rid] = []
            grouped[parent_rid].append(obj)
        
        for parent_rid, children in grouped.items():
            parent_obj = all_objects_by_rid.get(parent_rid)
            if parent_obj is None:
                continue
            
            clean_children = _clean_child_objects(children, all_objects_by_rid)
            _add_children_to_array(parent_obj, arr_name, clean_children, 
                                  parent_rid, arrays_with_markers, rid_to_parent)


def _clean_child_objects(children: List[Dict], all_objects_by_rid: Dict) -> List[Dict]:
    """
    Clean child objects by removing system fields.
    
    Removes internal fields (_rid, _parent_rid, _has_array_*) from child objects
    before adding them to parent arrays.
    
    Args:
        children: List of child objects to clean
        all_objects_by_rid: Dictionary to update with cleaned objects (by RID)
        
    Returns:
        List of cleaned child objects
    """
    clean_children = []
    for child in children:
        child_rid = str(child.get("_rid", ""))
        clean = {k: v for k, v in child.items() 
                if k not in ("_rid", "_parent_rid") and not k.startswith("_has_array_")}
        clean_children.append(clean)
        
        if child_rid and child_rid in all_objects_by_rid:
            all_objects_by_rid[child_rid] = clean
    
    return clean_children


def _add_children_to_array(parent_obj: Dict, arr_name: str, clean_children: List[Dict],
                           parent_rid: str, arrays_with_markers: Dict, rid_to_parent: Dict):
    """
    Add children to the appropriate array in parent object.
    
    Determines the correct nested location for the array based on markers and
    adds the cleaned child objects.
    
    Args:
        parent_obj: Parent object to modify
        arr_name: Name of the array (e.g., "orders" or "orders.items")
        clean_children: List of cleaned child objects to add
        parent_rid: RID of the parent object
        arrays_with_markers: Dictionary mapping array names to RIDs with markers
        rid_to_parent: Dictionary mapping RIDs to parent objects
    """
    has_marker_in_parent = False
    actual_marker_name = None
    
    if parent_rid in arrays_with_markers.get(arr_name, set()):
        has_marker_in_parent = True
        actual_marker_name = arr_name
    else:
        parent_markers = [marker for marker, rids in arrays_with_markers.items() 
                         if parent_rid in rids]
        
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


def _clean_documents(documents: List[Dict]):
    """
    Recursively clean all documents.
    
    Removes all system fields (_rid, _parent_rid, _has_array_*) from documents
    and their nested structures.
    
    Args:
        documents: List of documents to clean (modified in place)
    """
    def clean_recursive(obj):
        if isinstance(obj, dict):
            return {k: clean_recursive(v) for k, v in obj.items() 
                   if k not in ("_rid", "_parent_rid") and not k.startswith("_has_array_")}
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


async def process_adls_to_cosmos_async(body: Dict) -> Dict:
    """
    Main processing function to transfer data from ADLS to Cosmos DB.
    
    Orchestrates the entire data transfer process:
    1. Validates parameters and retrieves secrets
    2. Connects to ADLS and Cosmos DB
    3. Discovers container configuration
    4. Processes data in batches with adaptive throttling
    5. Returns comprehensive statistics
    
    Args:
        body: Request body containing configuration parameters:
            - cosmos_url: Cosmos DB account URL
            - cosmos_db: Database name
            - cosmos_container: Container name
            - key_vault_name: Key Vault name for secrets
            - cosmos_secret_name: Secret name for Cosmos key
            - adls_account_name: Storage account name
            - adls_file_system: ADLS file system (container) name
            - adls_directory: Base directory path in ADLS
            - truncate_sink_before_write: Whether to truncate container
            - batch_size: Number of parent rows per batch (default: 100000)
        
    Returns:
        Dictionary with processing results and statistics:
        - status: "completed" or "completed_with_errors"
        - cosmos_configuration: Container config details
        - performance_configuration: Concurrency settings used
        - data_processing: Counts of documents/tables processed
        - results: Success/failure counts and rates
        - performance_metrics: Timing and RU consumption
        - failed_document_ids: Sample of failed document IDs
        
    Raises:
        ValueError: If required parameters are missing or invalid
        Exception: For various processing errors (connection, CSV parsing, etc.)
    """
    # Extract and validate parameters
    params = _extract_parameters(body)
    _validate_parameters(params)
    
    cosmos_key = get_secret(key_vault_name=params["key_vault"], secretname=params["secret_name"])
    export_dir = f"{params['adls_directory']}/{params['cosmos_container']}".strip("/")
    
    try:
        # Initialize ADLS client
        credential = DefaultAzureCredential()
        service_client = DataLakeServiceClient(
            account_url=f"https://{params['adls_account_name']}.dfs.core.windows.net",
            credential=credential
        )
        fs_client = service_client.get_file_system_client(params["adls_file_system"])
        
        # Get parent CSV info
        parent_full_path = f"{export_dir}/{params['cosmos_container']}.csv"
        total_parent_rows = get_csv_row_count(service_client, params["adls_file_system"], 
                                             parent_full_path, chunk_size=params["batch_size"])
        
        # Get all CSV paths
        paths = list(fs_client.get_paths(path=export_dir))
        csv_paths = [p.name for p in paths if (not p.is_directory) and p.name.endswith(".csv")]
        num_child_tables = sum(1 for p in csv_paths if not p.endswith(f"/{params['cosmos_container']}.csv"))
        
        # Process with Cosmos DB
        async with CosmosClient(params["cosmos_url"], credential=cosmos_key) as client:
            database = client.get_database_client(params["cosmos_db"])
            container = database.get_container_client(params["cosmos_container"])
            
            # Handle truncate if needed
            if params["truncate"]:
                await _handle_truncate(database, container, params["cosmos_container"])
                container = database.get_container_client(params["cosmos_container"])
            
            # Discover configuration
            config = await discover_container_configuration(database, container, params["cosmos_container"])
            
            # Calculate concurrency settings
            concurrency_config, throttle_manager = await _initialize_concurrency_settings(
                service_client, params, parent_full_path, config, total_parent_rows
            )
            
            # Process all batches
            result = await _process_all_batches(
                service_client, params, parent_full_path, csv_paths, export_dir,
                total_parent_rows, num_child_tables, container, config,
                throttle_manager, concurrency_config
            )
        
        return _build_response(result, config, concurrency_config, params, 
                              total_parent_rows, num_child_tables)
        
    except Exception as e:
        logging.error(f"Processing failed: {str(e)}")
        raise


def _extract_parameters(body: Dict) -> Dict:
    """
    Extract parameters from request body.
    
    Parses the HTTP request body and extracts all required and optional parameters
    with appropriate defaults.
    
    Args:
        body: Request body dictionary
        
    Returns:
        Dictionary with extracted parameters
    """
    return {
        "cosmos_url": body.get("cosmos_url"),
        "cosmos_db": body.get("cosmos_db"),
        "key_vault": body.get("key_vault_name"),
        "secret_name": body.get("cosmos_secret_name"),
        "cosmos_container": body.get("cosmos_container"),
        "adls_account_name": body.get("adls_account_name"),
        "adls_file_system": body.get("adls_file_system"),
        "adls_directory": body.get("adls_directory", ""),
        "truncate": body.get("truncate_sink_before_write"),
        "batch_size": body.get("batch_size", 100000)
    }


def _validate_parameters(params: Dict):
    """
    Validate required parameters.
    
    Checks that all required parameters are present and raises ValueError if any
    are missing.
    
    Args:
        params: Parameters dictionary from _extract_parameters()
        
    Raises:
        ValueError: If any required parameters are missing or invalid
    """
    missing = [
        k for k, v in {
            "Cosmos URL": params["cosmos_url"],
            "Cosmos Secret name": params["secret_name"],
            "Cosmos Database name": params["cosmos_db"],
            "Key Vault name": params["key_vault"],
            "Cosmos Container name": params["cosmos_container"],
            "Storage account name": params["adls_account_name"],
            "ADLS container name": params["adls_file_system"],
            "ADLS Directory": params["adls_directory"]
        }.items() if not v
    ]

    if missing:
        error_msg = f"Missing required parameters: {missing}"
        logging.error(error_msg)
        raise ValueError(error_msg)
    
    if params["truncate"] is None:
        error_msg = "Missing required parameter: truncate_sink_before_write"
        logging.error(error_msg)
        raise ValueError(error_msg)


async def _handle_truncate(database, container, cosmos_container: str):
    """
    Handle container truncation and recreation.
    
    Reads the existing container properties, deletes the container, and recreates
    it with the same partition key and throughput settings.
    
    Args:
        database: Cosmos database client (async)
        container: Cosmos container client (async)
        cosmos_container: Container name
        
    Raises:
        ValueError: If container doesn't exist (can't get properties for recreation)
    """
    try:
        existing_container_properties = await container.read()
    except exceptions.CosmosResourceNotFoundError as e:
        error_msg = (f"truncate_sink_before_write is true, but container '{cosmos_container}' "
                    f"does not exist. Cannot fetch properties to recreate it.")
        logging.error(error_msg)
        raise ValueError(error_msg) from e

    existing_pk = existing_container_properties.get("partitionKey", {})
    partition_key_paths = existing_pk.get("paths", ["/id"])

    previous_offer_props = None
    try:
        offer = await container.get_throughput()
        if offer:
            previous_offer_props = offer.properties
    except Exception as e:
        logging.info(f"No throughput settings found for container: {str(e)}")

    await database.delete_container(cosmos_container)

    offer_content = previous_offer_props.get("content", {}) if previous_offer_props else {}
    autoscale = offer_content.get("offerAutopilotSettings")
    manual_ru = offer_content.get("offerThroughput")
    
    if autoscale:
        max_ru = autoscale.get("maxThroughput", 4000)
        await database.create_container_if_not_exists(
            id=cosmos_container,
            partition_key=PartitionKey(path=partition_key_paths),
            offer_throughput=ThroughputProperties(auto_scale_max_throughput=max_ru)
        )
    elif manual_ru:
        await database.create_container_if_not_exists(
            id=cosmos_container,
            partition_key=PartitionKey(path=partition_key_paths),
            offer_throughput=manual_ru
        )
    else:
        await database.create_container_if_not_exists(
            id=cosmos_container,
            partition_key=PartitionKey(path=partition_key_paths)
        )


async def _initialize_concurrency_settings(service_client, params, parent_full_path, 
                                           config, total_parent_rows):
    """
    Initialize concurrency settings based on sample documents.
    
    Reads a sample of documents to estimate average document size, then calculates
    optimal concurrency settings and creates the throttle manager.
    
    Args:
        service_client: ADLS service client
        params: Parameters dictionary
        parent_full_path: Full path to parent CSV
        config: Container configuration
        total_parent_rows: Total number of parent rows
        
    Returns:
        Tuple of (concurrency_config, throttle_manager)
    """
    first_batch_df = read_csv_from_adls_batched(
        service_client, params["adls_file_system"], 
        parent_full_path, skip_rows=0, nrows=min(100, params["batch_size"])
    )
    
    first_batch_flat = [row.dropna().to_dict() for _, row in first_batch_df.iterrows()]
    sample_parents = [
        unflatten_dict({k: v for k, v in flat.items() if k != "_parent_rid"}) 
        for flat in first_batch_flat[:min(100, len(first_batch_flat))]
    ]
    sample_docs = [cosmos_safe(strip_system_fields(doc)) for doc in sample_parents]
    
    avg_doc_size_kb = (
        sum(estimate_document_size_kb(doc) for doc in sample_docs) / len(sample_docs)
        if sample_docs else 1
    )
    
    concurrency_config = calculate_optimal_concurrency(config, total_parent_rows, avg_doc_size_kb)
    throttle_manager = AdaptiveThrottleManager(concurrency_config)
    
    del first_batch_df, first_batch_flat, sample_parents, sample_docs
    
    return concurrency_config, throttle_manager


async def _process_all_batches(service_client, params, parent_full_path, csv_paths, 
                               export_dir, total_parent_rows, num_child_tables,
                               container, config, throttle_manager, concurrency_config):
    """
    Process all batches of parent documents.
    
    Iterates through all parent rows in batches, processing each batch with its
    associated child records and upserting to Cosmos DB.
    
    Args:
        service_client: ADLS service client
        params: Parameters dictionary
        parent_full_path: Full path to parent CSV
        csv_paths: List of all CSV paths
        export_dir: Export directory path
        total_parent_rows: Total number of parent rows
        num_child_tables: Number of child tables
        container: Cosmos container client
        config: Container configuration
        throttle_manager: Throttle manager instance
        concurrency_config: Concurrency configuration
        
    Returns:
        Dictionary with aggregated results from all batches
    """
    total_successful = 0
    total_failed = 0
    total_rus = 0
    total_elapsed = 0
    total_throttles = 0
    all_failed_docs = []
    processed_parents = 0
    total_child_rows_processed = 0
    
    num_batches = (total_parent_rows + params["batch_size"] - 1) // params["batch_size"]
    
    for batch_num in range(num_batches):
        skip_rows = batch_num * params["batch_size"]
        current_batch_size = min(params["batch_size"], total_parent_rows - skip_rows)
        
        logging.info(f"Processing batch {batch_num + 1}/{num_batches}: "
                    f"rows {skip_rows} to {skip_rows + current_batch_size}")
        
        parent_df = read_csv_from_adls_batched(
            service_client, params["adls_file_system"], 
            parent_full_path, skip_rows=skip_rows, nrows=current_batch_size
        )
        
        batch_parents = _prepare_batch_parents(parent_df)
        
        batch_result, batch_child_rows = await process_batch(
            batch_parents, service_client, params["adls_file_system"], csv_paths, 
            export_dir, params["cosmos_container"], container, config, 
            throttle_manager, concurrency_config
        )
        
        total_successful += batch_result["successful"]
        total_failed += batch_result["failed"]
        total_rus += batch_result["total_rus_consumed"]
        total_elapsed += batch_result["elapsed_seconds"]
        total_throttles += batch_result["total_throttles"]
        all_failed_docs.extend(batch_result["failed_docs"][:20])
        processed_parents += len(batch_parents)
        total_child_rows_processed += batch_child_rows
        
        del parent_df, batch_parents, batch_result
    
    return {
        "total": total_parent_rows,
        "successful": total_successful,
        "failed": total_failed,
        "failed_docs": all_failed_docs[:20],
        "elapsed_seconds": total_elapsed,
        "document_rate_per_second": total_parent_rows / total_elapsed if total_elapsed > 0 else 0,
        "total_rus_consumed": total_rus,
        "ru_rate_per_second": total_rus / total_elapsed if total_elapsed > 0 else 0,
        "avg_ru_per_document": total_rus / total_parent_rows if total_parent_rows > 0 else 0,
        "total_throttles": total_throttles,
        "num_batches": num_batches,
        "num_child_tables": num_child_tables,
        "total_child_rows": total_child_rows_processed
    }


def _prepare_batch_parents(parent_df: pd.DataFrame) -> List[Dict]:
    """
    Prepare parent documents from DataFrame.
    
    Converts a DataFrame of parent rows to a list of nested document structures,
    preserving array markers for later child assignment.
    
    Args:
        parent_df: DataFrame containing parent rows
        
    Returns:
        List of parent document dictionaries with nested structures
        
    """
    parent_flat = [row.dropna().to_dict() for _, row in parent_df.iterrows()]
    batch_parents = []
    
    for p in parent_flat:
        flat = dict(p)
        has_array_fields = {k: v for k, v in flat.items() if k.startswith("_has_array_")}
        regular_fields = {k: v for k, v in flat.items() 
                         if k != "_parent_rid" and not k.startswith("_has_array_")}
        
        nested = unflatten_dict(regular_fields)
        nested.update(has_array_fields)
        batch_parents.append(nested)
    
    return batch_parents


def _build_response(result: Dict, config: Dict, concurrency_config: Dict, 
                   params: Dict, total_parent_rows: int, num_child_tables: int) -> Dict:
    """
    Build final response dictionary.
    
    Assembles all processing results and statistics into a comprehensive response
    structure for the caller.
    
    Args:
        result: Results from _process_all_batches()
        config: Container configuration
        concurrency_config: Concurrency configuration used
        params: Original parameters
        total_parent_rows: Total parent rows processed
        num_child_tables: Number of child tables processed
        
    Returns:
        Comprehensive response dictionary with all statistics and results
    """
    return {
        "status": "completed" if result["failed"] == 0 else "completed_with_errors",
        "cosmos_configuration": {
            "partition_key": config["partition_key_path"],
            "throughput_type": config["throughput_type"],
            "provisioned_rus": config["provisioned_rus"],
            "is_autoscale": config["is_autoscale"],
            "is_serverless": config["is_serverless"],
            "uses_shared_throughput": config["uses_shared_throughput"],
        },
        "performance_configuration": {
            "max_concurrent_operations": concurrency_config["max_concurrent_operations"],
            "initial_batch_size": concurrency_config["initial_batch_size"],
            "estimated_ru_per_doc": concurrency_config["estimated_ru_per_doc"],
            "batch_size": params["batch_size"],
            "num_batches_processed": result["num_batches"],
        },
        "data_processing": {
            "parent_documents": total_parent_rows,
            "child_tables_processed": num_child_tables,
            "total_child_rows": result["total_child_rows"],
        },
        "results": {
            "total_documents": result["total"],
            "successful": result["successful"],
            "failed": result["failed"],
            "success_rate_percent": round(
                100 * result["successful"] / result["total"], 2
            ) if result["total"] > 0 else 0,
            "total_throttles": result["total_throttles"],
        },
        "performance_metrics": {
            "elapsed_seconds": round(result["elapsed_seconds"], 2),
            "document_rate_per_second": round(result["document_rate_per_second"], 2),
            "total_rus_consumed": round(result["total_rus_consumed"], 2),
            "ru_rate_per_second": round(result["ru_rate_per_second"], 2),
            "avg_ru_per_document": round(result["avg_ru_per_document"], 2),
        },
        "failed_document_ids": [
            doc.get("id", "unknown") for doc in result["failed_docs"][:20]
        ],
    }


@app.activity_trigger(input_name="params")
def process_adls_to_cosmos_activity(params: dict):
    """
    Activity trigger for processing ADLS to Cosmos DB transfer.
    
    This is the Durable Functions activity that wraps the async processing function.
    It's invoked by the orchestrator.
    
    Args:
        params: Parameters dictionary containing all configuration
        
    Returns:
        Processing results dictionary from process_adls_to_cosmos_async()
    """
    return asyncio.run(process_adls_to_cosmos_async(params))


@app.orchestration_trigger(context_name="context")
def adls_to_cosmos_orchestrator(context: df.DurableOrchestrationContext):
    """
    Orchestrator for ADLS to Cosmos DB transfer.
    
    Args:
        context: Durable orchestration context
        
    Returns:
        Activity result
    """
    params = context.get_input()
    result = yield context.call_activity("process_adls_to_cosmos_activity", params)
    return result


@app.route(route="ADLS_to_Cosmos_v1", methods=["POST"])
@app.durable_client_input(client_name="client")
async def adls_to_cosmos_http_start(req: func.HttpRequest, client) -> func.HttpResponse:
    """
    HTTP trigger to start ADLS to Cosmos DB transfer.
    
    Args:
        req: HTTP request
        client: Durable orchestration client
        
    Returns:
        HTTP response with status
    """
    try:
        body = req.get_json()
    except Exception as e:
        logging.error(f"Invalid JSON body: {str(e)}")
        return func.HttpResponse("Invalid JSON body", status_code=400)
    
    try:
        instance_id = await client.start_new("adls_to_cosmos_orchestrator", None, body)
        response = client.create_check_status_response(req, instance_id)
        return response
        
    except Exception as e:
        logging.error(f"HTTP start failed: {str(e)}", exc_info=True)
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )