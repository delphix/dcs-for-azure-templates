import gc
import json
import logging
import time
import uuid
from collections import deque
from datetime import datetime
from io import StringIO
from typing import Dict, Iterator, List, Optional, Tuple

import azure.durable_functions as df
import azure.functions as func
import pandas as pd
from azure.core.exceptions import (ClientAuthenticationError,
                                   ResourceNotFoundError)
from azure.cosmos import CosmosClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.filedatalake import DataLakeServiceClient

# ============================================================================
# CONSTANTS
# ============================================================================

# Default batch size for processing documents
DEFAULT_BATCH_SIZE = 500

# Default throughput if detection fails (in RU/s)
DEFAULT_THROUGHPUT = 400

# Reserve percentage of throughput to avoid throttling
DEFAULT_RESERVE_PERCENT = 0.2

# Limits how many child rows are processed at once to avoid high memory usage.
# A single record can generate many child rows, so we use a safe fixed size.
ARRAY_PROCESSING_BATCH_SIZE = 2000

# Retry configuration
DEFAULT_MAX_RETRIES = 5
DEFAULT_BASE_DELAY = 0.1  # seconds
DEFAULT_MAX_DELAY = 60.0  # seconds

# File Handling Constants
PIPE_DELIMITER = "|"
ESCAPE_CHARACTER = "\\"

# Transient error codes that should be retried
RETRYABLE_ERROR_CODES = [
    "429",  # Request Rate Too Large (throttling)
    "408",  # Request Timeout
    "503",  # Service Unavailable
    "RequestRateTooLarge",
    "RequestTimeout",
    "ServiceUnavailable",
]

# ============================================================================
# AZURE FUNCTIONS APP
# ============================================================================

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# ============================================================================
# KEY VAULT UTILITIES
# ============================================================================


def get_secret(key_vault_name: str, secretname: str) -> str:
    """
    Retrieve a secret from Azure Key Vault.

    Args:
        key_vault_name: Name of the Key Vault
        secretname: Name of the secret to retrieve

    Returns:
        The secret value as a string

    Raises:
        Exception: If secret retrieval fails
    """
    kv_uri = f"https://{key_vault_name}.vault.azure.net"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=kv_uri, credential=credential)
    secret = client.get_secret(secretname)
    return secret.value


# ============================================================================
# COSMOS DB METADATA UTILITIES
# ============================================================================


class CosmosMetadata:
    """
    Utility class for extracting metadata from Cosmos DB containers.
    Provides methods to discover throughput, partition keys, and partition values.
    """

    @staticmethod
    def get_throughput_info(database, container_name: str) -> Dict:
        """
        Detect the provisioned throughput for a Cosmos DB container or database.

        Tries to read throughput at container level first, then falls back to database level.

        Args:
            database: Cosmos database client
            container_name: Name of the container

        Returns:
            Dictionary containing:
                - level: 'container', 'database', or 'unknown'
                - throughput: RU/s value
                - is_autoscale: Boolean indicating autoscale mode
        """
        try:
            container = database.get_container_client(container_name)

            # Try container-level throughput first
            try:
                offer = container.read_offer()
                if offer:
                    is_autoscale = "maximumThroughput" in offer.get("content", {})
                    throughput = offer["content"].get("maximumThroughput") or offer[
                        "content"
                    ].get("throughput", DEFAULT_THROUGHPUT)
                    return {
                        "level": "container",
                        "throughput": throughput,
                        "is_autoscale": is_autoscale,
                    }
            except Exception:
                pass

            # Fall back to database-level throughput
            try:
                db_offer = database.read_offer()
                if db_offer:
                    is_autoscale = "maximumThroughput" in db_offer.get("content", {})
                    throughput = db_offer["content"].get(
                        "maximumThroughput"
                    ) or db_offer["content"].get("throughput", DEFAULT_THROUGHPUT)
                    return {
                        "level": "database",
                        "throughput": throughput,
                        "is_autoscale": is_autoscale,
                    }
            except Exception:
                pass

            return {
                "level": "unknown",
                "throughput": DEFAULT_THROUGHPUT,
                "is_autoscale": False,
            }

        except Exception:
            return {
                "level": "unknown",
                "throughput": DEFAULT_THROUGHPUT,
                "is_autoscale": False,
            }

    @staticmethod
    def get_partition_key_paths(container) -> List[str]:
        """
        Extract partition key paths from a Cosmos DB container.

        Args:
            container: Cosmos container client

        Returns:
            List of partition key paths (e.g., ['/userId'])
        """
        try:
            properties = container.read()
            pk_definition = properties.get("partitionKey", {})
            paths = pk_definition.get("paths", [])
            return paths if paths else []
        except Exception:
            return []

    @staticmethod
    def discover_partition_values(container, pk_path: str) -> List:
        """
        Discover all existing partition key values in a container.

        Attempts to use DISTINCT query first for efficiency. Falls back to
        querying and deduplicating if DISTINCT is not supported.

        Args:
            container: Cosmos container client
            pk_path: Partition key path (e.g., '/userId')

        Returns:
            List of all partition key values found in the container
        """
        try:
            # Convert partition key path to Cosmos query format
            pk_field = "c." + pk_path.strip("/").replace("/", ".")

            # Try DISTINCT query first (more efficient)
            try:
                query = f"SELECT DISTINCT VALUE {pk_field} FROM c"
                items = list(
                    container.query_items(
                        query=query, enable_cross_partition_query=True
                    )
                )
                return [v for v in items if v is not None]
            except Exception:
                # DISTINCT may not be supported in all scenarios
                pass

            # Fallback: Query all and deduplicate
            query = f"SELECT {pk_field} as pk FROM c"
            items = list(
                container.query_items(query=query, enable_cross_partition_query=True)
            )
            return list(
                set([item.get("pk") for item in items if item.get("pk") is not None])
            )

        except Exception:
            return []


# ============================================================================
# DATA TRANSFORMATION UTILITIES
# ============================================================================


def flatten_dict(doc: Dict, parent_key: str = "", sep: str = ".") -> Dict:
    """
    Recursively flatten a nested dictionary.

    Args:
        doc: Dictionary to flatten
        parent_key: Key prefix for nested values
        sep: Separator between nested keys

    Returns:
        Flattened dictionary

    Example:
        {'a': {'b': 1}} -> {'a.b': 1}
    """
    items = {}
    for k, v in doc.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items


# ============================================================================
# REQUEST UNIT (RU) MANAGEMENT
# ============================================================================


class TokenBucketRUManager:
    """
    Token bucket algorithm for managing Cosmos DB Request Units (RU) consumption.

    Prevents throttling by rate-limiting requests to stay within provisioned throughput.
    Uses a reserve percentage to provide a safety buffer for burst traffic and
    other concurrent operations.
    """

    def __init__(
        self, throughput: int, reserve_percent: float = DEFAULT_RESERVE_PERCENT
    ):
        """
        Initialize the RU manager with token bucket algorithm.

        Args:
            throughput: Provisioned throughput in RU/s
            reserve_percent: Percentage of throughput to reserve as safety buffer
        """
        self.max_rus_per_second = int(throughput * (1 - reserve_percent))
        self.tokens = float(self.max_rus_per_second)
        self.last_refill = time.time()
        self.total_consumed = 0.0
        self.operations_count = 0

    def _refill_tokens(self):
        """
        Refill the token bucket based on elapsed time.
        Tokens refill at a constant rate of max_rus_per_second.
        """
        now = time.time()
        elapsed = now - self.last_refill
        if elapsed > 0:
            refill_amount = elapsed * self.max_rus_per_second
            self.tokens = min(self.max_rus_per_second, self.tokens + refill_amount)
            self.last_refill = now

    def consume(self, ru_charge: float):
        """
        Consume RUs for an operation, sleeping if necessary to avoid throttling.

        Args:
            ru_charge: Number of RUs consumed by the operation
        """
        self._refill_tokens()

        # If insufficient tokens, sleep until enough are available
        if ru_charge > self.tokens:
            deficit = ru_charge - self.tokens
            wait_time = deficit / self.max_rus_per_second
            time.sleep(wait_time)
            self._refill_tokens()

        self.tokens -= ru_charge
        self.total_consumed += ru_charge
        self.operations_count += 1

    def get_stats(self) -> Dict:
        """
        Get statistics about RU consumption.

        Returns:
            Dictionary with total RUs consumed, operation count, and averages
        """
        avg_ru = (
            self.total_consumed / self.operations_count
            if self.operations_count > 0
            else 0
        )
        return {
            "total_rus_consumed": round(self.total_consumed, 2),
            "operations_count": self.operations_count,
            "avg_ru_per_operation": round(avg_ru, 2),
            "current_tokens": round(self.tokens, 2),
        }


# ============================================================================
# RETRY STRATEGY
# ============================================================================


class CosmosRetryStrategy:
    """
    Retry strategy for transient Cosmos DB errors with exponential backoff.

    Handles rate limiting (429), timeouts (408), service unavailability (503),
    and other transient errors with intelligent retry logic.
    """

    def __init__(
        self,
        max_retries: int = DEFAULT_MAX_RETRIES,
        base_delay: float = DEFAULT_BASE_DELAY,
        max_delay: float = DEFAULT_MAX_DELAY,
    ):
        """
        Initialize retry strategy.

        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Initial delay in seconds for exponential backoff
            max_delay: Maximum delay between retries in seconds
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.retry_count_by_error = {}

    def execute_with_retry(self, operation, operation_name: str = "operation"):
        """
        Execute an operation with retry logic for transient errors.

        Args:
            operation: Callable that returns a result
            operation_name: Description of the operation for logging

        Returns:
            Result from the operation

        Raises:
            Exception: If all retries are exhausted
        """
        import random

        for attempt in range(self.max_retries + 1):
            try:
                result = operation()
                return result

            except Exception as e:
                error_msg = str(e)

                # Check if this is a retryable error
                is_retryable = any(code in error_msg for code in RETRYABLE_ERROR_CODES)

                if is_retryable:
                    # Track retry counts by error type
                    error_type = self._classify_error(error_msg)
                    self.retry_count_by_error[error_type] = (
                        self.retry_count_by_error.get(error_type, 0) + 1
                    )

                    # For 429 errors, respect the Retry-After header
                    retry_after = None
                    if "429" in error_msg or "RequestRateTooLarge" in error_msg:
                        retry_after = self._extract_retry_after(e)

                    if attempt < self.max_retries:
                        wait_time = (
                            retry_after
                            if retry_after
                            else self._calculate_backoff(attempt)
                        )
                        jitter = random.uniform(0, 0.1 * wait_time)
                        total_wait = wait_time + jitter

                        logging.warning(
                            f"Retryable error in {operation_name} ({error_type}): "
                            f"attempt {attempt + 1}/{self.max_retries + 1}. "
                            f"Waiting {total_wait:.2f}s before retry."
                        )

                        time.sleep(total_wait)
                        continue

                # If non-retryable error or exhausted retries, raise
                if attempt == self.max_retries:
                    logging.error(
                        f"Operation {operation_name} failed after {self.max_retries + 1} attempts"
                    )
                raise

    def _classify_error(self, error_msg: str) -> str:
        """
        Classify error type for statistics tracking.

        Args:
            error_msg: Error message string

        Returns:
            Error classification string
        """
        if "429" in error_msg or "RequestRateTooLarge" in error_msg:
            return "429_RateLimited"
        elif "408" in error_msg or "RequestTimeout" in error_msg:
            return "408_Timeout"
        elif "503" in error_msg or "ServiceUnavailable" in error_msg:
            return "503_ServiceUnavailable"
        else:
            return "Other"

    def _extract_retry_after(self, exception) -> Optional[float]:
        """
        Extract Retry-After header value from exception.

        Args:
            exception: The exception that may contain retry headers

        Returns:
            Retry delay in seconds, or None if not found
        """
        try:
            if hasattr(exception, "headers"):
                retry_ms = exception.headers.get("x-ms-retry-after-ms")
                if retry_ms:
                    return float(retry_ms) / 1000.0
        except Exception:
            pass
        return None

    def _calculate_backoff(self, attempt: int) -> float:
        """
        Calculate exponential backoff delay.

        Args:
            attempt: Current attempt number (0-indexed)

        Returns:
            Delay in seconds, capped at max_delay
        """
        return min(self.base_delay * (2**attempt), self.max_delay)

    def get_stats(self) -> Dict:
        """
        Get retry statistics.

        Returns:
            Dictionary with retry counts by error type
        """
        total_retries = sum(self.retry_count_by_error.values())
        return {
            "total_retries": total_retries,
            "retries_by_error_type": self.retry_count_by_error.copy(),
        }


# ============================================================================
# STREAMING COSMOS DB READER
# ============================================================================


class StreamingCosmosReader:
    """
    Stream documents from Cosmos DB in batches with RU management and retry logic.

    Minimizes memory usage by processing one batch at a time and supports
    both partition-specific and cross-partition queries.
    """

    def __init__(
        self,
        container,
        pk_path: Optional[str],
        pk_values: Optional[List],
        batch_size: int,
        throughput: int = DEFAULT_THROUGHPUT,
    ):
        """
        Initialize the streaming reader.

        Args:
            container: Cosmos container client
            pk_path: Partition key path (None for cross-partition query)
            pk_values: List of partition values to query (None for all)
            batch_size: Number of documents per batch
            throughput: Provisioned throughput in RU/s
        """
        self.container = container
        self.pk_path = pk_path
        self.pk_values = pk_values
        self.batch_size = batch_size
        self.ru_manager = TokenBucketRUManager(
            throughput, reserve_percent=DEFAULT_RESERVE_PERCENT
        )
        self.retry_strategy = CosmosRetryStrategy(max_retries=DEFAULT_MAX_RETRIES)

    def _extract_ru_charge(self) -> float:
        """
        Extract RU charge from last response headers.

        Returns:
            RU charge as float, defaults to 5.0 if extraction fails
        """
        try:
            headers = self.container.client_connection.last_response_headers
            return float(headers.get("x-ms-request-charge", 0))
        except Exception:
            return 5.0

    def stream_documents(self) -> Iterator[List[Dict]]:
        """
        Stream documents from Cosmos DB in batches.

        Yields:
            Lists of documents (batches) from the container
        """
        if self.pk_values:
            for pk_value in self.pk_values:
                yield from self._stream_partition(pk_value)
        else:
            yield from self._stream_all_documents()

    def _stream_partition(self, pk_value) -> Iterator[List[Dict]]:
        """
        Stream documents from a single partition in batches.

        Args:
            pk_value: Partition key value to query

        Yields:
            Batches of documents from the specified partition
        """
        key_path_parts = [k for k in self.pk_path.strip("/").split("/") if k]
        cosmos_key_path = "c." + ".".join(key_path_parts)
        query = f"SELECT * FROM c WHERE {cosmos_key_path} = @value"

        def _execute_query():
            return self.container.query_items(
                query=query,
                parameters=[{"name": "@value", "value": pk_value}],
                enable_cross_partition_query=True,
                max_item_count=self.batch_size,
            )

        try:
            query_iterator = self.retry_strategy.execute_with_retry(
                _execute_query, f"stream_partition_{pk_value}"
            )
        except Exception as e:
            logging.error(f"Failed to query partition {pk_value}: {str(e)}")
            return

        yield from self._process_query_results(query_iterator)

    def _stream_all_documents(self) -> Iterator[List[Dict]]:
        """
        Stream all documents using cross-partition query.

        Yields:
            Batches of documents from all partitions
        """
        query = "SELECT * FROM c"

        def _execute_query():
            return self.container.query_items(
                query=query,
                enable_cross_partition_query=True,
                max_item_count=self.batch_size,
            )

        try:
            query_iterator = self.retry_strategy.execute_with_retry(
                _execute_query, "stream_all_documents"
            )
        except Exception as e:
            logging.error(f"Failed to query all documents: {str(e)}")
            return

        yield from self._process_query_results(query_iterator)

    def _process_query_results(self, query_iterator) -> Iterator[List[Dict]]:
        """
        Process query results in batches with RU management.

        Common logic for both partition-specific and cross-partition queries.
        Handles RU consumption tracking and memory-efficient batch accumulation.

        Args:
            query_iterator: Cosmos DB query iterator

        Yields:
            Batches of documents
        """
        batch = []

        for page in query_iterator.by_page():
            items = list(page)

            # Track RU consumption and rate limit if necessary
            ru_charge = self._extract_ru_charge()
            self.ru_manager.consume(ru_charge)

            # Accumulate items into batches
            for item in items:
                batch.append(item)
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
                    gc.collect()

        # Yield final partial batch if any
        if batch:
            yield batch
            gc.collect()

    def get_stats(self) -> Dict:
        """
        Get statistics from RU manager and retry strategy.

        Returns:
            Combined statistics dictionary
        """
        ru_stats = self.ru_manager.get_stats()
        retry_stats = self.retry_strategy.get_stats()
        return {**ru_stats, **retry_stats}


# ============================================================================
# ARRAY EXTRACTION AND NORMALIZATION
# ============================================================================


def extract_arrays_iterative_optimized(
    doc: Dict,
) -> Tuple[Dict, Dict[str, pd.DataFrame]]:
    """
    Extract nested arrays from a document into normalized tables.

    This function processes a hierarchical JSON document and:
    1. Flattens scalar fields into a parent record
    2. Extracts arrays of objects into separate child tables
    3. Maintains relationships via _rid and _parent_rid fields
    4. Handles arbitrary nesting depth using iterative processing

    Algorithm Overview:
    - Uses breadth-first traversal with a queue to process nested structures
    - Each array of objects becomes a separate table
    - Nested arrays create hierarchical table names (e.g., "orders.items")
    - Primitive arrays remain in the parent as JSON arrays
    - Each record gets a unique _rid and references its parent via _parent_rid

    Args:
        doc: The document to process

    Returns:
        Tuple of:
            - parent_fields: Dict of scalar fields for the parent table
            - child_tables: Dict mapping table names to DataFrames

    Example:
        Input: {"id": 1, "items": [{"name": "A"}, {"name": "B"}]}
        Output:
            parent_fields = {"id": 1, "_has_array_items": True, "_rid": "..."}
            child_tables = {"items": DataFrame([{"name": "A", "_rid": "...", "_parent_rid": "..."}])}
    """
    parent_fields = {}
    child_tables = {}

    root_rid = doc.get("_rid") or str(uuid.uuid4())
    doc["_rid"] = root_rid

    queue = deque()

    def flatten_no_arrays(d: Dict, parent: str = "") -> Dict:
        """
        Flatten a dictionary, keeping arrays as-is.

        Args:
            d: Dictionary to flatten
            parent: Parent key prefix

        Returns:
            Flattened dictionary with arrays preserved
        """
        flat = {}
        for k, v in d.items():
            key = f"{parent}.{k}" if parent else k
            if isinstance(v, list):
                flat[key] = v
            elif isinstance(v, dict):
                flat.update(flatten_no_arrays(v, key))
            else:
                flat[key] = v
        return flat

    flat_root = flatten_no_arrays(doc)

    # Process root-level fields and arrays
    for key, value in flat_root.items():
        if not isinstance(value, list):
            parent_fields[key] = value
            continue

        primitive_items = [v for v in value if not isinstance(v, dict)]
        dict_items = [v for v in value if isinstance(v, dict)]

        if primitive_items and not dict_items:
            parent_fields[key] = value
            continue

        parent_fields[f"_has_array_{key}"] = True

        for item in dict_items:
            item_rid = str(uuid.uuid4())
            item["_rid"] = item_rid
            item["_parent_rid"] = root_rid
            table_name = key
            queue.append((item, table_name, root_rid))

    current_batch = {}

    # Process queue: extract nested arrays iteratively
    while queue:
        current, table_name, parent_rid = queue.popleft()
        flat = flatten_no_arrays(current)
        row = {"_rid": current["_rid"], "_parent_rid": parent_rid}

        for key, value in flat.items():
            if not isinstance(value, list):
                row[key] = value
                continue

            primitive_items = [v for v in value if not isinstance(v, dict)]
            dict_items = [v for v in value if isinstance(v, dict)]

            if primitive_items and not dict_items:
                row[key] = value
                continue

            row[f"_has_array_{key}"] = True

            for item in dict_items:
                item_rid = str(uuid.uuid4())
                item["_rid"] = item_rid
                item["_parent_rid"] = current["_rid"]

                nested_table_name = f"{table_name}.{key}"
                queue.append((item, nested_table_name, current["_rid"]))

        if table_name not in current_batch:
            current_batch[table_name] = []
        current_batch[table_name].append(row)

        if len(current_batch.get(table_name, [])) >= ARRAY_PROCESSING_BATCH_SIZE:
            if table_name not in child_tables:
                child_tables[table_name] = []
            child_tables[table_name].append(pd.DataFrame(current_batch[table_name]))
            current_batch[table_name] = []

    # Process remaining batches
    for table_name, rows in current_batch.items():
        if rows:
            if table_name not in child_tables:
                child_tables[table_name] = []
            child_tables[table_name].append(pd.DataFrame(rows))

    # Combine batch DataFrames into final tables
    final_children = {}
    for k, v in child_tables.items():
        try:
            final_children[k] = pd.concat(v, ignore_index=True)
        except ValueError:
            final_children[k] = pd.DataFrame(v)

    return parent_fields, final_children


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean a DataFrame by removing unparseable values.

    Handles:
    - Nested dictionaries and complex objects -> None
    - Arrays of dictionaries -> None
    - Simple values (str, int, float, bool) -> preserved
    - Simple arrays -> preserved

    Args:
        df: DataFrame to clean

    Returns:
        Cleaned DataFrame
    """
    if df is None or df.empty:
        return pd.DataFrame()

    def fix_value(val):
        """Clean individual cell values."""
        if val is None:
            return None
        if isinstance(val, (str, int, float, bool)):
            return val
        if isinstance(val, list) and all(not isinstance(i, dict) for i in val):
            return val
        if isinstance(val, dict):
            return None
        if isinstance(val, list) and any(isinstance(i, dict) for i in val):
            return None
        try:
            return str(val)
        except Exception:
            return None

    for col in df.columns:
        df[col] = df[col].apply(fix_value)

    return df


# ============================================================================
# ADLS FILE UPLOAD
# ============================================================================


def upload_csv_to_adls(
    service_client,
    file_system: str,
    directory: str,
    file_name: str,
    df: pd.DataFrame,
    mode: str = "write",
    known_columns: Optional[set] = None,
) -> set:
    """
    Upload a DataFrame to Azure Data Lake Storage as a CSV file.

    Features:
    - Supports write and append modes
    - Schema evolution: automatically expands schema when new columns appear
    - Pipe-delimited format for better handling of embedded commas
    - UTF-8 encoding for Unicode support

    Args:
        service_client: ADLS service client
        file_system: ADLS container name
        directory: Target directory path
        file_name: Name of the CSV file
        df: DataFrame to upload
        mode: 'write' (overwrite) or 'append'
        known_columns: Set of previously seen column names (for schema tracking)

    Returns:
        Updated set of all known columns
    """
    df = clean_dataframe(df)
    if df is None or df.empty:
        return known_columns or set()

    fs_client = service_client.get_file_system_client(file_system)

    # Create directory hierarchy
    dir_segments = directory.strip("/").split("/") if directory else []
    curr = ""
    for seg in dir_segments:
        curr = f"{curr}/{seg}" if curr else seg
        try:
            fs_client.get_directory_client(curr).create_directory()
        except Exception:
            pass

    def serialize_cell(v):
        """Serialize cell values for CSV output."""
        if v is None:
            return ""
        return v

    dir_client = fs_client.get_directory_client(directory)
    file_client = dir_client.get_file_client(file_name)

    current_columns = set(df.columns)

    if known_columns is None:
        known_columns = current_columns

    new_columns = current_columns - known_columns

    # Handle schema evolution in append mode
    if mode == "append" and new_columns:
        try:
            properties = file_client.get_file_properties()
            if properties.size > 0:
                download = file_client.download_file()
                existing_content = download.readall().decode("utf-8")
                existing_df = pd.read_csv(
                    StringIO(existing_content),
                    sep=PIPE_DELIMITER,
                    keep_default_na=False,
                )

                for col in new_columns:
                    existing_df[col] = None

                known_columns = known_columns | new_columns

                for col in known_columns:
                    if col not in df.columns:
                        df[col] = None

                column_order = sorted(known_columns)
                existing_df = existing_df[column_order]
                df = df[column_order]

                combined_df = pd.concat([existing_df, df], ignore_index=True)

                df_out = combined_df.copy()
                for col in df_out.columns:
                    df_out[col] = df_out[col].apply(serialize_cell)

                csv_buffer = StringIO()
                df_out.to_csv(
                    csv_buffer,
                    index=False,
                    sep=PIPE_DELIMITER,
                    header=True,
                    quoting=0,
                    escapechar=ESCAPE_CHARACTER,
                )
                content = csv_buffer.getvalue()

                file_client.delete_file()
                file_client = dir_client.create_file(file_name)
                content_bytes = content.encode("utf-8")
                file_client.append_data(
                    content_bytes, offset=0, length=len(content_bytes)
                )
                file_client.flush_data(len(content_bytes))

                return known_columns

        except Exception:
            pass

    # Standard write/append
    for col in known_columns:
        if col not in df.columns:
            df[col] = None

    if len(known_columns) > 0:
        df = df[sorted(known_columns)]

    df_out = df.copy()
    for col in df_out.columns:
        df_out[col] = df_out[col].apply(serialize_cell)

    try:
        if mode == "write":
            try:
                file_client.delete_file()
            except Exception:
                pass

            csv_buffer = StringIO()
            df_out.to_csv(
                csv_buffer,
                index=False,
                sep=PIPE_DELIMITER,
                header=True,
                quoting=0,
                escapechar=ESCAPE_CHARACTER,
            )
            content = csv_buffer.getvalue()

            file_client = dir_client.create_file(file_name)
            content_bytes = content.encode("utf-8")
            file_client.append_data(content_bytes, offset=0, length=len(content_bytes))
            file_client.flush_data(len(content_bytes))
        else:
            # Append mode
            file_exists = False
            current_size = 0

            try:
                properties = file_client.get_file_properties()
                current_size = properties.size
                file_exists = True
            except Exception:
                file_exists = False

            csv_buffer = StringIO()
            include_header = not file_exists
            df_out.to_csv(
                csv_buffer,
                index=False,
                sep=PIPE_DELIMITER,
                header=include_header,
                quoting=0,
                escapechar=ESCAPE_CHARACTER,
            )
            content = csv_buffer.getvalue()

            if not file_exists:
                file_client = dir_client.create_file(file_name)
                current_size = 0

            content_bytes = content.encode("utf-8")
            file_client.append_data(
                content_bytes, offset=current_size, length=len(content_bytes)
            )
            file_client.flush_data(current_size + len(content_bytes))

    except Exception as e:
        logging.error(f"Upload failed for {file_name}: {str(e)}")
        raise

    known_columns = known_columns | current_columns
    return known_columns


# ============================================================================
# CONNECTION VALIDATION
# ============================================================================


def validate_cosmos_connection(
    cosmos_url: str, cosmos_key: str, cosmos_db: str, cosmos_container: str
) -> Tuple:
    """
    Validate Cosmos DB connection and return clients.

    Args:
        cosmos_url: Cosmos DB endpoint URL
        cosmos_key: Cosmos DB access key
        cosmos_db: Database name
        cosmos_container: Container name

    Returns:
        Tuple of (client, database, container)

    Raises:
        ValueError: If validation fails with descriptive error message
    """
    try:
        client = CosmosClient(cosmos_url, credential=cosmos_key)
        database = client.get_database_client(cosmos_db)
        database.read()

        container = database.get_container_client(cosmos_container)
        container.read()

        return client, database, container

    except ClientAuthenticationError:
        raise ValueError(
            "Cosmos authentication failed. Invalid cosmos_key or endpoint."
        )
    except ResourceNotFoundError as e:
        raise ValueError(f"Cosmos resource not found: {str(e)}")
    except Exception as e:
        raise ValueError(f"Cosmos connection validation failed: {str(e)}")


def validate_adls_connection(adls_account_name: str, adls_file_system: str):
    """
    Validate ADLS connection and return service client.

    Args:
        adls_account_name: Storage account name
        adls_file_system: Container/filesystem name

    Returns:
        DataLakeServiceClient instance

    Raises:
        ValueError: If validation fails with descriptive error message
    """
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
        raise ValueError(
            "ADLS authentication failed. Ensure managed identity has proper permissions."
        )
    except ResourceNotFoundError:
        raise ValueError(f"ADLS filesystem '{adls_file_system}' does not exist.")
    except Exception as e:
        raise ValueError(f"ADLS connection validation failed: {str(e)}")


# ============================================================================
# PARAMETER VALIDATION AND EXTRACTION
# ============================================================================


def validate_and_extract_params(params: dict) -> dict:
    """
    Validate and extract parameters from the input dictionary.

    Args:
        params: Input parameters dictionary

    Returns:
        Validated and processed parameters

    Raises:
        ValueError: If required parameters are missing or invalid
    """
    cosmos_url = params.get("cosmos_url")
    key_vault = params.get("key_vault_name")
    secret_name = params.get("cosmos_secret_name")
    cosmos_db = params.get("cosmos_db")
    cosmos_container = params.get("cosmos_container")
    adls_account_name = params.get("adls_account_name")
    adls_file_system = params.get("adls_file_system")

    missing = [
        k
        for k, v in {
            "Cosmos URL": cosmos_url,
            "Cosmos Secret name": secret_name,
            "Cosmos Database name": cosmos_db,
            "Key Vault name": key_vault,
            "Cosmos Container name": cosmos_container,
            "Storage account name": adls_account_name,
            "ADLS container name": adls_file_system,
        }.items()
        if not v
    ]

    if missing:
        raise ValueError(f"Missing required parameters: {missing}")

    partition_key_path = params.get("partition_key_path")
    partition_key_values = params.get("partition_key_value")
    adls_directory = params.get("adls_directory", "")
    separate_files_per_batch = params.get("separate_files_per_batch", False)

    batch_size = params.get("batch_size", DEFAULT_BATCH_SIZE)
    try:
        batch_size = int(batch_size)
        if batch_size < 1:
            batch_size = DEFAULT_BATCH_SIZE
    except (ValueError, TypeError):
        batch_size = DEFAULT_BATCH_SIZE

    return {
        "cosmos_url": cosmos_url,
        "key_vault": key_vault,
        "secret_name": secret_name,
        "cosmos_db": cosmos_db,
        "cosmos_container": cosmos_container,
        "partition_key_path": partition_key_path,
        "partition_key_values": partition_key_values,
        "adls_account_name": adls_account_name,
        "adls_file_system": adls_file_system,
        "adls_directory": adls_directory,
        "batch_size": batch_size,
        "separate_files_per_batch": separate_files_per_batch,
    }


def setup_partition_configuration(
    params: dict, container
) -> Tuple[Optional[str], Optional[List], bool]:
    """
    Setup partition key configuration for querying.

    Args:
        params: Validated parameters
        container: Cosmos container client

    Returns:
        Tuple of (pk_path, pk_values, user_provided_both)

    Raises:
        ValueError: If partition configuration is invalid
    """
    partition_key_path = params["partition_key_path"]
    partition_key_values = params["partition_key_values"]

    has_pk_path = bool(partition_key_path)
    has_pk_values = bool(partition_key_values)

    if has_pk_path != has_pk_values:
        raise ValueError(
            "Invalid partition configuration: "
            "Both 'partition_key_path' AND 'partition_key_value' must be provided together."
        )

    user_provided_both = has_pk_path and has_pk_values

    if user_provided_both:
        pk_path = partition_key_path

        if isinstance(partition_key_values, str):
            try:
                pk_values = json.loads(partition_key_values)
            except Exception:
                pk_values = [partition_key_values]
        elif isinstance(partition_key_values, list):
            pk_values = partition_key_values
        else:
            pk_values = [partition_key_values]

        if not pk_values:
            raise ValueError("partition_key_value was provided but is empty.")

        actual_pk_paths = CosmosMetadata.get_partition_key_paths(container)
        if pk_path not in actual_pk_paths:
            raise ValueError(
                f"Provided partition_key_path '{pk_path}' does not match any of the "
                f"container's partition key paths: {actual_pk_paths}"
            )

        discovered_values = CosmosMetadata.discover_partition_values(container, pk_path)
        invalid_values = set(pk_values) - set(discovered_values)
        if invalid_values:
            raise ValueError(
                f"Invalid partition_key_value(s): {list(invalid_values)} "
                f"do not exist in container."
            )

        return pk_path, pk_values, user_provided_both
    else:
        pk_paths = CosmosMetadata.get_partition_key_paths(container)
        if pk_paths:
            pk_path = pk_paths[0]
            pk_values = CosmosMetadata.discover_partition_values(container, pk_path)
        else:
            pk_path = None
            pk_values = None

        return pk_path, pk_values, user_provided_both


def process_document_batch(
    batch_docs: List[Dict], batch_start: int
) -> Tuple[pd.DataFrame, Dict]:
    """
    Process a batch of documents into parent and child tables.

    Args:
        batch_docs: List of documents to process
        batch_start: Starting index for this batch

    Returns:
        Tuple of (parent_df, batch_child_tables)
    """
    parent_records = []
    batch_child_tables = {}

    for idx, doc in enumerate(batch_docs):
        parent, children = extract_arrays_iterative_optimized(doc)
        parent_records.append(parent)

        for arr_name, df in children.items():
            batch_child_tables.setdefault(arr_name, []).append(df)

    parent_df = pd.DataFrame(parent_records)
    return parent_df, batch_child_tables


def upload_child_tables(
    service_client,
    adls_file_system: str,
    export_dir: str,
    cosmos_container: str,
    batch_child_tables: Dict,
    child_schemas: Dict,
    batch_num: int,
    mode: str,
    separate_files_per_batch: bool,
) -> Dict:
    """
    Upload child tables to ADLS.

    Args:
        service_client: ADLS service client
        adls_file_system: ADLS container name
        export_dir: Base export directory
        cosmos_container: Cosmos container name
        batch_child_tables: Dictionary of child table DataFrames
        child_schemas: Dictionary of known schemas per table
        batch_num: Current batch number
        mode: 'write' or 'append'
        separate_files_per_batch: Whether to create separate files per batch

    Returns:
        Updated child_schemas dictionary
    """
    for arr_name, df_list in batch_child_tables.items():
        try:
            merged_df = pd.concat(df_list, ignore_index=True)
        except ValueError:
            merged_df = pd.DataFrame(df_list)

        merged_df = clean_dataframe(merged_df)

        if merged_df is not None and not merged_df.empty:
            path_parts = arr_name.split(".")
            final_dir = f"{export_dir}/{'/'.join(path_parts)}"

            if separate_files_per_batch:
                file_name = f"{path_parts[-1]}_batch_{batch_num:03d}.csv"
                child_mode = "write"
                if arr_name in child_schemas:
                    child_schemas[arr_name] = None
            else:
                file_name = f"{path_parts[-1]}.csv"
                child_mode = mode
                if arr_name not in child_schemas:
                    child_schemas[arr_name] = None

            child_schemas[arr_name] = upload_csv_to_adls(
                service_client,
                adls_file_system,
                final_dir,
                file_name,
                merged_df,
                child_mode,
                child_schemas[arr_name],
            )

    return child_schemas


# ============================================================================
# MAIN ACTIVITY FUNCTION
# ============================================================================


@app.activity_trigger(input_name="params")
def process_cosmos_to_adls_activity(params: dict):
    """
    Azure Durable Function activity to export Cosmos DB data to ADLS.

    This function:
    1. Validates connections to Cosmos DB and ADLS
    2. Discovers or validates partition configuration
    3. Streams documents in batches to minimize memory usage
    4. Extracts nested arrays into normalized tables
    5. Uploads data to ADLS as pipe-delimited CSV files
    6. Tracks RU consumption and retry statistics

    Args:
        params: Dictionary containing configuration parameters

    Returns:
        Dictionary with execution results and statistics
    """
    start_time = datetime.utcnow()

    try:
        params = validate_and_extract_params(params)

        cosmos_key = get_secret(
            key_vault_name=params["key_vault"], secretname=params["secret_name"]
        )

        client, database, container = validate_cosmos_connection(
            params["cosmos_url"],
            cosmos_key,
            params["cosmos_db"],
            params["cosmos_container"],
        )

        service_client = validate_adls_connection(
            params["adls_account_name"], params["adls_file_system"]
        )

        pk_path, pk_values, user_provided_both = setup_partition_configuration(
            params, container
        )

        throughput_info = CosmosMetadata.get_throughput_info(
            database, params["cosmos_container"]
        )
        throughput = throughput_info.get("throughput", DEFAULT_THROUGHPUT)

        streaming_reader = StreamingCosmosReader(
            container=container,
            pk_path=pk_path,
            pk_values=pk_values,
            batch_size=params["batch_size"],
            throughput=throughput,
        )

        export_dir = (
            f"{params['adls_directory']}/{params['cosmos_container']}"
            if params["adls_directory"]
            else params["cosmos_container"]
        )

        docs_processed = 0
        total_parent_rows = 0
        child_table_names = set()
        batch_num = 0
        parent_schema = None
        child_schemas = {}

        # Process documents in batches
        for batch_docs in streaming_reader.stream_documents():
            batch_num += 1
            batch_start = docs_processed

            parent_df, batch_child_tables = process_document_batch(
                batch_docs, batch_start
            )
            total_parent_rows += len(parent_df)

            # Upload parent table
            if params["separate_files_per_batch"]:
                parent_file_name = (
                    f"{params['cosmos_container']}_batch_{batch_num:03d}.csv"
                )
                mode = "write"
                parent_schema = None
            else:
                parent_file_name = f"{params['cosmos_container']}.csv"
                mode = "append" if batch_num > 1 else "write"

            parent_schema = upload_csv_to_adls(
                service_client,
                params["adls_file_system"],
                export_dir,
                parent_file_name,
                parent_df,
                mode,
                parent_schema,
            )

            # Upload child tables
            for arr_name in batch_child_tables.keys():
                child_table_names.add(arr_name)

            child_schemas = upload_child_tables(
                service_client,
                params["adls_file_system"],
                export_dir,
                params["cosmos_container"],
                batch_child_tables,
                child_schemas,
                batch_num,
                mode,
                params["separate_files_per_batch"],
            )

            docs_processed += len(batch_docs)

            del parent_df
            del batch_child_tables
            del batch_docs
            gc.collect()

        if docs_processed == 0:
            return {
                "status": "success",
                "message": "No documents found.",
                "docs_processed": 0,
            }

        streaming_stats = streaming_reader.get_stats()
        duration = (datetime.utcnow() - start_time).total_seconds()

        return {
            "status": "success",
            "docs_processed": docs_processed,
            "parent_rows": total_parent_rows,
            "child_tables_created": len(child_table_names),
            "batch_size": params["batch_size"],
            "batches_processed": batch_num,
            "separate_files_per_batch": params["separate_files_per_batch"],
            "duration_seconds": round(duration, 2),
            "auto_detected": not user_provided_both,
            "partition_count": len(pk_values) if pk_values else 0,
            "throughput_ru_per_sec": throughput,
            "total_rus_consumed": streaming_stats.get("total_rus_consumed", 0),
            "avg_ru_per_operation": streaming_stats.get("avg_ru_per_operation", 0),
            "total_retries": streaming_stats.get("total_retries", 0),
            "retries_by_error_type": streaming_stats.get("retries_by_error_type", {}),
        }

    except Exception as e:
        logging.error(f"Activity failed: {str(e)}", exc_info=True)
        return {"status": "error", "message": str(e)}


# ============================================================================
# ORCHESTRATOR AND HTTP TRIGGER
# ============================================================================


@app.orchestration_trigger(context_name="context")
def cosmos_to_adls_orchestrator(context: df.DurableOrchestrationContext):
    """
    Durable orchestrator for the Cosmos to ADLS export process.

    Args:
        context: Durable orchestration context

    Returns:
        Result from the activity function
    """
    params = context.get_input()
    result = yield context.call_activity("process_cosmos_to_adls_activity", params)
    return result


@app.route(route="Cosmos_to_ADLS_V1", methods=["POST"])
@app.durable_client_input(client_name="client")
async def cosmos_to_adls_http_start(req: func.HttpRequest, client) -> func.HttpResponse:
    """
    HTTP trigger to start the Cosmos to ADLS export orchestration.

    Args:
        req: HTTP request with JSON body containing export parameters
        client: Durable orchestration client

    Returns:
        HTTP response with orchestration status URLs
    """
    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse("Invalid JSON body", status_code=400)

    try:
        instance_id = await client.start_new("cosmos_to_adls_orchestrator", None, body)
        response = client.create_check_status_response(req, instance_id)
        return response

    except Exception as e:
        logging.error(f"HTTP start failed: {str(e)}", exc_info=True)
        return func.HttpResponse(
            json.dumps({"error": str(e)}), mimetype="application/json", status_code=500
        )
