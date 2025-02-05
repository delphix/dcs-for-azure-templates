# dcsazure_ADLS_to_ADLS_parquet_discovery_pl
## Delphix Compliance Services (DCS) for Azure - ADLS to ADLS Parquet Discovery Pipeline

This pipeline will perform automated sensitive data discovery on your parquet data in Azure Data Lake Storage (ADLS).

### Prerequisites

1. Configure the hosted metadata database and associated Azure SQL service (version `V2025.01.15.0`).
1. Configure the DCS for Azure REST service.
1. Configure the Azure Data Lake Storage (Gen 2) service associated with your ADLS source data.

### Importing
There are several linked services that will need to be selected in order to perform the profiling and data discovery
of your delimited text ADLS data.

These linked services types are needed for the following steps:

`Azure Data Lake Storage` (source) - Linked service associated with ADLS source data. This will be used for the
following steps:
* dcsazure_ADLS_to_ADLS_parquet_container_and_directory_discovery_ds (Parquet dataset)
* dcsazure_ADLS_to_ADLS_parquet_sub_directory_discovery_ds (Parquet dataset)
* dcsazure_ADLS_to_ADLS_parquet_file_schema_discovery_ds (Parquet dataset)
* dcsazure_ADLS_to_ADLS_parquet_data_discovery_df/ADLSDelta1MillRowDataSampling (dataFlow)

`Azure SQL` (metadata) - Linked service associated with your hosted metadata store. This will be used for the following
steps:
* Persist Metadata To Database (Stored procedure activity)
* Update Discovery State (Stored procedure activity)
* Update Discovery State Failed (Stored procedure activity)
* Check If We Should Rediscover Data (If Condition activity)
* dcsazure_ADLS_to_ADLS_parquet_metadata_discovery_ds (Azure SQL Database dataset)
* dcsazure_ADLS_to_ADLS_parquet_data_discovery_df/MetadataStoreRead (dataFlow)
* dcsazure_ADLS_to_ADLS_parquet_data_discovery_df/WriteToMetadataStore (dataFlow)

`REST` (DCS for Azure) - Linked service associated with calling DCS for Azure. This will be used for the following
steps:
* dcsazure_ADLS_to_ADLS_parquet_data_discovery_df (dataFlow)


### How It Works
The discovery pipeline has a few stages:
* Check If We Should Rediscover Data
  * If we should, Mark Tables Undiscovered. This is done by updating the metadata store to indicate that tables
    have not had their sensitive data discovered
* Get Folder List
  * Using a `Get Metadata` activity, collect the items under the specified `P_SOURCE_PATH` directory
  * For each item in that list (which should be a list of folders)
    * Delete the `_SUCCESS` file in this directory
    * Get Files List - Get the child items and the item type of the files in this directory
    * Get File Structure of First File - Using the Get Metadata step activity, get the structure for the first file from
      the files list
    * Persist Metadata To Database - Call a stored procedure to store the resulting structure in the metadata store
* Select Discovered Tables - In this case, we consider the table to be all parquet files in the found subdirectory
  * After the previous step, we query the database for all tables parquet files in each distinct path of the
    storage container and perform profiling for sensitive data discovery in those folders that have not yet been
    discovered.
* For Each Discovered Table
  * Each table that we've discovered needs to be profiled, the process for that is as follows:
    * Run the data discovery dataflow with the appropriate parameters

### Variables

If you have configured your database using the metadata store scripts, these variables will not need editing. If you
have customized your metadata store, then these variables may need editing.

* `METADATA_SCHEMA` - This is the schema to be used for in the self-hosted AzureSQL database for storing metadata
  (default `dbo`).
* `METADATA_RULESET_TABLE` - This is the table to be used for storing the discovered ruleset (default
  `discovered_ruleset`).
* `FILE_STRUCTURE_PROCEDURE_NAME` - This is the stored procedure on the AzureSQL database that can
  accept, in part, the file structure from the `Get Metadata` ADF pipeline Activity
  (default `get_columns_from_parquet_file_structure_sp`).
* `DATASET` - This is used to identify data that belongs to this pipeline in the metadata store
  (default `ADLS-PARQUET`).
* `METADATA_EVENT_PROCEDURE_NAME` - This is the name of the procedure used to capture pipeline information in the 
  metadata data store and sets the discovery state on the items discovered during execution
  (default `insert_adf_discovery_event`).
* `NUMBER_OF_ROWS_TO_PROFILE` - This is the number of rows we should select for profiling, note that raising this value
  could cause requests to fail (default `1000`).

### Parameters

* `P_STORAGE_CONTAINER_TO_PROFILE` - This is the storage container whose contents need review
* `P_SOURCE_PATH` - This is the directory within the storage container that contains additional folders that will be
  scanned
* `P_REDISCOVER` - This is a Bool that specifies if we should re-execute the data discovery dataflow for previously
  discovered files that have not had their schema modified (default `true`)