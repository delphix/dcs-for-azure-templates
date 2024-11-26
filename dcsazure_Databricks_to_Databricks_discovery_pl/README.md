# dcsazure_Databricks_to_Databricks_prof_pl
## Delphix Compliance Services (DCS) for Azure - Databricks to Databricks Profiling Pipeline

This pipeline will perform automated sensitive data discovery on your Databricks Delta Lake.

### Prerequisites

1. Configure the hosted metadata database and associated Azure SQL service (version `V2024.10.24.0`+).
1. Configure the DCS for Azure REST service.
1. Configure the Azure Data Lake Storage service associated with your Databricks source data.
1. Configure the Azure Databricks Delta Lake service associated with your Databricks Deltalake source data.

### Importing
There are several linked services that will need to be selected in order to perform the profiling and data discovery of
your Databricks instance.

These linked services types are needed for the following steps:

`Azure Blob Storage` (staging) - Linked service associated with a blob storage container that can be used to stage data
when performing a data copy from Snowflake. This will be used for the following steps:
* Schema Discovery From Databricks (Copy data activity)

`Azure Data Lake Storage` (source) - Linked service associated with Databricks source data. This will be used for the
following steps:
* dcsazure_Databricks_to_Databricks_data_discovery_df/Source1MillRowDataSampling (dataFlow)

`Azure Databricks Delta Lake` (source) - Linked service associated with Databricks Delta Lake. This will be used for the
following step:
* dcsazure_Databricks_to_Databricks_for_prof_query_ds (Azure Databricks Delta Lake dataset)

`Azure SQL` (metadata) - Linked service associated with your hosted metadata store. This will be used for the following
steps:
* If Table Contains Data (If Condition activity)
* Check If We Should Rediscover Data (If Condition activity)
* dcsazure_Databricks_to_Databricks_discovery_metadata_ds (Azure SQL Database dataset)
* dcsazure_Databricks_to_Databricks_no_data_discovery_df/Ruleset (dataFlow)
* dcsazure_Databricks_to_Databricks_no_data_discovery_df/WriteToMetadataStore (dataFlow)
* dcsazure_Databricks_to_Databricks_data_discovery_df/MetadataStoreRead (dataFlow)
* dcsazure_Databricks_to_Databricks_data_discovery_df/WriteToMetadataStore (dataFlow)

`REST` (DCS for Azure) - Linked service associated with calling DCS for Azure. This will be used for the following
steps:
* dcsazure_Databricks_to_Databricks_data_discovery_df (dataFlow)

### How It Works
The discovery pipeline has a few stages:
* Check If We Should Rediscover Data
  * If we should, Mark Tables Undiscovered. This is done by updating the metadata store to indicate that tables
    have not had their sensitive data discovered
* Schema Discovery From Databricks
  * We query the `information_schema` for more details about the columns within each table, persisting the data into the
    `discovered_ruleset` table of the metadata store
* Select Discovered Tables
  * After the previous step, we query the database for all tables we found in the specified schema and perform profiling
    and data discovery
* For Each Discovered Table
  * Each table that we've discovered needs to be profiled for data discovery, the process for that is as follows:
    * Get the row count from the table
    * Get details for the table
      * Convert table details into table metadata
    * Check that the table is not empty
      * If the table contains data, run the `dcsazure_Databricks_to_Databricks_data_discovery_df` dataflow, which
        samples the data from the source, and calls the DCS for Azure service to profile the data, persisting the
        metadata we acquired earlier, as well as the results of the data discovery to the metadata store
      * If the table does not contain data, run the `dcsazure_Databricks_to_Databricks_no_data_discovery_df` which
        updates the row count and metadata accordingly, marking the table as discovered according to the setting of the
        `MARK_EMPTY_TABLES_UNDISCOVERED` variable

### Variables

If you have configured your database using the metadata store scripts, these variables will not need editing. If you
have customized your metadata store, then these variables may need editing.

* `P_STAGING_STORAGE_PATH` - This is a path that specifies where we should stage data as it moves through the pipeline
  and should reference a storage container in a storage account (default `staging-container`).
* `P_METADATA_SCHEMA` - This is the schema to be used for in the self-hosted AzureSQL database for storing metadata
  (default `dbo`).
* `P_METADATA_RULESET_TABLE` - This is the table to be used for storing the discovered ruleset (default
  `discovered_ruleset`).
* `METADATA_EVENT_PROCEDURE_NAME` - This is the name of the procedure used to capture pipeline information in the
  metadata data store and sets the discovery state on the items discovered during execution
  (default `insert_adf_discovery_event`).
* `DATASET` - This is used to identify data that belongs to this pipeline in the metadata store (default `DATABRICKS`)
* `NUMBER_OF_ROWS_TO_PROFILE` - This is the number of rows we should select for profiling, note that raising this value
  could cause requests to fail (default `1000`).
* `MARK_EMPTY_TABLES_UNDISCOVERED` - This is used to keep tables that have no rows marked as undiscovered so that should(default `true`).

### Parameters

* `P_SOURCE_CATALOG` - String - This is the catalog in Databricks that may contain sensitive data
* `P_SOURCE_SCHEMA` - String - This is the schema within the above source catalog that may contain sensitive data
* `P_REDISCOVER` - This is a Bool that specifies if we should re-execute the data discovery dataflow for previously
  discovered files that have not had their schema modified (default `true`)
