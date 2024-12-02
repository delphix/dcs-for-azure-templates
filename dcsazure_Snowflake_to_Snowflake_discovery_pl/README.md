# dcsazure_Snowflake_to_Snowflake_discovery_pl
## Delphix Compliance Services (DCS) for Azure - Snowflake to Snowflake Discovery Pipeline

This pipeline will perform automated sensitive data discovery on your Snowflake Instance.

### Prerequisites
1. Configure the hosted metadata database and associated Azure SQL service (version `V2024.10.24.0`+).
1. Configure the DCS for Azure REST service.
1. Configure the Snowflake linked service.
  * It is helpful for the linked service to be parameterized with the following parameters:
    * `LS_DATABASE` - this is the database name in the linked service
    * `LS_WAREHOUSE` - this is the warehouse name in the linked service
    * `LS_ROLE` - this is the role the linked service should use
1. Configure the Blob Storage linked service to the container named `staging-container`.

### Importing
There are several linked services that will need to be selected in order to perform the profiling and data discovery of
your Snowflake instance.

These linked services types are needed for the following steps:

`Azure Blob Storage` (staging) - Linked service associated with a blob storage container that can be used to stage data
when performing a data copy from Snowflake. This will be used for the following steps:
* Schema Discovery From Snowflake (Copy data activity)

`Snowflake` (source) - Linked service associated with unmasked Snowflake data. This will be used for the following
steps:
* dcsazure_Snowflake_to_Snowflake_discovery_source_ds (Snowflake dataset)
* dcsazure_Snowflake_to_Snowflake_data_discovery_df/Source1MillRowDataSampling (dataFlow)

`Azure SQL` (metadata) - Linked service associated with your hosted metadata store. This will be used for the following
steps:
* Update Discovery State (Stored procedure activity)
* Update Discovery State Failed (Stored procedure activity)
* Check If We Should Rediscover Data (If Condition activity)
* dcsazure_Snowflake_to_Snowflake_discovery_metadata_ds (Azure SQL Database dataset)
* dcsazure_Snowflake_to_Snowflake_data_discovery_df/MetadataStoreRead (dataFlow)
* dcsazure_Snowflake_to_Snowflake_data_discovery_df/WriteToMetadataStore (dataFlow)

`REST` (DCS for Azure) - Linked service associated with calling DCS for Azure. This will be used for the following
steps:
* dcsazure_Snowflake_to_Snowflake_data_discovery_df (dataFlow)

### How It Works
The discovery pipeline has a few stages:
* Check If We Should Rediscover Data
  * If we should, Mark Tables Undiscovered. This is done by updating the metadata store to indicate that tables
    have not had their sensitive data discovered
* Schema Discovery From Snowflake
  * Query metadata from Snowflake `information_schema` to identify tables and columns in the Snowflake instance
* Select Discovered Tables
  * After persisting the metadata to the metadata store, collect the list of discovered tables
* For Each Discovered Table
  * Call the `dcsazure_Snowflake_to_Snowflake_data_discovery_df` data flow


### Variables

If you have configured your database using the metadata store scripts, these variables will not need editing. If you
have customized your metadata store, then these variables may need editing.

* `STAGING_STORAGE_PATH` - This is a path that specifies where we should stage data as it moves through the pipeline
  and should reference a storage container in a storage account (default `staging-container`)
* `METADATA_SCHEMA` - This is the schema to be used for in the self-hosted AzureSQL database for storing metadata
  (default `dbo`)
* `METADATA_RULESET_TABLE` - This is the table to be used for storing the discovered ruleset
  (default `discovered_ruleset`)
* `DATASET` - This is used to identify data that belongs to this pipeline in the metadata store (default `SNOWFLAKE`)
* `METADATA_EVENT_PROCEDURE_NAME` - This is the name of the procedure used to capture pipeline information in the
  metadata data store and sets the discovery state on the items discovered during execution
  (default `insert_adf_discovery_event`).
* `NUMBER_OF_ROWS_TO_PROFILE` - This is the number of rows we should select for profiling, note that raising this value
  could cause requests to fail (default `1000`).
* `SNOWFLAKE_WAREHOUSE` - This is the name of the Snowflake warehouse that should be used for executing queries - this
  is fed into the parameters of the linked service (default `DEFAULT`)
* `SNOWFLAKE_ROLE` - This is the Snowflake role that is used for executing queries against - this is fed into the
  parameters of the linked service, and this role must have permission to access the information schema (default
  `SYSADMIN`)

### Parameters

* `P_SOURCE_DATABASE` - String - This is the database in Snowflake that may contain sensitive data
* `P_SOURCE_SCHEMA` - String - This is the schema within the above source database that may contain sensitive data
* `P_REDISCOVER` - This is a Bool that specifies if we should re-execute the data discovery dataflow for previously
  discovered files that have not had their schema modified (default `true`)