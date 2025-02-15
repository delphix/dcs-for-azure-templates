# dcsazure_AzureSQL_MI_to_AzureSQL_MI_discovery_pl
## Delphix Compliance Services (DCS) for Azure - AzureSQL_MI to AzureSQL_MI Discovery Pipeline

This pipeline will perform automated sensitive data discovery on your AzureSQL Managed Instance.

### Prerequisites
1. Configure the hosted metadata database and associated Azure SQL service (version `V2025.02.04.0`+).
1. Configure the DCS for Azure REST service.
1. Configure the Azure SQL MI linked service.

### Importing
There are several linked services that will need to be selected in order to perform the profiling and data discovery of your Azure SQL Managed Instance.

These linked services types are needed for the following steps:


`Azure SQL MI` (source) - Linked service associated with unmasked Azure SQL MI data. This will be used for the following
steps:
* dcsazure_AzureSQL_MI_to_AzureSQL_MI_discovery_source_ds (AzureSQL_MI Database dataset)
* dcsazure_AzureSQL_MI_to_AzureSQL_MI_discovery_df/Source1MillRowDataSampling (dataFlow)

`Azure SQL` (metadata) - Linked service associated with your hosted metadata store. This will be used for the following
steps:
* Update Discovery State (Stored procedure activity)
* Update Discovery State Failed (Stored procedure activity)
* Check If We Should Rediscover Data (If Condition activity)
* dcsazure_AzureSQL_MI_to_AzureSQL_MI_discovery_metadata_ds (Azure SQL Database dataset),
* dcsazure_AzureSQL_MI_to_AzureSQL_MI_discovery_df/MetadataStoreRead (dataFlow),
* dcsazure_AzureSQL_MI_to_AzureSQL_MI_discovery_df/WriteToMetadataStore (dataFlow)

`REST` (DCS for Azure) - Linked service associated with calling DCS for Azure. This will be used for the following
  steps:
* dcsazure_AzureSQL_MI_to_AzureSQL_MI_discovery_df (dataFlow)

### How It Works

* Check If We Should Rediscover Data
  * If we should, Mark Tables Undiscovered. This is done by updating the metadata store to indicate that tables have not had their sensitive data discovered
* Schema Discovery From Azure SQL MI
  * Query metadata from Azure SQL MI `information_schema` to identify tables and columns in the Azure SQL Managed Instance
* Select Discovered Tables
  * After persisting the metadata to the metadata store, collect the list of discovered tables
* For Each Discovered Table
  * Call the `dcsazure_AzureSQL_MI_to_AzureSQL_MI_discovery_df` data flow

### Variables

If you have configured your database using the metadata store scripts, these variables will not need editing. If you
have customized your metadata store, then these variables may need editing.

* `METADATA_SCHEMA` - This is the schema to be used for in the self-hosted AzureSQL database for storing metadata
  (default `dbo`)
* `METADATA_RULESET_TABLE` - This is the table to be used for storing the discovered ruleset
  (default `discovered_ruleset`)
* `DATASET` - This is used to identify data that belongs to this pipeline in the metadata store (default `AZURESQL-MI`)
* `METADATA_EVENT_PROCEDURE_NAME` - This is the name of the procedure used to capture pipeline information in the metadata data store and sets the discovery state on the items discovered during execution (default `insert_adf_discovery_event`).
* `NUMBER_OF_ROWS_TO_PROFILE` - This is the number of rows we should select for profiling, note that raising this value could cause requests to fail (default `1000`).

### Parameters

* `P_SOURCE_DATABASE` - String - This is the database in Azure SQL MI that may contain sensitive data
* `P_SOURCE_SCHEMA` - String - This is the schema within the above source database that may contain sensitive data
* `P_REDISCOVER` - This is a Bool that specifies if we should re-execute the data discovery dataflow for previously discovered tables that have not had their schema modified (default `true`)
