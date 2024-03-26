# dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_prof_pl
## Delphix Compliance Services (DCS) for Azure - Snowflake (Legacy) to Snowflake (Legacy) Profiling Pipeline

This pipeline will perform automated sensitive data discovery on your Snowflake Instance.

### Prerequisites
1. Configure the hosted metadata database and associated Azure SQL service.
1. Configure the DCS for Azure REST service.
1. Configure the Snowflake (Legacy) linked service.
1. Configure the Blob Storage linked service.

### Importing
There are several linked services that will need to be selected in order to perform the profiling of your Snowflake 
instance.

These linked services types are needed for the following steps:

`Azure Blob Storage` (staging) - Linked service associated with a blob storage container that can be used to stage data
when performing a data copy from Snowflake. This will be used for the following steps:
* Schema Discovery From Snowflake (Copy data activity)

`Snowflake (Legacy)` (source) - Linked service associated with unmasked Snowflake data. This will be used for the following
steps:
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_prof_source_ds (Snowflake (Legacy) dataset)
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_prof_df/SnowflakeSource1MillRowDataSampling (dataFlow)

`Azure SQL` (metadata) - Linked service associated with your hosted metadata store. This will be used for the following
steps:
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_prof_metadata_ds (Azure SQL Database dataset),
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_prof_df/MetadataStoreRead (dataFlow),
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_prof_df/WriteToMetadataStore (dataFlow)

`REST` (DCS for Azure) - Linked service associated with calling DCS for Azure. This will be used for the following
  steps:
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_prof_df (dataFlow)

### How It Works

* Schema Discovery From Snowflake
  * Query metadata from Snowflake `information_schema` to identify tables and columns in the Snowflake instance
* Select Discovered Tables
  * After persisting the metadata to the metadata store, collect the list of discovered tables
* For Each Discovered Table
  * Call the `dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_prof_df` data flow

### Parameters
* `P_SOURCE_DATABASE` - This is the catalog in Snowflake that we will profile
* `P_SOURCE_SCHEMA` - This is the schema within the above catalog that we will profile
* `P_STAGING_STORAGE_PATH` - This is the name of a storage container in
* `P_METADATA_SCHEMA` - This is the schema to be used for in the self-hosted AzureSQL database for storing metadata (default `dbo`)
* `P_METADATA_RULESET_TABLE` - This is the table to be used for storing the discovered ruleset (default `discovered_ruleset`)


