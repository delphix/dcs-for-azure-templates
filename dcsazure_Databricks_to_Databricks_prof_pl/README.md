# dcsazure_Databricks_to_Databricks_prof_pl
## Delphix Compliance Services (DCS) for Azure - Databricks to Databricks Profiling Pipeline

This pipeline will perform automated sensitive data discovery on your Databricks Delta Lake.

### Prerequisites

1. Configure the hosted metadata database and associated Azure SQL service (version `V2024.01.01.0`+).
1. Configure the DCS for Azure REST service.
1. Configure the Azure Data Lake Storage service associated with your Databricks source data.
1. Configure the Azure Databricks Delta Lake service associated with your Databricks Deltalake source data.

### Importing
There are several linked services that will need to be selected in order to perform the profiling of your Databricks
instance.

These linked services types are needed for the following steps:

`Azure Data Lake Storage` (source) - Linked service associated with Databricks source data. This will be used for the
following steps:
* Schema Discovery From Databricks (Copy data activity)
* dcsazure_Databricks_to_Databricks_prof_df/DatabricksSource1MillRowDataSampling (dataFlow)

`Azure Databricks Delta Lake` (source) - Linked service associated with Databricks Delta Lake. This will be used for the
following step:
* dcsazure_Databricks_to_Databricks_for_prof_query_ds (Azure Databricks Delta Lake dataset)

`Azure SQL` (metadata) - Linked service associated with your hosted metadata store. This will be used for the following
steps:
* dcsazure_Databricks_to_Databricks_metadata_prof_ds (Azure SQL Database dataset)
* dcsazure_Databricks_to_Databricks_prof_df_empty_tables/MetadataStoreRead (dataFlow)
* dcsazure_Databricks_to_Databricks_prof_df_empty_tables/WriteToMetadataStore (dataFlow)
* dcsazure_Databricks_to_Databricks_prof_df/MetadataStoreRead (dataFlow)
* dcsazure_Databricks_to_Databricks_prof_df/WriteToMetadataStore (dataFlow)

`REST` (DCS for Azure) - Linked service associated with calling DCS for Azure. This will be used for the following
steps:
* dcsazure_Databricks_to_Databricks_prof_df (dataFlow)

### How It Works
The profiling pipeline has a few stages:
* Schema Discovery From Databricks
  * We query the `information_schema` for more details about the columns within each table, persisting the data into the
    `discovered_ruleset` table of the metadata store
* Select Discovered Tables
  * After the previous step, we query the database for all tables we found in the specified schema and perform profiling
* For Each Discovered Table
  * Each table that we've discovered needs to be profiled, the process for that is as follows:
    * Get the row count from the table
    * Get details for the table
    * Check that the table is not empty and that the table can be read
      * If the table contains data and can be read, run the `dcsazure_Databricks_to_Databricks_prof_df` dataflow, which
        samples the data from the source, and calls the DCS for Azure service to profile the data, persisting the
        metadata we acquired earlier, as well as the results of the profiling to the metadata store
      * If the table either does not contain data or cannot be read, run the
        `dcsazure_Databricks_to_Databricks_prof_empty_tables_df` which updates the row count and metadata accordingly

### Variables

If you have configured your database using the metadata store scripts, these variables will not need editing. If you
have customized your metadata store, then these variables may need editing.

* `P_STAGING_STORAGE_PATH` - This is a path that specifies where we should stage data as it moves through the pipeline
  and should reference a storage container in a storage account (default `staging-container`)
* `P_METADATA_SCHEMA` - This is the schema to be used for in the self-hosted AzureSQL database for storing metadata (default `dbo`)
* `P_METADATA_RULESET_TABLE` - This is the table to be used for storing the discovered ruleset (default `discovered_ruleset`)

The following variables are used by the pipeline and should not be edited unless directed.
* `unprofilable_tables` - Array - Default value `[]`, this is used internally to track the tables we couldn't profile
* `readerVersion` - Integer - Default value `2`, this is the value of the maximum reader version supported by ADF's
  internal Spark cluster

### Parameters

* `P_SOURCE_CATALOG` - String - This is the catalog in Databricks that contains data we wish to profile
* `P_SOURCE_SCHEMA` - String - This is the schema within the above source catalog that we will profile
