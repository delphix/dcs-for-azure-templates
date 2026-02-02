# dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_discovery_pl
## Delphix Compliance Services (DCS) for Azure - Cosmos NoSQL to Cosmos NoSQL Discovery Pipeline

This pipeline will perform automated sensitive data discovery on your Azure Cosmos DB (NoSQL API) containers.

### Prerequisites

1. Configure the hosted metadata database and associated Azure SQL service (version `V2026.02.02.0`).
1. Configure the DCS for Azure REST service.
1. Configure the Azure Data Lake Storage (Gen 2) service for staging exported Cosmos DB data.
1. [Assign a managed identity with a storage blob data contributor role for the Data Factory instance within the storage account](https://help.delphix.com/dcs/current/Content/DCSDocs/Configure_ADLS_delimited_pipelines.htm).
1. [Repeat the above step for the Azure Function by assigning a managed identity with the Storage Blob Data Contributor role](External_Document_URL).
1. [Configure an Azure Function for exporting Cosmos DB data to Azure Data Lake Storage (ADLS)](External_Document_URL) (version `Cosmos_to_ADLS_V1`).
1. [Configure an Azure Key Vault for storing the Cosmos DB access key and assign a managed identity with the Key Vault Secrets User role to the Azure Function](External_Document_URL).

### Importing
There are several linked services that will need to be selected in order to perform the profiling and data discovery of your Cosmos NoSQL containers.

These linked service types are needed for the following steps:

`Azure Function` (Cosmos to ADLS) - Linked service associated with exporting Cosmos DB data to ADLS. This will be used for the following steps:
* Check If We Should Copy Data To ADLS (If Condition activity)

`Azure Data Lake Storage Gen2` (staging) - Linked service associated with the ADLS account used for staging Cosmos DB exports. This will be used for the following steps:
* dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_container_and_directory_discovery_ds (DelimitedText dataset),
* dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_data_discovery_df/SourceData1MillRowDataSampling (dataFlow),
* dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_header_file_schema_discovery_ds (DelimitedText dataset)

`Azure SQL` (metadata) - Linked service associated with your hosted metadata store. This will be used for the following steps:
* Set Source Metadata (Script activity),
* Check Cosmos To ADLS Status (If Condition activity),
* Check If We Should Update Copy State (If Condition activity),
* Update Discovery State (Stored procedure activity),
* Update Discovery State Failed (Stored procedure activity),
* Check If We Should Rediscover Data (If Condition activity),
* dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_metadata_discovery_ds (Azure SQL Database dataset),
* dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_data_discovery_df/MetadataStoreRead (dataFlow),
* dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_data_discovery_df/WriteToMetadataStore (dataFlow),
* Persist Metadata To Database (Stored procedure activity)

`REST` (DCS for Azure) - Linked service associated with calling DCS for Azure. This will be used for the following steps:
* dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_data_discovery_df (dataFlow)

### How It Works

* Check If We Should Copy Data To ADLS
  * Copy Cosmos Data to ADLS
    * Export documents from a Cosmos DB container to ADLS using an Azure Function
* Until Cosmos to ADLS Durable Function is Success
  * Poll the Azure Function execution status until the export completes
* Check Cosmos to ADLS Status
  * Validate that the export completed successfully, otherwise fail the pipeline
* Discover Sensitive Data
  * Check If We Should Rediscover Data
    * If we should, Mark Tables Undiscovered. This is done by updating the metadata store to indicate that tables have not had their sensitive data discovered
  * Identify Nested Schemas
    * Using the child pipeline `dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_container_and_directory_discovery_pl`, we collect all the identified schemas under the specified directory.
    * For each item in that list, identify if the schema of the files in that child directory is expected to be homogeneous.
  * Schema Discovery
    * For each of the directories with homogeneous schema, identify the schema for each file with one of the suffixes to scan, determine the structure of the file by calling the child `dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_file_discovery_pl` pipeline with the appropriate parameters.
  * Select Discovered Tables - In this case, we consider the table to be items with the same schema.
    * After the previous step, we query the database for all tables (file suffixes within each distinct path of the storage container) and perform profiling for sensitive data discovery in those files that have not yet been discovered.
  * ForEach Discovered Table
    * Each table that we've discovered needs to be profiled, the process for that is as follows:
      * Run the `dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_data_discovery_df` dataflow with the appropriate parameters.
* Set Source Metadata
  * After sensitive data discovery completes successfully, update the metadata store to enrich the discovered objects with Cosmos-specific source context, including logical partition values, ensuring discovery results are partition-aware and accurately traceable.


### Variables

If you have configured your database using the metadata store scripts, these variables will not need editing. If you have customized your metadata store, then these variables may need editing.

* `METADATA_SCHEMA` - This is the schema to be used for storing metadata (default `dcsazure_metadata_store`)
* `METADATA_RULESET_TABLE` - This is the table to be used for storing the discovered ruleset (default `discovered_ruleset`)
* `DATASET` - This is used to identify data that belongs to this pipeline in the metadata store (default `COSMOS_NOSQL`)
* `METADATA_EVENT_PROCEDURE_NAME` - This is the name of the procedure used to capture pipeline execution information and set discovery state (default `insert_adf_discovery_event`)
* `NUMBER_OF_ROWS_TO_PROFILE` - This is the number of rows selected for profiling (default `1000`)
* `COLUMNS_FROM_ADLS_FILE_STRUCTURE_PROCEDURE_NAME` - Stored procedure used to infer columns from delimited files (default `get_columns_from_delimited_file_structure_sp`)
* `STORAGE_ACCOUNT` - Azure Data Lake Storage account name
* `MAX_LEVELS_TO_RECURSE` - Maximum directory recursion depth (default `10`)
* `COSMOS_TO_ADLS_BATCH_SIZE` - This is the number of rows per batch while copying the data from Cosmos NoSQL database to ADLS (default `50000`)
* `COSMOS_KEY_VAULT_NAME` – Name of the Azure Key Vault that stores the Cosmos DB access key
* `COSMOS_SECRET_NAME` – Name of the secret in Key Vault containing the Cosmos DB access key

### Parameters

* `P_SOURCE_DATABASE` - String - Cosmos DB database name
* `P_SOURCE_CONTAINER` - String - Cosmos DB container name
* `P_COSMOS_ENDPOINT` - String - Cosmos DB endpoint URL
* `P_LOGICAL_PARTITION_KEY` - String - Logical partition key path
* `P_LOGICAL_PARTITION_ID` - Array - Optional logical partition values
* `P_STORAGE_ACCOUNT_NAME` - String - Azure Data Lake Storage account name
* `P_ADLS_CONTAINER_NAME` - String - Azure Data Lake Storage filesystem / container name
* `P_REDISCOVER` - Bool - Specifies if previously discovered data should be rediscovered (default `true`)
* `P_COPY_COSMOS_DATA_TO_ADLS` – Bool – Specifies whether data should be copied from Cosmos DB to ADLS (default `true`)

### Notes

* When creating the Azure Function used for Cosmos DB export, choose the hosting plan based on data volume:
  * The default timeout for the Consumption plan is 10 minutes.
  * The default timeout for the Flex Consumption plan is 60 minutes.
  * For containers with millions of documents, it is recommended to use an App Service plan with at least 4 GB of memory.
    * This allows the function to run without time limits until all records are processed.
    * This approach is especially recommended when the target container has low RU provisioning or a very large number of records.
* If the Azure Function fails with out-of-memory errors (exit code 137), adjust the `COSMOS_TO_ADLS_BATCH_SIZE` to reduce memory pressure.
* Update the `COSMOS_KEY_VAULT_NAME` and `COSMOS_SECRET_NAME` variables to match the target Cosmos DB account before triggering the pipeline.
* To filter records by Cosmos DB partition, both `P_LOGICAL_PARTITION_KEY` and `P_LOGICAL_PARTITION_ID` must be provided.
* This pipeline operates at the container level. When an array of `P_LOGICAL_PARTITION_ID` is specified, only data for those partitions is exported to ADLS and included in discovery and masking.
* The `source_metadata` column in the `discovered_ruleset` table can be used to identify which partition data is currently staged in ADLS prior to running the masking pipeline. For example:
  ```sql
  SELECT
      d.dataset,
      d.specified_database,
      d.specified_schema,
      d.identified_table,
      d.identified_column,
      pv.value AS partition_value
  FROM discovered_ruleset d
  CROSS APPLY OPENJSON(d.source_metadata, '$.partition_values') pv
  WHERE d.dataset = 'COSMOS_NOSQL'
    AND d.specified_schema LIKE 'COSMOS-DATABASE/COSMOS-CONTAINER-NAME%';
* If the pipeline is rerun for the same container with a different set of `P_LOGICAL_PARTITION_ID`, the data in ADLS is overwritten with the new partition’s data, and the corresponding partition metadata in the ruleset is updated.
* Historical information about previously discovered partitions can be obtained from the `adf_events` log table.
