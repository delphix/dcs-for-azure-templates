# dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_mask_pl
## Delphix Compliance Services (DCS) for Azure - Cosmos NoSQL to Cosmos NoSQL Masking Pipeline

This pipeline will perform masking of your Azure Cosmos DB (NoSQL API) data.

### Prerequisites

1. Configure the hosted metadata database and associated Azure SQL service (version `V2026.02.02.0`).
1. Configure the DCS for Azure REST service.
1. Configure the Azure Data Lake Storage service associated with your ADLS source data.
1. Configure the Azure Data Lake Storage service associated with your ADLS sink data.
1. [Assign a managed identity with a storage blob data contributor role for the Data Factory instance within the storage account](https://help.delphix.com/dcs/current/content/docs/configure_adls_delimited_pipelines.htm).
1. [Repeat the above step for the Azure Function by assigning a managed identity with the Storage Blob Data Contributor role](External_Document_URL).
1. [Configure an Azure Function for exporting masked data from Azure Data Lake Storage(ADLS) data to Cosmos DB](External_Document_URL) (version `ADLS_to_Cosmos_V1`).
1. [Configure an Azure Key Vault for storing the Cosmos DB access key and assign a managed identity with the Key Vault Secrets User role to the Azure Function](External_Document_URL).


### Importing
There are several linked services that will need to be selected in order to perform the masking of your Cosmos NoSQL data.

These linked service types are needed for the following steps:

`Azure Function` (Cosmos to ADLS) – Linked service associated with exporting Cosmos DB data to ADLS. This will be used for the following steps:
* Check If We Should Copy Data To Cosmos (If Condition activity)

`Azure Data Lake Storage Gen2` (Source) – Linked service associated with the ADLS account used for staging Source Cosmos DB data. This will be used for the following steps:
* dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_filter_test_utility_df/Source (dataFlow),
* dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_container_and_directory_mask_ds (DelimitedText dataset),
* dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_unfiltered_mask_df/Source (dataFlow),
* dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_filtered_mask_df/Source (dataFlow),
* dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_copy_df/Source (dataFlow)

`Azure Data Lake Storage Gen2` (Sink) – Linked service associated with the ADLS account used for staging Sink Cosmos DB data. This will be used for the following steps:
* dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_filter_test_utility_df/Sink (dataFlow),
* dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_unfiltered_mask_df/Sink (dataFlow),
* dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_filtered_mask_df/Sink (dataFlow),
* dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_copy_df/Sink (dataFlow)

`Azure SQL` (metadata) – Linked service associated with your hosted metadata store. This will be used for the following steps:
* Check ADLS To Cosmos Status (If Condition activity),
* Update logs If Copy Data To ADLS Is True (If Condition activity),
* Check For Conditional Masking (If Condition activity),
* Check For Conditional Masking (If Condition activity),
* Check For Conditional Masking (If Condition activity),
* Check For Conditional Masking (If Condition activity),
* If Copy Via Dataflow (If Condition activity),
* If Copy Via Dataflow (If Condition activity),
* If Copy Via Dataflow (If Condition activity),
* If Copy Via Dataflow (If Condition activity),
* Check If We Should Reapply Mapping (If Condition activity),
* Configure Masked Status (Script activity),
* dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_metadata_mask_ds (Azure SQL Database dataset)

`REST` (DCS for Azure) – Linked service associated with calling DCS for Azure. This will be used for the following steps:
* dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_unfiltered_mask_df (dataFlow),
* dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_filtered_mask_df (dataFlow)

### How It Works

* Execute ADLS Masking Pipeline
  * Check If We Should Reapply Mapping
    * If we should, Mark Table Mapping Incomplete. This is done by updating the metadata store to indicate that tables have not had their mapping applied
  * Select Directories We Should Purge
    * Select sink directories with an incomplete mapping and based on the value of P_TRUNCATE_SINK_BEFORE_WRITE, create a list of directories that we should purge
      * For Each Directory To Purge:
        * Check For The Directory
        * If the directory exists, delete everything in that directory
  * Select Tables Without Required Masking. This is done by querying the metadata store.
    * Filter If Copy Unmask Enabled. This is done by applying a filter based on the value of P_COPY_UNMASKED_TABLES
      * For Each Table With No Masking. Provided we have any rows left after applying the filter
        * If Copy Via Dataflow - based on the value of P_COPY_USE_DATAFLOW
          * If the data flow is to be used for copy then call `dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_copy_df`
            * Update the mapped status based on the success of this dataflow, and fail accordingly
          * If the data flow is not to be used for copy, then use a copy activity
            * Update the mapped status based on the success of this dataflow, and fail accordingly
  * Select Tables That Require Masking. This is done by querying the metadata store. This will provide a list of tables that need masking, and if they need to be masked leveraging conditional algorithms, the set of required filters.
    * Configure Masked Status. Set the masked status based on the defined filters that need to be applied for the table to be marked as completely mapped.
    * For Each Table To Mask
      * Check if the table must be masked with a filter condition
        * If no filter needs to be applied:
          * Call the `dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_unfiltered_mask_df` data flow, passing in parameters as generated by the Lookup Masking Parameters activity
          * Update the mapped status based on the success of this dataflow, and fail accordingly
        * If a filter needs to be applied:
          * Call the `dcsazure_Cosmos_NoSQL_to_Cosmos_NoSQL_ADLS_delimited_filterd_mask_df data` flow, passing in parameters as generated by the Lookup Masking Parameters activity and the filter as determined by the output of For Each Table To Mask
          * Update the mapped status based on the success of this dataflow, and fail accordingly
  * Note that there is a deactivated activity Test Filter Conditions that exists in order to support importing the filter test utility dataflow, this is making it easier to test writing filter conditions leveraging a dataflow debug session
* Check If We Should Copy Data To Cosmos
  * Copy ADLS Data to Cosmos
    * Export documents from ADLS to Cosmos DB container using an Azure Function
* Until ADLS to Cosmos Durable Function is Success
  * Poll the Azure Function execution status until the export completes
* Check ADLS to Cosmos Status
  * Validate that the export completed successfully, otherwise fail the pipeline

### Variables

If you have configured your database using the metadata store scripts, these variables will not need editing. If you
have customized your metadata store, then these variables may need editing.

* `METADATA_SCHEMA` – Schema used for storing metadata (default `dcsazure_metadata_store`)
* `METADATA_RULESET_TABLE` – Table used for storing discovered rulesets (default `discovered_ruleset`)
* `METADATA_SOURCE_TO_SINK_MAPPING_TABLE` – Table defining source-to-sink mappings (default `adf_data_mapping`)
* `METADATA_ADF_TYPE_MAPPING_TABLE` – Table mapping dataset data types to ADF data types (default `adf_type_mapping`)
* `TARGET_BATCH_SIZE` – Target number of rows per batch during masking (default `50000`)
* `DATASET` – Dataset identifier used in the metadata store (default `COSMOS_NOSQL`)
* `CONDITIONAL_MASKING_RESERVED_CHARACTER` – Reserved character used for shorthand column references in conditional masking filters (default `%`)
* `METADATA_EVENT_PROCEDURE_NAME` – Stored procedure used to capture masking execution events and update masking state (default `insert_adf_masking_event`)
* `METADATA_MASKING_PARAMS_PROCEDURE_NAME` – Stored procedure used to generate Cosmos NoSQL masking parameters (default `generate_cosmos_no_sql_masking_parameters`)
* `COLUMN_WIDTH_ESTIMATE` – Estimated column width used for batch size calculation when schema width is unavailable (default `1000`)
* `STORAGE_ACCOUNT` – Azure Data Lake Storage account name used for staging masked data
* `COSMOS_TO_ADLS_BATCH_SIZE` - This is the number of rows per batch while copying the data from Cosmos NoSQL database to ADLS (default `50000`)
* `COSMOS_KEY_VAULT_NAME` – Name of the Azure Key Vault that stores the Cosmos DB access key
* `COSMOS_SECRET_NAME` – Name of the secret in Key Vault containing the Cosmos DB access key

### Parameters

* `P_COSMOS_SOURCE_DATABASE` – String – Source Cosmos DB database name
* `P_COSMOS_SINK_DATABASE` – String – Target Cosmos DB database name for masked data
* `P_COSMOS_SINK_ENDPOINT` – String – Cosmos DB endpoint URL for the masked target
* `P_COSMOS_SINK_KEY` – SecureString – Cosmos DB access key for the masked target
* `P_COSMOS_CONTAINER` – String – Cosmos DB container name
* `P_ADLS_SOURCE_CONTAINER` – String – ADLS filesystem/container for unmasked data
* `P_ADLS_SINK_CONTAINER` – String – ADLS filesystem/container for masked data
* `P_ADLS_SINK_STORAGE_KEY` – SecureString – ADLS storage account key
* `P_FAIL_ON_NONCONFORMANT_DATA` – Bool – Fail pipeline if non-conformant data is encountered (default `true`)
* `P_COPY_UNMASKED_TABLES` – Bool – Copy data even when no masking rules are defined (default `true`)
* `P_COPY_USE_DATAFLOW` – Bool – Use dataflow instead of copy activity when copying data (default `false`)
* `P_TRUNCATE_SINK_BEFORE_WRITE` – Bool – Truncate target Cosmos container before writing masked data (default `true`)
* `P_REAPPLY_MAPPING` – Bool – Reapply source-to-sink mapping before masking (default `true`)
* `P_COPY_ADLS_DATA_TO_COSMOS` – Bool – Specifies whether data should be copied from ADLS to Cosmos DB (default `true`)

### Notes

* When creating the Azure Function used for Cosmos DB export, choose the hosting plan based on data volume:
  * The default timeout for the Consumption plan is 10 minutes.
  * The default timeout for the Flex Consumption plan is 60 minutes.
  * For containers with millions of documents, it is recommended to use an App Service plan with at least 4 GB of memory.
    * This allows the function to run without time limits until all records are processed.
    * This approach is especially recommended when the target container has low RU provisioning or a very large number of records.
    * The Azure Function timeout is explicitly configured to **12 hours** using the `functionTimeout` setting to support large Cosmos DB containers.
* If the Azure Function fails with out-of-memory errors (exit code 137), adjust the `COSMOS_TO_ADLS_BATCH_SIZE` to reduce memory pressure.
* Update the `COSMOS_KEY_VAULT_NAME` and `COSMOS_SECRET_NAME` variables to match the target Cosmos DB account before triggering the pipeline.
* The `source_metadata` column in the `discovered_ruleset` table can be used to determine which partition data is currently staged in ADLS prior to running the masking pipeline. For example:
  ```sql
  SELECT
      d.dataset,
      d.specified_database,
      d.specified_schema,
      d.identified_table,
      d.identified_column,
      pv.value AS partition_value
  FROM <METADATA_SCHEMA>.<METADATA_RULESET_TABLE> d
  CROSS APPLY OPENJSON(d.source_metadata, '$.partition_values') pv
  WHERE d.dataset = 'COSMOS_NOSQL'
    AND d.specified_schema LIKE 'COSMOS-DATABASE/COSMOS-CONTAINER-NAME%';
* The Cosmos container name must be the same in both the source and sink databases for the masking pipeline to function correctly.
* Ensure that all schemas associated with the Cosmos DB container are added to the `adf_data_mapping` table before triggering the masking pipeline.
* If a column exists in some records but is missing in others, the pipeline will still include that column in the masked output, populating `null` values for records where the column was not originally present.
* Conditional masking is not supported by this template.

### Limitations and Workarounds

* **Array of strings masking**
  * When a Cosmos DB document contains an array of primitive string values (for example, an array of email addresses), the discovery and masking pipeline treats the entire array as a single string value.
  * Applying a string-based masking algorithm (such as `dlpx-core:Email Unique`) results in a single masked string, causing the original array structure and data type to be lost.

* **Reason for the limitation**
  * Arrays of primitive values do not contain explicit keys and are indexed only by position.
  * During flattening to a delimited format, there is no reliable way to map positional array elements to distinct columns.
  * As a result, individual array elements cannot be independently discovered or masked using standard DCS templates.

* **Workaround**
  * When masking arrays of strings, use an algorithm that preserves the overall structure of the value.
  * The built-in `dlpx-core:CM Alpha-Numeric` algorithm can be used to mask array elements without relying on schema-aware decomposition.
  * Alternatively, a custom masking algorithm can be created using a regex-based approach to decompose the array and apply masking to individual elements while preserving the array format.
  * Regex-based decomposition requires an upper bound on the expected number of elements in the array, as capture groups must be defined in advance.
  * If the array contains fewer elements than expected, empty capture groups may cause the algorithm to fail.
  * If the array contains more elements than defined capture groups, additional values may not be masked and the algorithm may fall back or fail.
  * This workaround is suitable only when the maximum number of elements in the array is known and bounded.
