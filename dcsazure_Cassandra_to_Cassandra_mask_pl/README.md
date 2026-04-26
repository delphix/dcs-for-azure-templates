# dcsazure_Cassandra_to_Cassandra_mask_pl
## Delphix Compliance Services (DCS) for Azure - Cassandra to Cassandra Masking Pipeline

This pipeline will mask Cassandra data that has been staged to ADLS and then write that masked data back to a Cassandra instance.

### Prerequisites

1. Configure the hosted metadata database and associated Azure SQL linked service (version `V2026.02.25.0`).
1. Configure the DCS for Azure REST linked service.
1. Configure the Azure Data Lake Storage linked service associated with your ADLS source data.
1. Configure the Azure Data Lake Storage linked service associated with your ADLS sink data.
1. [Assign a managed identity with a Storage Blob Data Contributor role for the Data Factory instance within the storage account](https://help.delphix.com/dcs/current/content/docs/configure_adls_delimited_pipelines.htm).
1. [Create an Azure Function app for exporting Cassandra DB data to Azure Data Lake Storage (ADLS)](https://help.delphix.com/dcs/current/content/docs/create_an_azure_function.htm) (version `Cassandra_to_ADLS_V1`).
1. [Assign a managed identity with a Storage Blob Data Contributor role for the Azure Function instance within the storage account](https://help.delphix.com/dcs/current/content/docs/create_an_azure_function.htm).
1. [Configure an Azure Key Vault for storing the Cassandra DB access key and assign a managed identity with the Key Vault Secrets User role to the Azure Function](https://help.delphix.com/dcs/current/content/docs/configure_azure_function_access_to_Cassandra_db_secret_using_azure_key_vault.htm).
1. [Deploy the Azure Function to the Function App created in the previous step](./Cassandra_to_ADLS/AzureFunctionDeployment.md).
1. [Configure the Azure Function Linked service](https://help.delphix.com/dcs/current/content/docs/linked_service_for_azure_Cassandra_db_nosql_source.htm).

### Importing
There are several linked services that will need to be selected in order to perform the masking of your Cassandra
Data.

These linked services types are needed for the following steps:

`Azure Function` (Cassandra to ADLS) - Linked service associated with exporting Cassandra DB data to ADLS. This will be used for the following steps:
* Check If We Should Copy Data To Cassandra (If Condition activity)

`Azure Data Lake Storage Gen2` (Sink) - Linked service associated with the ADLS account used for staging Sink Cassandra DB data. This will be used for the following steps:
  * dcsazure_Cassandra_to_Cassandra_ADLS_delimited_unfiltered_mask_df/Sink (dataFlow)
  * dcsazure_Cassandra_to_Cassandra_ADLS_delimited_copy_df/Sink (dataFlow)

  `Azure Data Lake Storage Gen2` (Source) - Linked service associated with the ADLS account used for staging Source Cassandra DB data. This will be used for the following steps:
  * dcsazure_Cassandra_to_Cassandra_ADLS_delimited_unfiltered_mask_df/Source (dataFlow)
  * dcsazure_Cassandra_to_Cassandra_ADLS_delimited_copy_df/Source (dataFlow)

* `Azure SQL` (metadata) - Linked service associated with your hosted metadata store. This will be used for the following steps:
  * Check ADLS To Cassandra Status (If Condition activity)
  * Update Logs If Copy Data To ADLS Is True (If Condition activity)
  * Update Masked State (Stored procedure activity)
  * Update Masked State Failed (Stored procedure activity)
  * If Copy Via Dataflow (If Condition activity)
  * If Copy Via Dataflow (If Condition activity)
  * If Copy Via Dataflow (If Condition activity)
  * If Copy Via Dataflow (If Condition activity)
  * Check If We Should Reapply Mapping (If Condition activity)
  * dcsazure_Cassandra_to_Cassandra_ADLS_delimited_metadata_mask_ds (Azure SQL Database dataset)

`REST` (DCS for Azure) - Linked service associated with calling DCS for Azure. This will be used for the following steps:
* dcsazure_Cassandra_to_Cassandra_ADLS_delimited_unfiltered_mask_df (dataFlow)

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
          * If the data flow is to be used for copy then call `dcsazure_Cassandra_to_Cassandra_ADLS_delimited_copy_df`
            * Update the mapped status based on the success of this dataflow, and fail accordingly
          * If the data flow is not to be used for copy, then use a copy activity
            * Update the mapped status based on the success of this dataflow, and fail accordingly
  * Select Tables That Require Masking. This is done by querying the metadata store. This will provide a list of tables that need masking, and if they need to be masked leveraging conditional algorithms, the set of required filters.
    * Configure Masked Status. Set the masked status based on the defined filters that need to be applied for the table to be marked as completely mapped.
    * For Each Table To Mask
      * Check if the table must be masked with a filter condition
        * If no filter needs to be applied:
          * Call the `dcsazure_Cassandra_to_Cassandra_ADLS_delimited_unfiltered_mask_df` data flow, passing in parameters as generated by the Lookup Masking Parameters activity
          * Update the mapped status based on the success of this dataflow, and fail accordingly
        * If a filter is used:
          * Fail the pipeline with the message `Conditional Masking is not supported`.
  * Note that there is a deactivated activity Test Filter Conditions that exists in order to support importing the filter test utility dataflow, this is making it easier to test writing filter conditions leveraging a dataflow debug session
* Check If We Should Copy Data To Cassandra
  * Copy ADLS Data to Cassandra
    * Export documents from ADLS to Cassandra DB container using an Azure Function
* Until ADLS to Cassandra Durable Function is Success
  * Poll the Azure Function execution status until the export completes
* Check ADLS to Cassandra Status
  * Validate that the export completed successfully, otherwise fail the pipeline
* Delete ADLS Directory. Based on the value of the `P_DELETE_ADLS_AFTER_LOAD` parameter, this cleans up the temporary ADLS staging area.
  * Select Directories We Should Purge. Creates a list of directories that are no longer needed.
  * For Each Directory To Purge. Iterates through the list and deletes the staged data from ADLS.

### Variables

If you have configured your database using the metadata store scripts, these variables will not need editing. If you
have customized your metadata store, then these variables may need editing.

* `METADATA_SCHEMA` - This is the schema to be used for in the self-hosted AzureSQL database for storing metadata
  (default `dbo`)
* `METADATA_RULESET_TABLE` - This is the table to be used for storing the discovered ruleset
  (default `discovered_ruleset`)
* `METADATA_SOURCE_TO_SINK_MAPPING_TABLE` - This is the table in the metadata schema that will contain the data
  mapping, defining where unmasked data lives, and where masked data should go (default `adf_data_mapping`)
* `METADATA_ADF_TYPE_MAPPING_TABLE` - This is the table that maps from data types in various datasets to the
  associated datatype required in ADF as needed for the pipeline (default `adf_type_mapping`)
* `TARGET_BATCH_SIZE` - This is the target number of rows per batch (default `50000`)
* `DATASET` - This is the way this data set is referred to in the metadata store (default `CASSANDRA`)
* `METADATA_EVENT_PROCEDURE_NAME` - This is the name of the procedure used to capture pipeline information in the
  metadata data store and sets the masked and mapping states on the items processed during execution
  (default `insert_adf_masking_event`).
* `METADATA_MASKING_PARAMS_PROCEDURE_NAME` - This is the name of the stored procedure used to generate masking
  parameters for both conditional and non-conditional masking scenarios (default `generate_masking_parameters`).
* `STORAGE_ACCOUNT` -  Azure Storage account name used during metadata discovery.(`Default: dcscassandra`).
* `COLUMN_WIDTH_ESTIMATE` - This variable is used for getting the size of the columns need to be masked (default `1000`).
* `CASSANDRA_KEY_VAULT_NAME` - Name of the Azure Key Vault that stores the Cassandra DB access key(`Default: DCS_PLACEHOLDER`).
* `CASSANDRA_SECRET_NAME` - Name of the secret in Key Vault containing the Cassandra DB access key(`Default: DCS_PLACEHOLDER`).
* `ADLS_TO_CASSANDRA_BATCH_SIZE` - This is the number of rows per batch while copying the data from Cassandra database to ADLS(default`1000`).
* `CASSANDRA_USERNAME` - String - Username used to authenticate against the Cassandra cluster.

### Parameters

* `P_CASSANDRA_CONTACT_POINTS` - String - Hostname or IP address(es) of the Cassandra node(s) used to establish the initial cluster connection (seed or preferred nodes)
* `P_CASSANDRA_PORT` - Int - Port number on which the Cassandra service is listening.
* `P_CASSANDRA_SOURCE_KEYSPACE` - String - Source Cassandra keyspace that contains the input (source) tables.
* `P_CASSANDRA_SINK_KEYSPACE` - String - Target Cassandra keyspace where the output (masked or transformed) data will be written.
* `P_CASSANDRA_TABLE` - String -  Name of the Cassandra table to be processed.
* `P_ADLS_SINK_CONTAINER_NAME` - String - ADLS container name where processed data will be written.
* `P_ADLS_SOURCE_CONTAINER_NAME` - String - ADLS container name where the data should be taken.
* `P_CASSANDRA_PREFERRED_NODE` - String - Specific Cassandra node (IP/hostname) to be preferred for read/write operations, enabling node-level control.
* `P_CASSANDRA_PREFERRED_PORT` - String - Specific Cassandra node (IP/hostname) to be preferred for read/write operations, enabling node-level control.
* `P_COPY_USE_DATAFLOW` - Bool - Indicates whether the pipeline should use a Copy Data Flow instead of a Copy Activity.
* `P_FAIL_ON_NONCONFORMANT_DATA` - Bool - Determines whether the pipeline should fail when non-conformant data is encountered (default: true).
* `P_REAPPLY_MAPPING` - Bool - Controls whether source-to-sink mappings should be reset and reapplied (default: true).
* `P_COPY_UNMASKED_TABLES` - Bool - Determines whether unmasked tables should also be copied as part of the pipeline execution.
* `P_TRUNCATE_SINK_BEFORE_WRITE` - Bool - Controls whether existing data in the sink location should be truncated before writing new data (default: true).
* `P_COPY_ADLS_DATA_TO_CASSANDRA` - Bool - Determines whether the pipeline should fail when non-conformant data is encountered (default: true).
* `P_DELETE_ADLS_AFTER_LOAD` - Bool - Determines whether the extracted files in ADLS should be deleted after the data has been successfully loaded into the target system (default: false).
* `P_DELETE_ADLS_RECORDS` - Bool - Determines whether existing records/files in the ADLS staging location should be deleted before the pipeline execution begins (default: false).
* `P_FUNCTION_NAME` - String - Name of the Azure Function.

### Notes

* When creating the Azure Function used for Cassandra DB export, choose the hosting plan based on data volume:
  * The default timeout for the Consumption plan is 10 minutes.
  * The default timeout for the Flex Consumption plan is 60 minutes.
  * For containers with millions of documents, it is recommended to use an App Service plan with at least 4 GB of memory.
    * This allows the function to run without time limits until all records are processed.
    * This approach is especially recommended when the target container has low RU provisioning or a very large number of records.
    * The Azure Function timeout is explicitly configured to **12 hours** using the `functionTimeout` setting to support large Cassandra DB containers.
* If the Azure Function fails with out-of-memory errors (exit code 137), adjust the `CASSANDRA_TO_ADLS_BATCH_SIZE` to reduce memory pressure.
* Update the `CASSANDRA_KEY_VAULT_NAME` and `CASSANDRA_SECRET_NAME` variables to match the target Cassandra DB account before triggering the pipeline.
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
  WHERE d.dataset = 'Cassandra'
    AND d.specified_schema LIKE 'Cassandra-DATABASE/Cassandra-CONTAINER-NAME%';
  ```
* The Cassandra container name must be the same in both the source and sink databases for the masking pipeline to function correctly.
* Ensure that all schemas associated with the Cassandra DB container are added to the `adf_data_mapping` table before triggering the masking pipeline.
* If a column exists in some records but is missing in others, the pipeline will still include that column in the masked output, populating `null` values for records where the column was not originally present.
* Conditional masking is not supported by this template.