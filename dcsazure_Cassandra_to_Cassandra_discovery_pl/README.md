# dcsazure_Cassandra_to_Cassandra_discovery_pl
## Delphix Compliance Services (DCS) for Azure - Cassandra to Cassandra Discovery Pipeline

This pipeline will perform automated sensitive data discovery on your Cassandra Instance.

### Prerequisites
1. Configure the hosted metadata database and associated Azure SQL linked service (version `V2026.02.02.0`).
2. Configure the DCS for Azure REST linked service.
3. Configure the Azure Data Lake Storage (Gen 2) linked service for staging exported Cassandra DB data.
4. [Assign a managed identity with a Storage Blob Data Contributor role for the Data Factory instance within the storage account](https://help.delphix.com/dcs/current/content/docs/configure_adls_delimited_pipelines.htm).
5. [Repeat the above step for the Azure Function by assigning a managed identity with the Storage Blob Data Contributor role](External_Document_URL).
6. [Create an Azure Function app for exporting Cassandra DB data to Azure Data Lake Storage (ADLS)](External_Document_URL) (version `Cassandra_to_ADLS_V1`).
7. [Deploy the Azure Function to the Function App created in the previous step](./Cassandra_to_ADLS/AzureFunctionDeployment.md).
8. [Configure an Azure Key Vault for storing the Cassandra DB access key and assign a managed identity with the Key Vault Secrets User role to the Azure Function](External_Document_URL).

### Importing
There are several linked services that will need to be selected in order to perform the profiling and data discovery of
your Cassandra containers.

These linked services types are needed for the following steps:

`Azure Function` (Cassandra to ADLS) – Linked service associated with exporting Cassandra DB data to ADLS. This will be used for the following steps:

* Check If We Should Copy Cassandra to ADLS (If Condition activity)

`Azure Data Lake Storage Gen2` (staging) - Linked service associated with the ADLS account used for staging Cassandra DB exports. This will be used for the following steps:
* dcsazure_Cassandra_to_Cassandra_ADLS_delimited_container_and_directory_discovery_ds (DelimitedText dataset),
* dcsazure_Cassandra_to_Cassandra_ADLS_delimited_data_discovery_df/SourceData1MillRowDataSampling (dataFlow),
* dcsazure_Cassandra_to_Cassandra_ADLS_delimited_header_file_schema_discovery_ds (DelimitedText dataset)

`Azure SQL` (metadata) - Linked service associated with your hosted metadata store. This will be used for the following
steps:
* Set Source Metadata (Script activity),
* Check Cassandra To ADLS Status (If Condition activity),
* Check If We Should Update Copy State (If Condition activity),
* Update Discovery State (Stored procedure activity),
* Update Discovery State Failed (Stored procedure activity),
* Check If We Should Rediscover Data (If Condition activity),
* dcsazure_Cassandra_to_Cassandra_discovery_metadata_ds (Azure SQL Database dataset)
* dcsazure_Cassandra_to_Cassandra_data_discovery_df/MetadataStoreRead (dataFlow)
* dcsazure_Cassandra_to_Cassandra_data_discovery_df/WriteToMetadataStore (dataFlow)
* Persist Metadata To Database (Stored procedure activity)

`REST` (DCS for Azure) - Linked service associated with calling DCS for Azure. This will be used for the following steps:
* dcsazure_Cassandra_to_Cassandra_ADLS_delimited_data_discovery_df (dataFlow)

### How It Works
* Check If We Should Copy Data To ADLS
  * Copy Cassandra Data to ADLS
    * Export documents from a Cassandra DB container to ADLS using an Azure Function
* Until Cassandra to ADLS Durable Function is Success
  * Poll the Azure Function execution status until the export completes
* Check Cassandra to ADLS Status
  * Validate that the export completed successfully, otherwise fail the pipeline
* Discover Sensitive Data
  * Check If We Should Rediscover Data
    * If we should, Mark Tables Undiscovered. This is done by updating the metadata store to indicate that tables have not had their sensitive data discovered
  * Identify Nested Schemas
    * Using the child pipeline `dcsazure_Cassandra_to_Cassandra_ADLS_delimited_container_and_directory_discovery_pl`, we collect all the identified schemas under the specified directory.
    * For each item in that list, identify if the schema of the files in that child directory is expected to be homogeneous.
  * Schema Discovery
    * For each of the directories with homogeneous schema, identify the schema for each file with one of the suffixes to scan, determine the structure of the file by calling the child `dcsazure_Cassandra_to_Cassandra_ADLS_delimited_file_discovery_pl` pipeline with the appropriate parameters.
  * Select Discovered Tables - In this case, we consider the table to be items with the same schema.
    * After the previous step, we query the database for all tables (file suffixes within each distinct path of the storage container) and perform profiling for sensitive data discovery in those files that have not yet been discovered.
  * ForEach Discovered Table
    * Each table that we've discovered needs to be profiled, the process for that is as follows:
      * Run the `dcsazure_Cassandra_to_Cassandra_ADLS_delimited_data_discovery_df` dataflow with the appropriate parameters.
* Set Source Metadata
  * After sensitive data discovery completes successfully, update the metadata store to enrich the discovered objects with Cassandra-specific source context, including logical partition values, ensuring discovery results are partition-aware and accurately traceable.


### Variables

If you have configured your database using the metadata store scripts, these variables will not need editing. If you
have customized your metadata store, then these variables may need editing.

* `METADATA_SCHEMA` - This is the schema to be used for in the self-hosted AzureSQL database for storing metadata
  (default `dbo`)
* `METADATA_RULESET_TABLE` - This is the table to be used for storing the discovered ruleset
  (default `discovered_ruleset`)
* `DATASET` - This is used to identify data that belongs to this pipeline in the metadata store (default `CASSANDRA`)
* `METADATA_EVENT_PROCEDURE_NAME` - This is the name of the procedure used to capture pipeline information in the
  metadata data store and sets the discovery state on the items discovered during execution
  (default `insert_adf_discovery_event`).
* `NUMBER_OF_ROWS_TO_PROFILE` - This is the number of rows we should select for profiling, note that raising this value
  could cause requests to fail (default `1000`).
* `COLUMNS_FROM_ADLS_FILE_STRUCTURE_PROCEDURE_NAME` - Stored procedure used to derive column metadata from ADLS file structures.  
  Default: get_columns_from_delimited_file_structure_sp.
* `STORAGE_ACCOUNT` -  Azure Storage account name used during metadata discovery.Default: dcscassandra.
* `MAX_LEVELS_TO_RECURSE` - Maximum directory recursion depth (default `10`)
* `CASSANDRA_TO_ADLS_BATCH_SIZE` –  This is the number of rows per batch while copying the data from Cassandra DB to ADLS.
* `CASSANDRA_KEY_VAULT_NAME` –  Name of the Azure Key Vault that stores the Cassandra DB access key
* `CASSANDRA_SECRET_NAME` – Name of the secret in Key Vault containing the Cassandra DB access key
* `ARRAY_PROCESSING_BATCH_SIZE` - This is the number of arrays per batch while copying the data from Cassandra DB database to ADLS (default `50000`)



### Parameters

* `P_CASSANDRA_CONTACT_POINTS` - String - Hostname or IP address(es) of the Cassandra node(s) used to establish the initial connection to the cluster.
* `P_CASSANDRA_PORT` - Int - Port number on which the Cassandra service is listening.
* `P_CASSANDRA_USERNAME` - String - Username used to authenticate against the Cassandra cluster.
* `P_CASSANDRA_KEYSPACE` - String - Cassandra keyspace that contains the target table.
* `P_CASSANDRA_TABLE` - String - Name of the Cassandra table to be read from or written to.
* `P_CASSANDRA_PREFERRED_NODE` - String - Preferred Cassandra node (IP or hostname) for node-specific read or write operations.
* `P_CASSANDRA_PREFERRED_PORT` - Int - Preferred port number of the Cassandra node to be used for node-specific read operations.
* `P_CASSANDRA_PARTITION_KEY` - String - Partition key value used to control data distribution and node-level reads/writes.
* `P_CASSANDRA_PARTITION_KEY_VALUE` - String - Partition key value to filter and process a specific subset of records.
* `P_ADLS_SOURCE_CONTAINER_NAME` - String - ADLS container name where data will be written or read.
* `P_REDISCOVER` - Bool - Flag to indicate whether metadata discovery or rediscovery should be performed for the Cassandra source.
* `P_COPY_CASSANDRA_DATA_TO_ADLS` - Bool - Specifies whether data should be copied from Cassandra DB to ADLS (default `true`)

### Notes

* When creating the Azure Function used for Cassandra DB export, choose the hosting plan based on data volume:
    * We have to use only an App Service plan or Premium Plan with at least 8 GB of memory. Other  Plan won't work because we need to configure Vnet/Subnet to Cassandra.
    * The Azure Function timeout is explicitly configured to **24 hours** using the `functionTimeout` setting to support large Cassandra DB Records.
* Update the `CASSANDRA_KEY_VAULT_NAME` and `CASSANDRA_SECRET_NAME` variables to match the target Cassandra DB account before triggering the pipeline.
* To filter records by Cassandra DB partition, both `P_CASSANDRA_PARTITION_KEY` and `P_CASSANDRA_PARTITION_KEY_VALUE` must be provided.
* This pipeline operates at the container level. When an array of `P_CASSANDRA_PARTITION_KEY_VALUE` is specified, only data for those partitions is exported to ADLS and included in discovery and masking.
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
  WHERE d.dataset = 'CASSANDRA'
    AND d.specified_schema LIKE 'CASSANDRA-KEYSPACE/CASSANDRA-TABLE%';
* If the pipeline is rerun for the same container with a different set of `P_CASSANDRA_PARTITION_KEY`, the data in ADLS is overwritten with the new partition’s data, and the corresponding partition metadata in the ruleset is updated.
* Historical information about previously discovered partitions can be obtained from the `adf_events` log table.
