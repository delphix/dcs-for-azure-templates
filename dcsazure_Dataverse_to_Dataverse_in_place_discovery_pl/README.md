# dcsazure_Dataverse_to_Dataverse_in_place_discovery_pl
## Delphix Compliance Services (DCS) for Azure - Dataverse to Dataverse In-place Discovery Pipeline

This pipeline will perform automated sensitive data discovery on your Microsoft Dataverse environment using Delphix Compliance Services (DCS) for Azure.

### Prerequisites
1. Configure the hosted metadata database and associated Azure SQL service.
2. Configure the DCS for Azure REST service.
3. Register an application in Azure for Dataverse and obtain the necessary credentials. 
   NOTE: To obtain the necessary credentials, refer to Delphix documentation(https://dcs.delphix.com/docs/latest/delphixcomplianceservices-dcsforazure-2_onboarding#RegisteringaServicePrincipal-Process).
4. Configure the Dataverse REST linked service. 
   * It is helpful for the linked service to be parameterized with the following parameter:
      * `LS_ORG_NAME` - Environment name in the linked service.
5. Configure the Dataverse linked service. 
   * It is helpful for the linked service to be parameterized with the following parameter:
      * `LS_ORG_NAME` - Environment name in the linked service.

### Importing

There are several linked services that will need to be selected in order to perform the profiling and data discovery of your Dataverse environment.

These linked services types are needed for the following steps:

`Dataverse` (source) - Linked service associated with unmasked AzureSQL data. This will be used for the following steps:
* dcsazure_Dataverse_to_Dataverse_in_place_discovery_df/Source1MillRowDataSampling (DataFlow)

`Azure SQL` (metadata) - Linked service associated with your hosted metadata store. This will be used for the following steps:
* Check If We Should Rediscover Data (If Condition activity),
* Update Discovery State (Stored procedure activity),
* Update Discovery State Failed (Stored procedure activity),
* Delete Temporary Table Records (Script activity),
* dcsazure_Dataverse_to_Dataverse_in_place_discovery_metadata_ds (Azure SQL Database dataset),
* dcsazure_Dataverse_to_Dataverse_in_place_import_columns_df/Sink (dataFlow),
* dcsazure_Dataverse_to_Dataverse_in_place_discovery_df/MetadataStoreRead (dataFlow),
* dcsazure_Dataverse_to_Dataverse_in_place_discovery_df/WriteToMetadataStore (dataFlow)

`REST` (DCS for Azure) - Linked service associated with calling DCS for Azure. This will be used for the following steps:
* dcsazure_Dataverse_to_Dataverse_in_place_discovery_df (dataFlow)

`Dataverse REST` (Source) - Linked service associated with Dataverse to fetch the metadata. This will be used for the following steps:
* dcsazure_Dataverse_to_Dataverse_in_place_discovery_source_ds (REST Dataset)
* dcsazure_Dataverse_to_Dataverse_in_place_import_columns_df/Source (dataFlow)

### How It Works

* Check If We Should Rediscover Data
  * If we should, Mark Tables Undiscovered. This is done by updating the metadata store to indicate that tables have not had their sensitive data discovered.
* Get tables from source 
  * Calls the Dataverse EntityDefinitions API to fetch all table names that are:
    * Valid for Advanced Find
    * Customizable
    * Not private
  * Stores the results in a discovered_ruleset metadata table with a dummy column name `DCS__PLACEHOLDER` and ordinal position as `-1`
* Fetch table names
  * Queries the temp metadata table to select only the discovered table names where `ordinal_position = -1`
* For Each Table  
  * Iterates over each table from `Fetch table names`. The following sub-activities run for each table:
    * Add Metadata to Ruleset table
      * Uses Dataverse API to fetch column metadata (`Attributes`) for the current table  
      * Stores column name, data type, max length, etc., into the discovered_ruleset metadata SQL table
      * Filters the columns that should be excluded (System modified, Readonly, polymorphic, lookup columns) and updates the `is_excluded` field in the discovered_ruleset metadata table.
      * Add's the metadata of the columns and reasons to exclude them to source_metadata column in a JSON format.
      * Use the query `SELECT JSON_VALUE(source_metadata, '$.Reason') AS reason FROM discovered_ruleset;` to get the reason for which the column is excluded.
* Delete Temporary Table Records
  * Deletes the rows from the discovered_ruleset metadata table where `ordinal_position = -1`.
* Select Discovered Tables
  * After persisting the metadata to the metadata store, collect the list of discovered tables
* For Each Discovered Table
  * Call the `dcsazure_Dataverse_to_Dataverse_in_place_discovery_df` for profiling and tagging sensitive data.

### Variables

If you have configured your database using the metadata store scripts, these variables will not need editing. If you have customized your metadata store, then these variables may need editing.

* `METADATA_SCHEMA` - This is the schema to be used for in the self-hosted AzureSQL database for storing metadata (default `dbo`)
* `METADATA_RULESET_TABLE` - This is the table to be used for storing the discovered ruleset (default `discovered_ruleset`)
* `DATASET` - This is used to identify data that belongs to this pipeline in the metadata store (default `DATAVERSE`)
* `METADATA_EVENT_PROCEDURE_NAME` - This is the name of the procedure used to capture pipeline information in the metadata data store and sets the discovery state on the items discovered during execution (default `insert_adf_discovery_event`)
* `NUMBER_OF_ROWS_TO_PROFILE` - This is the number of rows we should select for profiling, note that raising this value could cause requests to fail (default `1000`)
* `METADATA_EVENTS_LOG_TABLE` - This is the table to log pipeline run information in the metadata data store (default `adf_events_log`)
* `API_REQUEST_TIMEOUT` - This is used to set the API timeout for the Dataverse APIs while fetching the metadata (default `30`)
* `TEMP_COLUMN_NAME` - This is used as the temporary value for identified_column/identified_column_type columns in discovered ruleset table. (default `DCS__PLACEHOLDER`)

### Parameters

* `P_SOURCE_DATABASE` - String - This is the Dataverse environment that may contain sensitive data
* `P_REDISCOVER` - This is a Bool that specifies if we should re-execute the data discovery dataflow for previously discovered files that have not had their schema modified (default `true`)

### Notes

* In the `METADATA_RULESET_TABLE`, a value of `1` in the `is_excluded` column indicates that the column has been excluded from masking.

* To check the reason for exclusion, run the following query:

```sql
SELECT
  JSON_VALUE(source_metadata, '$.Reason') AS reason
FROM
  discovered_ruleset;
```
