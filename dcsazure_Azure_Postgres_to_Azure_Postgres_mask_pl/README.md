# dcsazure_Azure_Postgres_to_Azure_Postgres_mask_pl
## Delphix Compliance Services (DCS) for Azure - Azure PostgreSQL to Azure PostgreSQL Masking Pipeline

This pipeline will perform masking of your Azure PostgreSQL Instance.

### Prerequisites

1. Configure the hosted metadata database and associated Azure SQL service (version `V2026.01.20.0`+).
1. Configure the DCS for Azure REST service.
1. Configure the Azure PostgreSQL linked service.
   * It is helpful for the linked service to be parameterized with the following parameter:
      * `LS_DATABASE` - database name in the linked service


### Importing
There are several linked services that will need to be selected in order to perform the masking of your Azure PostgreSQL instance.

These linked services types are needed for the following steps:

`Azure PostgreSQL` (source) - Linked service associated with Azure PostgreSQL source data. This will be used for the
following steps:
* dcsazure_Azure_Postgres_to_Azure_Postgres_filter_test_utility_df/Source (dataFlow)
* dcsazure_Azure_Postgres_to_Azure_Postgres_unfiltered_mask_df/Source (dataFlow)
* dcsazure_Azure_Postgres_to_Azure_Postgres_filtered_mask_df/Source (dataFlow)
* dcsazure_Azure_Postgres_to_Azure_Postgres_mask_source_ds (AzurePostgreSqlTable dataset)
* dcsazure_Azure_Postgres_to_Azure_Postgres_copy_df/SourceData (dataFlow)

`Azure PostgreSQL` (sink) - Linked service associated with Azure PostgreSQL sink data. This will be used for the
following steps:
* Truncate Selected Table (Script activity)
* dcsazure_Azure_Postgres_to_Azure_Postgres_filter_test_utility_df/Sink (dataFlow)
* dcsazure_Azure_Postgres_to_Azure_Postgres_unfiltered_mask_df/Sink (dataFlow)
* dcsazure_Azure_Postgres_to_Azure_Postgres_filtered_mask_df/Sink (dataFlow)
* dcsazure_Azure_Postgres_to_Azure_Postgres_mask_sink_ds (AzurePostgreSqlTable dataset)
* dcsazure_Azure_Postgres_to_Azure_Postgres_copy_df/SinkData (dataFlow)

`Azure SQL` (metadata) - Linked service associated with your hosted metadata store. This will be used for the following
steps:
* Check For Conditional Masking (If Condition activity)
* If Use Copy Dataflow (If Condition activity)
* Check If We Should Reapply Mapping (If Condition activity)
* Configure Masked Status (Script activity)
* Lookup Masking Parameters (Lookup activity)
* dcsazure_Azure_Postgres_to_Azure_Postgres_mask_metadata_ds (Azure SQL Database dataset)

`REST` (DCS for Azure) - Linked service associated with calling DCS for Azure. This will be used for the following
steps:
* dcsazure_Azure_Postgres_to_Azure_Postgres_unfiltered_mask_df (dataFlow)
* dcsazure_Azure_Postgres_to_Azure_Postgres_filtered_mask_df (dataFlow)

### How It Works
* Check If We Should Reapply Mapping
  * If we should, Mark Table Mapping Incomplete. This is done by updating the metadata store to indicate that tables have not had their mapping applied
* Execute drop constraint pipeline
  * Call the `dcsazure_Azure_Postgres_to_Azure_Postgres_mask_drop_constraint_pl` child pipeline to drop the foreign key constraints from the sink schema.
  * The child pipeline records all the foreign key constraints in the metadata store table `capture_constraints` before dropping them.
  * **Note**: Only drops constraints related to sink tables whose data mapping in the `METADATA_SOURCE_TO_SINK_MAPPING_TABLE` has not been successfully mapped. This includes unmasked tables, independent of `P_COPY_UNMASKED_TABLES`.
* Select Tables We Should Truncate
  * Select sink tables with an incomplete mapping and based on the value of `P_TRUNCATE_SINK_BEFORE_WRITE`, create a list of tables that we should truncate
    * For Each Table To Truncate, execute a query to truncate the sink table
* Select Tables That Require Masking
  * Configure Masked Status
  * For Each Table To Mask
* Select Tables Without Required Masking
  * Filter If Copy Unmasked Enabled
  * For Each Table With No Masking
* Select Tables Without Required Masking. This is done by querying the metadata store.
  * Filter If Copy Unmask Enabled. This is done by applying a filter based on the value of `P_COPY_UNMASKED_TABLES`
    * For Each Table With No Masking. Provided we have any rows left after applying the filter
      * Get Sink Table Details No Masking. Query Azure PostgreSQL for sink table details
      * Get Sink Table Metadata No Masking. Query Metadata store to construct metadata information for sink table
      * If Copy Via Dataflow - based on the value of `P_COPY_USE_DATAFLOW`
        * If the data flow is to be used for copy, then call `dcsazure_Azure_Postgres_to_Azure_Postgres_copy_df`
        * If the data flow is not to be used for copy, then use a copy activity
        * Update the mapped status based on the success of this dataflow, and fail accordingly
* Select Tables That Require Masking. This is done by querying the metadata store. This will provided a list of tables that need masking, and if they need to be masked leveraging conditional algorithms, the set of required filters.
  * For Each Table To Mask
    * Lookup Masking Parameters - calls the `generate_masking_parameters` stored procedure to generate masking parameters for both conditional and non-conditional masking scenarios
    * Check if the table must be masked with a filter condition
      * If no filter needs to be applied:
        * Call the `dcsazure_Azure_Postgres_to_Azure_Postgres_unfiltered_mask_df` data flow, passing in parameters as generated by the `Lookup Masking Parameters` activity
        * Update the mapped status based on the success of this dataflow, and fail accordingly
      * If a filter needs to be applied:
        * Call the `dcsazure_Azure_Postgres_to_Azure_Postgres_filtered_mask_df` data flow, passing in parameters as generated by the `Lookup Masking Parameters` activity and the filter as determined by the output of For Each Table To Mask
        * Update the mapped status based on the success of this dataflow, and fail accordingly
* Execute create constraint pipeline
  * Call the `dcsazure_Azure_Postgres_to_Azure_Postgres_mask_create_constraint_pl` child pipeline to re-enable the constraints that were dropped in the beginning.
  * It queries all the foreign key constraints from the metadata store table `capture_constraints` and re-enable them table by table in the sink schema.
  * This pipeline is called regardless of the last status of the masking activity, thus ensuring that the sink schema and table always remains in the same state as it was before running the pipeline.
* Check and set pipeline status
  * This final activity correctly sets the overall pipeline status to success or failure.
* Note that there is a deactivated activity `Test Filter Conditions` that exists in order to support importing the filter test utility dataflow, this is making it easier to test writing filter conditions leveraging a dataflow debug session

### Variables

If you have configured your database using the metadata store scripts, these variables will not need editing. If you
have customized your metadata store, then these variables may need editing.

* `METADATA_SCHEMA` - This is the schema to be used for in the self-hosted AzureSQL database for storing metadata (default `dbo`)
* `METADATA_RULESET_TABLE` - This is the table to be used for storing the discovered ruleset (default `discovered_ruleset`)
* `METADATA_SOURCE_TO_SINK_MAPPING_TABLE` - This is the table in the metadata schema that will contain the data
  mapping, defining where unmasked data lives, and where masked data should go (default `adf_data_mapping`)
* `METADATA_ADF_TYPE_MAPPING_TABLE` - This is the table that maps from data types in various datasets to the
  associated datatype required in ADF as needed for the pipeline (default `adf_type_mapping`)
* `DATASET` - This is the way this data set is referred to in the metadata store (default `AZURE_POSTGRES`)
* `CONDITIONAL_MASKING_RESERVED_CHARACTER` - This is a string (preferably a character) reserved as for shorthand for
  when referring to the key column when defining filter conditions, in the pipeline this will be expanded out to use the
  ADF syntax for referencing the key column (default `%`)
* `TARGET_BATCH_SIZE` - This is the target number of rows per batch (default `50000`) use for conditional masking
* `COLUMN_WIDTH_ESTIMATE` - This is the estimated width of the columns when the actual width is not available from database schema to perform the calculation of optimal number of batches in unconditional masking (default `1000`).
* `METADATA_EVENT_PROCEDURE_NAME` - This is the name of the procedure used to capture pipeline information in the metadata data store and sets the masked and mapping states on the items processed during execution (default `insert_adf_masking_event`).
* `METADATA_MASKING_PARAMS_PROCEDURE_NAME` - This is the name of the stored procedure used to generate masking parameters for both conditional and non-conditional masking scenarios (default `generate_masking_parameters`).
* `METADATA_CONSTRAINT_TABLE` - This table is used to store the foreign key constraints for each table in the sink schema that are required for masking. The table is queried to drop and recreate those constraints back (default `capture_constraints`).
* `PIPELINE_FAILED` - This variable is used internally to set the correct pipeline status. It is important to note that, this variable should not be modified when running the pipeline. The default value is `false`

### Parameters

* `P_COPY_UNMASKED_TABLES` - Bool - This enables the pipeline to copy data from source to destination when a mapping
exists, but no algorithms have been defined (default `false`)
* `P_USE_COPY_DATAFLOW` -   Bool - This enables the pipeline to control the copy behaviour. When True, the copy data flow activity was invoked and unmasked tables were copied from the source to the sink (default `false`)
* `P_FAIL_ON_NONCONFORMANT_DATA` - Bool - This will fail the pipeline if non-conformant data is encountered (default
`true`)
* `P_TRUNCATE_SINK_BEFORE_WRITE`:  Bool - When enabled (default `true`), truncates sink/target tables before writing new masked data.  When set to False, sink tables are not truncated (expect PK violation if keys exist, or row counts increase by source row count if no unique key).
* `P_REAPPLY_MAPPING` - Bool -  When `true` mask all tables listed in adf_data_mapping regardless of mapping_complete; 
    when `false`, mask only tables where mapping_complete=false (default `true`)
* `P_SOURCE_DATABASE` - String - This is the source database in Azure PostgreSQL that contains the unmasked data
* `P_SINK_DATABASE` - String - This is the sink database in Azure PostgreSQL that will serve as a destination for masked data
* `P_SOURCE_SCHEMA` - String - This is the schema within the above source database that we will mask
* `P_SINK_SCHEMA` - String - This is the schema within the above sink database where we will place masked data
