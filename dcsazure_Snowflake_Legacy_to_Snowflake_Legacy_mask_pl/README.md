# dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_mask_pl
## Delphix Compliance Services (DCS) for Azure - Snowflake (Legacy) to Snowflake (Legacy) Masking Pipeline

This pipeline will perform masking of your Snowflake Instance.

### Prerequisites
1. Configure the hosted metadata database and associated Azure SQL service (version `V2024.10.24.0`+).
1. Configure the DCS for Azure REST service.
1. Configure the Snowflake (Legacy) linked service
   * It is helpful for the linked service to be parameterized with the following parameters:
     * `LS_DATABASE` - this is the database name in the linked service
     * `LS_WAREHOUSE` - this is the warehouse name in the linked service
     * `LS_ROLE` - this is the role the linked service should use
1. Configure the Blob Storage linked service to the container named `staging-container`.

### Importing
There are several linked services that will need to be selected in order to perform the masking of your Snowflake
instance.

These linked services types are needed for the following steps:

`Azure Blob Storage` (staging) - Linked service associated with a blob storage container that can be used to stage data
when performing a data copy from Snowflake. This will be used for the following steps:
* If Copy Via Dataflow (If Condition activity)

`Snowflake (Legacy)` (source) - Linked service associated with unmasked Snowflake data. This will be used for the following
steps:
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_filter_test_utility_df/SourceData (dataFlow)
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_unfiltered_mask_df/Source (dataFlow)
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_filtered_mask_df/Source (dataFlow)
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_mask_source_ds (Snowflake (Legacy) dataset)
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_copy_df/SourceData (dataFlow)


`Snowflake (Legacy)` (sink) - Linked service associated with masked Snowflake data. This will be used for the following
steps:
* Truncate Selected Table (Script activity)
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_filter_test_utility_df/SinkData (dataFlow)
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_unfiltered_mask_df/Sink (dataFlow)
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_filtered_mask_df/Sink (dataFlow)
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_copy_df/SinkData (dataFlow)

`Azure SQL` (metadata) - Linked service associated with your hosted metadata store. This will be used for the following
steps:
* Check For Conditional Masking (If Condition activity)
* If Copy Via Dataflow (If Condition activity)
* Check If We Should Reapply Mapping (If Condition activity)
* Configure Masked Status (Script activity)
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_mask_metadata_ds (Azure SQL Database dataset)
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_unfiltered_mask_params_df/Ruleset (dataFlow)
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_unfiltered_mask_params_df/TypeMapping (dataFlow)
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_filtered_mask_params_df/Ruleset (dataFlow)
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_filtered_mask_params_df/TypeMapping (dataFlow)

`REST` (DCS for Azure) - Linked service associated with calling DCS for Azure. This will be used for the following
steps:
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_unfiltered_mask_df (dataFlow)
* dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_filtered_mask_df (dataFlow)

### How It Works
* Check If We Should Reapply Mapping
  * If we should, Mark Table Mapping Incomplete. This is done by updating the metadata store to indicate that tables
    have not had their mapping applied
* Select Tables We Should Truncate
  * Select sink tables with an incomplete mapping and based on the value of `P_TRUNCATE_SINK_BEFORE_WRITE`, create a
    list of tables that we should truncate
    * For Each Table To Truncate, execute a query to truncate the sink table
* Select Tables Without Required Masking. This is done by querying the metadata data store.
  * Filter If Copy Unmasked Enabled. This is done by applying a filter based on the value of `P_COPY_UNMASKED_TABLES`
    * For Each Table With No Masking. Provided we have any rows left after applying the filter
      * If Copy Via Dataflow - based on the value of `P_COPY_USE_DATAFLOW`
        * If the data flow is to be used for copy, then call `dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_copy_df`
        * If the data flow is not to be used for copy, then use a copy activity
        * Update the mapped status based on the success of this dataflow, and fail accordingly
* Select Tables That Require Masking. This is done by querying the metadata store. This will provided a list of tables
that need masking, and if they need to be masked leveraging conditional algorithms, the set of required filters.
  * For Each Table To Mask
    * Check if the table must be masked with a filter condition
      * If no filter needs to be applied:
        * Call the `dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_unfiltered_mask_params_df` data flow to generate
          masking parameters
        * Call the `dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_unfiltered_mask_df` data flow, passing in parameters
          as generated by the unfiltered masking parameters dataflow
        * Update the mapped status based on the success of this dataflow, and fail accordingly
      * If a filter needs to be applied:
        * Call the `dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_filtered_mask_params_df` data flow to generate masking
          parameters using the filter alias and the filter as determined by the output of For Each Table To Mask
        * Call the `dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_filtered_mask_df` data flow, passing in parameters as
          generated by the filtered masking parameters dataflow
        * Update the mapped status based on the success of this dataflow, and fail accordingly
* Note that there is a deactivated activity `Test Filter Conditions` that exists in order to support importing the
filter test utility dataflow, this is making it easier to test writing filter conditions leveraging a dataflow debug
session

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
* `STAGING_STORAGE_PATH` - This is a path that specifies where we should stage data as it moves through the pipeline
    and should reference a storage container in a storage account (default `staging-container`)
* `DATASET` - This is the way this data set is referred to in the metadata store (default `SNOWFLAKE`)
* `CONDITIONAL_MASKING_RESERVED_CHARACTER` - This is a string (preferably a character) reserved as for shorthand for
  when referring to the key column when defining filter conditions, in the pipeline this will be expanded out to use the
  ADF syntax for referencing the key column (default `%`)
* `TARGET_BATCH_SIZE` - This is the target number of rows per batch (default `2000`)
* `METADATA_EVENT_PROCEDURE_NAME` - This is the name of the procedure used to capture pipeline information in the
  metadata data store and sets the masked and mapping states on the items processed during execution
  (default `insert_adf_masking_event`).
* `SOURCE_SNOWFLAKE_WAREHOUSE` - This is the name of the Snowflake warehouse that should be used for connecting the
  source Snowflake instance and is fed into the parameters of the source linked service (default `DEFAULT`)
* `SOURCE_SNOWFLAKE_ROLE` - This is the Snowflake role that is used for connecting to the source Snowflake instance and
  is fed into the parameters of the source linked service. This role must have permission to access and modify all
  source tables (default `SYSADMIN`)
* `SINK_SNOWFLAKE_WAREHOUSE` - This is the name of the Snowflake warehouse that should be used for connecting the
  sink Snowflake instance and is fed into the parameters of the sink linked service (default `DEFAULT`)
* `SINK_SNOWFLAKE_ROLE` - This is the Snowflake role that is used for connecting to the sink Snowflake instance and
  is fed into the parameters of the sink linked service. This role must have permission to access and modify all
  sink tables (default `SYSADMIN`)

### Parameters

* `P_COPY_UNMASKED_TABLES` - Bool - This is a flag to indicate whether we should copy over data that does not have any
  assigned algorithms (default `false`)
* `P_COPY_USE_DATAFLOW` - Bool - This is a flag to indicate whether we should use a copy activity or a copy data flow
  and is only relevant when using `P_COPY_UNMASKED_TABLES` (default `false`)
* `P_FAIL_ON_NONCONFORMING_DATA` - Bool - This enables the pipeline to handle non-conformant data errors without failing
  the pipeline - if set to `true`, unmasked data that did not conform to the format required to apply the specified
  algorithm will appear in the output data; if set to `false`, data that did not conform to the format required to apply
  the specified algorithm will cause the pipeline to fail (default `true`)
* `P_REAPPLY_MAPPING` - Bool - This controls whether we should reset the mapping between source and sink tables, this
  will mark all mappings as incomplete (default `true`)
* `P_TRUNCATE_SINK_BEFORE_WRITE` - Bool - This controls whether we should purge the directories in the sink locations
  that have not already been completed by this pipeline, note that the set of locations to purge does _not_ depend on
  the value of `P_COPY_UNMASKED_TABLES` (default `true`)
* `P_SOURCE_DATABASE` - String - This is the database in Snowflake that contains data we will mask
* `P_SINK_DATABASE` - String - This is the database in Snowflake that contains the location where we should put masked
  data
* `P_SOURCE_SCHEMA` - String - This is the schema within the above source database that we will mask
* `P_SINK_SCHEMA` - String - This is the schema within the above sink database where we will place masked data

