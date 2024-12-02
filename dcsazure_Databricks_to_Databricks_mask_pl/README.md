# dcsazure_Databricks_to_Databricks_mask_pl
## Delphix Compliance Services (DCS) for Azure - Databricks to Databricks Masking Pipeline

This pipeline will perform masking of data from your Databricks Delta Lake from one Delta Lake to another.

### Prerequisites

1. Configure the hosted metadata database and associated Azure SQL service (version `V2024.10.24.0`+).
1. Configure the DCS for Azure REST service.
1. Configure the Azure Data Lake Storage service associated with your Databricks source data.
1. Configure the Azure Data Lake Storage service associated with your Databricks sink data (if required).
1. Configure the Azure Databricks Delta Lake service associated with your Databricks Deltalake source data.


### Importing
There are several linked services that will need to be selected in order to perform the masking of your Databricks
instance.

These linked services types are needed for the following steps:

`Azure Data Lake Storage` (source) - Linked service associated with Databricks source data. This will be used for the
following steps:
* dcsazure_Databricks_to_Databricks_filter_test_utility_df/SourceData (dataFlow)
* dcsazure_Databricks_to_Databricks_copy_df/SourceData (dataFlow)
* dcsazure_Databricks_to_Databricks_unfiltered_mask_df/Source (dataFlow)
* dcsazure_Databricks_to_Databricks_filtered_mask_df/Source (dataFlow)

`Azure Data Lake Storage` (sink) - Linked service associated with Databricks sink data. This will be used for the
following steps:
* dcsazure_Databricks_to_Databricks_filter_test_utility_df/SinkData (dataFlow)
* dcsazure_Databricks_to_Databricks_copy_df/SinkData (dataFlow)
* dcsazure_Databricks_to_Databricks_unfiltered_mask_df/Sink (dataFlow)
* dcsazure_Databricks_to_Databricks_filtered_mask_df/Sink (dataFlow)

`Azure Databricks Delta Lake` (sink) - Linked service associated with Databricks Delta Lake. This will be used for the
following steps:
* dcsazure_Databricks_to_Databricks_for_sink_query_ds (Azure Databricks Delta Lake dataset)

`Azure SQL` (metadata) - Linked service associated with your hosted metadata store. This will be used for the following
steps:
* Check For Conditional Masking (If Condition activity)
* Update Copy State (Stored procedure activity)
* Update Copy State Failed (Stored procedure activity)
* Check If We Should Reapply Mapping (If Condition activity)
* Configure Masked Status (Script activity)
* dcsazure_Databricks_to_Databricks_metadata_mask_ds (Azure SQL Database dataset)
* dcsazure_Databricks_to_Databricks_unfiltered_mask_params_df/Ruleset (dataFlow)
* dcsazure_Databricks_to_Databricks_unfiltered_mask_params_df/TypeMapping (dataFlow)
* dcsazure_Databricks_to_Databricks_filtered_mask_params_df/Ruleset (dataFlow)
* dcsazure_Databricks_to_Databricks_filtered_mask_params_df/TypeMapping (dataFlow)

`REST` (DCS for Azure) - Linked service associated with calling DCS for Azure. This will be used for the following
steps:
* dcsazure_Databricks_to_Databricks_unfiltered_mask_df (dataFlow)
* dcsazure_Databricks_to_Databricks_filtered_mask_df (dataFlow)

### How It Works
* Check If We Should Reapply Mapping
  * If we should, Mark Table Mapping Incomplete. This is done by updating the metadata store to indicate that tables
    have not had their mapping applied
* Select Tables We Should Truncate
  * Select sink tables with an incomplete mapping and based on the value of `P_TRUNCATE_SINK_BEFORE_WRITE`, create a
    list of tables that we should truncate
    * For Each Table To Truncate, execute a query to truncate the sink table
* Select Tables Without Required Masking. This is done by querying the metadata store.
  * Filter If Copy Unmask Enabled. This is done by applying a filter based on the value of `P_COPY_UNMASKED_TABLES`
    * For Each Table With No Masking. Provided we have any rows left after applying the filter
      * Get Sink Table Details No Masking. Query Databricks for sink table details.
      * Get Sink Table Metadata No Masking. Query Metadata store to construct metadata information for sink table.
      * Copy data by calling `dcsazure_Databricks_to_Databricks_copy_df`
          * Update the mapped status based on the success of this dataflow, and fail accordingly
* Select Tables That Require Masking. This is done by querying the metadata store. This will provide a list of tables
  that need masking, and if they need to be masked leveraging conditional algorithms, the set of required filters.
  * Configure Masked Status. Set the masked status based on the defined filters that need to be applied for the table to
    be marked as completely mapped.
  * For Each Table To Mask
    * For each table that requires masking:
      * Get Source Metadata Mask. Query Metadata store to fetch metadata information for the source table 
      * Get Sink Table Details. Query Databricks for sink table details.
      * Get Sink Table Metadata. Query Metadata store to construct metadata information for sink table.
      * If no filter needs to be applied:
        * Call the `dcsazure_Databricks_to_Databricks_unfiltered_mask_params_df` data flow to generate masking
          parameters
        * Call the `dcsazure_Databricks_to_Databricks_unfiltered_mask_df` data flow, passing in parameters as
          generated by the generate masking parameters dataflow
        * Update the mapped status based on the success of this dataflow, and fail accordingly
      * If a filter needs to be applied:
        * Call the `dcsazure_Databricks_to_Databricks_filtered_mask_params_df` data flow to generate masking
          parameters using the filter alias
        * Call the `dcsazure_Databricks_to_Databricks_filtered_mask_df` data flow, passing in parameters as
          generated by the generate masking parameters dataflow and the filter as determined by the output of For
          Each Table To Mask
        * Update the mapped status based on the success of this dataflow, and fail accordingly
* Note that there is a deactivated activity `Test Filter Conditions` that exists in order to support importing the
  filter test utility dataflow, this is making it easier to test writing filter conditions leveraging a dataflow debug
  session

### Variables

If you have configured your database using the metadata store scripts, these variables will not need editing. If you
have customized your metadata store, then these variables may need editing.

* `METADATA_SCHEMA` - This is the schema to be used for in the self-hosted AzureSQL database for storing metadata (default `dbo`)
* `METADATA_RULESET_TABLE` - This is the table to be used for storing the discovered ruleset (default `discovered_ruleset`)
* `METADATA_SOURCE_TO_SINK_MAPPING_TABLE` - This is the table in the metadata schema that will contain the data
  mapping, defining where unmasked data lives, and where masked data should go (default `adf_data_mapping`)
* `METADATA_ADF_TYPE_MAPPING_TABLE` - This is the table that maps from data types in various datasets to the
  associated datatype required in ADF as needed for the pipeline (default `adf_type_mapping`)
* `BLOB_STORE_STAGING_STORAGE_PATH` - This is a path that specifies where we should stage data as it moves through the
  pipeline and should reference a storage container in a storage account (default `staging-container`)
* `DATASET` - This is the way this data set is referred to in the metadata store (default `ADLS`)
* `CONDITIONAL_MASKING_RESERVED_CHARACTER` - This is a string (preferably a character) reserved as for shorthand for
  when referring to the key column when defining filter conditions, in the pipeline this will be expanded out to use the
  ADF syntax for referencing the key column (default `%`)
* `TARGET_BATCH_SIZE` - This is the target number of rows per batch (default `2000`)
* `METADATA_EVENT_PROCEDURE_NAME` - This is the name of the procedure used to capture pipeline information in the
  metadata data store and sets the masked and mapping states on the items processed during execution
  (default `insert_adf_masking_event`).

### Parameters

* `P_COPY_UNMASKED_TABLES` - Bool - This enables the pipeline to copy data from source to destination when a mapping
exists, but no algorithms have been defined (default `false`)
* `P_FAIL_ON_NONCONFORMANT_DATA` - Bool - This will fail the pipeline if non-conformant data is encountered (default
`true`)
* `P_REAPPLY_MAPPING` - Bool - This controls whether we should reset the mapping between source and sink tables, this
  will mark all mappings as incomplete (default `true`)
* `P_TRUNCATE_SINK_BEFORE_WRITE` - Bool - This controls whether we should purge the directories in the sink locations
  that have not already been completed by this pipeline, note that the set of locations to purge does _not_ depend on
  the value of `P_COPY_UNMASKED_TABLES` (default `true`)
* `P_SOURCE_CATALOG` - String - This is the source catalog in Databricks that contains the unmasked data
* `P_SINK_CATALOG` - String - This is the sink catalog in Databricks that will serve as a destination for masked data
* `P_SOURCE_SCHEMA` - String - This is the source schema in Databricks (that lives under the specified source catalog)
and that contains the unmasked data
* `P_SINK_SCHEMA` - String - This is the sink schema in Databricks (that lives under the specified sink catalog) and
that will serve as a destination for masked data

