# dcsazure_adls_to_adls_mask_pl
## Delphix Compliance Services (DCS) for Azure - ADLS to ADLS Masking Pipeline

This pipeline will perform masking of data from your Azure Data Lake (ADLS) Data from one location to another.

### Prerequisites

1. Configure the hosted metadata database and associated Azure SQL service (version `V2024.04.18.0`+).
1. Configure the DCS for Azure REST service.
1. Configure the Azure Data Lake Storage service associated with your ADLS source data.
1. Configure the Azure Data Lake Storage service associated with your ADLS sink data.


### Importing
There are several linked services that will need to be selected in order to perform the masking of your ADLS
instance.

These linked services types are needed for the following steps:

`Azure Data Lake Storage` (source) - Linked service associated with ADLS source data. This will be used for the
following steps:
* Switch On Copy Methodology And Supported Write (Switch activity)
* dcsazure_adls_to_adls_mask_df/adlsSource (dataFlow)
* dcsazure_adls_to_adls_copy_df/adlsSource (dataFlow)

`Azure Data Lake Storage` (sink) - Linked service associated with ADLS sink data. This will be used for the
following steps:
* dcsazure_adls_to_adls_mask_df/adlsSink (dataFlow)
* dcsazure_adls_to_adls_copy_df/adlsSink (dataFlow)

`Azure ADLS ` (source) - Linked service associated with ADLS. This will be used for the
following steps:
* dcsazure_adls_to_adls_for_mask_query_ds (Azure adls  dataset)
* dcsazure_adls_to_adls_copy_ds (Azure adls  dataset)

`Azure SQL` (metadata) - Linked service associated with your hosted metadata store. This will be used for the following
steps:
* dcsazure_adls_to_adls_metadata_mask_ds (Azure SQL Database dataset)
* dcsazure_adls_to_adls_mask_params_df/Ruleset (dataFlow)
* dcsazure_adls_to_adls_mask_params_df/TypeMapping (dataFlow)

`REST` (DCS for Azure) - Linked service associated with calling DCS for Azure. This will be used for the following
steps:
* dcsazure_adls_to_adls_mask_df (dataFlow)

### How It Works
* Select Tables That Require Masking
    * Choose, based on what is present in the metadata data store, which tables require masking
      * For each table that requires masking:
        * Call the `dcsazure_adls_to_adls_mask_params_df` data flow to generate masking parameters
        * Call the `dcsazure_adls_to_adls_mask_df` data flow, passing in parameters as generated by
          the generate masking parameters dataflow
* Select Tables Without Required Masking
    * Choose, based on what is present in the metadata data store, which tables do not require masking
      * For each table that does not require masking, perform a copy activity or copy via dataflow.

### Notes
As ADLS does not provide us the number of rows in the files, and as delimited files don't have a fixed width to columns,
we make a request run into request size limits or have a suboptimal number of batches.
* If you are running into request size limits, decrease the number of rows in a batch by modifying the `ROWS_PER_BATCH`
  variable.
* If you are not hitting request size limits, but are finding that your masking pipeline is taking a long time, you may
  wish to increase the number of rows per batch.

### Variables

If you have configured your database using the metadata store scripts, these variables will not need editing. If you
have customized your metadata store, then these variables may need editing.

* `METADATA_SCHEMA` - This is the schema to be used for in the self-hosted AzureSQL database for storing metadata
  (default `dbo`).
* `METADATA_RULESET_TABLE` - This is the table to be used for storing the discovered ruleset (default
  `discovered_ruleset`).
* `METADATA_SOURCE_TO_SINK_MAPPING_TABLE` - This is the table used for determining where data that is run through the
  pipeline starts and ends (default `adf_data_mapping`)
* `METADATA_ADF_TYPE_MAPPING_TABLE` - This is the table used for determining how Azure Data Factory should interpret
  data that flows through the pipeline (default `adf_type_mapping`)
* `ROWS_PER_BATCH` - This is the number of rows per batch (default `1000`)

### Parameters

* `P_COPY_UNMASKED_TABLES` - Bool - This enables the pipeline to copy data from source to destination when a mapping
  exists, but no algorithms have been defined (default `false`)
* `P_COPY_USE_DATAFLOW` - Bool - This enables the pipeline to use a data flow to copy data from source to sink when
  there is no data to mask (this value does not matter if `P_COPY_UNMASKED_TABLES` is `false`)
* `P_FAIL_ON_NONCONFORMANT_DATA` - Bool - This enables the pipeline to handle non-conformant data errors without failing
  the pipeline - if set to `true`, unmasked data that did not conform to the format required to apply the specified
  algorithm will appear in the output data; if set to `false`, data that did not conform to the format required to apply
  the specified algorithm will cause the pipeline to fail (default `true`)
* `P_SOURCE_DATABASE` - String - This is the source directory in ADLS that contains the unmasked data
* `P_SINK_DATABASE` - String - This is the sink directory in ADLS that will serve as a destination for masked data
* `P_SOURCE_DIRECTORY` - String - This is the source schema in ADLS and was discovered during the profiling pipeline
  run - it will have a format that defines the prefix of a file name in ADLS (with its full location) and that contains
  the unmasked data
* `P_SINK_DIRECTORY` - String - This is the sink schema in ADLS that will have a structure similar to the source schema
  name, except that anything after the last `/` is ignored and that will serve as a destination for masked data (file
  names will be preserved)