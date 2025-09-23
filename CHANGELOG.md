# CHANGELOG

# 0.0.31
* Add GitHub Actions workflow for automated pre-commit validation on pull requests

# 0.0.30
* Re-introduce the parameterization for source and sink database linked services in the Snowflake masking pipeline
`dcsazure_Snowflake_to_Snowflake_mask_pl`
* Add a pre-commit check to validate the expected count of LinkedService parameter names in each pipeline

# 0.0.29
* Replace masking parameter dataflows with SQL stored procedure in `dcsazure_Snowflake_to_Snowflake_mask_pl`

# 0.0.28
* Replace masking parameter dataflows with SQL stored procedure in `dcsazure_AzureSQL_to_AzureSQL_mask_pl`
  * Unified and optimized conditional and non-conditional masking parameter generation in a single stored procedure
* Fixed type casting bug where `treat_as_string` flag caused zero rows in sink table (#65)  
* Add SQLFluff configuration for T-SQL formatting standards

# 0.0.27
* Fixed an issue where many small requests are being sent instead of fewer larger requests in `dcsazure_ADLS_to_ADLS_delimited_mask_pl` and `dcsazure_ADLS_to_ADLS_parquet_mask_pl`.

# 0.0.26
* Updated the resource activity `Determine All Heterogeneous Schema` name to title case in `dcsazure_ADLS_to_ADLS_delimited_discovery_pl` pipeline template.

# 0.0.25 
* Introduced functionality for identifying nested directories in the `dcsazure_ADLS_to_ADLS_delimited_discovery_pl` pipeline.
* Modified queries to select the root schema and execute the masking job at the entire container level in the `dcsazure_ADLS_to_ADLS_delimited_masking_pl` pipeline.

# 0.0.24
* Add support for parameterized linked services in the `dcsazure_AzureSQL_to_AzureSQL_discovery_pl` and `dcsazure_AzureSQL_to_AzureSQL_mask_pl` pipelines.

# 0.0.23
* Add support for parameterized linked services in the `dcsazure_AzureSQL_MI_to_AzureSQL_MI_discovery_pl` and `dcsazure_AzureSQL_MI_to_AzureSQL_MI_mask_pl` pipelines.

# 0.0.22
* Fix bugs in `dcsazure_Databricks_to_Databricks_mask_pl` that could cause incorrect table selections in mask and copy

# 0.0.21
* Fix bugs in `dcsazure_AzureSQL_MI_to_AzureSQL_MI_mask_pl`
  * Add the metadata schema to constraint pipelines in SQL where it was missing.
  * Metadata store fixes for Azure SQL MI.
  * Update `docker-compose.yaml` to include Azure SQL MI templates.

# 0.0.20
* Fix bugs in `dcsazure_AzureSQL_to_AzureSQL_mask_pl`
   * Correct the schema for the Ruleset source table to reflect metadata store version `V2025.01.15.0`.
   * Fix an issue in dataflow logic that prevents the masking pipeline from masking JSON, XML, and UUID column types in both filtered and unfiltered masking dataflows.

# 0.0.19
* Add support for AzureSQL Managed Instances (MI) to AzureSQL MI Discovery and Masking Pipelines

# 0.0.18
* Fix a bug in `dcsazure_AzureSQL_to_AzureSQL_mask_pl` that causes the Create Constraints pipeline to fail when the metadata store schema is not present in the sink database.

# 0.0.17
* Support casting columns to strings and back in masking pipelines
  * Supports casting from source data type to String and then back to the following ADF types:
    Binary, Boolean, Date, Double, Float, Integer, Long, and Timestamp
  * Added to the following pipelines:
    * `dcsazure_ADLS_to_ADLS_parquet_mask_pl`
    * `dcsazure_AzureSQL_to_AzureSQL_mask_pl`
    * `dcsazure_Databricks_to_Databricks_mask_pl`
    * `dcsazure_Snowflake_to_Snowflake_mask_pl`
  * Note: this support is not added to `dcsazure_ADLS_to_ADLS_parquet_mask_pl` because all underlying types are string
* Separate algorithm metadata and source metadata to reduce errors when modifying masking configurations
  * A new column, `algorithm_metadata` has been added
  * Migrate previously set algorithm configurations to the `algorithm_metadata` column from the `metadata` column, this
    includes `date_format`, `key_column`, and `conditions`
  * Rename `metadata` column to `source_metadata`
* Allow multi-line input in ADLS to ADLS delimited files for discovery masking and copying
* Address #32
* Address #33

# 0.0.16
* Fix a bug in `dcsazure_AzureSQL_to_AzureSQL_mask_pl` that causes the pipeline to fail when the metadata store schema
  is not present in the sink database.

# 0.0.15
* Add support for ADLS to ADLS Parquet discovery and masking pipelines
* Update existing ADLS to ADLS discovery and masking pipelines to differentiate between new pipeline and existing
  pipeline with focus on delimited files - including changing current references to `ADLS` to `ADLS-DELIMITED`
* Update stored procedures for parsing metadata output to take the dataset as an argument and remove hard-coded
  references to datasets in ADLS pipelines

# 0.0.14
* Add support for DCS for Azure SQL to Azure SQL Discovery Pipeline
* Add support for DCS for Azure SQL to Azure SQL Masking Pipeline

# 0.0.13
* Remove support for Snowflake (Legacy) connectors

# 0.0.12
* Add support for checkpointing and error handling to all pipelines
* Rename `profiling` components to `discovery` to be consistent with industry-standard terminology
* Add feature to `dcsazure_adls_to_adls_discovery_pl` to support profiling files in directories with a large number of
  files

# 0.0.11
* Add support for conditional masking to the following pipelines:
  * `dcsazure_adls_to_adls_mask_pl`
  * `dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_mask_pl`
  * `dcsazure_Snowflake_to_Snowflake_mask_pl`
  * `dcsazure_Databricks_to_Databricks_mask_pl`
* Update documentation to describe how to use conditional masking
* Update pipeline documentation to describe how conditionally masked tables are masked
* Update documentation to describe how masking parameters are computed

# 0.0.10
* Fix a bug in `dcsazure_adls_to_adls_prof_pl` that causes the pipeline to fail when there are no directories with
  heterogeneous schemas to profile

# 0.0.9
* Update dataflows for generating masking params to fix a bug in custom date format handling.
  * This bug is encountered when only some columns of a table have date format specified.
    * This is not encountered when no columns in a table have date format specified.
    * This is not encountered when all columns in a table have date format specified.
* Specifically the updated dataflows are:
  * `dcsazure_adls_to_adls_mask_params_df`
  * `dcsazure_Databricks_to_Databricks_mask_params_df`
  * `dcsazure_Snowflake_to_Snowflake_mask_params_df`
  * `dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_mask_params_df`
* Update `dcsazure_Databricks_to_Databricks_mask_pl` pipeline to fix an issue where selecting distinct metadata causes
  the same table to be selected for masking multiple times, leading to duplicate data in the destination.

# 0.0.8
* Update DCS for Azure Databricks to Databricks Masking pipeline:
  * Since Delta Lake Copy Activities are unsupported, remove this option from the pipeline

# 0.0.7
* Address [#4](https://github.com/delphix/dcs-for-azure-templates/issues/4)
  * Update DCS for Azure Databricks to Databricks Profile Pipeline
    * Determine table metadata using `describe detail` query
    * Rename some parameters and variables to be more consistent
    * Reduce number of parameters by moving reasonably static components to variables
  * Update DCS for Azure Databricks to Databricks Masking Pipeline
    * Determine sink table metadata using `describe detail` query
    * Rename some parameters and variables to be more consistent
    * Add support for handling non-conformant data
    * Add support for handling custom date format
    * Reduce number of parameters by moving reasonably static components to variables
* Provide descriptions for steps in the following dataflows:
  * `dcsazure_Databricks_to_Databricks`
    * `dcsazure_Databricks_to_Databricks_copy_df`
    * `dcsazure_Databricks_to_Databricks_mask_df`
    * `dcsazure_Databricks_to_Databricks_mask_params_df`
    * `dcsazure_Databricks_to_Databricks_prof_df`
    * `dcsazure_Databricks_to_Databricks_prof_df_empty_tables`

# 0.0.6
* Add support for handling custom date format in DCS for Azure Snowflake to Snowflake Masking Pipeline (with Snowflake linked service type)
* Add support for handling custom date format in DCS for Azure Snowflake to Snowflake Masking Pipeline (with Snowflake (Legacy) linked service type)
* Add support for handling custom date format in DCS for Azure ADLS to ADLS Masking Pipeline (for Gen2 Data Lake)
* Improve computation for number of batches in DCS for Azure Snowflake to Snowflake Masking Pipeline (with Snowflake linked service type)
* Improve computation for number of batches in DCS for Azure Snowflake to Snowflake Masking Pipeline (with Snowflake (Legacy) linked service type)
* Correct location of datasets for DCS for Azure ADLS to ADLS Profiling Pipeline
* Provide descriptions for steps in the following dataflows
  * `dcsazure_adls_to_adls`
    * `dcsazure_adls_to_adls_delimited_copy_df`
    * `dcsazure_adls_to_adls_delimited_mask_df`
    * `dcsazure_adls_to_adls_delimited_prof_df`
    * `dcsazure_adls_to_adls_mask_params_df`
  * `dcsazure_Snowflake_Legacy_to_Snowflake_Legacy`
    * `dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_copy_df`
    * `dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_mask_df`
    * `dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_mask_params_df`
    * `dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_prof_df`
  * `dcsazure_Snowflake_to_Snowflake`
    * `dcsazure_Snowflake_to_Snowflake_copy_df`
    * `dcsazure_Snowflake_to_Snowflake_mask_df`
    * `dcsazure_Snowflake_to_Snowflake_mask_params_df`
    * `dcsazure_Snowflake_to_Snowflake_prof_df`

# 0.0.5
* Add support for handling non-conformant data to DCS for Azure Snowflake to Snowflake Masking Pipeline (with Snowflake linked service type)
* Add support for handling non-conformant data to DCS for Azure Snowflake to Snowflake Masking Pipeline (with Snowflake (Legacy) linked service type)
* Improve usability of DCS for Azure Snowflake to Snowflake Masking Pipeline (with Snowflake linked service type)
* Improve usability of DCS for Azure Snowflake to Snowflake Masking Pipeline (with Snowflake (Legacy) linked service type)
* Improve usability of DCS for Azure Snowflake to Snowflake Profiling Pipeline (with Snowflake linked service type)
* Improve usability of DCS for Azure Snowflake to Snowflake Profiling Pipeline (with Snowflake (Legacy) linked service type)

# 0.0.4
* Improve DCS for Azure ADLS to ADLS Profiling Pipeline (for Gen2 Data Lake)
  * Reduce complexity of pipeline parameters by removing unsupported parameters
  * Reduce number of parameters by moving reasonably static components to variables
* Improve DCS for Azure ADLS to ADLS Masking Pipeline (for Gen2 Data Lake)
  * Simplify the way in with batch sizes are managed
  * Add support for not failing the pipeline when non-conformant data errors are encountered
  * Reduce number of parameters by moving reasonably static components to variables

# 0.0.3
* Add support for DCS for Azure ADLS to ADLS Profiling Pipeline (for Gen2 Data Lake)
* Add support for DCS for Azure ADLS to ADLS Masking Pipeline (for Gen2 Data Lake)

# 0.0.2
* Add support for DCS for Azure Snowflake to Snowflake Profile Pipeline (with Snowflake linked service type)
* Add support for DCS for Azure Snowflake to Snowflake Masking Pipeline (with Snowflake linked service type)

# 0.0.1
* Rename DCS for Azure Snowflake to Snowflake Profile Pipeline to reflect that it uses the Snowflake (Legacy) linked service type
* Rename DCS for Azure Snowflake to Snowflake Masking Pipeline to reflect that it uses the Snowflake (Legacy) linked service type
* Naming improvements to components in these pipelines for consistency and disambiguation on import of both pipelines

# 0.0.0
* Add support for DCS for Azure Databricks to Databricks Profile Pipeline
* Add support for DCS for Azure Databricks to Databricks Masking Pipeline
* Add support for DCS for Azure Snowflake to Snowflake Profile Pipeline
* Add support for DCS for Azure Snowflake to Snowflake Masking Pipeline
