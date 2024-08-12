# CHANGELOG

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
