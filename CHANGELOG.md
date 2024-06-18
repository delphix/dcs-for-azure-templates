# CHANGELOG

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
