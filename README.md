# dcs-for-azure-templates
A collection of templates that can be used to leverage Delphix Compliance Services in Azure.

## Setup
To use Delphix Compliance Services for Azure, and specifically to leverage these pipelines you will need to have the
following linked services in your data factory:
* REST service for talking to DCS for Azure
* Azure SQL Database for storing metadata about the data you wish to profile and mask

### To Import Latest Version Templates

Run `docker-compose -f docker-compose.yaml up`, this will create the latest version of all templates and put them
in the `releases` directory. From there, you can import the template into your data factory using the Data Factory
Studio: 
* From the `Author` tab, click the `+` next to the search bar for the Factory Resources
* Select `Pipeline`, then `Import from pipeline template`
  * This will open a file explorer window, you can select the recently built template
  from the `releases` folder. 
* After selecting the appropriate template, you will be asked to to select the linked
services for various steps in the pipeline.
* Each template will be different, please refer to the `README.md` file
in the template's folder to familiarize yourself with the linked services that should be selected.


### Self-Hosted Metadata Store

This metadata store will consist of a few tables, a series of scripts for defining the tables required by the metadata
store has been provided under the [metadata_store_scripts](./metadata_store_scripts) directory.

The metadata store consists of the following tables that all must be in the same schema:
* `discovered_ruleset` - This table is used to define a ruleset. The ruleset uniquely identifies columns and the
algorithms that should be applied to those columns. This table is populated in one of two ways:
    1. Automatically, via the profiling pipeline. The data in this table is populated in two stages.
       1. Probe the information schema or other metadata about the specified database, schema, catalog, etc. Populate
       the `discovered_ruleset` table with as much of the following information as possible:
          * `dataset` (static) - determines which type of data this is, it will be something like `SNOWFLAKE`, but
             will be representative of the datasource; this value and will be statically populated in the appropriate
             profiling pipeline, and will be checked in masking pipelines to make sure that rules are applied only when
             they are referring to the appropriate dataset
          * `specified_database` (parameter) - the top-level identifier for the dataset, in the case of things
             like AzureSQL and Snowflake this will be the `database`, for things like Databricks it will mean the
             `catalog`
          * `speficied_schema` (parameter) - the second-level identifier for the dataset, in the case of things
             like AzureSQL, Snowflake, and Databricks this will be the `schema`, but may vary in the future for other
             data sources that have an analogous concept, but with a different name
          * `identified_table` (determined) - the third-level identifier for the dataset, in the case of things
             like AzureSQL, Snowflake, and Databricks this will be the `table`, but may vary in the future for other
             data sources that have an analogous concept, but with a different name
          * `identified_column` (determined) - the fourth-level identifier for the dataset, in the case of things
             like AzureSQL, Snowflake, and Databricks this will be the `column`, but may vary in the future for other
             data sources that have an analogous concept, but with a different name
          * `identified_column_type` (determined) - either the exact data type provided by the information schema or
             a slightly modified version of what's provided by the information schema, this modification is used in
             order for it to be more easily joined with the `adf_type_mapping` table
          * `ordinal_position` (determined) - the ordinal position of the column in the table, which refers to the
             column's location when ordering within a table
          * `row_count` (determined) - the number of rows in the table, if this is available in metadata then it is
             populated in this stage, if it is not, then it is added in the next stage
          * `metadata` (determined) - additional JSON-structured metadata whose specific structure will vary based on
             the dataset and in some cases may not be required
       2. Collect data from the specified table and column combination and perform data profiling to determine if the
          data is likely to be sensitive. This is done by calling the `profiling` endpoint in DCS for Azure services,
          collecting the results of profiling, and persisting them to the `discovered_ruleset` metadata table,
          populating for key `(dataset, specified_database, specified_schema, identified_table, identified_column)`, the
          following values:
          * `row_count` (determined) - the number of rows in the table, if this was not populated by the previous phase,
             it is populated in this phase using a `SELECT COUNT(1)` type operation
          * `profiled_domain` (determined) - the domain of the data that resulted when profiling determined the
             existence of sensitive data
          * `profiled_algorithm` (determined) - the algorithm that could be applied to this data for masking as
             determined by profiling for the existence of sensitive data
          * `confidence_score` (determined) - the confidence score as it applies to the profiling result, represented as
             a value from `[0.00000, 1.0000]` that represents how closely the profiled data matches the sensitive data
             classifiers for the profiled domain and algorithm pair
          * `rows_profiled` - the number of rows included in the profiling request
    2. Manually, where users enter values to create rows for fields in each of the tables to define which rules (masking
algorithms) should be applied when masking data. This is not preferred as it is error-prone.
* `adf_data_mapping` - This table is used to define source to destination mappings for the masking pipeline. The table
   maps a source `dataset, database, schema, table` to a corresponding destination `dataset, database, schema, table`.
   The rules in the ruleset (defined in `discovered_ruleset`) will be applied when performing masking and will copy data
   (either masked or unmasked, depending on how the ruleset is configured) from the source to the sink.
* `adf_type_mapping` - This table is used to track how data should be treated as it flows through masking pipeline in
   Azure Data Factory, where applicable. The values in this table are inserted in the migration scripts.
