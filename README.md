# dcs-for-azure-templates
A collection of templates that can be used to leverage Delphix Compliance Services in Azure.

## Setup
To use Delphix Compliance Services for Azure, and specifically to leverage these pipelines you will need to have the
following linked services in your data factory:
* REST service for talking to DCS for Azure
* Azure SQL Database for storing metadata about the data you wish to profile and mask

### To Import Latest Version Templates

Run `docker-compose -f docker-compose.yaml up && stty sane`, this will create the latest version of all templates
and put them in the `releases` directory. The `stty sane` is to fix the terminal as docker-compose doesn't always
clean up after itself correctly. From there, you can import the template into your data factory using the Data
Factory Studio:
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

Database versions are determined by date. If you're just getting started, we've combined all required SQL commands
into [metadata_store_scripts/bootstrap.sql](./metadata_store_scripts/bootstrap.sql). If you've already got a version of
the metadata store, you may need to apply migrations for subsequent versions in order to update the metadata store to
the required version in order for a new version of the templates to work - each template will contain a README that will
indicate what the minimum required version of the metadata store is required. We aim to make it so that all migrations
will not break existing pipelines.

The migration scripts do not specify a schema, as you are free to create these scripts in independent schemas per your
needs. However, this means before running the script, you will need to specify the schema you are working in (unless you
choose to leverage the default `dbo` schema).

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
            * When a custom date format is required for an algorithm (besides `yyyy-MM-dd` or
              `yyyy-MM-dd'T'HH:mm:ss'Z'` which are the default), the key `date_format` must be specified in the
              metadata
              * As an example, if you have a column `transaction_date` in your `Snowflake` instance, and each time that
                column appears it has a date formated as `yyyyMMdd`, you should specify this format using an update
                statement like
                ```sql
                UPDATE discovered_ruleset
                SET metadata = JSON_MODIFY(coalesce(metadata,'{}'), '$.date_format', 'yyyyMMdd')
                WHERE dataset = 'SNOWFLAKE' AND identified_column = 'transaction_date';
                ```
                Setting this value incorrectly will cause non-conformant data errors when the data violates the
                specified pattern
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

## Additional Resources

* Dataflows documentation
  * [Profiling dataflow](./documentation/dataflows/prof_df.md)
  * [Masking dataflow](./documentation/dataflows/mask_df.md)
  * [Masking Parameters dataflow](./documentation/dataflows/mask_params_df.md)
  * [Copy dataflow](./documentation/dataflows/copy_df.md)
* Pipeline documentation - Documentation for each pipeline is included in the released version of the template
  * [dcsazure_Databricks_to_Databricks_mask_pl](./documentation/pipelines/dcsazure_Databricks_to_Databricks_mask_pl.md)
  * [dcsazure_Databricks_to_Databricks_prof_pl](./documentation/pipelines/dcsazure_Databricks_to_Databricks_prof_pl.md)
  * [dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_mask_pl](./documentation/pipelines/dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_mask_pl.md)
  * [dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_prof_pl](./documentation/pipelines/dcsazure_Snowflake_Legacy_to_Snowflake_Legacy_prof_pl.md)
  * [dcsazure_Snowflake_to_Snowflake_mask_pl](./documentation/pipelines/dcsazure_Snowflake_to_Snowflake_mask_pl.md)
  * [dcsazure_Snowflake_to_Snowflake_prof_pl](./documentation/pipelines/dcsazure_Snowflake_to_Snowflake_prof_pl.md)
  * [dcsazure_adls_to_adls_mask_pl](./documentation/pipelines/dcsazure_adls_to_adls_mask_pl.md)
  * [dcsazure_adls_to_adls_prof_pl](./documentation/pipelines/dcsazure_adls_to_adls_prof_pl.md)


## Contribution
Contributions are welcome!

If you'd like to contribute be sure you're starting with the latest templates (should they exist) before you make
changes. Once you're ready to contribute your changes back, the best way to get the templates ready to add to this repo
is by leveraging the `Export template` option from the tab of the pipeline detail view in Data Factory Studio.
(Tip: before exporting your template make sure variables are set to common-sense default values.)

To make this easier to follow, let's use an example. Suppose you'd like to add `sample_template_pl` to the set of
pipelines that are available in this repository. Your exported template will come from Azure with the name
`sample_template_pl.zip`. Once you unzip this file, it will produce a directory with structure like:
```
sample_template_pl
├── manifest.json
└── sample_template_pl.json
```

These files, since they are meant to be imported directly into Data Factory Studio are not well-suited to version
control (as they are one very long line). To make these files more suited to version control, you can leverage `jq`.

For the files in the example above, this can be done with:
`cat manifest.json | jq '.' > manifest.json.fmt && mv manifest.json.fmt manifest.json` and
`cat sample_template_pl.json | jq '.' > sample_template_pl.json.fmt && mv sample_template_pl.json.fmt sample_template_pl.json`

New templates will need to have an associated `README.md` and for them to be built into the `releases` folder, edits
will have to be made to the `docker-compose.yaml`. For this example, you'd have to add a line
`zip sample_template_pl.zip sample_template_pl/* &&` somewhere between `apt-get install -y zip &&`
and `mv *.zip releases/."`.

If Metadata store scripts are needed to support your new templates or new features, please update the `metadata_store_scripts`
directory to include those changes. New statements should play nicely with previous statements - isolate the changes in
a particular versioned script, and follow the version naming convention, as well as adding them to the end of
`bootstrap.sql` (please also edit the comment at the top of `bootstrap.sql` to indicate which version scripts have been
added). Versioned scripts follow the naming convention `V<version_number>__<comment>.sql`, where `<version_number>` is
`YYYY.MM.DD.#` and represents the date when this script is added (with the final digit being used to allow multiple
versions to be tagged with the same date).
