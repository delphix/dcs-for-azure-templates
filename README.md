## Purpose
Azure Data Factory supports 100+ connectors, including those for SaaS applications and PaaS databases. Data that resides
in disparate locations often needs masking in order to be leveraged by engineers for feature development or issue
triage and debugging. These things must never be done in production as the risk to data exposure is too great. Data
protection is also required in accordance with regulations such as GDPR, FINRA, CCPA, HIPAA, and PCI DSS. For these
reasons, it is essential that it's possible to identify and mask sensitive data from production into lower environments.

Delphix provides several APIs that perform data discovery and data masking. Microsoft's Azure Data Factory provides a
framework for processing data using those APIs, making it possible to perform automated sensitive data discovery, assign
masking algorithms, and mask sensitive data such that it maintains referential integrity.

<!-- Internal testing comment for pre-commit validation -->

## What Is This Code For?

This repository contains a collection of templates that combine the power of Delphix Compliance Services in Azure and
Azure Data Factory to perform data discovery on a collection of sources and mask data that has been found to be
sensitive, producing production-quality data without data exposure, which can then be made available to downstream teams
without compromising data security.

To use these templates you will need a Delphix Compliance Services account, for that more information can be found
here: https://www.perforce.com/products/delphix/compliance-services

Don't see a data source you're hoping to profile and mask? If you feel compelled, you can use these provided templates
as a jumping off point and create a new template to discover and mask sensitive data using the Delphix compliance
services APIs. [Contributions](./README.md#Contribution) are highly appreciated, and if you need engineering support
tailored to your specific use case, contributing templates back to this repository will enable our engineering team to
assist when issues arise. If you don't know where to start but uncover a need, please file a feature request under the
[Issues](https://github.com/delphix/dcs-for-azure-templates/issues) tab of this repository.

## Setup
To use Delphix Compliance Services for Azure, and specifically to leverage these pipelines you will need to have the
following linked services in your data factory:
* REST service for talking to DCS for Azure
* Azure SQL Database for storing metadata about the data you have discovered and rules for masking

### To Import Latest Version Templates

To create the latest version of the template, you will need to zip the content in this repository.

For convenience, we have provided a Docker file that will create the latest version of all templates (and include the
version in the archive) and put them in the `releases` directory. If you have Docker installed you can leverage the
provided [docker-compose.yaml](docker-compose.yaml) file to build release artifacts:
`docker-compose -f docker-compose.yaml up`

If you're unable to install Docker on your machine, you can manually create artifacts for any of the templates in this
repository, in any number of ways. In all of the following examples, the directory you for each pipeline is
`<template_dir>`:
* On Windows:
  * Leverage the file explorer for these purposes:
    https://support.microsoft.com/en-us/windows/zip-and-unzip-files-8d28fa72-f2f9-712f-67df-f80cf89fd4e5
  * Use Powershell's `Compress-Archive` to zip the directory like: `Compress-Archive <template_dir> <template_dir>.zip`
  * (Windows 10) Use the command prompt's `tar` executable like: `tar.exe -a -c -f <template_dir>.zip <template_dir>`
* On Mac:
  * Leverage Finder to compress (zip) the directory
  * Leverage the commandline and the `zip` command like: `zip <template_dir>.zip <template_dir>/`
* On Linux:
  * Leverage the commandline and the `zip` command like: `zip <template_dir>.zip <template_dir>/`

Once you have created a template, you can import the template into your data factory using the Data Factory Studio:
* From the `Author` tab, click the `+` next to the search bar for the Factory Resources
* Select `Pipeline`, then `Import from pipeline template`
  * This will open a file explorer window, you can select the recently built template from the `releases` folder or
    wherever it exists.
* After selecting the appropriate template, you will be asked to to select the linked services for various steps in the
pipeline.
* Each template will be different, please refer to the `README.md` file in the template's folder to familiarize yourself
with the linked services that should be selected.


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

To change the schema, run the following command:
```sql
ALTER USER <UserName> WITH DEFAULT_SCHEMA = <SchemaName>;
```

**Note:** Ensure the following requirements are met before running the migration scripts:
* The user has the necessary permissions to access the schema and execute the statement.
* The user is **not** a member of the `sysadmin` fixed server role, as this will cause the `DEFAULT_SCHEMA` value to be ignored, defaulting to the `dbo` schema.

The metadata store consists of the following tables that all must be in the same schema:
* `discovered_ruleset` - This table is used to define a ruleset. The ruleset uniquely identifies columns and the
algorithms that should be applied to those columns. This table is populated in one of two ways:
    1. Automatically, via the discovery pipeline. The data in this table is populated in two stages.
       1. Probe the information schema or other metadata about the specified database, schema, catalog, etc. Populate
       the `discovered_ruleset` table with as much of the following information as possible:
          * `dataset` (static) - determines which type of data this is, it will be something like `SNOWFLAKE`, but
             will be representative of the datasource; this value and will be statically populated in the appropriate
             discovery pipeline, and will be checked in masking pipelines to make sure that rules are applied only when
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
          * `source_metadata` (determined) - additional JSON-structured metadata whose specific structure will vary
             based on the dataset and in some cases may not be required
          * `algorithm_metadata` (user-configured) - additional JSON-structured metadata used to configure masking
            behavior on a specific field
       2. Collect data from the specified table and column combination and perform data discovery to determine if the
          data is likely to be sensitive. This is done by calling the `profile` endpoint in the discovery component of
          DCS for Azure services, collecting the results of profiling, and persisting them to the `discovered_ruleset`
          metadata table, populating for key `(dataset, specified_database, specified_schema, identified_table,
          identified_column)`, the
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
          * `discovery_complete` (determined) - bit representing whether the data in this column has had data discovery
            performed on it
          * `latest_event` (determined) - unique identified that is used to refer to events in event logging table which
            is useful when attempting to decipher why discovery may have failed
    2. Manually, where users enter values to create rows for fields in each of the tables to define which rules (masking
       algorithms) should be applied when masking data. This is not preferred as it is error-prone.
* `adf_data_mapping` - This table is used to define source to destination mappings for the masking pipeline. The table
   maps a source `dataset, database, schema, table` to a corresponding destination `dataset, database, schema, table`.
   The rules in the ruleset (defined in `discovered_ruleset`) will be applied when performing masking and will copy data
   (either masked or unmasked, depending on how the ruleset is configured) from the source to the sink.
  * Whether this mapping has been successfully completed is tracked in `mapping_complete` and `masked_status`, and these
    values are automatically updated based on the results of the pipeline, and success and failure can be tracked by
    leveraging the event logging table and `latest_event`
* `adf_type_mapping` - This table is used to track how data should be treated as it flows through masking pipeline in
   Azure Data Factory, where applicable. The values in this table are inserted in the migration scripts.

## Building A Ruleset

For data to be masked, there must be a mapping in the `adf_data_mapping` table, and at least one `assigned_algorithm`
assigned to the table. Note that the `profiled_algorithm` that is populated as a result of the profiling pipeline should
always produce a valid algorithm name.

### Controlling Masking Behavior

Masking behavior can be controlled in several ways:
* In Pipeline Parameters:
  * `P_COPY_UNMASKED_TABLES` - Default: `false` - This controls whether tables that have a data mapping but no assigned
    algorithms will be copied from the source to the sink. By default, no unmasked data is copied.
  * `P_USE_COPY_DATAFLOW` - Default: `false` - When available, this option controls whether tables that have a data
    mapping but no assigned algorithm are copied via a copy dataflow (in lieu of a Copy pipeline activity). This is not
    available in all pipelines, but a Copy activity is preferred for cost reasons.
  * `P_FAIL_ON_NONCONFORMANT_DATA` - Default: `true` - This controls the behavior of non-conformant data errors that may
    be encountered when masking is performed. By default, since non-conformant data errors mean no masking can be
    applied, this is true so that no unmasked data is in the sink. If you'd prefer that non-conformant data errors don't
    fail the pipeline, the non-conformant data (unmasked) can land in the sink instead by setting this to `true`; in
    such cases the data that doesn't generate non-conformant data errors will be masked before being written to the
    sink.
  * `P_REAPPLY_MAPPING` - Default: `true` - This controls whether to attempt to re-apply the source to sink mappings
    that had previously been successfully completed. By default, don't copy or mask data that we have already copied or
    masked.
  * `P_TRUNCATE_SINK_BEFORE_WRITE` - Default: `true` - This controls whether the sink should be cleared of data before
    running the pipeline. By default, truncate the sink so that we don't have collisions, duplicate data, or other such
    errors that could cause the masking pipeline to fail because of preexisting data in the sink.
* In `assigned_algorithm`:
  * Making algorithms are assigned to columns based on the value in the `assigned_algorithm`, this determines how data
    in a given column is masked.
  * If no masking is to be applied to a given column, this value can either be `NULL` or `''`.
  * If masking is to be applied, the value to assign based on available algorithms can be found in the web-UI.
  * If the data in this column changes based on the value in a different column, then the algorithm that is assigned to
    this column can be masked leveraging conditional masking. Additional details can be found in the below conditional
    masking section.
* In `algorithm_metadata`:
  * `date_format` - When a custom date format is required for a date algorithm the key `date_format` must be specified
    in the `algorithm_metadata` JSON column. Note that this is not required, as there is lenient matching of
    `yyyy-MM-dd'T'HH:mm:ss'Z'` by default.
    * Setting this value incorrectly will cause non-conformant data errors when the data violates the specified pattern.
    * The important thing here is that the JSON value in the `algorithm_metadata` column contains a `date_format` key at
      the root, and that the value of that key is the string that represents the format.
      * As an example, if you have a column `transaction_date` in your `Snowflake` instance, and each time that column
        appears it has a date formated as `yyyyMMdd`, you should specify this format using an update statement like
        ```sql
        UPDATE dbo.discovered_ruleset
        SET algorithm_metadata = JSON_MODIFY(coalesce(algorithm_metadata, JSON_OBJECT()),
                                             '$.date_format',
                                             'yyyyMMdd')
        WHERE dataset = 'SNOWFLAKE' AND identified_column = 'transaction_date';
        ```
  * `treat_as_string` - When a field has a type that incompatible with a type required for masking (e.g. when a numeric
    column represents a date), the masking API cannot convert from the numeric type to the required type. To remedy this
    the API needs to receive the numeric type as a string. This can be accomplished by setting `treat_as_string` to true
    and the masking dataflows will convert the designated column to a string, and then cast it back to the appropriate
    type before attempting to sink.
  * `key_column` & `conditions` - When conditional masking is applied, `date_format` settings can be set on a per-alias
    basis. Additional details can be found in the below conditional masking section.

### Conditional Masking

To apply different algorithms based on data assigned to a key column, it is important to set the `assigned_algorithm`
correctly for the masking pipeline to work.

#### Defining the Key Conditions

The key column is the column whose value must be evaluated to determine which algorithm to apply. The assigned algorithm
of a key column is to be a JSON array consisting of JSON objects that contain "alias" and "condition" keys. As such the
key column itself cannot be masked.

The `alias` key defines the alias for the filter condition, this will be used when defining conditional algorithms. All
conditions should be mutually exclusive, otherwise you can end up with the same input rows being masked with different
algorithms. The conditions must be compatible with ADF expression language.
Note that `default` can be used as an alias and should not have any associated conditions, this is because
the query to determine the conditions will automatically build the conditions for the default condition by combining
with a logical and the negation of all other conditions. Also note that we support a shortcut for referencing the value
in the key column (`%` by default), so that more complex filter conditions can be built.

Consider a key column with values `KEY_1`, `KEY_2`, `KEY_3`, `KEY_4`, `KEY_5`, and `KEY_6`. Consider a few different
key aliases that help us identify `KEY_1`, `KEY_2`, `KEY_3`, and set a default behavior for everything else as well.

We can construct the value of `assigned_algorithm` using a structure like the following:
```json
[
    {
        "alias": "K1",
        "condition":"% == 'KEY_1'"
    },
    {
        "alias": "K2",
        "condition":"endsWith(%, '2')"
    },
    {
        "alias": "K3",
        "condition":"startsWith(%, 'KEY_3')"
    },
    {
        "alias": "default"
    }
]
```

What this means is that the table will need to me masked in 4 stages (one for each condition), `K1` will mask all rows
where the value in the key column is exactly equal to `KEY_1`, `K2` will mask all rows where the value in the key column
ends with `2`, `K3` will mask all rows where the value in the key column starts with `KEY_3`, and `default` will mask
all rows that are not equal to `KEY_1` and that don't end with `2` and that don't start with `KEY_3`.

Note that the alias names must be unique per table. Further,there can be only one key column defined in each table.
Should you need to use multiple key columns in the same table, this is only supported by referring to the additional key
columns in the conditions of the primary key column. Let's suppose we want to add a secondary key column,
`secondary_row_type`. To do this, in the definitions of the conditions for `row_type` (our primary key column), we would
need to refer to the value of the secondary key column, which can be done by using `byName('secondary_row_type')`
instead of `%`. It is important to consider that conditions must still be mutually exclusive, and so if possible
try to avoid the need for multiple key columns in the same table as the number of conditions and the logic for defining
them is more complicated.

#### Defining the Conditional Algorithms

Conditional algorithms are defined by specifying the key colum, using the key column's aliases and assigning an
algorithm names in those conditions.

Considering the example above that describes how to specify the key column's aliases, let's suppose that column's name
is `row_type`, we can conditionally define the algorithms to the conditions based on their aliases as follows:

```json
{
    "key_column":"row_type",
    "conditions": [
        {
            "alias":"K1",
            "algorithm":"dlpx-core: Phone US"
        },
        {
            "alias":"K2",
            "algorithm":"dlpx-core: Email SL"
        },
        {
            "alias":"K3",
            "algorithm":"dlpx-core: CM Digits"
        },
        {
            "alias":"default",
            "algorithm":"dlpx-core: CM Alpha-Numeric"
        }
    ]
}
```

Note that we're specifying the key column and referring to the column by name, we're specifying the conditions in which
we will apply masking, which are referred to by the alias defined in the key column, and we specify the algorithms to
apply to each of the conditions separately.

#### Defining a Conditional Date Format

Conditional date formats are defined by specifying the key column, using the key column's aliases and assign a date
format in those conditions in the `algorithm_metadata` column.

Considering the example above that describes how to specify the key column's aliases, let's suppose that column's name
is `row_type`, we can conditionally define the date format to the conditions based on their aliases as follows:

```json
{
  "key_column":"row_type",
  "conditions": [
    {
      "alias":"K1",
      "date_format":"yyyyMMdd"
    },
    {
      "alias":"K2",
      "date_format":"yyyy-MM-dd"
    },
    {
      "alias":"K3",
      "date_format":"yyyy-MM-dd'T'HH:mm"
    },
    {
      "alias":"default",
      "date_format":"yyyy-MM-dd'T'HH:mm:ss'Z'"
    }
  ]
}
```

Note that this requires that the `assigned_algorithm` for the `row_type` column in this table to define the conditions
and their aliases. Further note that this means you should not specify `date_format` at the top-level of the `metadata`
when there is a conditional date format.

## Pipeline Failures

To identify the cause of a pipeline failure, you can use the "Monitor" page and consider the failed pipeline run,
however that can be difficult to follow. To make this easier, select activities (discovery and masking dataflows) and
their success are tracked in the metadata datastore.

To identify if a ruleset was derived and a table had discovery performed on it, a query can be made against the
`discovered_ruleset` table, joining the `adf_events_log` table on the event ID, for example:

```sql
SELECT
    rs.*,
    lg.pipeline_run_id,
    lg.pipeline_success,
    lg.error_message
FROM discovered_ruleset rs
JOIN adf_events_log lg
ON (rs.latest_event = lg.event_id);
```

To identify if a mapping was successful, a query can be made against the `adf_data_mapping` table, joining the
`adf_events_log` table on the event ID, for example:

```sql
SELECT
    dm.*,
    lg.pipeline_run_id,
    lg.pipeline_success,
    lg.error_message
FROM adf_data_mapping dm
JOIN adf_events_log lg
ON (dm.latest_event = lg.event_id);
```

In the event of conditional masking, the `adf_data_mapping` table contains a column `masked_status`, that will report
whether a particular alias has been successfully mapped. If a pipeline fails, there may be more than one event for a
particular table, in which case, a query using the `pipeline_run_id` can help identify errors, for example:

```sql
SELECT
    *
FROM adf_events_log
WHERE
    pipeline_success = CAST(0 AS BIT)
    AND pipeline_run_id = '<pipeline_run_id>';
```

Consider adding additional clauses to the `WHERE` condition, such as `filter_alias IS NOT NULL` or
`source_table = '<source_table>'`

## Decode Column Names In Error Messages

Part of the masking pipeline will encode column names so that responses from the API can always be parsed by ADF. This
necessarily means that sometimes failure messages will contain references to columns that don't exist in the source
data. To decode the column name, a query can be run against the metadata datastore.

The following query will provide a list of all encoded column names (as they will appear in API error messages) for the
specified dataset, database, and schema. Note: You will have to provide the dataset (`<DATASET>`), database
(`<DATABASE>`), and schema (`<SCHEMA>`) in the appropriate location

```sql
SELECT
    identified_table,
    ordinal_position,
    identified_column,
    LOWER(CONCAT('x',CONVERT(varchar(max),CONVERT(varbinary, identified_column),2))) AS encoded_column_name
FROM
    dbo.discovered_ruleset
WHERE
    dataset = '<DATASET>'
    AND specified_database = '<DATABASE>'
    AND specified_schema LIKE '<SCHEMA>%'
ORDER BY
    identified_table ASC, ordinal_position ASC
```

Alternatively, you can look up by the encoded column name the possible sources with the following query. Note: You will
have to provide the encoded column name (`<ENCODED_COLUMN>`)
```sql
WITH column_encode_decode AS (
    SELECT
        dataset,
        specified_database,
        specified_schema,
        identified_table,
        ordinal_position,
        identified_column,
        LOWER(CONCAT('x',CONVERT(varchar(max),CONVERT(varbinary, identified_column),2))) AS encoded_column_name
    FROM
        dbo.discovered_ruleset
)
SELECT
    *
FROM
    column_encode_decode
WHERE
    encoded_column_name = '<ENCODED_COLUMN>';
```

## Additional Resources

* Dataflows documentation
  * [Copy dataflow](./documentation/dataflows/copy_df.md)
  * [Data Discovery dataflow](./documentation/dataflows/data_discovery_df.md)
  * [Filter Test Utility dataflow](./documentation/dataflows/filter_test_utility_df.md)
  * [Filtered Masking dataflow](./documentation/dataflows/filtered_mask_df.md)
  * [Filtered Masking Parameters dataflow](./documentation/dataflows/filtered_mask_params_df.md)
  * [Unfiltered Masking dataflow](./documentation/dataflows/unfiltered_mask_df.md)
  * [Unfiltered Masking Parameters dataflow](./documentation/dataflows/unfiltered_mask_params_df.md)
* Pipeline documentation - Documentation for each pipeline is included in the released version of the template
  * [dcsazure_ADLS_to_ADLS_delimited_discovery_pl](./dcsazure_ADLS_to_ADLS_delimited_discovery_pl/README.md)
  * [dcsazure_ADLS_to_ADLS_delimited_mask_pl](./dcsazure_ADLS_to_ADLS_delimited_mask_pl/README.md)
  * [dcsazure_ADLS_to_ADLS_parquet_discovery_pl](./dcsazure_ADLS_to_ADLS_parquet_discovery_pl/README.md))
  * [dcsazure_ADLS_to_ADLS_parquet_mask_pl](./dcsazure_ADLS_to_ADLS_parquet_mask_pl/README.md)
  * [dcsazure_AzureSQL_MI_to_AzureSQL_MI_discovery_pl](./dcsazure_AzureSQL_MI_to_AzureSQL_MI_discovery_pl/README.md)
  * [dcsazure_AzureSQL_MI_to_AzureSQL_MI_mask_pl](./dcsazure_AzureSQL_MI_to_AzureSQL_MI_mask_pl/README.md)
  * [dcsazure_AzureSQL_to_AzureSQL_discovery_pl](./dcsazure_AzureSQL_to_AzureSQL_discovery_pl/README.md)
  * [dcsazure_AzureSQL_to_AzureSQL_mask_pl](./dcsazure_AzureSQL_to_AzureSQL_mask_pl/README.md)
  * [dcsazure_Databricks_to_Databricks_discovery_pl](./dcsazure_Databricks_to_Databricks_discovery_pl/README.md)
  * [dcsazure_Databricks_to_Databricks_mask_pl](./dcsazure_Databricks_to_Databricks_mask_pl/README.md)
  * [dcsazure_Snowflake_to_Snowflake_discovery_pl](./dcsazure_Snowflake_to_Snowflake_discovery_pl/README.md)
  * [dcsazure_Snowflake_to_Snowflake_mask_pl](./dcsazure_Snowflake_to_Snowflake_mask_pl/README.md)
  * [dcsazure_Dataverse_to_Dataverse_in_place_discovery_pl](./dcsazure_Dataverse_to_Dataverse_in_place_discovery_pl/README.md)
  * [dcsazure_Dataverse_to_Dataverse_in_place_mask_pl](./dcsazure_Dataverse_to_Dataverse_in_place_mask_pl/README.md)
* [Test Plan and Contributor Testing Guidelines](./documentation/guides/test-plan.md)
