# dcsazure_adls_to_adls_prof_pl
## Delphix Compliance Services (DCS) for Azure - ADLS to ADLS Profiling Pipeline

This pipeline will perform automated sensitive data discovery on your Azure Data Lake Storage (ADLS) Data.

### Prerequisites

1. Configure the hosted metadata database and associated Azure SQL service (version `V2024.05.02.0`).
1. Configure the DCS for Azure REST service.
1. Configure the Azure Data Lake Storage (Gen 2) service associated with your ADLS source data.

### Importing
There are several linked services that will need to be selected in order to perform the profiling of your delimited text
ADLS data.

These linked services types are needed for the following steps:

`Azure Data Lake Storage` (source) - Linked service associated with ADLS source data. This will be used for the
following steps:
* For dcsazure_adls_container_and_directory (DelimitedText dataset)
* dcsazure_adls_sub_directory (DelimitedText dataset)
* dcsazure_adls_to_adls_delimited_prof_df/SourceData1MillRowDataSampling (dataFlow)
* dcsazure_adls_delimited_header (DelimitedText dataset)

`Azure SQL` (metadata) - Linked service associated with your hosted metadata store. This will be used for the following
steps:
* For Determine All Heterogeneous Schema (Script activity)
* dcsazure_adls_to_adls_delta_metadata_prof_ds (Azure SQL Database dataset)
* If Match (If Condition activity)
* dcsazure_adls_to_adls_delimited_prof_df/MetadataStoreRead (dataFlow)
* dcsazure_adls_to_adls_delimited_prof_df/WriteToMetadataStore (dataFlow)

`REST` (DCS for Azure) - Linked service associated with calling DCS for Azure. This will be used for the following
steps:
* For dcsazure_adls_to_adls_delimited_prof_df (dataFlow)

### How It Works
The profiling pipeline has a few stages:
* Reset Discovery Condition
  * Checks against the `P_RESET_DISCOVERY` parameter to determine if we should profile from scratch or profile from where it left off after last execut
* Identify Nested Schemas
  * Using a `Get Metadata` activity, collect the items under the specified `P_DIRECTORY` directory
  * For each item in that list, identify if the schema of the files in that child directory is expected to be
    homogeneous or heterogeneous. In the case where it's expected to be homogeneous, the child directory is added to
    an array-type variable, similarly for heterogeneous schemas.
* Schema Discovery Using Azure Data Factory Metadata Discovery
  * For each of the directories with heterogeneous schema, identify the schema for each file with one of the suffixes to
    scan, determine the structure of the file by calling the child `dcsazure_adls_to_adls_prof_file_pl` pipeline with
    the appropriate parameters.
  * For each of the directories with a homogeneous schema, and for each of the prefixes/suffix combinations specified in
    the `P_MIXED_FILE_SCHEMA_DISAMBIGUATION` variable, determine the structure of the file by calling the child
    `dcsazure_adls_to_adls_prof_file_pl` pipeline with the appropriate parameters.
* Select Discovered Tables - In this case, we consider the table to be items with the same schema.
  * After the previous step, we query the database for all tables (file suffixes within each distinct path of the
    storage container) and perform profiling of the data in those files.
* ForEach Discovered Table
  * Each table that we've discovered needs to be profiled, the process for that is as follows:
    * Run the profile dataflow with the appropriate parameters.

### Variables

If you have configured your database using the metadata store scripts, these variables will not need editing. If you
have customized your metadata store, then these variables may need editing.

* `METADATA_SCHEMA` - This is the schema to be used for in the self-hosted AzureSQL database for storing metadata
  (default `dbo`).
* `METADATA_RULESET_TABLE` - This is the table to be used for storing the discovered ruleset (default
  `discovered_ruleset`).
* `COLUMNS_FROM_ADLS_FILE_STRUCTURE_PROCEDURE_NAME` - This is the stored procedure on the AzureSQL database that can
  accept, in part, the file structure from the `Get Metadata` ADF pipeline Activity.
* `HETEROGENEOUS_SCHEMAS_TO_CHECK` - This variable is modified during execution of the pipeline, and serves as an
  accumulator for the list of directories with heterogeneous schemas.
* `HETEROGENEOUS_SCHEMAS_TO_CHECK` - This variable is modified during execution of the pipeline, and serves as an
  accumulator for the list of directories with homogeneous schemas.
* `CAPTURE_LOG_PROCEDURE_NAME` - This is the procedure to be used for capturing the logs and marked the profiling and masking flags
  (default `capture_adf_execution_sp)

### Parameters

* `P_STORAGE_CONTAINER_TO_PROFILE` - This is the storage container whose contents need profiling
* `P_DIRECTORY` - This is the directory within the storage container that contains additional folders that will be
  scanned
* `P_SUFFIXES_TO_SCAN` - This list can be used to limit which kinds of files are scanned for data, note that these
  represent suffixes to file names, not a true extension as `.` is not supported in the keys of the object definition
  in `P_SUFFIX_DELIMITER_MAP` (default `["csv","txt","NO_EXT"]`)
* `P_SUFFIX_DELIMITER_MAP` - This map is used to define the parameters needed in order to correctly interpret
  (default `{"csv":{"column_delimiter":",","quote_character":"\"","escape_character":"\\","null_value":""},"txt":{"column_delimiter":"|","quote_character":"\"","escape_character":"\\","null_value":""},"NO_EXT":{"column_delimiter":"|","quote_character":"\"","escape_character":"\\","null_value":""}}`)
* `P_SUB_DIRECTORY_WITH_MIXED_FILE_SCHEMAS` - This is the list of directories whose contents have mixed schemas, see the
  details in the notes below (default `[]`)
* `P_MIXED_FILE_SCHEMA_DISAMBIGUATION` - This is the list of file prefixes that can be used to disambiguate directories
  with heterogeneous schemas, see the details in the notes below (default
  `{"DCS_EXAMPLE_PREFIX":{"suffixes":["csv","txt","NO_EXT"]}}`)
* `P_RESET_DISCOVERY` - Boolean - This is the flag to control whether to re-profile from start, or from where it left off after last execution. Default value is true, which will re-profile from start.


#### Notes
The default value of `P_SUFFIXES_TO_SCAN` is the list containing all supported suffixes. It will not suffice to add an
alternative value to this list without first editing the pipeline.

The default value of `P_SUFFIX_DELIMITER_MAP` can be more easily read when we apply formatting:
```json
{
  "csv": {
    "column_delimiter": ",",
    "quote_character": "\"",
    "escape_character": "\\",
    "null_value": ""
  },
  "txt": {
    "column_delimiter": "|",
    "quote_character": "\"",
    "escape_character": "\\",
    "null_value": ""
  },
  "NO_EXT": {
    "column_delimiter": "|",
    "quote_character": "\"",
    "escape_character": "\\",
    "null_value": ""
  }
}
```
As the pipeline runs, it will refer to these values when parsing the files in ADLS. If a configuration for the row or
column delimiter is incorrect, the profiling may or may not fail. If the column delimiter was incorrect, then the
results in the `discovered_ruleset` table will be incorrect. For example, if the ADLS store has a `csv` file whose
header is: `column1|column2|column3`, and the column delimiter for `csv` is specified as `,`, then there will be an
entry in the `discovered_ruleset` table corresponding to table name `csv` with `identified_column` containing
`column1|column2|column3`. To correct this, you will need to remove the erroneous row from the table, and re-run the
pipeline with the correct column delimiter.

If you have a mix of delimiters in files of the same extension across subdirectories, you can either:
1. Run the pipeline multiple times, changing the values in `P_SUFFIX_DELIMITER_MAP` and then proceeding to clean up the
`discovered_ruleset` table to remove the incorrect column entries.
1. Deactivate the `Identify Nested Schemas` and the `For Each Schema Found` steps in the pipeline (by setting the step's
activity state to `Deactivated` under the `General` settings for each of those steps), modifying the
`HOMOGENEOUS_SCHEMAS_TO_CHECK` and `HETEROGENEOUS_SCHEMAS_TO_CHECK` default values to include the names of the
subdirectories that match one set of delimiters in `P_SUFFIX_DELIMITER_MAP` then publish and run the pipeline. After
this is complete, change the default values of `HOMOGENEOUS_SCHEMAS_TO_CHECK` and `HETEROGENEOUS_SCHEMAS_TO_CHECK`
accordingly, then publish and re-run the pipeline with the alternative values in `P_SUFFIX_DELIMITER_MAP`, and this
should pick up another set of files. Repeat as necessary. (Tip: You can also change the value of `P_SUFFIXES_TO_SCAN`
to not waste time scanning unchanged suffixes between runs.)
1. Combine steps the first two options as you see fit.

Note that there are a few things worth noting with respect to this parameter.
1. In order for ADF to interpret `\` as a lone character correctly, it needs to be first be escaped, so it will need
to be specified as `\\`. When this character is persisted to the database via metadata, it will appear in the database
as `\\\\`, this is the value that is required for use in dataflows, as each of the above `\` needs to also be escaped,
and since the database is used to define the arguments to the dataflow, we store the parameter we need.
1. The only supported row delimiter is the Default for ADF Delimited datasets, which will match `\r`, or `\n`, or `\r\n`.
1. It is important that you get the column delimiter correct. Files that have the wrong column delimiter will yield
strange column names and may fail the profiling dataflow (and therefore the pipeline).
1. If it appears as though a directory's files have not been scanned, this often happens when the parameters are of
`P_SUFFIX_DELIMITER_MAP` are incorrect, it is recommended to double-check this value.

When scanning through folders, all subdirectories are expected to contain files wherein those files with the same
suffix contain the same metadata as one another. Specifically, the header row of `prefix1_file1.csv` must be the same as
`prefix2_file2.csv` if they are in the same subdirectory. (Note that this does not apply across file types, i.e. the
header rows of `prefix1_file1.csv` and `prefix2_file2.txt` are considered to be different.) Support for scanning
subdirectories with multiple file names with separate suffixes is managed by leveraging the parameter
`P_SUB_DIRECTORY_WITH_MIXED_FILE_SCHEMAS`, which contains the names of subdirectories that need additional handling.
The disambiguation of the schema is done by leveraging file prefixes (which may not be a subset of one another) and
specifying how to interpret the prefixes using the parameter `P_MIXED_FILE_SCHEMA_DISAMBIGUATION`.

The default value of `P_MIXED_FILE_SCHEMA_DISAMBIGUATION` can be more easily understood with formatting applied:
```json
{
  "DCS_EXAMPLE_PREFIX_": {
    "suffixes": [
      "csv",
      "txt",
      "NO_EXT"
    ]
  }
}
```

This default value will likely not conflict with any of your profiling, but is provided as such to illustrate how to
specify prefixes. Going back to our example of `prefix1_file1.csv` and `prefix2_file2.csv` having different schemas,
lets suppose our storage container has a structure as follows:
```
directory_to_profile
├── heterogeneous_subdirectory
│   ├── prefix1_file1.csv
│   ├── prefix1_file2.csv
│   ├── prefix2_file1.csv
│   └── prefix2_file2.csv
└── homogeneous_subdirectory
    ├── file1
    ├── file1.csv
    ├── file1.txt
    ├── file2
    ├── file2.csv
    └── file2.txt
```

In order to correctly profile, we'd have to specify `P_SUB_DIRECTORY_WITH_MIXED_FILE_SCHEMAS` as 
`["heterogeneous_subdirectory"]` and `P_MIXED_FILE_SCHEMA_DISAMBIGUATION` as:
```json
{
  "prefix1_": {
    "suffixes": [
      "csv"
    ]
  },
  "prefix2_": {
    "suffixes": [
      "csv"
    ]
  }
}
```
This will treat `heterogeneous_subdirectory/prefix1_file1.csv` the same as `heterogeneous_subdirectory/prefix1_file2.csv`,
and will treat `heterogeneous_subdirectory/prefix2_file1.csv` the same as `heterogeneous_subdirectory/prefix2_file2.csv`;
neither `heterogeneous_subdirectory/prefix1_*.csv` nor `heterogeneous_subdirectory/prefix2_*.csv` will be treated as if
they have the same schema.

Note also that when profiling `homogeneous_subdirectory`, there will be entries in the `discovered_ruleset` table where
the `identified_table` has value `NO_EXT`, `csv`, and `txt` with `directory_to_profile/homogeneous_subdirectory/` as
the specified schema, whereas there will be entries with schema `directory_to_profile/heterogeneous_subdirectory/prefix1_`,
and `directory_to_profile/heterogeneous_subdirectory/prefix2`, both of which will have identified table `csv`, as those
are the only suffixes we told the pipeline to scan for the heterogeneous subdirectory.

Note that `NO_EXT` is a special value that represents files with no extension, specifically with no `.` in the file
name. Also note that we do not scan specifically for `.txt` or `.csv`, so if you have a file that is named `.pcsv`,
then `csv` will match. Finally, note that addition of other suffixes will require pipeline changes, please feel free to
file an issue should this need arise (or add them and raise a PR).
