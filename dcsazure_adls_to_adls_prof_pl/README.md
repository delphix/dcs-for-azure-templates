# dcsazure_adls_to_adls_prof_pl
## Delphix Compliance Services (DCS) for Azure - ADLS to ADLS Profiling Pipeline

This pipeline will perform automated sensitive data discovery on your Azure Data Lake Storage (ADLS) Data.

### Prerequisites

1. Configure the hosted metadata database and associated Azure SQL service (version `V2024.04.18.0`).
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
* Identify Nested Schemas
  * Using a `Get Metadata` step, collect the items under the specified `P_DIRECTORY` directory
  * For each item in that list, identify if the schema of the files in that child directory is expected to be
    homogeneous or heterogeneous. In the case where it's expected to be homogeneous, the child directory is added to
    an array-type variable, similarly for heterogeneous schemas.
* Schema Discovery Using Azure Data Factory Metadata Discovery
  * Using a `Get Metadata` step store details about the columns within each file, persisting the data into the
    `discovered_ruleset` table of the metadata store
* Select Discovered Tables
  * After the previous step, we query the database for all tables we found in the specified schema and perform profiling
* ForEach Discovered Table
  * Each table that we've discovered needs to be profiled, the process for that is as follows:
    * Get the row count from the table
    * Get details for the table
    * Check that the table is not empty and that the table can be read
      * If the table contains data and can be read, run the `dcsazure_Databricks_to_Databricks_prof_df` dataflow, which
        samples the data from the source, and calls the DCS for Azure service to profile the data
      * If the table either does not contain data or cannot be read, run the
        `dcsazure_Databricks_to_Databricks_prof_empty_tables_df` which updates the row count accordingly

### Parameters

* `P_STORAGE_CONTAINER_TO_PROFILE` - This is the storage container whose contents need profiling
* `P_DIRECTORY` - This is the directory within the storage container that contains additional folders that will be
  scanned
* `P_SUFFIXES_TO_SCAN` - This list can be used to limit which kinds of files are scanned for data, note that these
  represent suffixes to file names, not a true extension as `.` is not supported in the keys of the object definition
  in `P_SUFFIX_DELIMITER_MAP` (default `["csv","txt","NO_EXT"]`)
* `P_SUFFIX_DELIMITER_MAP` - This map is used to define the parameters needed in order to correctly interpret
  (default `{"csv":{"column_delimiter":",","row_delimiter":"\\r\\n","quote_character":"\"","escape_character":"\\\\","first_row_as_header":true,"null_value":""},"txt":{"column_delimiter":"|","row_delimiter":"\\r\\n","quote_character":"\"","escape_character":"\\\\","first_row_as_header":true,"null_value":""},"NO_EXT":{"column_delimiter":"|","row_delimiter":"\\r\\n","quote_character":"\"","escape_character":"\\\\","first_row_as_header":true,"null_value":""}}`)
* `P_SUB_DIRECTORY_WITH_MIXED_FILE_SCHEMAS` - (default `[]`)
* `P_MIXED_FILE_SCHEMA_DISAMBIGUATION` - (default `{"DCS_SAMPLE_PREFIX_":{"suffixes":["csv","txt","NO_EXT"]}}`)
* `P_COLUMNS_FROM_ADLS_FILE_STRUCTURE_PROCEDURE_NAME` - (default `get_columns_from_adls_file_structure_sp`)
* `P_METADATA_SCHEMA` - This is the schema to be used for in the self-hosted AzureSQL database for storing metadata (default `dbo`)
* `P_METADATA_RULESET_TABLE` - This is the table to be used for storing the discovered ruleset (default `discovered_ruleset`)

#### Notes
The default value of `P_SUFFIXES_TO_SCAN` is the list containing all supported suffixes. It will not suffice to add an
alternative value to this list without first editing the pipeline.

The default value of `P_SUFFIX_DELIMITER_MAP` can be more easily read when we apply formatting:
```json
{
  "csv": {
    "column_delimiter": ",",
    "row_delimiter": "\\r\\n",
    "quote_character": "\"",
    "escape_character": "\\\\",
    "first_row_as_header": true,
    "null_value": ""
  },
  "txt": {
    "column_delimiter": "|",
    "row_delimiter": "\\r\\n",
    "quote_character": "\"",
    "escape_character": "\\\\",
    "first_row_as_header": true,
    "null_value": ""
  },
  "NO_EXT": {
    "column_delimiter": "|",
    "row_delimiter": "\\r\\n",
    "quote_character": "\"",
    "escape_character": "\\\\",
    "first_row_as_header": true,
    "null_value": ""
  }
}
```
As the pipeline runs, it will refer to these values when parsing the files in ADLS. If a configuration for the row or
column delimiter is incorrect, the profiling may or may not fail. If the column delimiter was incorrect, then the
results in the `discovered_ruleset` table will be incorrect. For example, if the ADLS store has a `csv` file whose
header is: `column1,column2,column3`, and the column delimiter for `csv` is specified as `|`, then there will be an
entry in the `discovered_ruleset` table corresponding to table name `csv` with `identified_column` containing
`column1,column2,column3`. To correct this, you will need to remove the erroneous row from the table, and re-run the
pipeline with the correct column delimiter.

Note that there are a few things worth noting with respect to this parameter.
1. In order for ADF to interpret these values correctly `\` characters need to be escaped, so `\r` needs to become `\\r`
2. In order for ADF to interpret `\` as a lone character correctly, it needs to be first be escaped, `\\` and each of
those values needs to be escaped per the first point, so an escape character of `\` in your file would need to be `\\\\`
3. It is assumed that `first_row_as_header` is true - but it is parameterized now in case additional support is added
at a later time.
4. It is important that you get the row delimiter correct. Files that don't have the correct row delimiter will yield
strange column names and may fail the profiling dataflow (and therefore the pipeline).
5. It is important that you get the column delimiter correct. Files that have the wrong column delimiter will yield
strange column names and may fail the profiling dataflow (and therefore the pipeline).
6. When getting the file metadata, the data set is not parameterized as there is a known issue with the `Get Metadata`
step not respecting parameterized row delimiters. To work around this issue, the data set that is used to get the file
metadata relies on the `Default` row delimiter, which matches `\r`, or `\n`, or `\r\n`.
7. If it appears as though a directory's files have not been scanned, this often happens when the parameters are of
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
  "DCS_SAMPLE_PREFIX_": {
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
