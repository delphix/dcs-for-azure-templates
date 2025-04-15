# dcsazure_ADLS_to_ADLS_delimited_discovery_pl
## Delphix Compliance Services (DCS) for Azure - ADLS to ADLS Delimited Discovery Pipeline

This pipeline will perform automated sensitive data discovery on your delimited data in Azure Data Lake Storage (ADLS).

### Prerequisites

1. Configure the hosted metadata database and associated Azure SQL service (version `V2025.01.15.0`).
1. Configure the DCS for Azure REST service.
1. Configure the Azure Data Lake Storage (Gen 2) service associated with your ADLS source data.
1. [Assign a managed identity with a storage blob data contributor role for the Data Factory instance within
   the storage account](https://help.delphix.com/dcs/current/Content/DCSDocs/Configure_ADLS_delimited_pipelines.htm).
### Importing
There are several linked services that will need to be selected in order to perform the profiling and data discovery
of your delimited text ADLS data.

These linked services types are needed for the following steps:

`Azure Data Lake Storage` (source) - Linked service associated with ADLS source data. This will be used for the
following steps:
* dcsazure_ADLS_to_ADLS_delimited_container_and_directory_discovery_ds (DelimitedText dataset)
* dcsazure_ADLS_to_ADLS_delimited_data_discovery_df/SourceData1MillRowDataSampling (dataFlow)
* dcsazure_ADLS_to_ADLS_delimited_header_file_schema_discovery_ds (DelimitedText dataset)

`Azure SQL` (metadata) - Linked service associated with your hosted metadata store. This will be used for the following
steps:
* Determine All Heterogeneous Schemas (Script activity)
* Update Discovery State (Stored procedure activity)
* Update Discovery State Failed (Stored procedure activity)
* Check If We Should Rediscover Data (If Condition activity)
* dcsazure_ADLS_to_ADLS_delimited_metadata_discovery_ds (Azure SQL Database dataset)
* If Match (If Condition activity)
* dcsazure_ADLS_to_ADLS_delimited_data_discovery_df/MetadataStoreRead (dataFlow)
* dcsazure_ADLS_to_ADLS_delimited_data_discovery_df/WriteToMetadataStore (dataFlow)

`REST` (DCS for Azure) - Linked service associated with calling DCS for Azure. This will be used for the following
steps:
* dcsazure_ADLS_to_ADLS_delimited_data_discovery_df (dataFlow)

### How It Works
The discovery pipeline has a few stages:
* Check If We Should Rediscover Data
  * If we should, Mark Tables Undiscovered. This is done by updating the metadata store to indicate that tables
    have not had their sensitive data discovered
* Identify Nested Schemas
  * Using the child pipeline `dcsazure_ADLS_to_ADLS_delimited_container_and_directory_discovery_pl`, we collect
    all the identified schemas under the specified `P_DIRECTORY` directory. An empty `P_DIRECTORY` would
    mean discovering schemas starting at the root level.
  * For each item in that list, identify if the schema of the files in that child directory is expected to be
    homogeneous or heterogeneous.
* Schema Discovery
  * For each of the directories with homogeneous schema, identify the schema for each file with one of the suffixes to
    scan, determine the structure of the file by calling the child `dcsazure_ADLS_to_ADLS_delimited_file_schema_discovery_pl`
    pipeline with the appropriate parameters.
  * For each of the directories with a hetreogenous schema, and for each of the prefixes/suffix combinations specified in
    the `P_MIXED_FILE_SCHEMA_DISAMBIGUATION` variable, determine the structure of the file by calling the child
    `dcsazure_ADLS_to_ADLS_delimited_file_schema_discovery_pl` pipeline with the appropriate parameters.
* Select Discovered Tables - In this case, we consider the table to be items with the same schema.
  * After the previous step, we query the database for all tables (file suffixes within each distinct path of the
    storage container) and perform profiling for sensitive data discovery in those files that have not yet been
    discovered.
* ForEach Discovered Table
  * Each table that we've discovered needs to be profiled, the process for that is as follows:
    * Run the data discovery dataflow with the appropriate parameters.

### Variables

If you have configured your database using the metadata store scripts, these variables will not need editing. If you
have customized your metadata store, then these variables may need editing.

* `METADATA_SCHEMA` - This is the schema to be used for in the self-hosted AzureSQL database for storing metadata
  (default `dbo`).
* `METADATA_RULESET_TABLE` - This is the table to be used for storing the discovered ruleset (default
  `discovered_ruleset`).
* `COLUMNS_FROM_ADLS_FILE_STRUCTURE_PROCEDURE_NAME` - This is the stored procedure on the AzureSQL database that can
  accept, in part, the file structure from the `Get Metadata` ADF pipeline Activity.
* `DATASET` - This is used to identify data that belongs to this pipeline in the metadata store (default `ADLS`).
* `METADATA_EVENT_PROCEDURE_NAME` - This is the name of the procedure used to capture pipeline information in the
  metadata data store and sets the discovery state on the items discovered during execution
  (default `insert_adf_discovery_event`).
* `NUMBER_OF_ROWS_TO_PROFILE` - This is the number of rows we should select for profiling, note that raising this value
  could cause requests to fail (default `1000`).
* `MAX_RESULTS` - This is the max number of blobs to be included in the Azure blob storage REST API call (default `5000`).
* `MSFT_API_VERSION` - This is the version of the Microsoft API to use for the Azure blob storage REST API call (default `2021-08-06`).
* `MSFT_BLOB_TYPE` - This is the type of blob to use for the Azure blob storage REST API call (default `BlockBlob`).
* `STORAGE_ACCOUNT` - The name of the Azure Blob Storage account default (`DCS_PLACEHOLDER`). It is required
  to set the storage account name during the initial setup and replace it with actual storage account name.

### Parameters

* `P_STORAGE_CONTAINER_TO_SCAN` - This is the storage container whose contents need review
* `P_DIRECTORY` - This is the directory within the storage container that contains additional folders that will be
  scanned. An empty value would mean discovering schemas starting at the root level.
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
* `P_REDISCOVER` - This is a Bool that specifies if we should re-execute the data discovery dataflow for previously
  discovered files that have not had their schema modified (default `true`)
* `P_DIRECTORIES_TO_EXCLUDE` - List of directories to be excluded from the discovered directories. Particularly useful,
  when we have many child directories that may not contain any relevant delimited files, and we want to exclude those
  directories from the schema discovery.
* `P_MAX_LEVELS_TO_RECURSE` - Limit the schema discovery to the given max levels. This is to prevent discovery of deeply
  nested directories. The default is `5`, which is good enough to discover schemas up to 5 sub nested directories.


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

If files with the same extension across subdirectories use mixed delimiters, follow these steps:
1. Execute the pipeline multiple times, updating the values in P_SUFFIX_DELIMITER_MAP for each run.
2. Afterward, clean up the discovered_ruleset table to remove incorrect column entries.

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
`["heterogeneous_subdirectory/"]` and `P_MIXED_FILE_SCHEMA_DISAMBIGUATION` as:
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
name. Finally, note that addition of other suffixes will require pipeline changes, please feel free to
file an issue should this need arise (or add them and raise a PR).

### Examples
1. **Discovering directories starting at the root level**

To discover directories and subdirectories starting at the root level,
leave the pipeline parameter `P_DIRECTORY` as empty.

Given this directory structure,
```
.
── address
│   └── address.csv
├── customers
│   └── customers.csv
├── organizations
│   └── organizations.csv
└── people
    └── people.csv
```
The pipeline will discover all directories under the root level,
namely `address`, `customers`, `organizations`, and `people`.

2. **Discovering Directories starting at a different level.**

To discover directories and subdirectories starting at a different level,
specify the absolute path to the directory in the pipeline parameter `P_DIRECTORY` including the trailing /

Given this directory structure,
```
.
├── address
│   ├── address.csv
│   ├── local
│   │   └── local-address.csv
│   └── office
│       └── office-address.csv
├── customers
│   └── customers.csv
├── organizations
│   └── organizations.csv
└── people
    └── people.csv
```
and the pipeline parameter as `P_DIRECTORY=address/` the pipeline will
discover all directories under the specified directory, namely `address/local/` and `address/office/`.

3. **Excluding a directory or set of directories from discovering**

To exclude certain directories from the discovery, specify the absolute path to the directory
including the trailing / in the pipeline parameter `P_DIRECTORIES_TO_EXCLUDE`.

For example, in the previous example if we wanted to exclude the `address/local/` directory,
we would specify `P_DIRECTORIES_TO_EXCLUDE=["address/local/"]`

To exclude multiple directories, separate them with commas and add it to `P_DIRECTORIES_TO_EXCLUDE`.

Note: You cannot exclude the starting directory and it is applicable only to child directories.

4. **Specifying directories with mixed file schemas**

To specify directories with mixed file schemas, specify the absolute path to the directory including
the trailing / in the pipeline parameter `P_SUB_DIRECTORY_WITH_MIXED_FILE_SCHEMAS`.

For example given the directory structure,
```
└── hetrogenous_subdir
    ├── prefix1_file1.csv
    ├── prefix1_file2.csv
    ├── prefix2_file1.csv
    └── prefix2_file2.csv
```
set the pipeline parameter `P_SUB_DIRECTORY_WITH_MIXED_FILE_SCHEMAS=["hetrogenous_subdir/"]`

Note: If the root folder contains mixed files with different schemas, you can specify
the root folder in the pipeline parameter as `P_SUB_DIRECTORY_WITH_MIXED_FILE_SCHEMAS=[""]`.

5. **Controlling the max levels to discover directories**
The default level to discover directories is 5, but you can customize it using the pipeline
parameter `P_MAX_LEVELS_TO_RECURSE`.

The value of this variable cannot be less than 0.

For example, given this directory structure
```
nested-example
├── nested1-level1
│   ├── level1.csv
│   ├── nested1-level2
│   │   └── level2.csv
│   └── nested2-level2
│       ├── level2.csv
│       ├── nested1-level3
│       └── nested2-level3
└── nested2-level1
    ├── level1.csv
    ├── nested1-level2
    │   └── level2.csv
    └── nested2-level2
        └── level2.csv
```
and we want to discover only till level 2 we specify the `P_MAX_LEVELS_TO_RECURSE=2`
which will only discover directories until the specified level, namely
[`nested1-level1/`, `nested2-level1/`, `nested1-level1/nested1-level2/`,
`nested1-level1/nested2-level2/`, `nested2-level1/nested1-level2/`, `nested2-level1/nested2-level2/`]
