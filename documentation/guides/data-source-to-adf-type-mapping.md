# Mapping Data Source Types to Azure Data Factory (ADF) Types

This document outlines the steps to determine and configure mappings between source data types and 
Azure Data Factory (ADF) types. 

## Overview

Pipelines can vary in how they handle multiple data types:

- **Single-Type Pipelines**: Some pipelines, like those processing delimited files in Azure Data Lake Storage (ADLS), 
only support a single data type (i.e., `string`) because delimited files do not contain any type information. 
In such cases, a direct mapping from `string` to `string` should be added to the `adf_type_mapping` table.
- **Multi-Type Pipelines**: Other pipelines, such as Azure Data Lake Storage (ADLS) for parquet files, Azure SQL, 
Databricks, Snowflake, etc., support multiple data types (e.g., `int`, `bigint`, `float`, `decimal`, `date`, 
`datetime`, etc.). For these, you need to map each data source type to its corresponding ADF type so that 
response bodies can be parsed by ADF.

## Steps to Determine Type Mappings

Follow these steps to identify and configure the type mappings:

1. **Review Data Source Documentation**: Check the official documentation of the data source to see if it provides 
a predefined mapping of its types to ADF types. For example, [this page](https://learn.microsoft.com/en-us/azure/data-factory/connector-azure-sql-database?tabs=data-factory#data-type-mapping-for-azure-sql-database) lists 
the mappings from Azure SQL Database data types to ADF types.

2. **Create Sample Data**:
    - Create a column or field for each data type supported by the data source.
    - Populate these columns with sample data.

3. **Use ADF Data Flow Debugging**:
    - Open any data flow in ADF that reads from the data source. For example, use the 
    `Source1MillRowDataSampling` activity in the `dcsazure_AzureSQL_to_AzureSQL_discovery_df` data flow.
    - Enable "Data Flow Debug" mode.
    - Navigate to the "Data Preview" tab of the source activity and click "Refresh" to view the data preview.
    For example, use the "Data Preview" tab of the `Source1MillRowDataSampling` activity in the discovery dataflow.
    ![data preview](../images/ADF%20Data%20Preview.png)

4. **Identify ADF Types**:
    - In the data preview, observe the ADF data types of each column. The screenshot below highlights where to locate 
    the ADF data types in the data preview.
    ![data preview](../images/ADF%20Data%20Type.png)
    - Note the corresponding ADF type for each data source type.

5. **Update the `adf_type_mapping` Table**:
    - Add the identified mappings to the `adf_type_mapping` table in the Metadata Datastore using 
    the following SQL template:
      ```sql
      INSERT INTO adf_type_mapping (dataset, source_type, target_type)
      VALUES
          ('<dataset>', '<data_source_type_1>', '<adf_type_1>'),
          ('<dataset>', '<data_source_type_2>', '<adf_type_2>'),
          -- Add more mappings as needed
      ;
      ```
      - Replace `<dataset>` with your dataset name (e.g., `AZURESQL`, `SNOWFLAKE`), 
      `<data_source_type_X>` with the source data type, and `<adf_type_X>` with the corresponding ADF type.

6. **Test the Mappings**:
    - After adding the new type mappings to the `adf_type_mapping` table, run a pipeline that uses these mappings and ensure that 
    the data is correctly stored in the sink data source.
    - If the pipeline fails despite using the identified ADF data types (e.g., see issue [#40](https://github.com/delphix/dcs-for-azure-templates/issues/40)), 
    adjust the mappings as needed to ensure the pipeline runs successfully and stores the masked data in the sink data source.

7. **Create a Migration Script**:
    - Once you have verified the new type mappings, create a versioned migration script to add them 
    to the `adf_type_mapping` table.  
    - Include this script in your pull request so the mappings are applied consistently 
    across all environments during deployment.  
    - For more details, see the [metadata_store_scripts](../../metadata_store_scripts) directory and and the main [README.md](../../README.md#self-hosted-metadata-store).

## Example

For a data source like Azure SQL, you might map Azure SQL data types to ADF types as follows:
- `tinyint` → `integer`
- `float` → `double`
- `smalldatetime` → `timestamp`

These mappings ensure that ADF correctly interprets and processes the data during pipeline execution.
