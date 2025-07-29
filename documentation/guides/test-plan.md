# Overview

This is a test plan that outlines the approach and scope for testing Azure Data Factory (ADF) templates. It is designed 
to ensure that the ADF templates effectively perform sensitive data discovery and masking.

## Test Objectives

1. **Validate sensitive data discovery** – Confirm that the pipeline/templates identify sensitive data.
2. **Validate data masking** – Ensure that the sensitive data is correctly masked using ADF templates, maintaining 
data integrity and referential consistency.

## In Scope

1. Testing of ADF templates for data masking and sensitive data discovery.
2. Testing with respect to feature completeness.

## Out of Scope

1. Testing and validation of referential integrity.

## Test Environment

1. Access to Azure Data Factory and necessary source and destination.
2. [An active Delphix Compliance Services Subscription](https://dcs.delphix.com/docs/latest/delphixcomplianceservices-dcsforazure-2_onboarding)
3. [An Azure SQL instance to manage rulesets, type mappings etc.](https://dcs.delphix.com/docs/latest/setting-up-an-azure-sql-metadata-store)

---

# Test Scenarios

## Datatype Testing

This is crucial to ensure that the source data is correctly processed and written back when it flows through 
the various stages of the pipeline.

### Prerequisites

1. Ensure that the source schema includes columns representing all combinations of data types supported by 
the data source. For example, here is the [full list](https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver16) 
of datatypes supported by Azure SQL.
2. Ensure that all data types in the source data are correctly mapped to their corresponding appropriate types 
in Azure Data Factory (ADF).
    - Here’s a script that shows type mappings for Azure SQL database – [link](https://github.com/delphix/dcs-for-azure-templates/blob/main/metadata_store_scripts/V2024.12.02.0__add_azuresql_to_azuresql_support.sql)

> [!NOTE]  
> The supported data types may vary depending on the source, and it is the author's responsibility to investigate 
> what data types are supported for the given source/sink.

---

## Discovery

1. Verify that it discovers all supported datatypes in the source schema and the ruleset table is populated with:
    - Identified table
    - Identified column
    - Identified column type
2. Verify that the row counts are correctly populated and should be equal to the number of rows in the source table.
3. Verify that the identified column length should be the actual data type length or –1 if length cannot be deduced 
or is not applicable (e.g., JSON, XML, UUID).

---

## Masking

1. Verify the minimum, maximum, and a valid mid value for each datatype and that the masked data can be 
appropriately fit into it with a correctly chosen algorithm.
    - If the masked values don’t fit within the datatype range, it should result in an error.
    - Example: Arithmetic overflow error for data type `tinyint`, value = 300000.
2. Verify that the values of columns with data types like JSON, XML, UUID should not get truncated or result in 
an error while saving the masked value into the sink schema.
    - It is important to note that while [a related issue](https://github.com/delphix/dcs-for-azure-templates/issues/40) 
    was first seen in an RDMBS data source (Azure SQL), care should be taken to watch out for similar related issues 
    to data types in any new data source that we support.

---

## Truncate Sink Tables

Verify that the feature to truncate sink tables in ADF masking pipelines correctly removes all existing data from 
the sink or target tables before writing new data.  
This feature is controlled by a pipeline parameter `P_TRUNCATE_SINK_BEFORE_WRITE` whose default value is `true`.

1. Verify that when the pipeline parameter is set to `false`, no tables in the sink schema are truncated. 
This behaviour can be verified by:
    - If the sink table has a primary key, then executing the masking pipeline should result in 
    a primary key violation error.
    - If the sink table lacks a primary or unique key, running the masking pipeline should increase 
    the total number of records in the sink table by the same number of records present in the source table.
2. Verify that when the pipeline parameter is set to `true`, only those tables in the sink schema that are mapped to 
the source tables and designated for masking are truncated. Source-to-sink mappings can be found 
in the `adf_data_mapping` table.
    - Test with empty tables in the target schema.
    - Tables with relationships (in case of RDBMS):
        - With cascading effect.
        - With triggers (on DELETE etc).
        - With large number of records.
    - Verify nothing is being truncated, if there are no tables to mask.
    - Verify that the activity fails if a source table is mapped to a non-existing table in the sink schema.

---

## Data Type Casting

Verify that we can cast a datatype to string appropriately for a masking algorithm.  
This feature is controlled by the value `{"treat_as_string": true}` and is stored in the `discovered_ruleset` table 
in the column name `algorithm_metadata`.

Verify this functionality for following supported datatypes:

1. **Integer**
    - Populate a column with integer ADF type using date-like integer values such as `010223`.
    - Set `algorithm_metadata` to `{"treat_as_string": true, "date_format": "<date-format>"}` for this column.
    - Apply a Date algorithm (e.g., `DateShiftFixed`) to mask the column.
    - Verify that the masking pipeline runs successfully and the corresponding column in sink data source 
    contains masked data.
2. **Long**
    - Populate a column with long ADF type using date-like long values such as `010223`.
    - Set `algorithm_metadata` as above.
    - Apply a Date algorithm.
    - Verify successful masking.
3. **Timestamp**
    - Populate a column with timestamp ADF type with timestamps.
    - Set `algorithm_metadata` as above.
    - Apply a Date algorithm.
    - Verify successful masking.
4. **Date**
    - Populate a column with date ADF type with dates like `01-10-2024`.
    - Set `algorithm_metadata` as above.
    - Apply a Date algorithm.
    - Verify successful masking.
5. **Binary**
    - Populate a column with binary ADF type with some random binary values.
    - Set `algorithm_metadata` as above.
    - Create an algorithm using Binary Secure Lookup framework and apply it.
    - Verify successful masking.
6. **Boolean**
    - Populate a column with boolean ADF type using boolean values.
    - Set `algorithm_metadata` as above.
    - Apply the Shuffle algorithm.
    - Verify successful masking.
        - *Note: Masked values may not differ from the unmasked values since boolean values are limited 
        to true or false.*
7. **Float**
    - Populate a column whose data type is mapped to float ADF type with date-like float values like `011024.1032` 
    (where the decimal part represents hours and minutes).
    - Set `algorithm_metadata` as above.
    - Apply a Date algorithm.
    - Verify successful masking.
8. **Double**
    - Populate a column with double ADF type with date-like double values like `011024.1032`.
    - Set `algorithm_metadata` as above.
    - Apply a Date algorithm.
    - Verify successful masking.

---

## Column Name Normalization

The column name normalization is part of both the discovery and masking pipeline to ensure that the source datastore 
supporting special characters and/or white spaces in column names are correctly handled by these pipelines during 
the various stages of data transformation.

1. Verify that the discovery pipeline correctly normalizes the column names containing spaces and special characters 
before sending it to the Discovery API and de-normalizes it back before saving the identified column name in 
the discovered ruleset table.
2. Verify that the discovery pipeline also normalizes the column name without any special characters and de-normalizes 
it back before saving the identified column name in the discovered ruleset table.
3. Verify that the masking pipeline normalizes column names with or without special characters while reading from 
the source and de-normalizes it back before writing to the sink.
    - Verify that the names are correctly normalized and that it can be denormalized back to verify 
    the source column name. See [this example](https://github.com/delphix/dcs-for-azure-templates?tab=readme-ov-file#decode-column-names-in-error-messages) 
    of how to decode a normalized column name back to its original name.
        - *Note: A (de-normalized) column name could be any name which may or may not contain special characters 
        or whitespace in it. A normalized column name would start with letter `x` followed by 
        some variable length hexadecimal characters.*
        - *For example, if the column name is `First Name`, then the normalized column name 
        would be `x4669727374204e616d65`.*
    - Verify that the field algorithm assignments and body type mapping contain the normalized column names.
        - Example:  
          `"DF_FIELD_ALGORITHM_ASSIGNMENT": "{\"x416464726573734c696e6532\":\"AddrLine2Lookup\",
          \"x416464726573734c696e6531\":\"dlpx-core:AddrLookup\",\"x43697479\":\"dlpx-core:AddrLookup\"}"`
        - Example:  
          `"DF_BODY_TYPE_MAPPING": "(timestamp as date, status as string, message as string, trace_id as string, 
          items as (DELPHIX_COMPLIANCE_SERVICE_BATCH_ID as long, x416464726573734c696e6532 as string, 
          x416464726573734c696e6531 as string, x43697479 as string)[])"`
    - Verify this behaviour for both filtered parameter dataflow and unfiltered parameter dataflow.
        - Use conditional masking, that will call the filtered parameter dataflow.

---

## Metadata Store and Migrations

Verifies that any new and existing migrations can be successfully applied to create or update a metadata store.  
As the pipelines refer to this metadata store within their activities, they should be capable of handling 
a custom schema name.  
*Default value of the metadata schema in the pipelines is `dbo`.*

1. Verify that any new migration scripts added as part of the development is valid and can be executed as is 
against the pipeline default schema (`dbo`).
2. Verify that any new migrations scripts can be used to setup a metadata datastore in a custom schema (other than 
the pipeline default `dbo`).
    - The command to set the custom schema as the default is documented in the Self-hosted Metadata Store section of 
    the root-level README.md file.
3. Verify that a metadata store can be setup using existing and new migrations in both default and custom schema.
4. Verify that both the masking and discovery pipelines can be run with the pipeline default schema (`dbo`).
5. Verify that masking and discovery pipelines can be run with a custom schema.
    - Go to the variable section of the pipeline and update the schema with your custom schema.
    - The metadata should be updated after discovery is completed.
    - Masking pipelines should be able to refer algorithms assigned in the discovered ruleset for this custom schema.

---

## Constraint Management (RDBMS only)

Verify that the constraints are dropped and recreated back before and after masking is completed.  
This is **required** only for RDBMS sink data sources that support constraints such as foreign keys (e.g., Azure SQL 
and Oracle).  
*Note: Only dropping and creating foreign key constraints is supported right now in our pipelines.*

1. **Dropping constraints**
    - Verify that only the constraints of tables to be masked in the current pipeline run are dropped.
    - Verify that only the dropped constraints are captured and written to the `captured_constraints` table.
    - Verify that the `pre-drop status` column accurately records whether the constraints were enabled or disabled 
    before they were dropped, and that the `drop timestamp` column correctly captures the time at which the constraints 
    were dropped.
2. **Recreating dropped constraints**
    - Verify that only the constraints that were previously dropped during the current pipeline run are recreated.
    - Verify that the `post-create status` column accurately records whether the recreated constraints are 
    enabled or disabled, and that the `create timestamp` column correctly captures the time at which the constraints 
    were recreated.

---

## Fail On Non-Conformant Error

When data being masked is non-conformant or invalid for an algorithm, 
the API results in an HTTP error saying that 
the data being masked is non-conformant. The masking pipeline will fail in such scenarios.  
This feature is controlled by the pipeline parameter `P_FAIL_ON_NONCONFORMANT_DATA` (default value: `true`).

1. Verify that the masking pipeline fails with error message reporting non-conformant data error when a column contains 
data that is non-conformant, and the parameter value is `true`, and no masked data is inserted into 
the sink data source.
    - Example: masking English words with the CM:Numeric Algorithm that expects numbers.
2. Verify that the masking pipeline does not fail, and the sink data source contains the original unmasked data 
when a column contains data that is non-conformant, and the parameter value is `false`.
    - Refer above example.

---

## Conditional Masking

This feature is exclusive to data masking activity and is applicable only to the masking pipeline.  
Conditional masking allows us to mask a column in a way that is dependent on the value of the dependent column. 
This also enables us to use different algorithms based on data assigned to a key column.

For example, masking a Phone number based on a key column Country Code or masking a date column containing dates in 
a different format based on a date format column.

1. Verify that a column with conditional masking applied can be masked correctly for one assigned rule or alias.
2. Verify that the masking falls back to the default rule if there is no matching data with the specified rule.
3. Verify conditional masking with multiple aliases/rules and a default rule.
4. Verify conditional masking with rules such as that is compatible and supported by ADF:
    - Equals (`==`)
    - Starts with
    - Ends with
5. Verify conditional masking with one or more rules specifying the different date formats:
    - `yyyyMMdd`
    - `yyyy-MM-dd`
    - `yyyy-MM-dd'T’HH:mm`
6. Verify that the dataflows and activities in “For Each Table to Mask” are run N times, where N is the number of 
filter aliases or rules created including the default.

---

## Checkpointing and Logging

Verifies that we correctly capture the state of the discovery and masking activities and that we can resume 
discovery and masking either from beginning or from the last checkpoint.

For example, we only discover or mask tables that have not been discovered or masked yet (could be due to 
last failure or yet to be masked).

### Discovery

#### Rediscovery

This feature is controlled by the `P_REDISCOVER` parameter for the discovery pipeline (default: `true`).

1. Verify that when the pipeline parameter is set to `true`, all tables in source schema are reprofiled or rediscovered.
    - Run discovery once for the source schema, make no changes in the source schema and run 
    the discovery pipeline again and verify that all tables in the source schema are picked up for profiling.
2. Verify that when the parameter is set to `false`, only those tables are profiled which have at least one column 
with discovery complete in discovered ruleset table set to `false`.
3. Verify that when a new column is added to the source schema, and on reprofiling with parameter set to `false`, 
only the new column is picked up and reprofiled for identifying and assigning profiled algorithm/domains.
4. Verify the discovered rulesets table has following column values for all columns on successful discovery:
    - Discovery completed as `true`
    - Latest event UUID
    - Profiled Algorithm
    - Assigned Domain
    - Confidence scores etc.

#### Logging

The discovery pipeline logs events into the `adf_event_logs` table on discovery completed or failure of 
later inspection and verification.

1. Verify that on a successful completion of a discovery run, events are logged in to the `adf_event_log` table.
    - Verify the following fields in the tables are populated for any new event added:
        - Start time
        - End time
        - Activity run id
        - Input parameters
        - Pipeline run id
        - Pipeline status (Success)
        - Source dataset
        - Source schema
        - Table etc.
2. Verify that on failure the error message and other fields are populated correctly in the event log table.
    - All above fields
    - Pipeline status as failed
    - Error message

---

### Masking

#### Reapply Mapping

This feature is controlled by the `P_REAPPLY_MAPPING` parameter in the masking pipeline (default: `true`).

1. Verify that when the pipeline parameter is set to `true`, all tables in the source schema listed in 
the `adf_data_mapping` tables are masked irrespective of whether mapping complete is set to `true` or not.
2. Verify that when the pipeline parameter is set to `false`, only those tables in the source schema listed 
in `adf_data_mapping` table are masked where mapping complete is set to `false`.

#### Logging

1. Verify that events are logged when masking is successful.
2. Verify that events are logged, and the appropriate error message is captured when masking fails.
3. Verify logging with conditional masking:
    - Start with a condition that’s invalid (use `=` for comparison in the condition instead of `==` for example).
        - If you have a default condition, this will also break the default case, so make sure you have at least 
        one case that is successful.
        - Run the pipeline and check that the filter with valid conditional expression is successful.
        - Verify that the table’s masking status matches success of applied filter, and the event log contains 
        the error details.
    - Update the filter condition to be valid, re-run the pipeline, check that it completes, the mapping complete flag 
    is set correctly, and confirm pipeline did not pick up the previously successful filter alias.

---

## Copying Unmasked Tables

Verifies that the pipeline copies the data for any tables that has no algorithm assigned to the sink along 
with its data.  
This is controlled by the pipeline parameter `P_COPY_UNMASKED_TABLES` and the copy behaviour is controlled by 
the parameter `P_USE_COPY_DATAFLOW`. The default value is `false`.

1. Verify that the unmasked tables (tables with no algorithms assigned) and those entries that are present in 
the `adf_data_mapping` are not copied over from the source to the sink when the 
`P_COPY_UNMASKED_TABLES` parameter value is `false`.
2. Verify that when copy unmasked parameter (`P_COPY_UNMASKED_TABLES`) value is `true`:
    - When use copy data flow (i.e., `P_USE_COPY_DATAFLOW` is set to `true`), the copy data flow activity was invoked 
    and unmasked tables were copied from the source to the sink.
    - When use copy data flow (`P_USE_COPY_DATAFLOW` is `false`), the unmasked data was copied from source to 
    the sink using the copy data activity.

---



## Data Source-Specific Testing Guidelines

1. **Understand the Source:** Review the documentation and metadata for your data source (e.g., Dynamics 365, 
Salesforce, Oracle, MongoDB, etc.).
2. **Enumerate Unique Features:** List any features, data types, behaviors, or APIs unique to this source (e.g., 
entity APIs, lookup columns, polymorphic fields, special authentication, etc.).
3. **Review Existing Coverage:** Check if similar tests exist for other sources in the test plan or repository.
4. **Design Test Scenarios:** For each unique feature, design test cases that verify correct discovery, masking, 
error handling, and integration with ADF pipelines.
5. **Prepare Test Data:** Create or identify datasets that exercise all unique features and edge cases for the source.
6. **Configure Pipelines:** Set up ADF pipelines with the correct parameters and mappings for your data source.
7. **Execute Tests:** Run the pipelines and observe results, logs, and any generated artifacts.

---

## Test Documentation Guidelines

### For Small Changes (Few Tests)
- **Document Directly in the Pull Request (PR):**
    - Use a clear, readable table format in the PR description.
    - For each test, include:
        - **Test Description:** What is being tested and why.
        - **Test Steps:** Step-by-step instructions to reproduce the test.
        - **Expected Result:** What should happen if the test passes.
        - **Actual Result:** What actually happened.
        - **Artifacts:** Attach or link to screenshots, logs, or reports (CSV, PDF, etc.) as needed.
    - Example table (Markdown):

        | Test Description | Test Steps | Expected Result | Actual Result | Artifacts |
        |------------------|-----------|-----------------|--------------|-----------|
        | Verify D365 entity API returns all entities | 1. Run discovery pipeline<br>2. Monitor API call | Entity list matches D365 org | Entity list correct | screenshot.png |

### For Large Changes (New Pipeline or Many Tests)
- **Document in a Spreadsheet (Excel/CSV):**
    - Use columns: Test Description, Test Steps, Expected Result, Actual Result, Artifacts.
    - Attach the spreadsheet to the PR and reference it in the PR description.
    - If possible, also provide a summary table in the PR for quick review.

### General Guidelines
- **Be Clear and Concise:** Write test steps and results so others can easily reproduce and verify.
- **Group Related Tests:** If many tests are similar, group them logically (e.g., by feature or data type).
- **Link to Artifacts:** Upload screenshots, logs, or reports to the PR or an accessible location and provide links.
- **Reference Test Plan:** If you add new scenarios, update the test plan or reference your additions in the PR.

### Submitting Your Tests
- **For PRs:**
    - Ensure your test documentation is included as described above.
    - If you add or update the test plan, mention this in your PR summary.
    - For new pipelines or major features, attach the full test spreadsheet and summarize key results in the PR.

---

# Miscellaneous Testcases
Some additional testing with positive, negative cases.

## Schema and Initial Metadata Discovery

1. Verify all tables in a source schema are discovered.
2. Verify with empty schema.
3. Verify schema having single table.
4. Verify schema with tables having complex types (JSON, ARRAY, etc).
5. Verify schema that is non-existent.
6. Verify schema with tables containing special characters in column name.
7. Verify schema with tables containing no rows.
8. Verify schema containing views, system tables.
9. Verify schema with no permissions.

---

## Nonfunctional Testcases

Testcases that may impact user experience and system performance.

### Scalability

1. Verify that multiple concurrent pipelines can be run with different source and sink data, and that the integrity 
is maintained.

### Load Testing

1. Verify that discovery pipeline can sample source dataset with many tables in a schema and each table having 
large amount of varying data (1K, 2K, 5K etc).
2. Verify that the masking pipeline can mask many tables with large number of varying data.

