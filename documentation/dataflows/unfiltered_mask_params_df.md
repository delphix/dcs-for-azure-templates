# Unfiltered Masking Parameters Dataflow

The masking parameters dataflow leverages the metadata store to compute some of the parameters are needed to run the
masking dataflow. Since many of the parameters depend on the database schema, the table sizes, the data types, the
assigned algorithms and other factors, this dataflow performs those computations so that they are consistently produced.

Populating the data needed for these parameters to be computed correctly is done via the profiling pipeline.

Note that changes to the metadata store may change the masking parameters computed by this dataflow.

## Structure

The general flow of the data in the dataflow is as follows:
```
[A] → [B] → [C] → [D] → [E] → [G] → [I] → [J] → [K] → [R] → [S]
       ↓              ↓              ↑                 ↑
       ↓              → [F] → [H] -→ ↑                 ↑
       ↓                                               ↑
      [N] → [O] → [P] → [Q] ------→------→------→------↑
       ↑                                        
[L] → [M]
```
The dataflow will vary slightly based on the data set it was constructed for (e.g. `SNOWFLAKE`, `DATABRICKS`, `ADLS`).
However, each version of the dataset will contain the following steps.

* `[A]` Data Source - `Ruleset` - Get the ruleset table from the metadata store at
`DF_METADATA_SCHEMA`.`DF_METADATA_RULESET_TABLE`
* `[B]` Filter - `FilterToSingleTable` - Filter ruleset table down to the table in question by specifying `dataset`,
`specified_database`, `specified_schema`, `identified_table`, and `assigned_algorithm` - making sure they match
the dataset associated with each version of the dataflow, `DF_SOURCE_DATABASE`, `DF_SOURCE_SCHEMA`, `DF_SOURCE_TABLE`,
and not empty or null (respectively). This filters the ruleset down to only the rules that need to be applied for
masking this particular table
* `[C]` Parse - `ParseMetadata` - Parse the content from the `metadata` column that contains JSON, specifically
handling parsing of known keys (i.e. `date_format`)
* `[D]` Derived Column - `DateFormatString` - Derive columns as necessary for handling the parsed data (i.e. consume
  `parsed_metadata.date_format` and put it in a column `date_format_string`), and add an `output_row` column that
  always contains `1` (used later for Aggregate and Join operations)
* Conditional Split - `SplitOnDateFormat` - Split the data into two streams, data that contains a specified date
format, and data that does not
  * `[E]` Conditional Split Stream - `ContainsDateFormat` - Stream of data that contains a date format
  * `[F]` Conditional Split Stream - `DoesNotContainDateFormat` - Stream of data that does not contain a date format
* `[G]` Aggregate - `DateFormatHeader` - Create DateFormatAssignment, grouped by output_row (which is always 1),
generating a JSON string that maps a column to its specified date format
* `[H]` Aggregate - `NoFormatHeader` - Create NoFormatHeader, that generates a JSON string containing an empty map when
all values are null, grouped by output_row (which is always 1)
* `[I]` Join - `JoinDateHeaders` - Full outer join both `DateFormatHeader` and `NoFormatHeader` where `output_row` =
`output_row`
* `[J]` Derived Column - `DateFormatHeaderHandlingNulls` - Update column `DateFormatAssignments` to coalesce
`DateFormatAssignments`, and `NoFormatHeader` (i.e. if `DateFormatAssignments` is null, take `NoFormatHeader`, which
won't be null), similarly with `output_row`
* `[K]` Select - `RemoveUnnecessaryColumns` - Remove intermediate columns
* `[L]` Data Source - `TypeMapping` - Get the type mapping table from the metadata store at
`DF_METADATA_STORE`.`DF_METADATA_ADF_TYPE_MAPPING_TABLE`
* `[M]` Filter - `FilterToDataSourceType` - Filter type mapping table down to only the dataset in question
* `[N]` Join - `RulesetWithTypes` - Join the ruleset table with the type mapping table based on the type of the column
and the translation of that type to an ADF type
* `[O]` Derived Column - `RulesetWithAlgorithmTypeMapping` - Generate several columns:
  * `output_row` that always contains `1` (used later for Aggregate and Join operations)
  * `adf_type_conversion` that contains a string like `<column_name> as <adf_type>`
  * `column_width_estimate` that contains an integer that uses `DF_COLUMN_WIDTH_ESTIMATE` as the width for any column
    where `identified_column_max_length` is not positive, and `identified_column_max_length` plus some padding otherwise
* `[P]` Aggregate - `GenerateMaskParameters` - Grouped by `output_row` produce the following aggregates
  * `FieldAlgorithmAssignments` - a JSON string that maps a column name to its assigned algorithm
  * `ColumnsToMask` - a list of the column names that have an algorithm assigned
  * `DataFactoryTypeMapping` - a string that can be used by ADF to parse the output of a call to the Delphix masking
    endpoint, leveraging the `adf_type_conversion` column derived previously
  * `NumberOfBatches` - an integer value determined by computing the number of batches leveraging the max `row_count`
    as specified in the ruleset table, and the sum of `column_width_estimate` column derived previously
  * `TrimLengths` - a list of the actual widths of the columns so that will be used by the masking data flow to trim
    output before sinking
* `[Q]` Derived Column - `ModifyNumberOfBatches` - Modifies the number of batches to be at least `1`
* `[R]` Join - `AllMaskingParameters` - Perform an inner join on `output_row` with the output of
  `RemoveUnnecessaryColumns` - combining all computed masking parameters into the same output stream
* `[S]` Sink - `MaskingParameterOutput` - Sink results of computing masking parameters to activity output cache