# Filtered Masking Parameters Dataflow

The masking parameters dataflow leverages the metadata store to compute some of the parameters are needed to run the
masking dataflow.

In particular, this data flow is used for when conditional algorithms are to be applied to a table. This means that
filter logic needs to be applied in order for masking to be correctly applied. In these scenarios, additional parsing of
the assigned_algorithm column of the discovered_ruleset table is required, in order to construct the appropriate masking
headers.

Since many of the parameters depend on the database schema, the table sizes, the data types, the
assigned algorithms and other factors, this dataflow performs those computations so that they are consistently produced.

Populating the data needed for these parameters to be computed correctly is done via the profiling pipeline.

Note that changes to the metadata store may change the masking parameters computed by this dataflow.

## Structure

The general flow of the data in the dataflow is as follows:
```
[A] → [B] → [C1] → [D] → [E] →  ↓
       ↓     ↓                 [H] → [I] → [J]
       ↓    [C2] → [F] → [G] →  ↑           ↓
       ↓     ↓                             [L] →  ↓
       ↓    [C3] → [K] →  →  →  →  →  →  →  ↑     ↓
       ↓                                         [O] → [P] → [Q] →
       ↓                             [M] → [N] →  ↑               ↓
       ↓                                                         [a] → [b]
        → [R] → [S1] → [S2] → [T] → [U1] → [V] → ↓                ↑
                                     ↓          [X] → [Y] → [Z] → 
                                    [U2] → [W] → ↑
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
* `[C]` Conditional Split - `HandleConditionalAlgorithms` - Conditionally distributing the data in assigned_algorithm
groups, based on the type of data in assigned_algorithm 
  * `[C1]` Conditional Split Stream - `KeyColumn` - Stream of data that contains a JSON array
  * `[C2]` Conditional Split Stream - `ConditionalAlgorithm` - Stream of data that contains a JSON object
  * `[C3]` Conditional Split Stream - `StandardAlgorithm` - Stream of data that does not contain JSON 
* `[D]` Parse - `ParseKeyColumn` - Parse key column conditions
* `[E]` Flatten - `FlattenKeyConditions` - Unroll the aliases and conditions from the key column
* `[F]` Parse - `ParseAlgorithm` - Parse conditional algorithm assignment
* `[G]` Flatten - `FlattenAlgorithmAssignments` - Unroll the conditions from the conditional algorithm assignment
* `[H]` Join - `JoinConditionalAlgorithms` - Join the key column, its conditions, and the algorithms assigned to those
conditions
* `[I]` Filter - `FilterToConditionKey` - Filter out rows that don't apply to this condition key, and rows that don't
include an assigned algorithm
* `[J]` Select - `SimplifyConditionalRulesetTable` - Rename columns in the conditional ruleset table to match the
simplified ruleset table
* `[K]` Select - `SimplifySimpleRulesetTable` - Simplify the columns of the ruleset table for algorithms that are always
applied
* `[L]` Union - `UnionAllRules` - Combining rows from conditional and non-conditional ruleset tables
* `[M]` Data Source - `TypeMapping` - Get the type mapping table from the metadata store at
  `DF_METADATA_STORE`.`DF_METADATA_ADF_TYPE_MAPPING_TABLE`
* `[N]` Filter - `FilterToDataSourceType` - Filter type mapping table down to only the dataset in question
* `[O]` Join - `RulesetWithTypes` - Join the ruleset table with the type mapping table based on the type of the column
  and the translation of that type to an ADF type
* `[P]` Derived Column - `RulesetWithAlgorithmTypeMapping` - Generate several columns:
  * `output_row` that always contains `1` (used later for Aggregate and Join operations)
  * `adf_type_conversion` that contains a string like `<column_name> as <adf_type>`
  * `column_width_estimate` that contains an integer that uses `DF_COLUMN_WIDTH_ESTIMATE` as the width for any column
    where `identified_column_max_length` is not positive, and `identified_column_max_length` plus some padding otherwise
* `[Q]` Aggregate - `GenerateMaskParameters` - Grouped by `output_row` produce the following aggregates
  * `FieldAlgorithmAssignments` - a JSON string that maps a column name to its assigned algorithm
  * `ColumnsToMask` - a list of the column names that have an algorithm assigned
  * `DataFactoryTypeMapping` - a string that can be used by ADF to parse the output of a call to the Delphix masking
    endpoint, leveraging the `adf_type_conversion` column derived previously
  * `TrimLengths` - a list of the actual widths of the columns so that will be used by the masking data flow to trim
    output before sinking
* `[R]` Parse - `ParseMetadata` - Parse the content from the `metadata` column that contains JSON, specifically
  handling parsing of known keys (i.e. `date_format` and conditional date formatting specifications)
* `[S1]` - Flatten - Unroll the conditions from the conditional date_format assignment
* `[S2]` - Filter - Filter down to only this filer alias, as necessary
* `[T]` Derived Column - `DateFormatString` - Derive columns as necessary for handling the parsed data (i.e. consume
  `parsed_metadata.date_format` and put it in a column `date_format_string`), and add an `output_row` column that
  always contains `1` (used later for Aggregate and Join operations)
* `[U]` Conditional Split - `SplitOnDateFormat` - Split the data into two streams, data that contains a specified date
  format, and data that does not
  * `[U1]` Conditional Split Stream - `ContainsDateFormat` - Stream of data that contains a date format
  * `[U2]` Conditional Split Stream - `DoesNotContainDateFormat` - Stream of data that does not contain a date format
* `[V]` Aggregate - `DateFormatHeader` - Create DateFormatAssignment, grouped by output_row (which is always 1),
  generating a JSON string that maps a column to its specified date format
* `[W]` Aggregate - `NoFormatHeader` - Create NoFormatHeader, that generates a JSON string containing an empty map when
  all values are null, grouped by output_row (which is always 1)
* `[X]` Join - `JoinDateHeaders` - Full outer join both `DateFormatHeader` and `NoFormatHeader` where `output_row` =
  `output_row`
* `[Y]` Derived Column - `DateFormatHeaderHandlingNulls` - Update column `DateFormatAssignments` to coalesce
  `DateFormatAssignments`, and `NoFormatHeader` (i.e. if `DateFormatAssignments` is null, take `NoFormatHeader`, which
  won't be null), similarly with `output_row`
* `[Z]` Select -  `RemoveUnnecessaryColumns` - Remove intermediate columns
* `[a]` Join - `AllMaskingParameters` - Perform an inner join on `output_row` with the output of
  `RemoveUnnecessaryColumns` - combining all computed masking parameters into the same output stream
* `[b]` Sink - `MaskingParameterOutput` - Sink results of computing masking parameters to activity output cache