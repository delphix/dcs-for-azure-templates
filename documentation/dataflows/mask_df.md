# Masking Dataflow

The masking dataflow is used to consume unmasked data from a source location, mask the data leveraging Delphix
Compliance Services APIs, and prepare it to be written to a sink location.

This dataflow requires several parameters that are computed leveraging the metadata store via the masking parameters
dataflow. These parameters are based on the database schema, the table sizes, the data types, the applied algorithms,
and other factors. Populating the data needed for these parameters to be computed correctly is done via the profiling
pipeline.

## Structure

The general flow of the data in the dataflow is as follows:
```
[A] → [B] → [C] → [D] →---→---→---→---→---→---→-----→ [L] → [M] → [N]
                      ↘︎                                     ↑
                        [E] → [F] → [G] → [H] → [I] → [J] → [K] 
```

* `[A]` Data Source - `Source` - Select source data at `DF_SOURCE_SCHEMA`.`DF_SOURCE_TABLE` using an inline dataset
* `[B]` Derived Column - `AddSortKey` - Create column `DELPHIX_COMPLIANCE_SERVICE_SORT_ID` that consists of SHA of the
data across all columns in the table - every row will have this value and it cannot be null
* `[C]` Sort - `SoryBySortKey` - Sort the table by the value in `DELPHIX_COMPLIANCE_SERVICE_SORT_ID`, as we need the
table to be in a particular order before we apply a surrogate key
* `[D]` Surrogate Key - `CreateSurrogateKey` - Add a `DELPHIX_COMPLIANCE_SERVICE_BATCH_ID` column that
increments by `1` and starts at `1` after applying the sorting
* `[E]` Derived Column - `WrapValuesInArray` - For each column we wish to mask, convert the value into an array, this
is needed to preserve `null` values as `null` when using `collect`, as `null` values become `[]`
* `[F]` Aggregate - `AggregateColumnsByBatch` - For each column we wish to mask, aggregate to a list using `collect`,
grouped by `DELPHIX_COMPLIANCE_SERVICE_BATCH_ID` modulo `DF_NUMBER_OF_BATCHES` - so there will be
`DF_NUMBER_OF_BATCHES` total such aggregations, name the group as `DELPHIX_COMPLIANCE_SERVICE_BATCH_GROUP`
* `[G]` Derived Column - `FlattenValuesOutOfArray` - For each column we wish to mask, flatten the value out of the
array, in the case where the value was previously `[]`, it becomes `null`
* `[H]` External Call - `DCSForAzureAPI` -  Call DCS for Azure services, using `/v1/masking/batchMaskByColumn`,
where the data from the `FlattenValuesOutOfArray` is included in the request body, and the following headers are
included in the request:
  * `'Run-Id'` - Used for tracing requests
  * `'Field-Algorithm-Assignment'` - Defines the algorithm to apply to which field, defined in
    `DF_FIELD_ALGORITHM_ASSIGNMENT`
  * `'Fail-On-Non-Conformant-Data'` - Defines failure behavior if a non-conformant data error is encountered, driven by
    parameter `DF_FAIL_ON_NONCONFORMANT_DATA`
  * `'Field-Date-Format'` - Defines the date format to apply to which field, defined in `DF_FIELD_DATE_FORMAT`
The format of the response is defined in `DF_BODY_TYPE_MAPPING`
* `[I]` Assert - `AssertNoFailures` - Confirm that we received a `200` response status from the API request
* `[J]` Flatten - `FlattenAggregateData` - Unroll the API response body into named columns
* `[K]` Derived Column - `TrimMaskedStrings` - For each column with a string type, trim the string to length based on
the value in `DF_TRIM_LENGTHS` - this is needed as masking a string may produce a longer string that exceeds the column
width in the sink
* `[L]` Select - `SelectColumnsUnmasked` - Select only columns that don't require masking
* `[M]` Join - `JoinMaskedAndUnmaskedData` - Inner join on `SelectColumnsUnmasked` and `TrimMaskedStrings` based on
matching `DELPHIX_COMPLIANCE_SERVICE_BATCH_ID`
* `[N]` Sink - `Sink` - Sink results of masking to data store by sinking the unrolled results of the masking call to
the columns of the same name in the data sink

### ADLS Modifications

As ADLS does not have an inherent order to the columns output for delimited text data sets, additional elements of the
dataflow are added to preserve column ordering. In this case, the data flow appears as follows:

```
[Z] → [B] → [C] → [D] →---→---→---→---→---→---→--[L] → [M] → [O]
                   ↓  ↘︎                                 ↑        ↘︎
                   ↓    [E] → [F*] → [G] → [H] → [I] → [J] → [K]  ↓
                    ↘︎                                             ↓
                      → [P] → [Q] -→---→---→---→---→---→---→---→ [R] → [S] → [T]
```

A few changes to explicitly call out:
* `[A]` has become `[Z]`
* `[K]` is no longer connected to the main data flow
* `[N]` has become `[T]` 
* `[F]` has become `[F*]`

Step `[F]` has been modified from the above - where instead of using modulo to assign rows to batches, since
we are unable to accurately determine number of batches without an approximation of row count, we instead assign rows
to batches by using `DF_TARGET_BATCH_SIZE` and the number of columns being masked.

All other steps whose letters exist above are the same, new steps are as follows:
* `[Z]` Data Source - `Source` - Select source data in source container `DF_SOURCE_CONTAINER`, and wildcard that is
constructed based on `DF_SOURCE_DIRECTORY`, `DF_SOURCE_PREFIX`, and `DF_SOURCE_TABLE` using an inline dataset, with
`DF_COLUMN_DELIMITER`, `DF_QUOTE_CHARACTER`, `DF_ESCAPE_CHARACTER`, and `DF_NULL_VALUE` as specified in the parameters,
and storing the file name in `DELPHIX_COMPLIANCE_SERVICE_FILE_NAME`
* `[O]` Select - `RemoveAmbiguousColumn` - Since both `FlattenAggregateData` and `SelectColumnsUnmasked` have a column
`DELPHIX_COMPLIANCE_SERVICE_BATCH_ID`, we need to remove that column, so select all columns whose name isn't
`DELPHIX_COMPLIANCE_SERVICE_BATCH_ID`
* `[P]` Filter - `RemoveAllData` - To preserve the order of the columns, we will remove all rows from the table by
filtering on `false()`
* `[Q]` Alter Row - `CreateAlterRow` - Add an alter row condition so that all rows will be inserted into the existing
table with the correct column order
* `[R]` Union - `CombineRows` - Union the output of `RemoveAmbiguousColumn` with the empty `CreateAlterRow` table, and
performing the union by name
* `[S]` Derived Column - `CreateSinkFileName` - Create column `DELPHIX_COMPLIANCE_SERVICE_SINK_FILE_NAME` by replacing
`DF_SOURCE_DIRECTORY` with `DF_SINK_DIRECTORY` in the value in `DELPHIX_COMPLIANCE_SERVICE_FILE_NAME`
* `[T]` Sink - `Sink` - Sink results of masking to data store by sinking all columns but
`DELPHIX_COMPLIANCE_SERVICE_BATCH_ID`, `DELPHIX_COMPLIANCE_SERVICE_SORT_ID`, and `DELPHIX_COMPLIANCE_SERVICE_FILE_NAME`
to the data sink, naming the file as `DELPHIX_COMPLIANCE_SERVICE_SINK_FILE_NAME`, and using the same metadata settings
as were used in the source with `DF_COLUMN_DELIMITER`, `DF_QUOTE_CHARACTER`, `DF_ESCAPE_CHARACTER`, and `DF_NULL_VALUE`