# Filter Test Utility Dataflow

The filter test utility dataflow is used to help users write ADF-approved filter conditions and validate that the data
that is returned is as expected.

Use data preview on Lookup "Select Tables That Require Masking" activity to confirm what filter conditions are to be
applied. Leverage data preview on this data flow to confirm your filter is working as expected.

## Structure

The general flow of the data in the dataflow is as follows:
```
[A] → [B] → [C]
```

* `[A]` Data Source - `SourceData` - Using an inline data set, specify the source as
`DF_SOURCE_SCHEMA`.`DF_SOURCE_TABLE`
* `[B]` Filter - `FilterToAppropriateRows` - Filter base table based on supplied filter
* `[C]` Sink - `SinkData` - Using an inline data set, specify the sink as `DF_SINK_SCHEMA`.`DF_SINK_TABLE`

### ADLS Modifications

As ADLS also takes in file names as part of the source data, the data flow appears as follows:

```
[W] → [X] → [Y] → [Z]
```

* `[W]` Data Source - `Source` - Select source data in source container `DF_SOURCE_CONTAINER`, and wildcard that is
  constructed based on `DF_SOURCE_DIRECTORY`, `DF_SOURCE_PREFIX`, and `DF_SOURCE_TABLE` using an inline dataset, with
  `DF_COLUMN_DELIMITER`, `DF_QUOTE_CHARACTER`, `DF_ESCAPE_CHARACTER`, and `DF_NULL_VALUE` as specified in the parameters,
  and storing the file name in `DELPHIX_COMPLIANCE_SERVICE_FILE_NAME`
* `[X]` Filter - `FilterToAppropriateRows` - Filter base table based on supplied filter
* `[Y]` Derived Column - `CreateSinkFileName` - Create column `DELPHIX_COMPLIANCE_SERVICE_SINK_FILE_NAME` by replacing
  `DF_SOURCE_DIRECTORY` with `DF_SINK_DIRECTORY` in the value in `DELPHIX_COMPLIANCE_SERVICE_FILE_NAME`
* `[Z]` Sink - `Sink` - Sink results of masking to data store by sinking all columns but
  `DELPHIX_COMPLIANCE_SERVICE_BATCH_ID`, `DELPHIX_COMPLIANCE_SERVICE_SORT_ID`, and `DELPHIX_COMPLIANCE_SERVICE_FILE_NAME`
  to the data sink, naming the file as `DELPHIX_COMPLIANCE_SERVICE_SINK_FILE_NAME`, and using the same metadata settings
  as were used in the source with `DF_COLUMN_DELIMITER`, `DF_QUOTE_CHARACTER`, `DF_ESCAPE_CHARACTER`, and `DF_NULL_VALUE`