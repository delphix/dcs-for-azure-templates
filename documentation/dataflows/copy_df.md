# Copy Dataflow

The copy dataflow is used to copy unmasked data from a source location to a sink location.

This dataflow is used when we allow copying unmasked data from a source (configured by pipeline parameter) and when we
are choosing not to use a copy activity (configured by pipeline parameter). Using the dataflow over the copy activity is
sometimes not possible depending on the source and sink linked services).

## Structure

The general flow of the data in the dataflow is as follows:
```mermaid
flowchart LR
    SourceData --> SinkData
```

* `SourceData` - Data Source - Using an inline data set, specify the source as
`DF_SOURCE_SCHEMA`.`DF_SOURCE_TABLE`
* `SinkData` - Sink - Using an inline data set, specify the sink as `DF_SINK_SCHEMA`.`DF_SINK_TABLE`

### ADLS Modifications

As ADLS also takes in file names as part of the source data, the data flow appears as follows:

```mermaid
flowchart LR
    SourceData --> 
    CreateSinkFileName --> SinkData
```

* `SourceData` - Data Source - Select source data in source container `DF_SOURCE_CONTAINER`, and wildcard that is
  constructed based on `DF_SOURCE_DIRECTORY`, `DF_SOURCE_PREFIX`, and `DF_SOURCE_TABLE` using an inline dataset, with
  `DF_COLUMN_DELIMITER`, `DF_QUOTE_CHARACTER`, `DF_ESCAPE_CHARACTER`, and `DF_NULL_VALUE` as specified in the parameters,
  and storing the file name in `DELPHIX_COMPLIANCE_SERVICE_FILE_NAME`
* `CreateSinkFileName`- Derived Column - Create column `DELPHIX_COMPLIANCE_SERVICE_SINK_FILE_NAME` by replacing
  `DF_SOURCE_DIRECTORY` with `DF_SINK_DIRECTORY` in the value in `DELPHIX_COMPLIANCE_SERVICE_FILE_NAME`
* `SinkData` - Sink - Sink results of masking to data store by sinking all columns but
  `DELPHIX_COMPLIANCE_SERVICE_BATCH_ID`, `DELPHIX_COMPLIANCE_SERVICE_SORT_ID`, and `DELPHIX_COMPLIANCE_SERVICE_FILE_NAME`
  to the data sink, naming the file as `DELPHIX_COMPLIANCE_SERVICE_SINK_FILE_NAME`, and using the same metadata settings
  as were used in the source with `DF_COLUMN_DELIMITER`, `DF_QUOTE_CHARACTER`, `DF_ESCAPE_CHARACTER`, and `DF_NULL_VALUE`