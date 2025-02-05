ALTER TABLE discovered_ruleset ADD algorithm_metadata NVARCHAR(MAX);

-- Move the `date_format` key/value pair out of the metadata column and into the `algorithm_metadata` column
UPDATE discovered_ruleset
SET
    algorithm_metadata = JSON_MODIFY(COALESCE(algorithm_metadata,'{}'), '$.date_format', JSON_VALUE(metadata, '$.date_format')),
    metadata = JSON_MODIFY(metadata, '$.date_format', NULL)
WHERE
    JSON_VALUE(metadata, '$.date_format') IS NOT NULL;
-- Move the `key_column` key/value pair out of the metadata column and into the `algorithm_metadata` column
UPDATE discovered_ruleset
SET
    algorithm_metadata = JSON_MODIFY(COALESCE(algorithm_metadata,'{}'), '$.key_column', JSON_VALUE(metadata, '$.key_column')),
    metadata = JSON_MODIFY(metadata, '$.key_column', NULL)
WHERE
    JSON_VALUE(metadata, '$.key_column') IS NOT NULL;
-- Move the `conditions` key/value pair out of the metadata column and into the `algorithm_metadata` column
UPDATE discovered_ruleset
SET
    algorithm_metadata = JSON_MODIFY(COALESCE(algorithm_metadata,'{}'), '$.conditions', JSON_VALUE(metadata, '$.conditions')),
    metadata = JSON_MODIFY(metadata, '$.conditions', NULL)
WHERE
    JSON_VALUE(metadata, '$.conditions') IS NOT NULL;

-- Rename the `metadata` column to `source_metadata` for clarity
EXEC sp_rename 'discovered_ruleset.metadata', 'source_metadata', 'COLUMN';

-- Update stored procedures to use new source_metadata column
ALTER PROCEDURE get_columns_from_parquet_file_structure_sp
    @adf_file_structure NVARCHAR(MAX),
    @database NVARCHAR(MAX),
    @schema NVARCHAR(MAX),
    @table NVARCHAR(MAX),
    @dataset NVARCHAR(255)
AS
BEGIN
    IF @adf_file_structure IS NOT NULL AND LEN(@adf_file_structure) > 0
        -- Check if the input adf_file_structure is not NULL or empty
        BEGIN
            MERGE discovered_ruleset AS rs
            USING (
                SELECT @dataset AS dataset,
                    @database AS specified_database,
                    @schema AS specified_schema,
                    @table AS identified_table,
                    structure.identified_column AS identified_column,
                    structure.identified_column_type AS identified_column_type,
                    -1 AS identified_column_max_length,
                    with_idx.[key] AS ordinal_position,
                    -1 AS row_count,
                    JSON_OBJECT(
                        'metadata_version': 1
                    ) AS source_metadata
                FROM
                    OPENJSON(@adf_file_structure,'$') with_idx
                    CROSS APPLY OPENJSON(with_idx.[value], '$')
                WITH
                    (
                        [identified_column] NVARCHAR(255) '$.name',
                        [identified_column_type] NVARCHAR(255) '$.type'
                    )
                structure
            ) AS parquet_schema
            ON
            (
                rs.dataset = parquet_schema.dataset
                AND rs.specified_database = parquet_schema.specified_database
                AND rs.specified_schema = parquet_schema.specified_schema
                AND rs.identified_table = parquet_schema.identified_table
                AND rs.identified_column = parquet_schema.identified_column
            )
            WHEN MATCHED THEN
                UPDATE
                SET
                    rs.identified_column_type = parquet_schema.identified_column_type,
                    rs.row_count = parquet_schema.row_count,
                    rs.source_metadata = parquet_schema.source_metadata
            WHEN NOT MATCHED THEN
                INSERT (
                    dataset,
                    specified_database,
                    specified_schema,
                    identified_table,
                    identified_column,
                    identified_column_type,
                    identified_column_max_length,
                    ordinal_position,
                    row_count,
                    source_metadata
                )
                VALUES (
                    parquet_schema.dataset,
                    parquet_schema.specified_database,
                    parquet_schema.specified_schema,
                    parquet_schema.identified_table,
                    parquet_schema.identified_column,
                    parquet_schema.identified_column_type,
                    parquet_schema.identified_column_max_length,
                    parquet_schema.ordinal_position,
                    parquet_schema.row_count,
                    parquet_schema.source_metadata
                );
        END
    ELSE
        BEGIN
            -- Handle NULL or empty adf_file_structure input
            PRINT 'adf_file_structure is NULL or empty';
        END
END;

ALTER PROCEDURE get_columns_from_delimited_file_structure_sp
    @adf_file_structure NVARCHAR(MAX),
    @database NVARCHAR(MAX),
    @schema NVARCHAR(MAX),
    @table NVARCHAR(MAX),
    @column_delimiter NVARCHAR(1),
    @quote_character NVARCHAR(1),
    @escape_character NVARCHAR(2),
    @null_value NVARCHAR(MAX),
    @dataset NVARCHAR(255)
AS
BEGIN
    IF @adf_file_structure IS NOT NULL AND LEN(@adf_file_structure) > 0
        -- Check if the input adf_file_structure is not NULL or empty
        BEGIN
            MERGE discovered_ruleset AS rs
            USING (
                SELECT @dataset AS dataset,
                    @database AS specified_database,
                    @schema AS specified_schema,
                    @table AS identified_table,
                    structure.identified_column,
                    structure.identified_column_type,
                    -1 AS identified_column_max_length,
                    with_idx.[key] AS ordinal_position,
                    -1 AS row_count,
                    JSON_OBJECT(
                        'metadata_version': 2,
                        'column_delimiter': @column_delimiter,
                        'quote_character': @quote_character,
                        'escape_character': @escape_character,
                        'null_value': @null_value
                    ) AS source_metadata
                FROM
                    OPENJSON(@adf_file_structure) with_idx
                    CROSS APPLY OPENJSON(with_idx.[value], '$')
                WITH
                    (
                        [identified_column] VARCHAR(255) '$.name',
                        [identified_column_type] VARCHAR(255) '$.type'
                    )
                structure
            ) AS adls_schema
            ON
            (
                rs.dataset = adls_schema.dataset
                AND rs.specified_database = adls_schema.specified_database
                AND rs.specified_schema = adls_schema.specified_schema
                AND rs.identified_table = adls_schema.identified_table
                AND rs.identified_column = adls_schema.identified_column
            )
            WHEN MATCHED THEN
                UPDATE
                    SET
                        rs.identified_column_type = adls_schema.identified_column_type,
                        rs.row_count = adls_schema.row_count,
                        rs.source_metadata = adls_schema.source_metadata
            WHEN NOT MATCHED THEN
                INSERT (
                    dataset,
                    specified_database,
                    specified_schema,
                    identified_table,
                    identified_column,
                    identified_column_type,
                    identified_column_max_length,
                    ordinal_position,
                    row_count,
                    source_metadata
                )
                VALUES (
                    adls_schema.dataset,
                    adls_schema.specified_database,
                    adls_schema.specified_schema,
                    adls_schema.identified_table,
                    adls_schema.identified_column,
                    adls_schema.identified_column_type,
                    adls_schema.identified_column_max_length,
                    adls_schema.ordinal_position,
                    adls_schema.row_count,
                    adls_schema.source_metadata
                );
        END
    ELSE
        -- Handle NULL or empty adf_file_structure input
        BEGIN
            PRINT 'adf_file_structure is NULL or empty';
        END
END;