/*
 * generate_dataverse_masking_parameters - Generates masking parameters for Azure Data Factory
 * dataflows.
 *
 * This stored procedure replaces the ADF dataflows previously used for generating
 * masking parameters, providing improved performance and maintainability. It reconstructs
 * the ruleset logic by filtering for the appropriate assigned algorithms and
 * conditional assignments (where applicable), and generates all necessary parameters
 * for masking a specific table using Delphix Compliance Services APIs within
 * Azure Data Factory pipelines.
 *
 * PARAMETERS:
 * @dataset - Dataset type identifier
 * @specified_database - Source database name
 * @specified_schema - Source schema name
 * @identified_table - Source table name
 * @column_width_estimate - Estimated column width for batch calculations (default: 1000)
 * @filter_alias - Optional filter key for conditional masking (default: '')
 *
 * OUTPUT PARAMETERS:
 * - FieldAlgorithmAssignments: JSON mapping of encoded column names to masking algorithms
 * - ColumnsToMask: JSON array of column names requiring masking
 * - DataFactoryTypeMapping: ADF schema definition for API response parsing
 * - TrimLengths: JSON array of column max lengths for output trimming
 * - DateFormatAssignments: JSON mapping of columns to their date format strings
 * - ColumnsToCastAsStrings: JSON array of columns requiring string casting
 * - ColumnsToCastBackTo*: JSON arrays of columns to cast back to specific ADF types
 * - DateOnlyColumns: JSON array of columns with type 'DateOnly'
 * - PrimaryKey: Comma-separated list of primary ID columns (IsPrimaryId = true, IsLogical = false)
 *
 * DEPENDENCIES:
 * - discovered_ruleset table: Contains masking rules, metadata, and algorithm assignments
 * - adf_type_mapping table: Maps database types to ADF types
 * - Supports conditional masking via JSON structures in assigned_algorithm column
 *
 * NOTES:
 * - Dataverse does not expose row counts in metadata, so batching logic is removed.
 * - Empty JSON arrays are converted to [""] for ADF UI compatibility.
 * - Uses hex-encoded column names to avoid ADF pipeline issues with special characters.
 * - Supports treat_as_string flag for type casting.
 * - Includes explicit handling for DateOnly columns and primary ID columns.
 */
CREATE OR ALTER PROCEDURE generate_dataverse_masking_parameters
    @dataset NVARCHAR(128),
    @specified_database NVARCHAR(128),
    @specified_schema NVARCHAR(128),
    @identified_table NVARCHAR(128),
    @column_width_estimate INT = 1000,
    @filter_alias NVARCHAR(128) = ''  -- Optional filter key for conditional masking
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StoredProcedureVersion VARCHAR(13) = 'V2025.10.10.0';
    DECLARE @filter_alias_display NVARCHAR(128);
    SET @filter_alias_display = CASE
        WHEN @filter_alias = '' THEN 'No filter set'
        ELSE @filter_alias
    END;

    DECLARE @msg NVARCHAR(400);
    SET
        @msg =
        'No masking rules found for the specified parameters.'
        + ' Ensure the discovered_ruleset table has '
        + 'valid entries for dataset: %s, '
        + 'database: %s, schema: %s, table: %s, filter: %s.';

    -- Validate ruleset existence
    IF
        NOT EXISTS (
            SELECT 1
            FROM discovered_ruleset AS r
            OUTER APPLY (
                SELECT 1 AS filter_found
                FROM
                    OPENJSON (r.assigned_algorithm)
                    WITH (
                        [key_column] NVARCHAR(255) '$.key_column',
                        [conditions] NVARCHAR(MAX) '$.conditions' AS JSON
                    ) AS aa
                CROSS APPLY
                    OPENJSON (aa.[conditions])
                    WITH (
                        [condition_alias] NVARCHAR(255) '$.alias',
                        [algorithm] NVARCHAR(255) '$.algorithm'
                    ) AS cond
                WHERE
                    r.assigned_algorithm IS NOT NULL
                    AND r.assigned_algorithm <> ''
                    AND ISJSON(r.assigned_algorithm) = 1
                    AND JSON_VALUE(r.assigned_algorithm, '$.key_column') IS NOT NULL
                    AND cond.[condition_alias] IS NOT NULL
                    AND cond.[condition_alias] = @filter_alias
            ) AS filter_check
            WHERE
                r.dataset = @dataset
                AND r.specified_database = @specified_database
                AND r.specified_schema = @specified_schema
                AND r.identified_table = @identified_table
                AND r.assigned_algorithm IS NOT NULL
                AND r.assigned_algorithm <> ''
                AND NOT (
                    ISJSON(r.assigned_algorithm) = 1
                    AND LEFT(LTRIM(r.assigned_algorithm), 1) = '['
                )
                AND (@filter_alias = '' OR filter_check.filter_found = 1)
        )
        BEGIN
            RAISERROR (
                @msg,
                16, 1,
                @dataset,
                @specified_database,
                @specified_schema,
                @identified_table,
                @filter_alias_display
            );
            RETURN;
        END;

    -- CTE: Get DateOnly columns
    WITH date_only_columns_cte AS (
        SELECT identified_column
        FROM discovered_ruleset
        WHERE
            dataset = @dataset
            AND specified_database = @specified_database
            AND specified_schema = @specified_schema
            AND identified_table = @identified_table
            AND identified_column_type = 'DateOnly'
    ),

    -- CTE: Get Primary ID columns
    primary_id_columns_cte AS (
        SELECT identified_column
        FROM discovered_ruleset
        WHERE
            dataset = @dataset
            AND specified_database = @specified_database
            AND specified_schema = @specified_schema
            AND identified_table = @identified_table
            AND source_metadata IS NOT NULL
            AND ISJSON(source_metadata) = 1
            AND JSON_VALUE(source_metadata, '$.IsPrimaryId') = 'true'
            AND JSON_VALUE(source_metadata, '$.IsLogical') = 'false'
    ),

    -- base_ruleset_filter
    base_ruleset_filter AS (
        SELECT
            r.dataset,
            r.specified_database,
            r.specified_schema,
            r.identified_table,
            r.identified_column,
            r.identified_column_type,
            r.identified_column_max_length,
            r.ordinal_position,
            r.assigned_algorithm,
            r.algorithm_metadata,
            t.adf_type,
            CONCAT(
                'x',
                CONVERT(VARCHAR(MAX), CONVERT(VARBINARY, r.identified_column), 2)
            ) AS encoded_column_name,
            JSON_VALUE(r.algorithm_metadata, '$.date_format') AS date_format,
            CONVERT(
                BIT,
                JSON_VALUE(r.algorithm_metadata, '$.treat_as_string')
            ) AS treat_as_string,
            CASE
                WHEN r.identified_column_max_length > 0
                    THEN r.identified_column_max_length + 4
                ELSE @column_width_estimate
            END AS column_width_estimate
        FROM discovered_ruleset AS r
        INNER JOIN adf_type_mapping AS t
            ON
                r.identified_column_type = t.dataset_type
                AND r.dataset = t.dataset
        WHERE
            r.dataset = @dataset
            AND r.specified_database = @specified_database
            AND r.specified_schema = @specified_schema
            AND r.identified_table = @identified_table
            AND r.assigned_algorithm IS NOT NULL
            AND r.assigned_algorithm <> ''
            AND NOT (
                ISJSON(r.assigned_algorithm) = 1
                AND LEFT(LTRIM(r.assigned_algorithm), 1) = '['
            )
    ),

    -- conditional_algorithm_extraction
    conditional_algorithm_extraction AS (
        SELECT
            brf.*,
            aa.[key_column],
            aa.[conditions],
            c.[condition_alias],
            c.[algorithm] AS resolved_algorithm
        FROM base_ruleset_filter AS brf
        CROSS APPLY
            OPENJSON (brf.assigned_algorithm)
            WITH (
                [key_column] NVARCHAR(255) '$.key_column',
                [conditions] NVARCHAR(MAX) '$.conditions' AS JSON
            ) AS aa
        CROSS APPLY
            OPENJSON (aa.[conditions])
            WITH (
                [condition_alias] NVARCHAR(255) '$.alias',
                [algorithm] NVARCHAR(255) '$.algorithm'
            ) AS c
        WHERE
            ISJSON(brf.assigned_algorithm) = 1
            AND JSON_VALUE(brf.assigned_algorithm, '$.key_column') IS NOT NULL
            AND c.[condition_alias] = @filter_alias
            AND @filter_alias <> ''
    ),

    -- algorithm_resolution
    algorithm_resolution AS (
        SELECT
            dataset,
            specified_database,
            specified_schema,
            identified_table,
            identified_column,
            identified_column_type,
            identified_column_max_length,
            ordinal_position,
            encoded_column_name,
            resolved_algorithm AS assigned_algorithm
        FROM conditional_algorithm_extraction
        UNION ALL
        SELECT
            dataset,
            specified_database,
            specified_schema,
            identified_table,
            identified_column,
            identified_column_type,
            identified_column_max_length,
            ordinal_position,
            encoded_column_name,
            assigned_algorithm
        FROM base_ruleset_filter
        WHERE
            assigned_algorithm IS NOT NULL
            AND assigned_algorithm <> ''
            AND ISJSON(assigned_algorithm) = 0
    ),

    -- generate_mask_parameters
    generate_mask_parameters AS (
        SELECT
            COALESCE(
                '{'
                + STRING_AGG(
                    '"' + LOWER(brf.encoded_column_name)
                    + '":"' + ar.assigned_algorithm + '"',
                    ','
                )
                + '}',
                '{}'
            ) AS fieldalgorithmassignments,
            COALESCE(
                JSON_QUERY(
                    '[' + STRING_AGG('"' + brf.identified_column + '"', ',') + ']'
                ),
                '[]'
            ) AS columnstomask,
            '''(timestamp as date, status as string, message as string, trace_id as string, 
                items as (DELPHIX_COMPLIANCE_SERVICE_BATCH_ID as long'
            + COALESCE(
                ', '
                + STRING_AGG(
                    LOWER(CONCAT(
                        brf.encoded_column_name,
                        ' as ',
                        CASE
                            WHEN brf.treat_as_string = 1 THEN 'string'
                            ELSE brf.adf_type
                        END
                    )),
                    ', '
                ),
                ''
            )
            + ')[])''' AS datafactorytypemapping,
            COALESCE(
                '['
                + STRING_AGG(
                    CONVERT(NVARCHAR(10), brf.identified_column_max_length),
                    ','
                )
                + ']',
                '[]'
            ) AS trimlengths
        FROM algorithm_resolution AS ar
        INNER JOIN base_ruleset_filter AS brf
            ON ar.identified_column = brf.identified_column
    ),

    -- conditional_date_formats
    conditional_date_formats AS (
        SELECT
            brf.identified_column,
            brf.encoded_column_name,
            c.[condition_alias],
            c.[date_format] AS conditional_date_format
        FROM base_ruleset_filter AS brf
        CROSS APPLY
            OPENJSON (brf.algorithm_metadata, '$.conditions')
            WITH (
                [condition_alias] NVARCHAR(255) '$.alias',
                [date_format] NVARCHAR(255) '$.date_format'
            ) AS c
        WHERE
            @filter_alias <> ''
            AND brf.algorithm_metadata IS NOT NULL
            AND c.[condition_alias] = @filter_alias
            AND c.[date_format] IS NOT NULL
    ),

    -- date_format_resolution
    date_format_resolution AS (
        SELECT
            brf.identified_column,
            brf.encoded_column_name,
            COALESCE(
                cdf.conditional_date_format,
                brf.date_format
            ) AS final_date_format
        FROM base_ruleset_filter AS brf
        LEFT JOIN conditional_date_formats AS cdf
            ON brf.identified_column = cdf.identified_column
        WHERE
            brf.date_format IS NOT NULL
            OR cdf.conditional_date_format IS NOT NULL
    ),

    -- date_format_parameters
    date_format_parameters AS (
        SELECT
            COALESCE(
                '{'
                + STRING_AGG(
                    '"'
                    + LOWER(encoded_column_name)
                    + '":"'
                    + REPLACE(final_date_format, '''', '\''')
                    + '"',
                    ','
                )
                + '}',
                '{}'
            ) AS dateformatassignments
        FROM date_format_resolution
    ),

    -- type_casting_parameters
    type_casting_parameters AS (
        SELECT
            COALESCE(
                '[' + STRING_AGG('"' + identified_column + '"', ',') + ']',
                '[""]'
            ) AS columnstocastasstrings,
            COALESCE(
                '['
                + STRING_AGG(
                    CASE
                        WHEN adf_type = 'binary' THEN '"' + identified_column + '"'
                    END,
                    ','
                )
                + ']',
                '[""]'
            ) AS columnstocastbacktobinary,
            COALESCE(
                '['
                + STRING_AGG(
                    CASE
                        WHEN adf_type = 'boolean'
                            THEN '"' + identified_column + '"'
                    END,
                    ','
                )
                + ']',
                '[""]'
            ) AS columnstocastbacktoboolean,
            COALESCE(
                '['
                + STRING_AGG(
                    CASE
                        WHEN adf_type = 'date'
                            THEN '"' + identified_column + '"'
                    END,
                    ','
                )
                + ']',
                '[""]'
            ) AS columnstocastbacktodate,
            COALESCE(
                '['
                + STRING_AGG(
                    CASE
                        WHEN adf_type = 'double'
                            THEN '"' + identified_column + '"'
                    END,
                    ','
                )
                + ']',
                '[""]'
            ) AS columnstocastbacktodouble,
            COALESCE(
                '['
                + STRING_AGG(
                    CASE
                        WHEN adf_type = 'float'
                            THEN '"' + identified_column + '"'
                    END,
                    ','
                )
                + ']',
                '[""]'
            ) AS columnstocastbacktofloat,
            COALESCE(
                '['
                + STRING_AGG(
                    CASE
                        WHEN adf_type = 'integer'
                            THEN '"' + identified_column + '"'
                    END,
                    ','
                )
                + ']',
                '[""]'
            ) AS columnstocastbacktointeger,
            COALESCE(
                '['
                + STRING_AGG(
                    CASE
                        WHEN adf_type = 'long'
                            THEN '"' + identified_column + '"'
                    END,
                    ','
                )
                + ']',
                '[""]'
            ) AS columnstocastbacktolong,
            COALESCE(
                '['
                + STRING_AGG(
                    CASE
                        WHEN adf_type = 'timestamp'
                            THEN '"' + identified_column + '"'
                    END,
                    ','
                )
                + ']',
                '[""]'
            ) AS columnstocastbacktotimestamp
        FROM base_ruleset_filter
        WHERE treat_as_string = 1
    ),

    -- date_only_columns_parameters
    date_only_columns_parameters AS (
        SELECT
            COALESCE(
                '[' + STRING_AGG('"' + identified_column + '"', ',') + ']',
                '[]'
            ) AS dateonlycolumns
        FROM date_only_columns_cte
    ),

    -- primary_id_columns_parameters
    primary_id_columns_parameters AS (
        SELECT COALESCE(STRING_AGG(identified_column, ','), '') AS [primarykey]
        FROM primary_id_columns_cte
    )

    -- final SELECT
    SELECT
        g.fieldalgorithmassignments,
        g.columnstomask,
        g.datafactorytypemapping,
        g.trimlengths,
        df.dateformatassignments,
        a.columnstocastasstrings,
        a.columnstocastbacktobinary,
        a.columnstocastbacktoboolean,
        a.columnstocastbacktodate,
        a.columnstocastbacktodouble,
        a.columnstocastbacktofloat,
        a.columnstocastbacktointeger,
        a.columnstocastbacktolong,
        a.columnstocastbacktotimestamp,
        doc.dateonlycolumns,
        -- Dataverse tables have only one Primary ID, this will never be a comma-separated string
        pic.primarykey,
        @StoredProcedureVersion AS storedprocedureversion
    FROM generate_mask_parameters AS g,
        date_format_parameters AS df,
        type_casting_parameters AS a,
        date_only_columns_parameters AS doc,
        primary_id_columns_parameters AS pic;
END
GO
