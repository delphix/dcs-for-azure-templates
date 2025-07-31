/*
 * generate_masking_parameters - Generates masking parameters for Azure Data Factory dataflows.
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
 * - NumberOfBatches: Calculated batch count for optimal processing
 * - TrimLengths: JSON array of column max lengths for output trimming
 * - DateFormatAssignments: JSON mapping of columns to their date format strings
 * - ColumnsToCastAsStrings: JSON array of columns requiring string casting
 * - ColumnsToCastBackTo*: JSON arrays of columns to cast back to specific ADF types
 *
 * DEPENDENCIES:
 * - discovered_ruleset table: Contains masking rules and algorithm assignments
 * - adf_type_mapping table: Maps database types to ADF types
 * - Supports conditional masking via JSON structures in assigned_algorithm column
 *
 * FALLBACK BEHAVIOR:
 * Always returns exactly one record. If no masking rules exist, returns default/empty
 * values to prevent ADF lookup activity failures.
 *
 * NOTES:
 * - Empty JSON arrays are converted to [""] for ADF UI compatibility
 * - Uses hex-encoded column names to avoid ADF pipeline issues with special characters
 * - Supports treat_as_string flag for type casting feature
 */
CREATE OR ALTER PROCEDURE generate_masking_parameters
    @dataset NVARCHAR(128),
    @specified_database NVARCHAR(128),
    @specified_schema NVARCHAR(128),
    @identified_table NVARCHAR(128),
    @column_width_estimate INT = 1000,
    @filter_alias NVARCHAR(128) = ''  -- Optional filter key for conditional masking
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StoredProcedureVersion VARCHAR(13) = 'V2025.07.22.0';

    -- base_ruleset_filter - Filter to target table, add ADF type mappings, 
    -- compute metadata, and exclude key columns
    WITH base_ruleset_filter AS (
        SELECT
            r.dataset,
            r.specified_database,
            r.specified_schema,
            r.identified_table,
            r.identified_column,
            r.identified_column_type,
            r.identified_column_max_length,
            r.row_count,
            r.ordinal_position,
            r.assigned_algorithm,
            r.algorithm_metadata,
            t.adf_type,
            CONCAT(
                'x',
                CONVERT(
                    VARCHAR(MAX),
                    CONVERT(VARBINARY, r.identified_column),
                    2
                )
            ) AS encoded_column_name,
            -- algorithm_metadata is always expected to be a JSON object (or NULL), 
            -- so JSON_VALUE is safe without ISJSON/CASE
            JSON_VALUE(r.algorithm_metadata, '$.date_format') AS [date_format],
            CONVERT(BIT, JSON_VALUE(r.algorithm_metadata, '$.treat_as_string')) AS treat_as_string,
            -- Add column width estimate here
            CASE
                WHEN r.identified_column_max_length > 0 THEN r.identified_column_max_length + 4
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
            -- Exclude key columns for conditional masking: key columns have 
            -- assigned_algorithm as a JSON array
            AND ISJSON(r.assigned_algorithm, ARRAY) = 0
    ),

    -- conditional_algorithm_extraction - Extract conditional algorithms for matching filter key
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

    -- standard_algorithm_handling - Handle non-conditional algorithms
    standard_algorithm_handling AS (
        SELECT
            brf.*,
            brf.assigned_algorithm AS resolved_algorithm
        FROM base_ruleset_filter AS brf
        WHERE
            ISJSON(brf.assigned_algorithm) = 0
            OR @filter_alias = ''
    ),

    -- algorithm_resolution - Combine conditional and standard algorithms
    algorithm_resolution AS (
        SELECT
            dataset,
            specified_database,
            specified_schema,
            identified_table,
            identified_column,
            identified_column_type,
            identified_column_max_length,
            row_count,
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
            row_count,
            ordinal_position,
            encoded_column_name,
            resolved_algorithm AS assigned_algorithm
        FROM standard_algorithm_handling
        WHERE
            resolved_algorithm IS NOT NULL
            AND resolved_algorithm <> ''
            AND ISJSON(resolved_algorithm) = 0
    ),

    -- generate_mask_parameters - Create core masking parameters with fallback defaults
    generate_mask_parameters AS (
        SELECT
            -- JSON mapping: encoded column name -> algorithm
            COALESCE(
                '{'
                + STRING_AGG(
                    '"' + LOWER(brf.encoded_column_name) + '":"' + ar.assigned_algorithm + '"',
                    ','
                )
                + '}',
                '{}'
            ) AS [FieldAlgorithmAssignments],
            -- JSON array of column names to mask
            COALESCE(
                JSON_QUERY(
                    '['
                    + STRING_AGG('"' + brf.identified_column + '"', ',')
                    + ']'
                ),
                '[]'
            ) AS [ColumnsToMask],
            -- ADF data flow expression for DCS masking API response parsing
            ''''
            + '(timestamp as date, status as string, message as string, trace_id as string, '
            + 'items as (DELPHIX_COMPLIANCE_SERVICE_BATCH_ID as long'
            + COALESCE(
                ', '
                + STRING_AGG(
                    LOWER(
                        CONCAT(
                            brf.encoded_column_name,
                            ' as ',
                            CASE WHEN brf.treat_as_string = 1 THEN 'string' ELSE brf.adf_type END
                        )
                    ),
                    ', '
                ),
                ''
            )
            + ')[])'
            + '''' AS [DataFactoryTypeMapping],
            COALESCE(
                CASE
                    WHEN
                        CEILING(
                            MAX(brf.row_count)
                            * (
                                SUM(brf.column_width_estimate)
                                + LOG10(MAX(brf.row_count))
                                + 1
                            )
                            / (2000000 * 0.9)
                        ) < 1
                        THEN 1
                    ELSE CEILING(
                        MAX(brf.row_count)
                        * (
                            SUM(brf.column_width_estimate)
                            + LOG10(MAX(brf.row_count))
                            + 1
                        )
                        / (2000000 * 0.9)
                    )
                END, 1
            ) AS [NumberOfBatches],   -- Optimal batch count (minimum 1)
            COALESCE(
                '[' + STRING_AGG(
                    CONVERT(NVARCHAR(10), brf.identified_column_max_length), ','
                ) + ']', '[]'
            ) AS [TrimLengths]   -- JSON array of column max lengths
        FROM algorithm_resolution AS ar
        INNER JOIN base_ruleset_filter AS brf
            ON ar.identified_column = brf.identified_column
    ),

    -- conditional_date_formats - Extract conditional date formats for filter key
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

    -- date_format_resolution - Merge conditional and standard date formats
    date_format_resolution AS (
        SELECT
            brf.identified_column,
            brf.encoded_column_name,
            COALESCE(cdf.conditional_date_format, brf.[date_format]) AS final_date_format
        FROM base_ruleset_filter AS brf
        LEFT JOIN conditional_date_formats AS cdf
            ON brf.identified_column = cdf.identified_column
        WHERE
            brf.[date_format] IS NOT NULL
            OR cdf.conditional_date_format IS NOT NULL
    ),

    -- date_format_parameters - Create JSON mapping with fallback default
    date_format_parameters AS (
        SELECT
            COALESCE(
                '{' + STRING_AGG(
                    '"' + LOWER(encoded_column_name) + '":"' + final_date_format + '"',
                    ','
                ) + '}',
                '{}'
            ) AS [DateFormatAssignments]
        FROM date_format_resolution
    ),

    -- type_casting_parameters - Create casting arrays with fallback defaults
    type_casting_parameters AS (
        SELECT
            COALESCE(
                '[' + STRING_AGG('"' + identified_column + '"', ',') + ']', '[""]'
            ) AS [ColumnsToCastAsStrings],
            COALESCE(
                '[' + STRING_AGG(
                    CASE WHEN adf_type = 'binary' THEN '"' + identified_column + '"' END, ','
                ) + ']', '[""]'
            ) AS [ColumnsToCastBackToBinary],
            COALESCE(
                '[' + STRING_AGG(
                    CASE WHEN adf_type = 'boolean' THEN '"' + identified_column + '"' END, ','
                ) + ']', '[""]'
            ) AS [ColumnsToCastBackToBoolean],
            COALESCE(
                '[' + STRING_AGG(
                    CASE WHEN adf_type = 'date' THEN '"' + identified_column + '"' END, ','
                ) + ']', '[""]'
            ) AS [ColumnsToCastBackToDate],
            COALESCE(
                '[' + STRING_AGG(
                    CASE WHEN adf_type = 'double' THEN '"' + identified_column + '"' END, ','
                ) + ']', '[""]'
            ) AS [ColumnsToCastBackToDouble],
            COALESCE(
                '[' + STRING_AGG(
                    CASE WHEN adf_type = 'float' THEN '"' + identified_column + '"' END, ','
                ) + ']', '[""]'
            ) AS [ColumnsToCastBackToFloat],
            COALESCE(
                '[' + STRING_AGG(
                    CASE WHEN adf_type = 'integer' THEN '"' + identified_column + '"' END, ','
                ) + ']', '[""]'
            ) AS [ColumnsToCastBackToInteger],
            COALESCE(
                '[' + STRING_AGG(
                    CASE WHEN adf_type = 'long' THEN '"' + identified_column + '"' END, ','
                ) + ']', '[""]'
            ) AS [ColumnsToCastBackToLong],
            COALESCE(
                '[' + STRING_AGG(
                    CASE WHEN adf_type = 'timestamp' THEN '"' + identified_column + '"' END, ','
                ) + ']', '[""]'
            ) AS [ColumnsToCastBackToTimestamp]
        FROM base_ruleset_filter AS brf
        WHERE treat_as_string = 1
    )

    -- Combine all parameters
    SELECT
        g.[FieldAlgorithmAssignments],
        g.[ColumnsToMask],
        g.[DataFactoryTypeMapping],
        g.[NumberOfBatches],
        g.[TrimLengths],
        df.[DateFormatAssignments],
        a.[ColumnsToCastAsStrings],
        a.[ColumnsToCastBackToBinary],
        a.[ColumnsToCastBackToBoolean],
        a.[ColumnsToCastBackToDate],
        a.[ColumnsToCastBackToDouble],
        a.[ColumnsToCastBackToFloat],
        a.[ColumnsToCastBackToInteger],
        a.[ColumnsToCastBackToLong],
        a.[ColumnsToCastBackToTimestamp],
        @StoredProcedureVersion AS [StoredProcedureVersion]
    FROM generate_mask_parameters AS g,
        date_format_parameters AS df,
        type_casting_parameters AS a;
END
GO
