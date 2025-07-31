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

    -- base_ruleset_filter - Filter to target table, add ADF type mappings
    WITH base_ruleset_filter AS (
        SELECT
            r.*,
            t.adf_type
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
    ),

    -- ruleset_computed - Add encoded column names and extract JSON metadata
    ruleset_computed AS (
        SELECT
            r.*,
            CONCAT(
                'x',
                CONVERT(
                    VARCHAR(MAX),
                    CONVERT(VARBINARY, r.identified_column),
                    2
                )
            ) AS encoded_column_name,
            JSON_VALUE(r.algorithm_metadata, '$.date_format') AS [date_format],
            CONVERT(BIT, JSON_VALUE(r.algorithm_metadata, '$.treat_as_string')) AS treat_as_string,
            CASE
                WHEN ISJSON(r.assigned_algorithm) = 1
                    THEN JSON_VALUE(r.assigned_algorithm, '$.key_column')
            END AS [key_column_name]
        FROM base_ruleset_filter AS r
    ),

    -- conditional_algorithm_extraction - Extract conditional algorithms for matching filter key
    conditional_algorithm_extraction AS (
        SELECT
            r.*,
            x.[key_column],
            x.[conditions],
            c.[condition_alias],
            c.[algorithm] AS resolved_algorithm
        FROM ruleset_computed AS r
        CROSS APPLY
            OPENJSON (r.assigned_algorithm)
            WITH (
                [key_column] NVARCHAR(255) '$.key_column',
                [conditions] NVARCHAR(MAX) '$.conditions' AS JSON
            ) AS x
        CROSS APPLY
            OPENJSON (x.[conditions])
            WITH (
                [condition_alias] NVARCHAR(255) '$.alias',
                [algorithm] NVARCHAR(255) '$.algorithm'
            ) AS c
        WHERE
            ISJSON(r.assigned_algorithm) = 1
            AND JSON_VALUE(r.assigned_algorithm, '$.key_column') IS NOT NULL
            AND c.[condition_alias] = @filter_alias
            AND @filter_alias <> ''
    ),

    -- standard_algorithm_handling - Handle non-conditional algorithms
    standard_algorithm_handling AS (
        SELECT
            r.*,
            r.assigned_algorithm AS resolved_algorithm
        FROM ruleset_computed AS r
        WHERE
            ISJSON(r.assigned_algorithm) = 0
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

    -- ruleset_with_types - Join with ADF types, calculate column width estimates
    ruleset_with_types AS (
        SELECT
            ar.*,
            f.adf_type,
            f.treat_as_string,
            CASE
                WHEN ar.identified_column_max_length > 0 THEN ar.identified_column_max_length + 4
                ELSE @column_width_estimate
            END AS column_width_estimate
        FROM algorithm_resolution AS ar
        INNER JOIN ruleset_computed AS f
            ON ar.identified_column = f.identified_column
        WHERE (
            f.[key_column_name] IS NULL
            OR f.identified_column <> f.[key_column_name]
        )
    ),

    -- generate_mask_parameters - Create core masking parameters with fallback defaults
    generate_mask_parameters AS (
        SELECT
            -- JSON mapping: encoded column name -> algorithm
            COALESCE(
                '{'
                + STRING_AGG(
                    '"' + LOWER(encoded_column_name) + '":"' + assigned_algorithm + '"',
                    ','
                )
                + '}',
                '{}'
            ) AS fieldalgorithmassignments,
            -- JSON array of column names to mask
            COALESCE(
                JSON_QUERY(
                    '['
                    + STRING_AGG('"' + identified_column + '"', ',')
                    + ']'
                ),
                '[]'
            ) AS columnstomask,
            -- ADF data flow expression for DCS masking API response parsing
            ''''
            + '(timestamp as date, status as string, message as string, trace_id as string, '
            + 'items as (DELPHIX_COMPLIANCE_SERVICE_BATCH_ID as long'
            + COALESCE(
                ', '
                + STRING_AGG(
                    LOWER(
                        CONCAT(
                            encoded_column_name,
                            ' as ',
                            CASE WHEN treat_as_string = 1 THEN 'string' ELSE adf_type END
                        )
                    ),
                    ', '
                ),
                ''
            )
            + ')[])'
            + '''' AS datafactorytypemapping,
            COALESCE(
                CASE
                    WHEN
                        CEILING(
                            MAX(row_count)
                            * (
                                SUM(column_width_estimate)
                                + LOG10(MAX(row_count))
                                + 1
                            )
                            / (2000000 * 0.9)
                        ) < 1
                        THEN 1
                    ELSE CEILING(
                        MAX(row_count)
                        * (
                            SUM(column_width_estimate)
                            + LOG10(MAX(row_count))
                            + 1
                        )
                        / (2000000 * 0.9)
                    )
                END, 1
            ) AS [NumberOfBatches],   -- Optimal batch count (minimum 1)
            COALESCE(
                '[' + STRING_AGG(
                    CONVERT(NVARCHAR(10), identified_column_max_length), ','
                ) + ']', '[]'
            ) AS [TrimLengths]   -- JSON array of column max lengths
        FROM ruleset_with_types
    ),

    -- conditional_date_formats - Extract conditional date formats for filter key
    conditional_date_formats AS (
        SELECT
            f.identified_column,
            f.encoded_column_name,
            c.[condition_alias],
            c.[date_format] AS conditional_date_format
        FROM ruleset_computed AS f
        CROSS APPLY
            OPENJSON (f.algorithm_metadata, '$.conditions')
            WITH (
                [condition_alias] NVARCHAR(255) '$.alias',
                [date_format] NVARCHAR(255) '$.date_format'
            ) AS c
        WHERE
            @filter_alias <> ''
            AND f.algorithm_metadata IS NOT NULL
            AND c.[condition_alias] = @filter_alias
            AND c.[date_format] IS NOT NULL
    ),

    -- date_format_resolution - Merge conditional and standard date formats
    date_format_resolution AS (
        SELECT
            f.identified_column,
            f.encoded_column_name,
            COALESCE(cdf.conditional_date_format, f.[date_format]) AS final_date_format
        FROM ruleset_computed AS f
        LEFT JOIN conditional_date_formats AS cdf
            ON f.identified_column = cdf.identified_column
        WHERE
            f.[date_format] IS NOT NULL
            OR cdf.conditional_date_format IS NOT NULL
    ),

    -- date_format_parameters - Create JSON mapping with fallback default
    date_format_parameters AS (
        SELECT
            COALESCE(
                '{' + STRING_AGG(
                    '"' + LOWER(dfr.encoded_column_name) + '":"' + dfr.final_date_format + '"',
                    ','
                ) + '}',
                '{}'
            ) AS [DateFormatAssignments]
        FROM date_format_resolution AS dfr
        WHERE dfr.final_date_format IS NOT NULL
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
        FROM ruleset_computed
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
