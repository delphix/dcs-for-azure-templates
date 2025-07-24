/*
 * generate_masking_parameters - Generates masking parameters for Azure Data Factory dataflows.
 * 
 * This stored procedure replaces the ADF dataflows generating masking parameters
 * for improved performance and maintainability. It generates all necessary parameters for masking
 * a specific table using Delphix Compliance Services APIs within Azure Data Factory pipelines.
 *
 * PARAMETERS:
 * @source_database        - Source database name
 * @source_schema    - Source schema name  
 * @source_table     - Source table name
 * @column_width_estimate - Estimated column width for batch calculations (default: 1000)
 * @filter_key       - Optional filter key for conditional masking (default: '')
 * @dataset          - Dataset type identifier (default: 'AZURESQL')
 *
 * OPERATING MODES:
 * 1. Standard Mode (@filter_key = ''): 
 *    - Generates standard masking parameters for all columns with assigned algorithms
 *    - Uses unconditional algorithms and default date formats
 *    
 * 2. Conditional Mode (@filter_key provided):
 *    - Applies conditional masking based on key column values
 *    - Uses alias-specific algorithms and date formats
 *    - Filters rules to match the specified condition alias
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
 * - Supports treat_as_string flag for type casting edge cases
 * - Optimized for maintainability: Removed unused CTEs, simplified conditional logic, extracted batch calculations
 */
CREATE OR ALTER PROCEDURE generate_masking_parameters
    @source_database NVARCHAR(128),
    @source_schema NVARCHAR(128),
    @source_table NVARCHAR(128),
    @dataset NVARCHAR(128),
    @column_width_estimate INT = 1000,
    @filter_key NVARCHAR(128) = ''  -- Optional filter key for conditional masking
AS
BEGIN
    SET NOCOUNT ON;

    /*
     * base_ruleset_filter - Filters discovered_ruleset to target table and adds ADF type mappings.
     * Applies WHERE conditions and joins with adf_type_mapping for complete base dataset.
     */
    WITH base_ruleset_filter AS (
        SELECT 
            r.*,
            t.adf_type
        FROM discovered_ruleset r
        INNER JOIN adf_type_mapping t
            ON r.identified_column_type = t.dataset_type 
            AND r.dataset = t.dataset
        WHERE r.dataset = @dataset
          AND r.specified_database = @source_database
          AND r.specified_schema = @source_schema
          AND r.identified_table = @source_table
          AND r.assigned_algorithm IS NOT NULL
          AND r.assigned_algorithm <> ''
    ),
    /*
     * ruleset_computed - Adds computed columns for encoding and metadata extraction.
     * Extracts date_format and treat_as_string settings from algorithm_metadata JSON.
     * Creates hex-encoded column names to avoid ADF pipeline issues with special characters.
     */
    ruleset_computed AS (
        SELECT 
            r.*,
            CONCAT('x', CONVERT(varchar(max), CONVERT(varbinary, r.identified_column), 2)) AS encoded_column_name,
            JSON_VALUE(r.algorithm_metadata, '$.date_format') AS date_format,
            CAST(JSON_VALUE(r.algorithm_metadata, '$.treat_as_string') AS BIT) AS treat_as_string
        FROM base_ruleset_filter r
    ),
    /*
     * conditional_algorithm_extraction - Extracts conditional algorithms when filter key matches.
     * Streamlined conditional algorithm parsing with single-purpose logic.
     */
    conditional_algorithm_extraction AS (
        SELECT 
            r.*,
            c.algorithm AS conditional_algorithm
        FROM ruleset_computed r
        CROSS APPLY OPENJSON(r.assigned_algorithm)
        WITH (
            key_column NVARCHAR(255) '$.key_column',
            conditions NVARCHAR(MAX) '$.conditions' AS JSON
        ) x
        CROSS APPLY OPENJSON(x.conditions)
        WITH (
            alias NVARCHAR(255) '$.alias',
            algorithm NVARCHAR(255) '$.algorithm'
        ) c
        WHERE ISJSON(r.assigned_algorithm) = 1
          AND JSON_VALUE(r.assigned_algorithm, '$.key_column') IS NOT NULL
          AND c.alias = @filter_key
          AND @filter_key <> ''
    ),
    /*
     * standard_algorithm_handling - Handles standard (non-conditional) algorithms.
     * Processes simple algorithm assignments and non-conditional scenarios.
     */
    standard_algorithm_handling AS (
        SELECT 
            r.*,
            r.assigned_algorithm AS resolved_algorithm
        FROM ruleset_computed r
        WHERE ISJSON(r.assigned_algorithm) = 0
           OR @filter_key = ''
    ),
    /*
     * algorithm_resolution - Combines conditional and standard algorithms into final result.
     * Uses clear data flow pattern with UNION ALL for different algorithm sources.
     */
    algorithm_resolution AS (
        SELECT 
            dataset, specified_database, specified_schema, identified_table,
            identified_column, identified_column_type, identified_column_max_length,
            row_count, ordinal_position, encoded_column_name,
            conditional_algorithm AS assigned_algorithm
        FROM conditional_algorithm_extraction
        
        UNION ALL
        
        SELECT 
            dataset, specified_database, specified_schema, identified_table,
            identified_column, identified_column_type, identified_column_max_length,
            row_count, ordinal_position, encoded_column_name,
            resolved_algorithm AS assigned_algorithm
        FROM standard_algorithm_handling
        WHERE resolved_algorithm IS NOT NULL
          AND resolved_algorithm <> ''
          AND ISJSON(resolved_algorithm) = 0
    ),
    /*
     * ruleset_with_types - Combines resolved rules with ADF type mappings and column parameters.
     * Calculates column width estimates and preserves treat_as_string settings.
     */
    ruleset_with_types AS (
        SELECT 
            ar.*,
            f.adf_type,
            CASE 
                WHEN ar.identified_column_max_length > 0 THEN ar.identified_column_max_length + 4
                ELSE @column_width_estimate
            END AS column_width_estimate,
            f.treat_as_string as treat_as_string
        FROM algorithm_resolution ar
        INNER JOIN ruleset_computed f
            ON ar.identified_column = f.identified_column
    ),
    /*
     * generate_mask_parameters - Creates core masking parameters and calculates batch count.
     * Generates JSON structures required by ADF: FieldAlgorithmAssignments, ColumnsToMask,
     * DataFactoryTypeMapping, NumberOfBatches (minimum 1), and TrimLengths.
     */
    generate_mask_parameters AS (
        SELECT
            -- JSON object mapping column name to assigned algorithm
            '{' + STRING_AGG(
                '"' + LOWER(encoded_column_name) + '":"' + assigned_algorithm + '"',
                ','
            ) + '}' AS FieldAlgorithmAssignments,
            -- List of columns to mask as JSON array
            JSON_QUERY('[' + STRING_AGG('"' + identified_column + '"', ',') + ']') AS ColumnsToMask,
            -- Data factory type mapping string for parsing API response
            (
                '''' + 
                '(timestamp as date, status as string, message as string, trace_id as string, ' + 
                'items as (DELPHIX_COMPLIANCE_SERVICE_BATCH_ID as long, ' +
                STRING_AGG(
                    LOWER(CONCAT(
                        encoded_column_name, 
                        ' as ', 
                        CASE 
                            WHEN treat_as_string = 1 THEN 'string'
                            ELSE adf_type
                        END
                    )), 
                    ', '
                ) + 
                ')[])' + '''' 
            ) AS DataFactoryTypeMapping,
            -- Calculate optimal batch count with minimum of 1
            CASE 
                WHEN CEILING((MAX(row_count) * (SUM(column_width_estimate) + LOG10(MAX(row_count)) + 1)) / (2000000 * 0.9)) < 1 
                THEN 1 
                ELSE CEILING((MAX(row_count) * (SUM(column_width_estimate) + LOG10(MAX(row_count)) + 1)) / (2000000 * 0.9))
            END AS NumberOfBatches,
            -- Array of column lengths for trimming
            '[' + STRING_AGG(CAST(identified_column_max_length AS NVARCHAR(10)), ',') + ']' AS TrimLengths
        FROM ruleset_with_types
    ),
    /*
     * conditional_date_formats - Extracts conditional date formats when filter key is specified.
     * Separated conditional date format extraction for cleaner logic.
     */
    conditional_date_formats AS (
        SELECT
            f.identified_column,
            f.encoded_column_name,
            c.date_format AS conditional_date_format
        FROM ruleset_computed f
        CROSS APPLY OPENJSON(f.algorithm_metadata, '$.conditions')
        WITH (
            alias NVARCHAR(255) '$.alias',
            date_format NVARCHAR(255) '$.date_format'
        ) AS c
        WHERE @filter_key <> ''
          AND f.algorithm_metadata IS NOT NULL
          AND c.alias = @filter_key
          AND c.date_format IS NOT NULL
    ),
    /*
     * date_format_resolution - Combines conditional and standard date formats with clear precedence.
     * Simplified logic using clear conditional vs standard separation.
     */
    date_format_resolution AS (
        SELECT
            f.identified_column,
            f.encoded_column_name,
            COALESCE(cdf.conditional_date_format, f.date_format) AS final_date_format
        FROM ruleset_computed f
        LEFT JOIN conditional_date_formats cdf 
            ON f.identified_column = cdf.identified_column
        WHERE f.date_format IS NOT NULL 
           OR cdf.conditional_date_format IS NOT NULL
    ),
    /*
     * string_casting_with_adf_type - Identifies columns requiring string casting and their target ADF types.
     * Used to generate ColumnsToCastBackTo* arrays for proper type conversion after masking.
     */
    string_casting_with_adf_type AS (
        SELECT
            f.*
        FROM ruleset_computed f
        WHERE f.treat_as_string = 1
    ),
    -- Create JSON mapping for date format assignments
    generate_date_format_assignments AS (
        SELECT
            COALESCE(
                '{' + STRING_AGG(
                    '"' + LOWER(dfr.encoded_column_name) + '":"' + dfr.final_date_format + '"',
                    ','
                ) + '}', 
                '{}'
            ) AS DateFormatAssignments
        FROM date_format_resolution dfr
        WHERE dfr.final_date_format IS NOT NULL
    ),
    -- Aggregate casting parameters and date format assignments
    aggregate_columns_to_cast_back_parameters AS (
        SELECT
            -- Get DateFormatAssignments as a scalar subquery
            (SELECT DateFormatAssignments FROM generate_date_format_assignments) AS DateFormatAssignments,
            COALESCE(NULLIF('[' + STRING_AGG('"' + s.identified_column + '"', ',') + ']', '[]'), '[""]') AS ColumnsToCastAsStrings,
            COALESCE(NULLIF('[' + STRING_AGG(CASE WHEN s.adf_type = 'binary' THEN '"' + s.identified_column + '"' END, ',') + ']', '[]'), '[""]') AS ColumnsToCastBackToBinary,
            COALESCE(NULLIF('[' + STRING_AGG(CASE WHEN s.adf_type = 'boolean' THEN '"' + s.identified_column + '"' END, ',') + ']', '[]'), '[""]') AS ColumnsToCastBackToBoolean,
            COALESCE(NULLIF('[' + STRING_AGG(CASE WHEN s.adf_type = 'date' THEN '"' + s.identified_column + '"' END, ',') + ']', '[]'), '[""]') AS ColumnsToCastBackToDate,
            COALESCE(NULLIF('[' + STRING_AGG(CASE WHEN s.adf_type = 'double' THEN '"' + s.identified_column + '"' END, ',') + ']', '[]'), '[""]') AS ColumnsToCastBackToDouble,
            COALESCE(NULLIF('[' + STRING_AGG(CASE WHEN s.adf_type = 'float' THEN '"' + s.identified_column + '"' END, ',') + ']', '[]'), '[""]') AS ColumnsToCastBackToFloat,
            COALESCE(NULLIF('[' + STRING_AGG(CASE WHEN s.adf_type = 'integer' THEN '"' + s.identified_column + '"' END, ',') + ']', '[]'), '[""]') AS ColumnsToCastBackToInteger,
            COALESCE(NULLIF('[' + STRING_AGG(CASE WHEN s.adf_type = 'long' THEN '"' + s.identified_column + '"' END, ',') + ']', '[]'), '[""]') AS ColumnsToCastBackToLong,
            COALESCE(NULLIF('[' + STRING_AGG(CASE WHEN s.adf_type = 'timestamp' THEN '"' + s.identified_column + '"' END, ',') + ']', '[]'), '[""]') AS ColumnsToCastBackToTimestamp
        FROM string_casting_with_adf_type s
    ),
    -- Consolidate all masking parameters with fallback behavior
    all_masking_parameters AS (
        SELECT
            g.*,
            a.*
        FROM generate_mask_parameters g
        CROSS JOIN aggregate_columns_to_cast_back_parameters a
        
        UNION ALL

        SELECT
            '{}' AS FieldAlgorithmAssignments,
            '[]' AS ColumnsToMask,
            '''(timestamp as date, status as string, message as string, trace_id as string, items as (DELPHIX_COMPLIANCE_SERVICE_BATCH_ID as long)[])''' AS DataFactoryTypeMapping,
            1 AS NumberOfBatches,
            COALESCE(
                (
                    SELECT '[' + STRING_AGG('-1', ',') + ']'
                    FROM (
                        SELECT identified_column
                        FROM ruleset_computed
                    ) AS cols
                ),
                '[]'
            ) AS TrimLengths,
            '{}' AS DateFormatAssignments,
            COALESCE(
                (
                    SELECT '[' + STRING_AGG('"' + f.identified_column + '"', ',') + ']'
                    FROM string_casting_with_adf_type f
                ), '[]'
            ) AS ColumnsToCastAsStrings,
            COALESCE(
                (
                    SELECT '[' + STRING_AGG('"' + f.identified_column + '"', ',')
                    FROM string_casting_with_adf_type f
                    WHERE f.adf_type = 'binary'
                ) + ']', '[]'
            ) AS ColumnsToCastBackToBinary,
            COALESCE(
                (
                    SELECT '[' + STRING_AGG('"' + f.identified_column + '"', ',')
                    FROM string_casting_with_adf_type f
                    WHERE f.adf_type = 'boolean'
                ) + ']', '[]'
            ) AS ColumnsToCastBackToBoolean,
            COALESCE(
                (
                    SELECT '[' + STRING_AGG('"' + f.identified_column + '"', ',')
                    FROM string_casting_with_adf_type f
                    WHERE f.adf_type = 'date'
                ) + ']', '[]'
            ) AS ColumnsToCastBackToDate,
            COALESCE(
                (
                    SELECT '[' + STRING_AGG('"' + f.identified_column + '"', ',')
                    FROM string_casting_with_adf_type f
                    WHERE f.adf_type = 'double'
                ) + ']', '[]'
            ) AS ColumnsToCastBackToDouble,
            COALESCE(
                (
                    SELECT '[' + STRING_AGG('"' + f.identified_column + '"', ',')
                    FROM string_casting_with_adf_type f
                    WHERE f.adf_type = 'float'
                ) + ']', '[]'
            ) AS ColumnsToCastBackToFloat,
            COALESCE(
                (
                    SELECT '[' + STRING_AGG('"' + f.identified_column + '"', ',')
                    FROM string_casting_with_adf_type f
                    WHERE f.adf_type = 'integer'
                ) + ']', '[]'
            ) AS ColumnsToCastBackToInteger,
            COALESCE(
                (
                    SELECT '[' + STRING_AGG('"' + f.identified_column + '"', ',')
                    FROM string_casting_with_adf_type f
                    WHERE f.adf_type = 'long'
                ) + ']', '[]'
            ) AS ColumnsToCastBackToLong,
            COALESCE(
                (
                    SELECT '[' + STRING_AGG('"' + f.identified_column + '"', ',')
                    FROM string_casting_with_adf_type f
                    WHERE f.adf_type = 'timestamp'
                ) + ']', '[]'
            ) AS ColumnsToCastBackToTimestamp
        WHERE NOT EXISTS (SELECT 1 FROM generate_mask_parameters)
    )
    SELECT 
        FieldAlgorithmAssignments,
        ColumnsToMask,
        DataFactoryTypeMapping,
        NumberOfBatches,
        TrimLengths,
        DateFormatAssignments,
        StoredProcedureVersion = 'V2025.07.22.0',
        ColumnsToCastAsStrings,
        ColumnsToCastBackToBinary,
        ColumnsToCastBackToBoolean,
        ColumnsToCastBackToDate,
        ColumnsToCastBackToDouble,
        ColumnsToCastBackToFloat,
        ColumnsToCastBackToInteger,
        ColumnsToCastBackToLong,
        ColumnsToCastBackToTimestamp
    FROM all_masking_parameters;
END
GO