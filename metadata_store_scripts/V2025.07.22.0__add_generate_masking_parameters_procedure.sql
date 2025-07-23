/*
 * generate_masking_parameters - Generates masking parameters for Azure Data Factory dataflows.
 * 
 * This stored procedure replaces the ADF dataflows generating masking parameters
 * for improved performance and maintainability. It generates all necessary parameters for masking
 * a specific table using Delphix Compliance Services APIs within Azure Data Factory pipelines.
 *
 * PARAMETERS:
 * @DF_SOURCE_DB        - Source database name
 * @DF_SOURCE_SCHEMA    - Source schema name  
 * @DF_SOURCE_TABLE     - Source table name
 * @DF_COLUMN_WIDTH_ESTIMATE - Estimated column width for batch calculations (default: 1000)
 * @DF_FILTER_KEY       - Optional filter key for conditional masking (default: '')
 * @DF_DATASET          - Dataset type identifier (default: 'AZURESQL')
 *
 * OPERATING MODES:
 * 1. Standard Mode (@DF_FILTER_KEY = ''): 
 *    - Generates standard masking parameters for all columns with assigned algorithms
 *    - Uses unconditional algorithms and default date formats
 *    
 * 2. Conditional Mode (@DF_FILTER_KEY provided):
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
 * - Uses hex-encoded column names to avoid ADF pipeline issues with special or space-containing names
 * - Supports treat_as_string flag for type casting edge cases
 */
CREATE OR ALTER PROCEDURE generate_masking_parameters
    @DF_SOURCE_DB NVARCHAR(128),
    @DF_SOURCE_SCHEMA NVARCHAR(128),
    @DF_SOURCE_TABLE NVARCHAR(128),
    @DF_DATASET NVARCHAR(128),
    @DF_COLUMN_WIDTH_ESTIMATE INT = 1000,
    /*
     * @DF_FILTER_KEY (optional) - Filter key for conditional algorithms.
     *   - Matches the alias in the conditional algorithm JSON structure.
     *   - If provided, the procedure filters the ruleset based on this key.
     *   - If empty, standard masking parameters are generated without filtering.
     */
    @DF_FILTER_KEY NVARCHAR(128) = ''
AS
BEGIN
    SET NOCOUNT ON;

    /*
     * FilterToSingleTable - Filters the discovered_ruleset table to only include rows for the specific table being masked.
     * Applies filters for dataset, database, schema, table name, and ensures assigned_algorithm is not null or empty.
     * Also extracts date_format and treat_as_string settings from algorithm_metadata JSON.
     * This is the starting point that narrows down all masking rules to just those applicable to the target table.
     */
    WITH FilterToSingleTable AS (
        SELECT 
            r.*,
            CONCAT('x', CONVERT(varchar(max), CONVERT(varbinary, r.identified_column), 2)) AS encoded_column_name,
            JSON_VALUE(r.algorithm_metadata, '$.date_format') AS date_format,
            CAST(JSON_VALUE(r.algorithm_metadata, '$.treat_as_string') AS BIT) AS treat_as_string
        FROM discovered_ruleset r
        WHERE r.dataset = @DF_DATASET
          AND r.specified_database = @DF_SOURCE_DB
          AND r.specified_schema = @DF_SOURCE_SCHEMA
          AND r.identified_table = @DF_SOURCE_TABLE
          AND r.assigned_algorithm IS NOT NULL
          AND r.assigned_algorithm <> ''
    ),
    /*
     * FilterToDataSourceType - Filters the adf_type_mapping table to only include mappings for the current dataset.
     * This provides the mapping between database column types and their corresponding ADF data types
     * needed for proper data flow processing and API response parsing.
     */
    FilterToDataSourceType AS (
        SELECT 
            t.*
        FROM adf_type_mapping t
        WHERE t.dataset = @DF_DATASET
    ),
    /*
     * KeyColumn - Identifies and flattens key column definitions for conditional masking.
     * These columns have assigned_algorithm values as JSON arrays with alias/condition pairs.
     * Each alias defines a filter condition that determines which rows get specific algorithms.
     * The key column itself cannot be masked - it controls how other columns are masked.
     * Expands conditions into individual rows for matching against the filter key.
     */
    KeyColumn AS (
        SELECT 
            fts.*,
            c.[alias],
            c.[condition]
        FROM FilterToSingleTable fts
        CROSS APPLY OPENJSON(fts.assigned_algorithm, '$')
        WITH (
            [alias] NVARCHAR(255) '$.alias',
            [condition] NVARCHAR(MAX) '$.condition'
        ) AS c
        WHERE ISJSON(fts.assigned_algorithm) = 1
          AND JSON_VALUE(fts.assigned_algorithm, '$[0].alias') IS NOT NULL
    ),
    /*
     * ConditionalAlgorithm - Identifies and flattens conditional algorithm assignments.
     * These columns reference a key column and specify which algorithm to apply for each alias/condition.
     * Contains JSON objects with key_column and conditions properties for conditional masking.
     * Expands conditions into individual rows with alias/algorithm pairs for matching with key conditions.
     */
    ConditionalAlgorithm AS (
        SELECT 
            fts.*,
            JSON_VALUE(fts.assigned_algorithm, '$.key_column') AS key_column,
            c.[alias],
            c.[algorithm]
        FROM FilterToSingleTable fts
        CROSS APPLY OPENJSON(JSON_QUERY(fts.assigned_algorithm, '$.conditions'))
        WITH (
            [alias] NVARCHAR(255) '$.alias',
            [algorithm] NVARCHAR(255) '$.algorithm'
        ) AS c
        WHERE ISJSON(fts.assigned_algorithm) = 1 
          AND JSON_VALUE(fts.assigned_algorithm, '$.key_column') IS NOT NULL
          AND JSON_QUERY(fts.assigned_algorithm, '$.conditions') IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM OPENJSON(JSON_QUERY(fts.assigned_algorithm, '$.conditions'))
          )
    ),
    /*
     * JoinConditionalAlgorithms - Links key conditions with their algorithm assignments.
     * Matches alias/condition pairs with alias/algorithm pairs to establish complete conditional masking rules.
     */
    JoinConditionalAlgorithms AS (
        SELECT 
            kc.*,
            ca.key_column,
            ca.[algorithm]
        FROM KeyColumn kc
        INNER JOIN ConditionalAlgorithm ca 
            ON kc.identified_column = ca.key_column
            AND kc.[alias] = ca.[alias]
    ),
    /*
     * ConditionalFiltering - Resolves the final algorithm for each column and filters to valid rules.
     * Selects conditional algorithms when filter key matches, otherwise uses standard algorithms.
     * Filters out null/empty algorithms and ensures only resolved (non-JSON) assignments remain.
     */
    ConditionalFiltering AS (
        SELECT
            resolved.*
        FROM (
            SELECT
                f.dataset,
                f.specified_database,
                f.specified_schema,
                f.identified_table,
                f.identified_column,
                f.identified_column_type,
                f.identified_column_max_length,
                f.row_count,
                f.ordinal_position,
                CASE 
                    -- Replace algorithm from conditional settings when key matches and format is appropriate
                    WHEN @DF_FILTER_KEY <> '' AND f.assigned_algorithm LIKE '{%}' THEN 
                        (
                            SELECT TOP 1 c.algorithm 
                            FROM OPENJSON(f.assigned_algorithm)
                            WITH (
                                key_column NVARCHAR(255) '$.key_column',
                                conditions NVARCHAR(MAX) '$.conditions' AS JSON
                            ) x
                            CROSS APPLY OPENJSON(x.conditions)
                            WITH (
                                alias NVARCHAR(255) '$.alias',
                                algorithm NVARCHAR(255) '$.algorithm'
                            ) c
                            WHERE c.alias = @DF_FILTER_KEY
                        )
                    -- For non-conditional algorithms or when no filter key is provided, use the original
                    ELSE f.assigned_algorithm 
                END AS assigned_algorithm
            FROM FilterToSingleTable f
        ) resolved
        WHERE resolved.assigned_algorithm IS NOT NULL
          AND resolved.assigned_algorithm <> ''
          AND ISJSON(resolved.assigned_algorithm) = 0
    ),
    /*
     * RulesetWithTypes - Adds ADF type mapping, column width estimates, and treat_as_string flags.
     * Joins resolved rules with type information and computes additional parameters needed for ADF processing.
     */
    RulesetWithTypes AS (
        SELECT 
            cf.*,
            t.adf_type,
            CASE 
                WHEN cf.identified_column_max_length > 0 THEN cf.identified_column_max_length + 4
                ELSE @DF_COLUMN_WIDTH_ESTIMATE
            END AS column_width_estimate,
            f.treat_as_string as treat_as_string
        FROM ConditionalFiltering cf
        INNER JOIN FilterToDataSourceType t
            ON cf.identified_column_type = t.dataset_type
        LEFT JOIN FilterToSingleTable f
            ON cf.identified_column = f.identified_column
    ),
    /*
     * GenerateMaskParameters - Creates core masking parameters for ADF dataflows with batch count validation.
     * Aggregates column rules into JSON structures: FieldAlgorithmAssignments, ColumnsToMask, 
     * DataFactoryTypeMapping, NumberOfBatches (with minimum of 1), and TrimLengths.
     */
    GenerateMaskParameters AS (
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
            -- Apply minimum batch count of 1 directly to calculation
            CASE 
                WHEN CEILING((MAX(row_count) * (SUM(column_width_estimate) + LOG10(MAX(row_count)) + 1)) / (2000000 * 0.9)) < 1 
                THEN 1 
                ELSE CEILING((MAX(row_count) * (SUM(column_width_estimate) + LOG10(MAX(row_count)) + 1)) / (2000000 * 0.9))
            END AS NumberOfBatches,
            -- Array of column lengths for trimming
            '[' + STRING_AGG(CAST(identified_column_max_length AS NVARCHAR(10)), ',') + ']' AS TrimLengths
        FROM RulesetWithTypes
    ),
    /*
     * DateFormatResolution - Resolves final date format for each column.
     * Prioritizes conditional formats (based on filter key) over standard formats from algorithm_metadata.
     */
    DateFormatResolution AS (
        SELECT
            f.identified_column,
            f.encoded_column_name,  -- Already available from FilterToSingleTable
            COALESCE(
                -- Get conditional date format if filter key matches
                CASE WHEN @DF_FILTER_KEY <> '' THEN 
                    (
                        SELECT TOP 1 c.[date_format]
                        FROM OPENJSON(f.algorithm_metadata, '$.conditions')
                        WITH (
                            [alias] NVARCHAR(255) '$.alias',
                            [date_format] NVARCHAR(255) '$.date_format'
                        ) AS c
                        WHERE c.[alias] = @DF_FILTER_KEY
                    )
                ELSE NULL END,
                -- Fall back to standard date format
                f.date_format
            ) AS final_date_format
        FROM FilterToSingleTable f
        WHERE f.date_format IS NOT NULL OR f.algorithm_metadata IS NOT NULL
    ),
    /*
     * GenerateDateFormatAssignments - Creates JSON mapping using pre-computed encoded column names.
     * Returns empty JSON object {} if no date formats are found.
     */
    GenerateDateFormatAssignments AS (
        SELECT
            COALESCE(
                '{' + STRING_AGG(
                    '"' + LOWER(dfr.encoded_column_name) + '":"' + dfr.final_date_format + '"',
                    ','
                ) + '}', 
                '{}'
            ) AS DateFormatAssignments
        FROM DateFormatResolution dfr
        WHERE dfr.final_date_format IS NOT NULL
    ),
    /*
     * StringCastingWithAdfType - Adds ADF type information to string casting columns.
     * Determines what type columns should be cast back to after string processing.
     */
    StringCastingWithAdfType AS (
        SELECT
            f.*,
            t.adf_type
        FROM FilterToSingleTable f
        INNER JOIN FilterToDataSourceType t
            ON f.identified_column_type = t.dataset_type
        WHERE f.treat_as_string = 1
    ),
    /*
     * AggregateColumnsToCastBackParameters - Creates ColumnsToCastBackTo* arrays for all ADF types.
     * Also creates ColumnsToCastAsStrings array from columns with treat_as_string flag.
     * Combines with DateFormatAssignments and ensures all parameters have valid default values.
     * Generates JSON arrays grouped by target ADF type (binary, boolean, date, etc.).
     */
    AggregateColumnsToCastBackParameters AS (
        SELECT
            d.DateFormatAssignments,
            COALESCE('[' + STRING_AGG('"' + s.identified_column + '"', ',') + ']', '[]') AS ColumnsToCastAsStrings,
            COALESCE('[' + STRING_AGG(CASE WHEN s.adf_type = 'binary' THEN '"' + s.identified_column + '"' END, ',') + ']', '[]') AS ColumnsToCastBackToBinary,
            COALESCE('[' + STRING_AGG(CASE WHEN s.adf_type = 'boolean' THEN '"' + s.identified_column + '"' END, ',') + ']', '[]') AS ColumnsToCastBackToBoolean,
            COALESCE('[' + STRING_AGG(CASE WHEN s.adf_type = 'date' THEN '"' + s.identified_column + '"' END, ',') + ']', '[]') AS ColumnsToCastBackToDate,
            COALESCE('[' + STRING_AGG(CASE WHEN s.adf_type = 'double' THEN '"' + s.identified_column + '"' END, ',') + ']', '[]') AS ColumnsToCastBackToDouble,
            COALESCE('[' + STRING_AGG(CASE WHEN s.adf_type = 'float' THEN '"' + s.identified_column + '"' END, ',') + ']', '[]') AS ColumnsToCastBackToFloat,
            COALESCE('[' + STRING_AGG(CASE WHEN s.adf_type = 'integer' THEN '"' + s.identified_column + '"' END, ',') + ']', '[]') AS ColumnsToCastBackToInteger,
            COALESCE('[' + STRING_AGG(CASE WHEN s.adf_type = 'long' THEN '"' + s.identified_column + '"' END, ',') + ']', '[]') AS ColumnsToCastBackToLong,
            COALESCE('[' + STRING_AGG(CASE WHEN s.adf_type = 'timestamp' THEN '"' + s.identified_column + '"' END, ',') + ']', '[]') AS ColumnsToCastBackToTimestamp
        FROM GenerateDateFormatAssignments d
        CROSS JOIN StringCastingWithAdfType s
    ),
    /*
     * AllMaskingParameters - Consolidates all masking parameters into the final result.
     * Combines core parameters with casting defaults and adjusted batch numbers.
     */
    AllMaskingParameters AS (
        SELECT
            g.*,
            a.*
        FROM GenerateMaskParameters g
        CROSS JOIN AggregateColumnsToCastBackParameters a
    ),
    /*
     * FinalMaskingParameters - Ensures procedure always returns exactly one record.
     * Returns actual parameters if they exist, otherwise returns default/empty values.
     * Prevents lookup activity failures when no masking rules are found.
     */
    FinalMaskingParameters AS (
        SELECT 
            amp.*
        FROM AllMaskingParameters amp
        
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
                        FROM FilterToSingleTable
                    ) AS cols
                ),
                '[]'
            ) AS TrimLengths,
            '{}' AS DateFormatAssignments,
            COALESCE(
                (
                    SELECT '[' + STRING_AGG('"' + f.identified_column + '"', ',') + ']'
                    FROM StringCastingWithAdfType f
                ), '[]'
            ) AS ColumnsToCastAsStrings,
            COALESCE(
                (
                    SELECT '[' + STRING_AGG('"' + f.identified_column + '"', ',')
                    FROM StringCastingWithAdfType f
                    WHERE f.adf_type = 'binary'
                ) + ']', '[]'
            ) AS ColumnsToCastBackToBinary,
            COALESCE(
                (
                    SELECT '[' + STRING_AGG('"' + f.identified_column + '"', ',')
                    FROM StringCastingWithAdfType f
                    WHERE f.adf_type = 'boolean'
                ) + ']', '[]'
            ) AS ColumnsToCastBackToBoolean,
            COALESCE(
                (
                    SELECT '[' + STRING_AGG('"' + f.identified_column + '"', ',')
                    FROM StringCastingWithAdfType f
                    WHERE f.adf_type = 'date'
                ) + ']', '[]'
            ) AS ColumnsToCastBackToDate,
            COALESCE(
                (
                    SELECT '[' + STRING_AGG('"' + f.identified_column + '"', ',')
                    FROM StringCastingWithAdfType f
                    WHERE f.adf_type = 'double'
                ) + ']', '[]'
            ) AS ColumnsToCastBackToDouble,
            COALESCE(
                (
                    SELECT '[' + STRING_AGG('"' + f.identified_column + '"', ',')
                    FROM StringCastingWithAdfType f
                    WHERE f.adf_type = 'float'
                ) + ']', '[]'
            ) AS ColumnsToCastBackToFloat,
            COALESCE(
                (
                    SELECT '[' + STRING_AGG('"' + f.identified_column + '"', ',')
                    FROM StringCastingWithAdfType f
                    WHERE f.adf_type = 'integer'
                ) + ']', '[]'
            ) AS ColumnsToCastBackToInteger,
            COALESCE(
                (
                    SELECT '[' + STRING_AGG('"' + f.identified_column + '"', ',')
                    FROM StringCastingWithAdfType f
                    WHERE f.adf_type = 'long'
                ) + ']', '[]'
            ) AS ColumnsToCastBackToLong,
            COALESCE(
                (
                    SELECT '[' + STRING_AGG('"' + f.identified_column + '"', ',')
                    FROM StringCastingWithAdfType f
                    WHERE f.adf_type = 'timestamp'
                ) + ']', '[]'
            ) AS ColumnsToCastBackToTimestamp
        WHERE NOT EXISTS (SELECT 1 FROM AllMaskingParameters)
    )
    SELECT 
        FieldAlgorithmAssignments,
        ColumnsToMask,
        DataFactoryTypeMapping,
        NumberOfBatches,
        TrimLengths,
        DateFormatAssignments,
        -- Stored Procedure version for debugging    
        StoredProcedureVersion = 'V2025.07.22.0',
        -- ADF UI shows validation errors when empty arrays [] are used, so convert to [""] for compatibility
        CASE 
            WHEN ColumnsToCastAsStrings = '[]' 
            THEN '[""]' 
            ELSE ColumnsToCastAsStrings 
        END AS ColumnsToCastAsStrings,
        CASE 
            WHEN ColumnsToCastBackToBinary = '[]' 
            THEN '[""]' 
            ELSE ColumnsToCastBackToBinary 
        END AS ColumnsToCastBackToBinary,
        CASE 
            WHEN ColumnsToCastBackToBoolean = '[]' 
            THEN '[""]' 
            ELSE ColumnsToCastBackToBoolean 
        END AS ColumnsToCastBackToBoolean,
        CASE 
            WHEN ColumnsToCastBackToDate = '[]' 
            THEN '[""]' 
            ELSE ColumnsToCastBackToDate 
        END AS ColumnsToCastBackToDate,
        CASE 
            WHEN ColumnsToCastBackToDouble = '[]' 
            THEN '[""]' 
            ELSE ColumnsToCastBackToDouble 
        END AS ColumnsToCastBackToDouble,
        CASE 
            WHEN ColumnsToCastBackToFloat = '[]' 
            THEN '[""]' 
            ELSE ColumnsToCastBackToFloat 
        END AS ColumnsToCastBackToFloat,
        CASE 
            WHEN ColumnsToCastBackToInteger = '[]' 
            THEN '[""]' 
            ELSE ColumnsToCastBackToInteger 
        END AS ColumnsToCastBackToInteger,
        CASE 
            WHEN ColumnsToCastBackToLong = '[]' 
            THEN '[""]' 
            ELSE ColumnsToCastBackToLong 
        END AS ColumnsToCastBackToLong,
        CASE 
            WHEN ColumnsToCastBackToTimestamp = '[]' 
            THEN '[""]' 
            ELSE ColumnsToCastBackToTimestamp 
        END AS ColumnsToCastBackToTimestamp
    FROM FinalMaskingParameters;
END
GO
