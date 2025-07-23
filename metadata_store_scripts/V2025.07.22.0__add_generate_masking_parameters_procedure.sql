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
     * This is the starting point that narrows down all masking rules to just those applicable to the target table.
     */
    WITH FilterToSingleTable AS (
        SELECT 
            r.*,
            CONCAT('x', CONVERT(varchar(max), CONVERT(varbinary, r.identified_column), 2)) AS encoded_column_name
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
            t.dataset,
            t.dataset_type,
            t.adf_type
        FROM adf_type_mapping t
        WHERE t.dataset = @DF_DATASET
    ),
    /*
     * KeyColumn - Identifies rows containing key column definitions for conditional masking.
     * These columns have assigned_algorithm values as JSON arrays with alias/condition pairs.
     * Each alias defines a filter condition that determines which rows get specific algorithms.
     * The key column itself cannot be masked - it controls how other columns are masked.
     */
    KeyColumn AS (
        SELECT fts.*
        FROM FilterToSingleTable fts
        WHERE ISJSON(fts.assigned_algorithm) = 1
          AND JSON_VALUE(fts.assigned_algorithm, '$[0].alias') IS NOT NULL
    ),
    /*
     * ConditionalAlgorithm - Identifies columns with conditional algorithm assignments.
     * These columns reference a key column and specify which algorithm to apply for each alias/condition.
     * Contains JSON objects with key_column and conditions properties for conditional masking.
     */
    ConditionalAlgorithm AS (
        SELECT fts.*
        FROM FilterToSingleTable fts
        WHERE ISJSON(fts.assigned_algorithm) = 1 
          AND JSON_VALUE(fts.assigned_algorithm, '$.key_column') IS NOT NULL
          AND JSON_QUERY(fts.assigned_algorithm, '$.conditions') IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM OPENJSON(JSON_QUERY(fts.assigned_algorithm, '$.conditions'))
          )
    ),
    /*
     * StandardAlgorithm - Identifies columns with simple, non-conditional algorithm assignments.
     * These contain string-based algorithm values applied directly to their columns.
     */
    StandardAlgorithm AS (
        SELECT fts.*
        FROM FilterToSingleTable fts
        WHERE ISJSON(fts.assigned_algorithm) = 0
    ),
    /*
     * ParseKeyColumn - Extracts the JSON array from key column definitions.
     * Prepares alias/condition pairs for individual processing.
     */
    ParseKeyColumn AS (
        SELECT 
            k.*,
            JSON_QUERY(k.assigned_algorithm, '$') AS conditions_set
        FROM KeyColumn k
    ),
    /*
     * FlattenKeyConditions - Expands key column conditions into individual rows.
     * Each row contains one alias/condition pair for matching against the filter key.
     */
    FlattenKeyConditions AS (
        SELECT 
            k.*,
            c.[alias],
            c.[condition]
        FROM ParseKeyColumn k
        CROSS APPLY OPENJSON(k.conditions_set)
        WITH (
            [alias] NVARCHAR(255) '$.alias',
            [condition] NVARCHAR(MAX) '$.condition'
        ) AS c
    ),
    /*
     * ParseAlgorithm - Extracts key_column and conditions from conditional algorithm JSON.
     * Prepares structured data for algorithm assignment processing.
     */
    ParseAlgorithm AS (
        SELECT
            ca.*,
            JSON_VALUE(ca.assigned_algorithm, '$.key_column') AS key_column,
            JSON_QUERY(ca.assigned_algorithm, '$.conditions') AS conditions
        FROM ConditionalAlgorithm ca
    ),
    /*
     * FlattenAlgorithmAssignments - Expands conditional algorithm assignments into individual rows.
     * Each row contains one alias/algorithm pair for matching with key conditions.
     */
    FlattenAlgorithmAssignments AS (
        SELECT
            pa.*,
            c.[alias],
            c.[algorithm]
        FROM ParseAlgorithm pa
        CROSS APPLY OPENJSON(pa.conditions)
        WITH (
            [alias] NVARCHAR(255) '$.alias',
            [algorithm] NVARCHAR(255) '$.algorithm'
        ) AS c
    ),
    /*
     * JoinConditionalAlgorithms - Links key conditions with their algorithm assignments.
     * Matches alias/condition pairs with alias/algorithm pairs to establish complete conditional masking rules.
     */
    JoinConditionalAlgorithms AS (
        SELECT 
            fkc.*,
            fa.key_column,
            fa.[algorithm]
        FROM FlattenKeyConditions fkc
        INNER JOIN FlattenAlgorithmAssignments fa 
            ON fkc.identified_column = fa.key_column
            AND fkc.[alias] = fa.[alias]
    ),
    /*
     * FilterToConditionKey - Selects conditional rules matching the specified filter key.
     * Filters to only the alias/algorithm pairs that apply to the current masking context.
     */
    FilterToConditionKey AS (
        SELECT
            fa.dataset,
            fa.specified_database,
            fa.specified_schema,
            fa.identified_table,
            fa.identified_column,
            fa.identified_column_type,
            fa.identified_column_max_length,
            fa.row_count,
            fa.ordinal_position,
            fa.[algorithm] AS assigned_algorithm
        FROM JoinConditionalAlgorithms jca
        INNER JOIN FlattenAlgorithmAssignments fa 
            ON jca.key_column = fa.key_column 
            AND jca.[alias] = fa.[alias]
        WHERE jca.[alias] = @DF_FILTER_KEY
          AND fa.[algorithm] IS NOT NULL 
          AND fa.[algorithm] <> ''
    ),
    /*
     * ConditionalFiltering - Resolves the final algorithm for each column.
     * Selects conditional algorithms when filter key matches, otherwise uses standard algorithms.
     */
    ConditionalFiltering AS (
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
    ),
    /*
     * UnionAllRules - Consolidates resolved masking rules ready for processing.
     * Filters out null/empty algorithms and ensures only resolved (non-JSON) assignments remain.
     */
    UnionAllRules AS (
        SELECT cf.* 
        FROM ConditionalFiltering cf
        WHERE cf.assigned_algorithm IS NOT NULL
          AND cf.assigned_algorithm <> ''
          AND ISJSON(cf.assigned_algorithm) = 0
    ),
    /*
     * RulesetWithTypes - Adds ADF type mapping and encoded column names to masking rules.
     * Joins resolved rules with type information needed for ADF processing.
     */
    RulesetWithTypes AS (
        SELECT 
            u.*,
            t.adf_type
        FROM UnionAllRules u
        INNER JOIN FilterToDataSourceType t
            ON u.identified_column_type = t.dataset_type
    ),
    /*
     * ParseMetadata - Extracts date_format and treat_as_string settings from algorithm_metadata.
     * Parses JSON configuration flags that control column processing during masking.
     */
    ParseMetadata AS (
        SELECT
            f.*,
            JSON_VALUE(f.algorithm_metadata, '$.date_format') AS date_format,
            CAST(JSON_VALUE(f.algorithm_metadata, '$.treat_as_string') AS BIT) AS treat_as_string
        FROM FilterToSingleTable f
    ),
    /*
     * RulesetWithAlgorithmTypeMapping - Prepares rules for final parameter aggregation.
     * Adds column width estimates and treat_as_string flags.
     */
    RulesetWithAlgorithmTypeMapping AS (
        SELECT
            r.*,
            CASE 
                WHEN r.identified_column_max_length > 0 THEN r.identified_column_max_length + 4
                ELSE @DF_COLUMN_WIDTH_ESTIMATE
            END AS column_width_estimate,
            pm.treat_as_string as treat_as_string
        FROM RulesetWithTypes r
        LEFT JOIN ParseMetadata pm
            ON r.identified_column = pm.identified_column
    ),
    /*
     * GenerateMaskParameters - Creates core masking parameters for ADF dataflows.
     * Aggregates column rules into JSON structures: FieldAlgorithmAssignments, ColumnsToMask, 
     * DataFactoryTypeMapping, NumberOfBatches, and TrimLengths.
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
            -- Raw number of batches calculation without minimum enforcement
            CEILING((MAX(row_count) * (SUM(column_width_estimate) + LOG10(MAX(row_count)) + 1)) / (2000000 * 0.9)) AS NumberOfBatches,
            -- Array of column lengths for trimming
            '[' + STRING_AGG(CAST(identified_column_max_length AS NVARCHAR(10)), ',') + ']' AS TrimLengths
        FROM RulesetWithAlgorithmTypeMapping
    ),
    /*
     * ModifyNumberOfBatches - Ensures batch count meets minimum requirements.
     * Enforces minimum of 1 batch to prevent zero/negative values.
     */
    ModifyNumberOfBatches AS (
        SELECT
            FieldAlgorithmAssignments,
            ColumnsToMask,
            DataFactoryTypeMapping,
            -- Apply minimum batch count of 1
            CASE 
                WHEN NumberOfBatches < 1 THEN 1 
                ELSE NumberOfBatches 
            END AS NumberOfBatches,
            TrimLengths
        FROM GenerateMaskParameters
    ),
    /*
     * DateFormatResolution - Resolves final date format for each column.
     * Prioritizes conditional formats (based on filter key) over standard formats from algorithm_metadata.
     */
    DateFormatResolution AS (
        SELECT
            p.identified_column,
            p.encoded_column_name,  -- Already available from ParseMetadata → FilterToSingleTable
            COALESCE(
                -- Get conditional date format if filter key matches
                CASE WHEN @DF_FILTER_KEY <> '' THEN 
                    (
                        SELECT TOP 1 c.[date_format]
                        FROM OPENJSON(p.algorithm_metadata, '$.conditions')
                        WITH (
                            [alias] NVARCHAR(255) '$.alias',
                            [date_format] NVARCHAR(255) '$.date_format'
                        ) AS c
                        WHERE c.[alias] = @DF_FILTER_KEY
                    )
                ELSE NULL END,
                -- Fall back to standard date format
                p.date_format
            ) AS final_date_format
        FROM ParseMetadata p
        WHERE p.date_format IS NOT NULL OR p.algorithm_metadata IS NOT NULL
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
     * FilterToRowsWithStringCasting - Identifies columns requiring string casting.
     * Filters to columns with treat_as_string flag set to true.
     */
    FilterToRowsWithStringCasting AS (
        SELECT
            p.*
        FROM ParseMetadata p
        WHERE p.treat_as_string = 1
    ),
    /*
     * StringCastingWithAdfType - Adds ADF type information to string casting columns.
     * Determines what type columns should be cast back to after string processing.
     */
    StringCastingWithAdfType AS (
        SELECT
            f.*,
            t.adf_type
        FROM FilterToRowsWithStringCasting f
        INNER JOIN FilterToDataSourceType t
            ON f.identified_column_type = t.dataset_type
    ),
    /*
     * AggregateColumnsToCastBackParameters - Creates ColumnsToCastBackTo* arrays for all ADF types.
     * Generates JSON arrays grouped by target ADF type (binary, boolean, date, etc.).
     */
    AggregateColumnsToCastBackParameters AS (
        SELECT
            '[' + COALESCE(STRING_AGG(CASE WHEN s.adf_type = 'binary' THEN '"' + s.identified_column + '"' END, ','), '') + ']' AS ColumnsToCastBackToBinary,
            '[' + COALESCE(STRING_AGG(CASE WHEN s.adf_type = 'boolean' THEN '"' + s.identified_column + '"' END, ','), '') + ']' AS ColumnsToCastBackToBoolean,
            '[' + COALESCE(STRING_AGG(CASE WHEN s.adf_type = 'date' THEN '"' + s.identified_column + '"' END, ','), '') + ']' AS ColumnsToCastBackToDate,
            '[' + COALESCE(STRING_AGG(CASE WHEN s.adf_type = 'double' THEN '"' + s.identified_column + '"' END, ','), '') + ']' AS ColumnsToCastBackToDouble,
            '[' + COALESCE(STRING_AGG(CASE WHEN s.adf_type = 'float' THEN '"' + s.identified_column + '"' END, ','), '') + ']' AS ColumnsToCastBackToFloat,
            '[' + COALESCE(STRING_AGG(CASE WHEN s.adf_type = 'integer' THEN '"' + s.identified_column + '"' END, ','), '') + ']' AS ColumnsToCastBackToInteger,
            '[' + COALESCE(STRING_AGG(CASE WHEN s.adf_type = 'long' THEN '"' + s.identified_column + '"' END, ','), '') + ']' AS ColumnsToCastBackToLong,
            '[' + COALESCE(STRING_AGG(CASE WHEN s.adf_type = 'timestamp' THEN '"' + s.identified_column + '"' END, ','), '') + ']' AS ColumnsToCastBackToTimestamp
        FROM StringCastingWithAdfType s
    ),
    /*
     * CombineAllStringCastingParameters - Combines all string casting parameters.
     * Creates ColumnsToCastAsStrings array and merges with all ColumnsToCastBackTo* arrays.
     */
    CombineAllStringCastingParameters AS (
        SELECT
            COALESCE(
                (
                    SELECT '[' + STRING_AGG('"' + f.identified_column + '"', ',') + ']' 
                    FROM FilterToRowsWithStringCasting f
                ), '[]'
            ) AS ColumnsToCastAsStrings,
            a.ColumnsToCastBackToBinary,
            a.ColumnsToCastBackToBoolean,
            a.ColumnsToCastBackToDate,
            a.ColumnsToCastBackToDouble,
            a.ColumnsToCastBackToFloat,
            a.ColumnsToCastBackToInteger,
            a.ColumnsToCastBackToLong,
            a.ColumnsToCastBackToTimestamp
        FROM AggregateColumnsToCastBackParameters a
    ),
    /*
     * JoinDateFormatAndStringCastingParameters - Combines date formatting with string casting parameters.
     * Cross join merges DateFormatAssignments with all string casting parameters.
     */
    JoinDateFormatAndStringCastingParameters AS (
        SELECT
            d.DateFormatAssignments,
            c.ColumnsToCastAsStrings,
            c.ColumnsToCastBackToBinary,
            c.ColumnsToCastBackToBoolean,
            c.ColumnsToCastBackToDate,
            c.ColumnsToCastBackToDouble,
            c.ColumnsToCastBackToFloat,
            c.ColumnsToCastBackToInteger,
            c.ColumnsToCastBackToLong,
            c.ColumnsToCastBackToTimestamp
        FROM DateFormatHeaderHandlingNulls d
        CROSS JOIN CombineAllStringCastingParameters c
    ),
    /*
     * ComputeCastingDefaultsIfMissing - Ensures all casting parameters have valid default values.
     * Replaces null values with empty JSON arrays to prevent errors and ensure consistency.
     */
    ComputeCastingDefaultsIfMissing AS (
        SELECT
            DateFormatAssignments,
            COALESCE(ColumnsToCastAsStrings, '[]') AS ColumnsToCastAsStrings,
            COALESCE(ColumnsToCastBackToBinary, '[]') AS ColumnsToCastBackToBinary,
            COALESCE(ColumnsToCastBackToBoolean, '[]') AS ColumnsToCastBackToBoolean,
            COALESCE(ColumnsToCastBackToDate, '[]') AS ColumnsToCastBackToDate,
            COALESCE(ColumnsToCastBackToDouble, '[]') AS ColumnsToCastBackToDouble,
            COALESCE(ColumnsToCastBackToFloat, '[]') AS ColumnsToCastBackToFloat,
            COALESCE(ColumnsToCastBackToInteger, '[]') AS ColumnsToCastBackToInteger,
            COALESCE(ColumnsToCastBackToLong, '[]') AS ColumnsToCastBackToLong,
            COALESCE(ColumnsToCastBackToTimestamp, '[]') AS ColumnsToCastBackToTimestamp
        FROM JoinDateFormatAndStringCastingParameters
    ),
    /*
     * AllMaskingParameters - Consolidates all masking parameters into the final result.
     * Combines core parameters with casting defaults and adjusted batch numbers.
     */
    AllMaskingParameters AS (
        SELECT
            m.FieldAlgorithmAssignments,
            m.ColumnsToMask,
            m.DataFactoryTypeMapping,
            m.NumberOfBatches,
            m.TrimLengths,
            c.DateFormatAssignments,
            c.ColumnsToCastAsStrings,
            c.ColumnsToCastBackToBinary,
            c.ColumnsToCastBackToBoolean,
            c.ColumnsToCastBackToDate,
            c.ColumnsToCastBackToDouble,
            c.ColumnsToCastBackToFloat,
            c.ColumnsToCastBackToInteger,
            c.ColumnsToCastBackToLong,
            c.ColumnsToCastBackToTimestamp
        FROM ModifyNumberOfBatches m
        CROSS JOIN ComputeCastingDefaultsIfMissing c
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
                    FROM FilterToRowsWithStringCasting f
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
