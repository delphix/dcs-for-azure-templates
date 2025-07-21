/*
 * GenerateMaskingParameters - Generates masking parameters for a given table
 * 
 * This procedure generates the necessary parameters for masking a table, including
 * algorithm assignments, column lists, data type mappings, and formatting options.
 *
 * The procedure supports two modes:
 * 1. Standard mode - When @DF_FILTER_KEY is empty (default), generates standard masking parameters
 * 2. Filtered mode - When @DF_FILTER_KEY is provided, applies conditional algorithms and formats
 *    based on the provided filter key
 */
CREATE OR ALTER PROCEDURE [dbo].[GenerateMaskingParameters]
    @DF_METADATA_SCHEMA NVARCHAR(128) = 'dbo',
    @DF_METADATA_RULESET_TABLE NVARCHAR(128) = 'discovered_ruleset',
    @DF_METADATA_ADF_TYPE_MAPPING_TABLE NVARCHAR(128) = 'adf_type_mapping',
    @DF_SOURCE_DB NVARCHAR(128),
    @DF_SOURCE_SCHEMA NVARCHAR(128),
    @DF_SOURCE_TABLE NVARCHAR(128),
    @DF_COLUMN_WIDTH_ESTIMATE INT = 1000,
    -- Optional filter key for conditional algorithms
    -- It will match the alias in the conditional algorithm JSON structure
    -- If provided, the procedure will filter the ruleset based on this key
    -- If empty, it will generate standard masking parameters without filtering
    @DF_FILTER_KEY NVARCHAR(128) = '',
    @DF_DATASET NVARCHAR(128) = 'AZURESQL'
AS
BEGIN
    SET NOCOUNT ON;

    -- FilterToSingleTable
    -- Filter ruleset table down to the table being masked.
    WITH FilterToSingleTable AS (
        SELECT 
            r.*
        FROM dbo.discovered_ruleset r
        WHERE r.dataset = @DF_DATASET
          AND r.specified_database = @DF_SOURCE_DB
          AND r.specified_schema = @DF_SOURCE_SCHEMA
          AND r.identified_table = @DF_SOURCE_TABLE
          AND r.assigned_algorithm IS NOT NULL
          AND r.assigned_algorithm <> ''
    ),
    -- FilterToDataSourceType
    -- Filter type mapping table down to only the dataset in question
    FilterToDataSourceType AS (
        SELECT 
            t.dataset,
            t.dataset_type,
            t.adf_type
        FROM dbo.adf_type_mapping t
        WHERE t.dataset = @DF_DATASET
    ),
    -- ConditionalProcessing
    -- The following CTEs are only used when @DF_FILTER_KEY is not empty
    -- They handle parsing and filtering of conditional rules
    KeyColumn AS (
        SELECT fts.*
        FROM FilterToSingleTable fts
        WHERE ISJSON(fts.assigned_algorithm) = 1
          AND JSON_VALUE(fts.assigned_algorithm, '$[0].alias') IS NOT NULL
    ),
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
    StandardAlgorithm AS (
        SELECT fts.*
        FROM FilterToSingleTable fts
        WHERE ISJSON(fts.assigned_algorithm) = 0
    ),
    ParseKeyColumn AS (
        SELECT 
            k.*,
            JSON_QUERY(k.assigned_algorithm, '$') AS conditions_set
        FROM KeyColumn k
    ),
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
    ParseAlgorithm AS (
        SELECT
            ca.*,
            JSON_VALUE(ca.assigned_algorithm, '$.key_column') AS key_column,
            JSON_QUERY(ca.assigned_algorithm, '$.conditions') AS conditions
        FROM ConditionalAlgorithm ca
    ),
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
    -- Add conditional filtering for @DF_FILTER_KEY
    -- Only use these when @DF_FILTER_KEY is provided
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
    -- UnionAllRules
    -- Simply use the result of conditional filtering
    UnionAllRules AS (
        SELECT cf.* 
        FROM ConditionalFiltering cf
        WHERE cf.assigned_algorithm IS NOT NULL
          AND cf.assigned_algorithm <> ''
          AND ISJSON(cf.assigned_algorithm) = 0
    ),
    -- RulesetWithTypes
    -- Join the ruleset table with the type mapping table based on the type of the column
    -- and the translation of that type to an ADF type
    RulesetWithTypes AS (
        SELECT 
            u.*,
            t.adf_type,
            CONCAT('x', CONVERT(varchar(max), CONVERT(varbinary, u.identified_column), 2)) AS encoded_column_name
        FROM UnionAllRules u
        INNER JOIN FilterToDataSourceType t
            ON u.identified_column_type = t.dataset_type
    ),
    -- ParseMetadata
    -- Parse the content from the metadata column that contains JSON,
    -- specifically handling parsing of known keys (i.e. date_format and treat_as_string)
    ParseMetadata AS (
        SELECT
            f.*,
            JSON_VALUE(f.algorithm_metadata, '$.date_format') AS date_format,
            CAST(JSON_VALUE(f.algorithm_metadata, '$.treat_as_string') AS BIT) AS treat_as_string
        FROM FilterToSingleTable f
    ),
    -- RulesetWithAlgorithmTypeMapping
    -- Generate several columns:
    -- - output_row that always contains 1 (used later for Aggregate and Join operations)
    -- - adf_type_conversion that contains a string like <column_name> as <adf_type>
    -- - column_width_estimate that contains an integer for width estimation
    RulesetWithAlgorithmTypeMapping AS (
        SELECT
            r.*,
            1 AS output_row,
            CASE 
                WHEN r.identified_column_max_length > 0 THEN r.identified_column_max_length + 4
                ELSE @DF_COLUMN_WIDTH_ESTIMATE
            END AS column_width_estimate,
            pm.treat_as_string as treat_as_string
        FROM RulesetWithTypes r
        LEFT JOIN ParseMetadata pm
            ON r.identified_column = pm.identified_column
    ),
    -- GenerateMaskParameters
    -- Grouped by output_row to produce the following aggregates:
    -- - FieldAlgorithmAssignments: a JSON string that maps a column name to its assigned algorithm
    -- - ColumnsToMask: a list of column names that have an algorithm assigned
    -- - DataFactoryTypeMapping: a string that can be used by ADF to parse the output of a call to the Delphix masking endpoint
    -- - NumberOfBatches: an integer value determined by computing the number of batches (ensures at least 1 batch)
    -- - TrimLengths: a list of the actual widths of the columns to be used by the masking data flow
    GenerateMaskParameters AS (
        SELECT
            output_row,
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
        GROUP BY output_row
    ),
    -- ModifyNumberOfBatches
    -- Ensures the number of batches is at least 1
    ModifyNumberOfBatches AS (
        SELECT
            output_row,
            CASE 
                WHEN NumberOfBatches > 0 THEN NumberOfBatches
                ELSE 1
            END AS AdjustedNumberOfBatches
        FROM GenerateMaskParameters
    ),
    -- Process conditional date formats when @DF_FILTER_KEY is provided
    -- These CTEs extract date formats from conditional formatting
    ConditionalDateFormats AS (
        SELECT
            p.identified_column,
            c.[date_format] AS conditional_date_format
        FROM ParseMetadata p
        OUTER APPLY OPENJSON(p.algorithm_metadata, '$.conditions')
        WITH (
            [alias] NVARCHAR(255) '$.alias',
            [date_format] NVARCHAR(255) '$.date_format'
        ) AS c
        WHERE (@DF_FILTER_KEY <> '' AND c.[alias] = @DF_FILTER_KEY)
           OR @DF_FILTER_KEY = ''
    ),
    -- DateFormatString
    -- Derive columns as necessary for handling the parsed data
    -- Uses conditional_date_format when available with @DF_FILTER_KEY, otherwise uses date_format
    DateFormatString AS (
        SELECT
            p.*,
            1 AS output_row,
            COALESCE(
                CASE WHEN @DF_FILTER_KEY <> '' THEN 
                    (SELECT TOP 1 conditional_date_format FROM ConditionalDateFormats c WHERE c.identified_column = p.identified_column)
                ELSE NULL END, 
                p.date_format
            ) AS date_format_string
        FROM ParseMetadata p
    ),
    -- SplitOnDateFormat - ContainsDateFormat
    -- Split the data into two streams, data that contains a specified date format, and data that does not
    DateFormatWithValues AS (
        SELECT dfs.* 
        FROM DateFormatString dfs
        WHERE dfs.date_format_string IS NOT NULL
    ),
    -- SplitOnDateFormat - DoesNotContainDateFormat
    DateFormatWithoutValues AS (
        SELECT dfs.* 
        FROM DateFormatString dfs
        WHERE dfs.date_format_string IS NULL
    ),
    -- NoFormatHeader
    -- Create NoFormatHeader, that generates a JSON string containing an empty map when all values are null
    NoFormatHeader AS (
        SELECT
            1 AS output_row,
            '{}' AS NoFormatHeader
    ),
    -- DateFormatHeader
    -- Create DateFormatAssignment, grouped by output_row, generating a JSON string that maps a column to its specified date format
    DateFormatHeader AS (
        SELECT
            1 AS output_row,
            '{' + STRING_AGG(
                '"' + LOWER(CONCAT('x', CONVERT(varchar(max), CONVERT(varbinary, identified_column), 2))) + '":"' + date_format_string + '"',
                ','
            ) + '}' AS DateFormatAssignments
        FROM DateFormatWithValues
        WHERE date_format_string IS NOT NULL
    ),
    -- JoinDateHeaders
    -- Full outer join both DateFormatHeader and NoFormatHeader where output_row = output_row
    JoinDateHeaders AS (
        SELECT
            COALESCE(d.output_row, n.output_row) AS output_row,
            d.DateFormatAssignments,
            n.NoFormatHeader
        FROM DateFormatHeader d
        FULL OUTER JOIN NoFormatHeader n ON d.output_row = n.output_row
    ),
    -- DateFormatHeaderHandlingNulls
    -- Update column DateFormatAssignments to coalesce DateFormatAssignments, and NoFormatHeader
    DateFormatHeaderHandlingNulls AS (
        SELECT
            output_row,
            COALESCE(DateFormatAssignments, NoFormatHeader) AS DateFormatAssignments
        FROM JoinDateHeaders
    ),
    -- FilterToRowsWithStringCasting
    -- Derive columns for handling string casting and filter rows where 'treat_as_string' is true
    FilterToRowsWithStringCasting AS (
        SELECT
            p.*,
            1 AS output_row
        FROM ParseMetadata p
        WHERE p.treat_as_string = 1
    ),
    -- StringCastingWithAdfType
    -- Join columns that need to be cast to string and the type mapping table,
    -- so we can determine casting requirements
    StringCastingWithAdfType AS (
        SELECT
            f.*,
            t.adf_type
        FROM FilterToRowsWithStringCasting f
        INNER JOIN FilterToDataSourceType t
            ON f.identified_column_type = t.dataset_type
    ),
    -- AggregateColumnsToCastBackParameters
    -- Creates all the ColumnsToCastBackTo* parameters directly from StringCastingWithAdfType
    AggregateColumnsToCastBackParameters AS (
        SELECT
            1 AS output_row,
            '[' + COALESCE(STRING_AGG(CASE WHEN s.adf_type = 'binary' THEN '"' + s.identified_column + '"' END, ','), '') + ']' AS ColumnsToCastBackToBinary,
            '[' + COALESCE(STRING_AGG(CASE WHEN s.adf_type = 'boolean' THEN '"' + s.identified_column + '"' END, ','), '') + ']' AS ColumnsToCastBackToBoolean,
            '[' + COALESCE(STRING_AGG(CASE WHEN s.adf_type = 'date' THEN '"' + s.identified_column + '"' END, ','), '') + ']' AS ColumnsToCastBackToDate,
            '[' + COALESCE(STRING_AGG(CASE WHEN s.adf_type = 'double' THEN '"' + s.identified_column + '"' END, ','), '') + ']' AS ColumnsToCastBackToDouble,
            '[' + COALESCE(STRING_AGG(CASE WHEN s.adf_type = 'float' THEN '"' + s.identified_column + '"' END, ','), '') + ']' AS ColumnsToCastBackToFloat,
            '[' + COALESCE(STRING_AGG(CASE WHEN s.adf_type = 'integer' THEN '"' + s.identified_column + '"' END, ','), '') + ']' AS ColumnsToCastBackToInteger,
            '[' + COALESCE(STRING_AGG(CASE WHEN s.adf_type = 'long' THEN '"' + s.identified_column + '"' END, ','), '') + ']' AS ColumnsToCastBackToLong,
            '[' + COALESCE(STRING_AGG(CASE WHEN s.adf_type = 'timestamp' THEN '"' + s.identified_column + '"' END, ','), '') + ']' AS ColumnsToCastBackToTimestamp
        FROM StringCastingWithAdfType s
        GROUP BY output_row
    ),
    -- CombineAllStringCastingParameters
    -- Creates ColumnsToCastAsStrings directly and combines with ColumnsToCastBackTo* parameters
    CombineAllStringCastingParameters AS (
        SELECT
            a.output_row,
            COALESCE(
                (
                    SELECT '[' + STRING_AGG('"' + f.identified_column + '"', ',') + ']' 
                    FROM FilterToRowsWithStringCasting f
                    WHERE f.output_row = a.output_row
                    GROUP BY f.output_row
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
    -- JoinDateFormatAndStringCastingParameters
    -- Combine date format and string casting related parameters into a single table
    JoinDateFormatAndStringCastingParameters AS (
        SELECT
            d.output_row,
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
        LEFT JOIN CombineAllStringCastingParameters c ON d.output_row = c.output_row
    ),
    -- ComputeCastingDefaultsIfMissing
    -- For all casting parameters that are currently null, set them instead to the empty list
    ComputeCastingDefaultsIfMissing AS (
        SELECT
            output_row,
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
    -- AllMaskingParameters
    -- Perform an inner join on output_row with the computed casting parameters and date format headers,
    -- combining all masking parameters into the same output stream
    AllMaskingParameters AS (
        SELECT
            m.output_row AS output_row,
            m.FieldAlgorithmAssignments,
            m.ColumnsToMask,
            m.DataFactoryTypeMapping,
            b.AdjustedNumberOfBatches AS NumberOfBatches,
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
        FROM GenerateMaskParameters m
        INNER JOIN ComputeCastingDefaultsIfMissing c ON m.output_row = c.output_row
        INNER JOIN ModifyNumberOfBatches b ON m.output_row = b.output_row
    ), 
    FinalMaskingParameters AS (
        SELECT 
            amp.*
        FROM AllMaskingParameters amp
        
        UNION ALL

        SELECT
            1 AS output_row,
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
        output_row,
        FieldAlgorithmAssignments,
        ColumnsToMask,
        DataFactoryTypeMapping,
        NumberOfBatches,
        TrimLengths,
        DateFormatAssignments,
        CASE 
            WHEN ColumnsToCastAsStrings = '[]' 
            THEN '[""]' 
            ELSE ColumnsToCastAsStrings 
        END AS ColumnsToCastAsStrings,
        CASE WHEN ColumnsToCastBackToBinary = '[]' THEN '[""]' ELSE ColumnsToCastBackToBinary END AS ColumnsToCastBackToBinary,
        CASE WHEN ColumnsToCastBackToBoolean = '[]' THEN '[""]' ELSE ColumnsToCastBackToBoolean END AS ColumnsToCastBackToBoolean,
        CASE WHEN ColumnsToCastBackToDate = '[]' THEN '[""]' ELSE ColumnsToCastBackToDate END AS ColumnsToCastBackToDate,
        CASE WHEN ColumnsToCastBackToDouble = '[]' THEN '[""]' ELSE ColumnsToCastBackToDouble END AS ColumnsToCastBackToDouble,
        CASE WHEN ColumnsToCastBackToFloat = '[]' THEN '[""]' ELSE ColumnsToCastBackToFloat END AS ColumnsToCastBackToFloat,
        CASE WHEN ColumnsToCastBackToInteger = '[]' THEN '[""]' ELSE ColumnsToCastBackToInteger END AS ColumnsToCastBackToInteger,
        CASE WHEN ColumnsToCastBackToLong = '[]' THEN '[""]' ELSE ColumnsToCastBackToLong END AS ColumnsToCastBackToLong,
        CASE WHEN ColumnsToCastBackToTimestamp = '[]' THEN '[""]' ELSE ColumnsToCastBackToTimestamp END AS ColumnsToCastBackToTimestamp
    FROM FinalMaskingParameters;
END
GO
