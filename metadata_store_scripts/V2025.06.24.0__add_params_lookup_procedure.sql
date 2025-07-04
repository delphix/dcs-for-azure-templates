CREATE PROCEDURE get_masking_parameters
    @source_dataset NVARCHAR(255),
    @source_database NVARCHAR(255),
    @source_schema NVARCHAR(255),
    @source_table NVARCHAR(255),
    @filter_alias NVARCHAR(255)
AS
BEGIN
    -- Prevents extra result sets from interfering with output
    SET NOCOUNT ON;

    -- Find the key column for the given table and dataset
    WITH key_column_name AS (
        SELECT TOP 1 r.identified_column AS key_col
        FROM discovered_ruleset AS r
        WHERE
            r.dataset = @source_dataset
            AND r.specified_database = @source_database
            AND r.specified_schema = @source_schema
            AND r.identified_table = @source_table
            AND ISJSON(r.assigned_algorithm) = 1
    ),

    -- Extract rules for columns with assigned algorithms (object form)
    conditioned_table_rules AS (
        SELECT
            r.dataset,
            r.specified_database,
            r.specified_schema,
            r.identified_table,
            r.identified_column,
            r.identified_column_type,
            r.identified_column_max_length,
            r.ordinal_position,
            r.row_count,
            r.source_metadata,
            r.rows_profiled,
            NULL AS algorithm_metadata,
            JSON_VALUE(a.[value], '$.algorithm') AS assigned_algorithm
        FROM discovered_ruleset AS r
        CROSS APPLY OPENJSON(r.assigned_algorithm, '$.conditions') AS a
        WHERE
            r.dataset = @source_dataset
            AND r.specified_database = @source_database
            AND r.specified_schema = @source_schema
            AND r.identified_table = @source_table
            AND ISJSON(assigned_algorithm) = 1
            AND JSON_VALUE(assigned_algorithm, '$.key_column')
            = (SELECT key_col FROM key_column_name)
            AND JSON_VALUE(a.[value], '$.alias') = @filter_alias
    ),

    -- Extract metadata for columns with assigned algorithms (object form)
    conditioned_metadata_rules AS (
        SELECT
            r.dataset,
            r.specified_database,
            r.specified_schema,
            r.identified_table,
            r.identified_column,
            r.identified_column_type,
            r.identified_column_max_length,
            r.ordinal_position,
            r.row_count,
            r.source_metadata,
            r.rows_profiled,
            NULL AS assigned_algorithm,
            CASE
                WHEN
                    JSON_PATH_EXISTS(r.algorithm_metadata, '$.treat_as_string')
                    = 1
                    THEN
                        JSON_OBJECT(
                            'treat_as_string': JSON_VALUE(
                                r.algorithm_metadata,
                                '$.treat_as_string'
                            ),
                            'date_format': JSON_VALUE(
                                m.[value],
                                '$.date_format'
                            )
                        )
                ELSE JSON_OBJECT('date_format': JSON_VALUE(
                    m.[value],
                    '$.date_format'
                ))
            END AS algorithm_metadata
        FROM discovered_ruleset AS r
        CROSS APPLY OPENJSON(r.assigned_algorithm, '$.conditions') AS a
        CROSS APPLY OPENJSON(r.algorithm_metadata, '$.conditions') AS m
        WHERE
            r.dataset = @source_dataset
            AND r.specified_database = @source_database
            AND r.specified_schema = @source_schema
            AND r.identified_table = @source_table
            AND ISJSON(assigned_algorithm) = 1
            AND JSON_VALUE(assigned_algorithm, '$.key_column')
            = (SELECT key_col FROM key_column_name)
            AND JSON_VALUE(a.[value], '$.alias') = @filter_alias
            AND ISJSON(algorithm_metadata) = 1
            AND JSON_VALUE(algorithm_metadata, '$.key_column')
            = (SELECT key_col FROM key_column_name)
            AND JSON_VALUE(m.[value], '$.alias') = @filter_alias
    ),

    -- Combine conditioned rules and metadata, 
    -- and add rules without JSON algorithms
    reconstructed_ruleset_table AS (
        SELECT
            ctr.dataset,
            ctr.specified_database,
            ctr.specified_schema,
            ctr.identified_table,
            ctr.identified_column,
            ctr.identified_column_type,
            ctr.identified_column_max_length,
            ctr.ordinal_position,
            ctr.row_count,
            ctr.source_metadata,
            ctr.rows_profiled,
            COALESCE(
                ctr.assigned_algorithm,
                cmr.assigned_algorithm
            ) AS assigned_algorithm,
            COALESCE(
                cmr.algorithm_metadata,
                ctr.algorithm_metadata
            ) AS algorithm_metadata
        FROM conditioned_table_rules AS ctr
        INNER JOIN conditioned_metadata_rules AS cmr
            ON
                (
                    ctr.dataset = cmr.dataset
                    AND ctr.specified_database = cmr.specified_database
                    AND ctr.specified_schema = cmr.specified_schema
                    AND ctr.identified_table = cmr.identified_table
                )
        UNION
        SELECT
            r.dataset,
            r.specified_database,
            r.specified_schema,
            r.identified_table,
            r.identified_column,
            r.identified_column_type,
            r.identified_column_max_length,
            r.ordinal_position,
            r.row_count,
            r.source_metadata,
            r.rows_profiled,
            r.assigned_algorithm,
            r.algorithm_metadata
        FROM discovered_ruleset AS r
        WHERE
            r.dataset = @source_dataset
            AND r.specified_database = @source_database
            AND r.specified_schema = @source_schema
            AND r.identified_table = @source_table
            AND ISJSON(assigned_algorithm) = 0
    ),

    -- Join with type mapping and compute ADF type conversions
    table_rules AS (
        SELECT
            r.dataset,
            r.specified_database,
            r.specified_schema,
            r.identified_table,
            r.identified_column,
            r.identified_column_type,
            t.adf_type,
            r.identified_column_max_length,
            r.ordinal_position,
            r.row_count,
            r.source_metadata,
            r.rows_profiled,
            r.assigned_algorithm,
            CONCAT(
                CONCAT('x', LOWER(
                    CONVERT(
                        VARCHAR(MAX), CONVERT(VARBINARY, r.identified_column), 2
                    )
                )),
                ' as ',
                CASE
                    WHEN
                        JSON_VALUE(r.algorithm_metadata, '$.treat_as_string')
                        = 'true'
                        THEN 'string'
                    ELSE t.adf_type
                END
            ) AS adf_type_conversion,
            CASE
                WHEN (identified_column_max_length) > 0
                    THEN identified_column_max_length + 4
                ELSE 1000
            END AS column_width_estimate,
            JSON_VALUE(algorithm_metadata, '$.treat_as_string')
                AS treat_as_string,
            JSON_VALUE(algorithm_metadata, '$.date_format')
                AS custom_date_format
        FROM reconstructed_ruleset_table AS r
        INNER JOIN adf_type_mapping AS t
            ON (r.identified_column_type = t.dataset_type)
        WHERE
            r.dataset = @source_dataset
            AND r.specified_database = @source_database
            AND r.specified_schema = @source_schema
            AND r.identified_table = @source_table
    ),

    -- Aggregate masking parameters for the pipeline
    masking_parameters AS (
        SELECT
            CONCAT('[', STRING_AGG(
                CONCAT('"', identified_column, '"'),
                ','
            ), ']') AS columnstomask,
            CONCAT(
                '{',
                STRING_AGG(
                    CONCAT(
                        '"x',
                        LOWER(
                            CONVERT(
                                VARCHAR(MAX),
                                CONVERT(VARBINARY, identified_column),
                                2
                            )
                        ),
                        '":"',
                        assigned_algorithm,
                        '"'
                    ),
                    ','
                ),
                '}'
            ) AS fieldalgorithmassignments,
            CONCAT(
                '''',
                '(timestamp as date, status as string, message as string, '
                + 'trace_id as string, items as (DELPHIX_COMPLIANCE_SERVICE_BATCH_ID as long, ',
                STRING_AGG(adf_type_conversion, ', '),
                ')[])',
                ''''
            ) AS datafactorytypemapping,
            CASE
                WHEN MAX(row_count) < 0 THEN -1
                WHEN LEN(@filter_alias) > 0 THEN -1
                WHEN
                    CEILING(
                        (
                            MAX(row_count)
                            * (
                                SUM(column_width_estimate)
                                + LOG10(MAX(row_count))
                                + 1
                            )
                            / (2000000 * .9)
                        )
                    )
                    < 1
                    THEN
                        CEILING(
                            (
                                MAX(row_count)
                                * (
                                    SUM(column_width_estimate)
                                    + LOG10(MAX(row_count))
                                    + 1
                                )
                                / (2000000 * .9)
                            )
                        )
                ELSE 1
            END AS numberofbatches,
            CONCAT('[', STRING_AGG(identified_column_max_length, ','), ']')
                AS trimlengths,
            CONCAT(
                '{"DELPHIX_COMPLIANCE_SERVICE_BATCH_ID":[1],',
                STRING_AGG(
                    CONCAT(
                        '"x',
                        LOWER(
                            CONVERT(
                                VARCHAR(MAX),
                                CONVERT(VARBINARY, identified_column),
                                2
                            )
                        ),
                        '":[null]'
                    ),
                    ','
                ),
                '}'
            ) AS fieldalgorithmtestbody
        FROM table_rules
        WHERE
            assigned_algorithm IS NOT NULL
            AND assigned_algorithm != ''
    ),

    -- Aggregate date format assignments for columns
    date_format_parameters AS (
        SELECT
            CONCAT(
                '{',
                STRING_AGG(
                    CONCAT(
                        '"x',
                        LOWER(
                            CONVERT(
                                VARCHAR(MAX),
                                CONVERT(VARBINARY, identified_column),
                                2
                            )
                        ),
                        '":"',
                        custom_date_format,
                        '"'
                    ),
                    ','
                ),
                '}'
            ) AS dateformatassignments
        FROM table_rules
        WHERE
            assigned_algorithm IS NOT NULL
            AND assigned_algorithm != ''
            AND custom_date_format != ''
    ),

    -- Aggregate columns to cast back by type
    casting_by_type AS (
        SELECT
            adf_type,
            CONCAT('["', STRING_AGG(identified_column, '","'), '"]')
                AS columnstocastbackto
        FROM table_rules
        WHERE
            assigned_algorithm IS NOT NULL
            AND assigned_algorithm != ''
            AND treat_as_string = 'true'
        GROUP BY adf_type
    ),

    -- Prepare casting parameters for each type
    casting_by_type_parameters AS (
        SELECT
            CASE
                WHEN adf_type = 'binary'
                    THEN columnstocastbackto
            END
                AS columnstocastbacktobinary,
            CASE
                WHEN adf_type = 'boolean'
                    THEN columnstocastbackto
            END
                AS columnstocastbacktoboolean,
            CASE
                WHEN adf_type = 'date'
                    THEN columnstocastbackto
            END
                AS columnstocastbacktodate,
            CASE
                WHEN adf_type = 'double'
                    THEN columnstocastbackto
            END
                AS columnstocastbacktodouble,
            CASE
                WHEN adf_type = 'float'
                    THEN columnstocastbackto
            END
                AS columnstocastbacktofloat,
            CASE
                WHEN adf_type = 'integer'
                    THEN columnstocastbackto
            END
                AS columnstocastbacktointeger,
            CASE
                WHEN adf_type = 'long'
                    THEN columnstocastbackto
            END
                AS columnstocastbacktolong,
            CASE
                WHEN adf_type = 'timestamp'
                    THEN columnstocastbackto
            END
                AS columnstocastbacktotimestamp
        FROM casting_by_type
    ),

    -- Aggregate all type casting parameters
    type_casting_parameters AS (
        SELECT
            COALESCE(STRING_AGG(columnstocastbacktobinary, ','), '[]')
                AS columnstocastbacktobinary,
            COALESCE(STRING_AGG(columnstocastbacktoboolean, ','), '[]')
                AS columnstocastbacktoboolean,
            COALESCE(STRING_AGG(columnstocastbacktodate, ','), '[]')
                AS columnstocastbacktodate,
            COALESCE(STRING_AGG(columnstocastbacktodouble, ','), '[]')
                AS columnstocastbacktodouble,
            COALESCE(STRING_AGG(columnstocastbacktofloat, ','), '[]')
                AS columnstocastbacktofloat,
            COALESCE(STRING_AGG(columnstocastbacktointeger, ','), '[]')
                AS columnstocastbacktointeger,
            COALESCE(STRING_AGG(columnstocastbacktolong, ','), '[]')
                AS columnstocastbacktolong,
            COALESCE(STRING_AGG(columnstocastbacktotimestamp, ','), '[]')
                AS columnstocastbacktotimestamp
        FROM casting_by_type_parameters
    )

    -- Return all computed masking and type mapping parameters
    SELECT * FROM
        masking_parameters,
        date_format_parameters,
        type_casting_parameters;
END;
