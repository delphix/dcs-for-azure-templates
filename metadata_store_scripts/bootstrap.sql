-- This file includes the queries from the following scripts:
-- * V2024.01.01.0__create_initial_tables
-- * V2024.04.18.0__add_adls_to_adls_support
-- * V2024.05.02.0__update_adls_to_adls_support
-- * V2024.08.25.0__add_conditional_masking_support
-- * V2024.10.24.0__add_checkpointing_and_logging
-- * V2024.12.02.0__add_azuresql_to_azuresql_support
-- * V2024.12.13.0__create_create_constraints_table
-- * V2024.12.26.0__add_adls_to_adls_parquet_support
-- * V20205.01.15.0__separate_algorithm_and_source_metadata
-- * V2024.12.06.0__add_adls_to_adls_parquet_support
-- The contents of each of those files follows


-- source: V2024.01.01.0__create_initial_tables
CREATE TABLE discovered_ruleset(
   dataset VARCHAR(255) NOT NULL,
   specified_database VARCHAR(255) NOT NULL,
   specified_schema VARCHAR(255) NOT NULL,
   identified_table VARCHAR(255) NOT NULL,
   identified_column VARCHAR(255) NOT NULL,
   identified_column_type VARCHAR(100) NOT NULL,
   identified_column_max_length INT NOT NULL,
   ordinal_position INT NOT NULL,
   row_count BIGINT,
   metadata NVARCHAR(MAX),
   profiled_domain VARCHAR(100),
   profiled_algorithm VARCHAR(100),
   confidence_score DECIMAL(6,5),
   rows_profiled BIGINT DEFAULT 0,
   assigned_algorithm VARCHAR(100),
   last_profiled_updated_timestamp DATETIME
);
ALTER TABLE
   discovered_ruleset ADD CONSTRAINT discovered_ruleset_pk
   PRIMARY KEY ("dataset", "specified_database", "specified_schema", "identified_table", "identified_column");

CREATE TABLE adf_data_mapping(
   source_dataset VARCHAR(255) NOT NULL,
   source_database VARCHAR(255) NOT NULL,
   source_schema VARCHAR(255) NOT NULL,
   source_table VARCHAR(255) NOT NULL,
   sink_dataset VARCHAR(255) NOT NULL,
   sink_database VARCHAR(255) NOT NULL,
   sink_schema VARCHAR(255) NOT NULL,
   sink_table VARCHAR(255) NOT NULL
);
ALTER TABLE
   adf_data_mapping ADD CONSTRAINT adf_data_mapping_pk
   PRIMARY KEY ("source_dataset", "source_database", "source_schema", "source_table");

CREATE TABLE adf_type_mapping(
   dataset VARCHAR(255) NOT NULL,
   dataset_type VARCHAR(255) NOT NULL,
   adf_type VARCHAR(255) NOT NULL
);
ALTER TABLE
   adf_type_mapping ADD CONSTRAINT adf_type_mapping_pk
   PRIMARY KEY ("dataset", "dataset_type");
INSERT INTO adf_type_mapping(dataset, dataset_type, adf_type)
   VALUES
   ('SNOWFLAKE', 'ARRAY', 'string'),
   ('SNOWFLAKE', 'BINARY', 'binary'),
   ('SNOWFLAKE', 'BOOLEAN','boolean'),
   ('SNOWFLAKE', 'DATE', 'date'),
   ('SNOWFLAKE', 'FLOAT', 'float'),
   ('SNOWFLAKE', 'GEOGRAPHY', 'string'),
   ('SNOWFLAKE', 'GEOMETRY', 'string'),
   ('SNOWFLAKE', 'NUMBER', 'float'),
   ('SNOWFLAKE', 'OBJECT', 'string'),
   ('SNOWFLAKE', 'TEXT', 'string'),
   ('SNOWFLAKE', 'TIME', 'string'),
   ('SNOWFLAKE', 'TIMESTAMP_LTZ', 'timestamp'),
   ('SNOWFLAKE', 'TIMESTAMP_NTZ', 'timestamp'),
   ('SNOWFLAKE', 'TIMESTAMP_TZ', 'timestamp'),
   ('SNOWFLAKE', 'VARIANT', 'string'),
   ('DATABRICKS', 'BOOLEAN', 'boolean'),
   ('DATABRICKS', 'INT', 'integer'),
   ('DATABRICKS', 'DOUBLE', 'double'),
   ('DATABRICKS', 'STRUCT', 'string'),
   ('DATABRICKS', 'LONG', 'long'),
   ('DATABRICKS', 'BINARY', 'binary'),
   ('DATABRICKS', 'TIMESTAMP', 'timestamp'),
   ('DATABRICKS', 'INTERVAL', 'string'),
   ('DATABRICKS', 'DECIMAL', 'integer'),
   ('DATABRICKS', 'ARRAY', 'string'),
   ('DATABRICKS', 'SHORT', 'integer'),
   ('DATABRICKS', 'DATE', 'date'),
   ('DATABRICKS', 'MAP', 'string'),
   ('DATABRICKS', 'FLOAT', 'float'),
   ('DATABRICKS', 'STRING', 'string')
;
-- source: V2024.04.18.0__add_adls_to_adls_support
CREATE PROCEDURE get_columns_from_adls_file_structure_sp
	@adf_file_structure NVARCHAR(MAX),
	@database NVARCHAR(MAX),
	@schema NVARCHAR(MAX),
	@table NVARCHAR(MAX),
	@column_delimiter NVARCHAR(1),
	@row_delimiter NVARCHAR(4),
	@quote_character NVARCHAR(1),
	@escape_character NVARCHAR(2),
	@first_row_as_header BIT,
	@null_value NVARCHAR(MAX)
AS
BEGIN
	IF @adf_file_structure IS NOT NULL AND LEN(@adf_file_structure) > 0
		-- Check if the input adf_file_structure is not NULL or empty
		BEGIN
			MERGE discovered_ruleset AS rs
			USING (
				SELECT 'ADLS' AS dataset,
					@database AS specified_database,
					@schema AS specified_schema,
					@table AS identified_table,
					structure.identified_column,
					structure.identified_column_type,
					-1 AS identified_column_max_length,
					with_idx.[key] AS ordinal_position,
					0 AS row_count,
					JSON_OBJECT(
						'metadata_version': 1,
						'column_delimiter': @column_delimiter,
						'row_delimiter': @row_delimiter,
						'quote_character': @quote_character,
						'escape_character': @escape_character,
						'first_row_as_header': @first_row_as_header,
						'null_value': @null_value,
						'persist_file_names': 'true'
					) AS metadata
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
						rs.metadata = adls_schema.metadata
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
					metadata
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
					adls_schema.metadata
				);
		END
	ELSE
		-- Handle NULL or empty adf_file_structure input
		BEGIN
			PRINT 'adf_file_structure is NULL or empty';
		END
END;
INSERT INTO adf_type_mapping(dataset, dataset_type, adf_type)
   VALUES
   ('ADLS', 'String', 'string');
-- source: V2024.05.02.0__update_adls_to_adls_support
ALTER PROCEDURE get_columns_from_adls_file_structure_sp
	@adf_file_structure NVARCHAR(MAX),
	@database NVARCHAR(MAX),
	@schema NVARCHAR(MAX),
	@table NVARCHAR(MAX),
	@column_delimiter NVARCHAR(1),
	@quote_character NVARCHAR(1),
	@escape_character NVARCHAR(2),
	@null_value NVARCHAR(MAX)
AS
BEGIN
	IF @adf_file_structure IS NOT NULL AND LEN(@adf_file_structure) > 0
		-- Check if the input adf_file_structure is not NULL or empty
		BEGIN
			MERGE discovered_ruleset AS rs
			USING (
				SELECT 'ADLS' AS dataset,
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
					) AS metadata
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
						rs.metadata = adls_schema.metadata
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
					metadata
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
					adls_schema.metadata
				);
		END
	ELSE
		-- Handle NULL or empty adf_file_structure input
		BEGIN
			PRINT 'adf_file_structure is NULL or empty';
		END
END;
-- source: V2024.08.25.0__add_conditional_masking_support
ALTER TABLE discovered_ruleset ALTER COLUMN assigned_algorithm VARCHAR(MAX);

-- source: V2024.10.24.0__add_checkpointing_and_logging
ALTER TABLE discovered_ruleset ADD
    discovery_complete BIT,
    latest_event UNIQUEIDENTIFIER;

ALTER TABLE adf_data_mapping ADD
    mapping_complete BIT,
    masked_status VARCHAR(MAX),
    latest_event UNIQUEIDENTIFIER;


CREATE TABLE adf_events_log (
    event_id UNIQUEIDENTIFIER NOT NULL,
    pipeline_run_id UNIQUEIDENTIFIER NOT NULL,
    activity_run_id UNIQUEIDENTIFIER NOT NULL,
    pipeline_name NVARCHAR(100),
    pipeline_type NVARCHAR(100),
    pipeline_success BIT,
    error_message VARCHAR(MAX),
    input_parameters VARCHAR(MAX),
    execution_start_time DATETIMEOFFSET,
    execution_end_time DATETIMEOFFSET,
    source_dataset VARCHAR(255),
    source_database VARCHAR(100),
    source_schema VARCHAR(100),
    source_table VARCHAR(100),
    source_metadata VARCHAR(MAX),
    sink_dataset VARCHAR(255),
    sink_database VARCHAR(255),
    sink_schema VARCHAR(100),
    sink_table VARCHAR(100),
    sink_metadata VARCHAR(MAX),
    filter_alias VARCHAR(255),
    filter_condition VARCHAR(MAX),
    last_inserted DATETIME DEFAULT getdate(),
CONSTRAINT adf_execution_log_pk PRIMARY KEY (event_id));

CREATE PROCEDURE insert_adf_discovery_event
(
    @pipeline_name NVARCHAR(100),
    @pipeline_run_id UNIQUEIDENTIFIER,
    @activity_run_id UNIQUEIDENTIFIER,
    @pipeline_success BIT,
    @error_message NVARCHAR(MAX),
    @input_parameters NVARCHAR(MAX),
    @execution_start_time DATETIMEOFFSET,
    @execution_end_time DATETIMEOFFSET,
    @source_dataset NVARCHAR(255),
    @source_database NVARCHAR(255),
    @source_schema NVARCHAR(255),
    @source_table NVARCHAR(255),
    @source_metadata NVARCHAR(MAX)
)
AS
BEGIN
    IF @pipeline_run_id IS NOT NULL AND len(@pipeline_run_id) > 0
        -- This is a valid event since there is a valid pipeline run ID
        BEGIN
            DECLARE @event_uuid UNIQUEIDENTIFIER;
            SET @event_uuid = NEWID();
            INSERT INTO adf_events_log
            (
                event_id,
                pipeline_run_id,
                activity_run_id,
                pipeline_name,
                pipeline_type,
                pipeline_success,
                error_message,
                input_parameters,
                execution_start_time,
                execution_end_time,
                source_dataset,
                source_database,
                source_schema,
                source_table,
                source_metadata
            )
            VALUES
            (
                @event_uuid,
                @pipeline_run_id,
                @activity_run_id,
                @pipeline_name,
                'DISCOVERY',
                @pipeline_success,
                @error_message,
                @input_parameters,
                @execution_start_time,
                @execution_end_time,
                @source_dataset,
                @source_database,
                @source_schema,
                @source_table,
                @source_metadata
            );
            UPDATE discovered_ruleset
            SET latest_event = @event_uuid,
                discovery_complete = @pipeline_success
            WHERE
                dataset = @source_dataset AND
                specified_database = @source_database AND
                specified_schema = @source_schema AND
                identified_table = @source_table;
        END
    ELSE
        -- The event isn't considered valid since there's no pipeline run ID
        BEGIN
            PRINT 'pipeline_run_id is invalid';
        END
END;

CREATE PROCEDURE insert_adf_masking_event
(
    @pipeline_name NVARCHAR(100),
    @pipeline_run_id UNIQUEIDENTIFIER,
    @activity_run_id UNIQUEIDENTIFIER,
    @is_masking_activity BIT,
    @pipeline_success BIT,
    @error_message NVARCHAR(MAX),
    @input_parameters NVARCHAR(MAX),
    @execution_start_time DATETIMEOFFSET,
    @execution_end_time DATETIMEOFFSET,
    @source_dataset NVARCHAR(255),
    @source_database NVARCHAR(255),
    @source_schema NVARCHAR(255),
    @source_table NVARCHAR(255),
    @source_metadata NVARCHAR(MAX),
    @sink_dataset NVARCHAR(255),
    @sink_database NVARCHAR(255),
    @sink_schema NVARCHAR(255),
    @sink_table NVARCHAR(255),
    @sink_metadata NVARCHAR(255),
    @filter_alias NVARCHAR(255),
    @filter_condition NVARCHAR(MAX)
)
AS
-- Begin stored procedure definition
BEGIN
    IF @pipeline_run_id IS NOT NULL AND len(@pipeline_run_id) > 0
    -- This is a valid event since there is a valid pipeline run ID
    BEGIN
        DECLARE @event_uuid UNIQUEIDENTIFIER;
        SET @event_uuid = NEWID();
        INSERT INTO adf_events_log
        (
            event_id,
            pipeline_run_id,
            activity_run_id,
            pipeline_name,
            pipeline_type,
            pipeline_success,
            error_message,
            input_parameters,
            execution_start_time,
            execution_end_time,
            source_dataset,
            source_database,
            source_schema,
            source_table,
            source_metadata,
            sink_dataset,
            sink_database,
            sink_schema,
            sink_table,
            sink_metadata,
            filter_alias,
            filter_condition
        )
        VALUES
        (
            @event_uuid,
            @pipeline_run_id,
            @activity_run_id,
            @pipeline_name,
            CASE
                WHEN @is_masking_activity = 1 THEN 'MASK'
                ELSE 'COPY'
            END,
            @pipeline_success,
            @error_message,
            @input_parameters,
            @execution_start_time,
            @execution_end_time,
            @source_dataset,
            @source_database,
            @source_schema,
            @source_table,
            @source_metadata,
            @sink_dataset,
            @sink_database,
            @sink_schema,
            @sink_table,
            @sink_metadata,
            @filter_alias,
            @filter_condition
        );
        IF @filter_alias IS NOT NULL AND len(@filter_alias) > 0
            -- A filter was applied to this table while masking, update the masked status for this filter
            BEGIN
                UPDATE adf_data_mapping
                SET latest_event = @event_uuid,
                    masked_status = JSON_MODIFY(masked_status, '$.' + @filter_alias, @pipeline_success)
                WHERE
                    source_dataset = @source_dataset AND
                    source_database = @source_database AND
                    source_schema = @source_schema AND
                    source_table = @source_table AND
                    sink_dataset = @sink_dataset AND
                    sink_database = @sink_database AND
                    sink_schema = @sink_schema AND
                    sink_table = @sink_table;
                -- Flip the mapping_complete bit if needed
                WITH
                filters_as_yet_unmasked AS
                (
                    SELECT kc.[key]
                    FROM adf_data_mapping dm
                        CROSS APPLY OPENJSON(dm.masked_status) kc
                    WHERE ISJSON(dm.masked_status) = 1
                        AND source_dataset = @source_dataset
                        AND source_database = @source_database
                        AND source_schema = @source_schema
                        AND source_table = @source_table
                        AND kc.value = 'false'
                ),
                count_of_filters_unmasked AS
                (
                    SELECT
                        COUNT(1) as remaining_filters
                    FROM
                        filters_as_yet_unmasked
                )
                UPDATE adf_data_mapping
                SET mapping_complete = CASE
                                    WHEN ((SELECT remaining_filters FROM count_of_filters_unmasked) = 0)
                                        THEN 1
                                        ELSE 0
                                    END
                WHERE
                    source_dataset = @source_dataset AND
                    source_database = @source_database AND
                    source_schema = @source_schema AND
                    source_table = @source_table AND
                    sink_dataset = @sink_dataset AND
                    sink_database = @sink_database AND
                    sink_schema = @sink_schema AND
                    sink_table = @sink_table;
            END
        ELSE
            BEGIN
                UPDATE adf_data_mapping
                SET latest_event = @event_uuid,
                    mapping_complete = @pipeline_success
                WHERE
                    source_dataset = @source_dataset AND
                    source_database = @source_database AND
                    source_schema = @source_schema AND
                    source_table = @source_table AND
                    sink_dataset = @sink_dataset AND
                    sink_database = @sink_database AND
                    sink_schema = @sink_schema AND
                    sink_table = @sink_table;
            END
        END
    ELSE
        -- The event isn't considered valid since there's no pipeline run ID
        BEGIN
            PRINT 'pipeline_run_id is invalid';
        END
-- End stored procedure definition
END;

-- source: V2024.12.02.0__add_azuresql_to_azuresql_support
INSERT INTO adf_type_mapping(dataset, dataset_type, adf_type)
   VALUES
('AZURESQL', 'tinyint', 'integer'),
('AZURESQL', 'smallint', 'short'),
('AZURESQL', 'int', 'integer'),
('AZURESQL', 'bigint', 'long'),
('AZURESQL', 'bit', 'boolean'),
('AZURESQL', 'decimal', 'decimal'),
('AZURESQL', 'numeric', 'decimal'),
('AZURESQL', 'money', 'decimal'),
('AZURESQL', 'smallmoney', 'decimal'),
('AZURESQL', 'float', 'double'),
('AZURESQL', 'real', 'float'),
('AZURESQL', 'date', 'date'),
('AZURESQL', 'time', 'timestamp'),
('AZURESQL', 'datetime2', 'timestamp'),
('AZURESQL', 'datetimeoffset', 'string'),
('AZURESQL', 'datetime', 'timestamp'),
('AZURESQL', 'smalldatetime', 'timestamp'),
('AZURESQL', 'char', 'string'),
('AZURESQL', 'varchar', 'string'),
('AZURESQL', 'text', 'string'),
('AZURESQL', 'nchar', 'string'),
('AZURESQL', 'nvarchar', 'string'),
('AZURESQL', 'ntext', 'string'),
('AZURESQL', 'binary', 'binary'),
('AZURESQL', 'varbinary', 'binary'),
('AZURESQL', 'image', 'binary'),
('AZURESQL', 'json', 'string'),
('AZURESQL', 'uniqueidentifier', 'string'),
('AZURESQL', 'xml', 'string')
;

-- source: V2024.12.13.0__create_create_constraints_table
CREATE TABLE capture_constraints (
    pipeline_run_id UNIQUEIDENTIFIER NOT NULL,
    dataset VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    specified_database VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    specified_schema VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    identified_parent_table VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    child_table VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    constraint_name VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    parent_columns VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    children_columns VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    pre_drop_status VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    drop_error_message NVARCHAR(MAX),
    drop_timestamp DATETIME,
    post_create_status VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    create_error_message NVARCHAR(MAX),
    create_timestamp DATETIME,
    CONSTRAINT capture_constraints_pk PRIMARY KEY (
        pipeline_run_id,
        dataset,
        specified_database,
        specified_schema,
        identified_parent_table,
        child_table,
        constraint_name
    )
);

-- source: V2024.12.26.0__add_adls_to_adls_parquet_support
CREATE PROCEDURE get_columns_from_parquet_file_structure_sp
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
                    ) AS metadata
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
                    rs.metadata = parquet_schema.metadata
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
                    metadata
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
                    parquet_schema.metadata
                );
        END
    ELSE
        BEGIN
            -- Handle NULL or empty adf_file_structure input
            PRINT 'adf_file_structure is NULL or empty';
        END
END;

INSERT INTO adf_type_mapping(dataset, dataset_type, adf_type)
   VALUES
   ('ADLS-PARQUET', 'Binary', 'binary'),
   ('ADLS-PARQUET', 'Boolean', 'boolean'),
   ('ADLS-PARQUET', 'Date', 'date'),
   ('ADLS-PARQUET', 'Decimal', 'float'),
   ('ADLS-PARQUET', 'Double', 'float'),
   ('ADLS-PARQUET', 'DateTime', 'date'),
   ('ADLS-PARQUET', 'Float', 'float'),
   ('ADLS-PARQUET', 'Int96', 'long'),
   ('ADLS-PARQUET', 'Int64', 'long'),
   ('ADLS-PARQUET', 'Int32', 'integer'),
   ('ADLS-PARQUET', 'Map', 'string'),
   ('ADLS-PARQUET', 'Simple', 'string'),
   ('ADLS-PARQUET', 'String', 'string'),
   ('ADLS-PARQUET', 'Time', 'timestamp'),
   ('ADLS-PARQUET', 'Timestamp', 'timestamp');

-- Drop existing procedure that is specific to delimited files
DROP PROCEDURE get_columns_from_adls_file_structure_sp;

-- Recreate procedure that is specific to delimited files
CREATE PROCEDURE get_columns_from_delimited_file_structure_sp
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
                    ) AS metadata
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
                        rs.metadata = adls_schema.metadata
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
                    metadata
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
                    adls_schema.metadata
                );
        END
    ELSE
        -- Handle NULL or empty adf_file_structure input
        BEGIN
            PRINT 'adf_file_structure is NULL or empty';
        END
END;

-- BEGIN: Rename the ADLS dataset to ADLS-DELIMITED to avoid confusion
-- Update this in the discovered ruleset table
UPDATE discovered_ruleset
SET
    dataset = 'ADLS-DELIMITED'
WHERE dataset='ADLS';

-- Update this in the type mapping table
UPDATE adf_type_mapping
SET
    dataset = 'ADLS-DELIMITED'
WHERE dataset='ADLS';

-- Update this for source datasets in the data mapping table
UPDATE adf_data_mapping
SET
    source_dataset = 'ADLS-DELIMITED'
WHERE source_dataset='ADLS';

-- Update this for the sink datasets in the data mapping table
UPDATE adf_data_mapping
SET
    sink_dataset = 'ADLS-DELIMITED'
WHERE sink_dataset = 'ADLS';
-- END: Rename the ADLS dataset to ADLS-DELIMITED to avoid confusion


-- source: V20205.01.15.0__separate_algorithm_and_source_metadata
ALTER TABLE [dbo].[discovered_ruleset] ADD algorithm_metadata NVARCHAR(MAX);

-- Move the `date_format` key/value pair out of the metadata column and into the `algorithm_metadata` column
UPDATE [dbo].[discovered_ruleset]
SET
    algorithm_metadata = JSON_MODIFY(COALESCE(algorithm_metadata,'{}'), '$.date_format', JSON_VALUE(metadata, '$.date_format')),
    metadata = JSON_MODIFY(metadata, '$.date_format', NULL)
WHERE
    JSON_VALUE(metadata, '$.date_format') IS NOT NULL;
-- Move the `key_column` key/value pair out of the metadata column and into the `algorithm_metadata` column
UPDATE [dbo].[discovered_ruleset]
SET
    algorithm_metadata = JSON_MODIFY(COALESCE(algorithm_metadata,'{}'), '$.key_column', JSON_VALUE(metadata, '$.key_column')),
    metadata = JSON_MODIFY(metadata, '$.key_column', NULL)
WHERE
    JSON_VALUE(metadata, '$.key_column') IS NOT NULL;
-- Move the `conditions` key/value pair out of the metadata column and into the `algorithm_metadata` column
UPDATE [dbo].[discovered_ruleset]
SET
    algorithm_metadata = JSON_MODIFY(COALESCE(algorithm_metadata,'{}'), '$.conditions', JSON_VALUE(metadata, '$.conditions')),
    metadata = JSON_MODIFY(metadata, '$.conditions', NULL)
WHERE
    JSON_VALUE(metadata, '$.conditions') IS NOT NULL;

EXEC sp_rename 'dbo.discovered_ruleset.metadata', 'source_metadata', 'COLUMN';

-- Update stored procedure to use new source_metadata column
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
						rs.ource_metadata = adls_schema.source_metadata
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

-- source: V2024.12.06.0__add_adls_to_adls_parquet_support
CREATE PROCEDURE get_columns_from_parquet_file_structure_sp
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
                    ) AS metadata
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
                    rs.metadata = parquet_schema.metadata
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
                    metadata
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
                    parquet_schema.metadata
                );
        END
    ELSE
        BEGIN
            -- Handle NULL or empty adf_file_structure input
            PRINT 'adf_file_structure is NULL or empty';
        END
END;

INSERT INTO adf_type_mapping(dataset, dataset_type, adf_type)
   VALUES
   ('ADLS-PARQUET', 'Binary', 'binary'),
   ('ADLS-PARQUET', 'Boolean', 'boolean'),
   ('ADLS-PARQUET', 'Date', 'date'),
   ('ADLS-PARQUET', 'Decimal', 'float'),
   ('ADLS-PARQUET', 'Double', 'float'),
   ('ADLS-PARQUET', 'DateTime', 'date'),
   ('ADLS-PARQUET', 'Float', 'float'),
   ('ADLS-PARQUET', 'Int96, 'long'),
   ('ADLS-PARQUET', 'Int64', 'long'),
   ('ADLS-PARQUET', 'Int32', 'integer'),
   ('ADLS-PARQUET', 'Map', 'string'),
   ('ADLS-PARQUET', 'Simple', 'string'),
   ('ADLS-PARQUET', 'String', 'string'),
   ('ADLS-PARQUET', 'Time', 'timestamp'),
   ('ADLS-PARQUET', 'Timestamp', 'timestamp');

-- Drop existing procedure that is specific to delimited files
DROP PROCEDURE get_columns_from_adls_file_structure_sp;

-- Recreate procedure that is specific to delimited files
CREATE PROCEDURE get_columns_from_delimited_file_structure_sp
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
                    ) AS metadata
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
                        rs.metadata = adls_schema.metadata
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
                    metadata
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
                    adls_schema.metadata
                );
        END
    ELSE
        -- Handle NULL or empty adf_file_structure input
        BEGIN
            PRINT 'adf_file_structure is NULL or empty';
        END
END;

-- BEGIN: Rename the ADLS dataset to ADLS-DELIMITED to avoid confusion
-- Update this in the discovered ruleset table
UPDATE discovered_ruleset
SET
    dataset = 'ADLS-DELIMITED'
WHERE dataset='ADLS';

-- Update this in the type mapping table
UPDATE adf_type_mapping
SET
    dataset = 'ADLS-DELIMITED'
WHERE dataset='ADLS';

-- Update this for source datasets in the data mapping table
UPDATE adf_data_mapping
SET
    source_dataset = 'ADLS-DELIMITED'
WHERE source_dataset='ADLS';

-- Update this for the sink datasets in the data mapping table
UPDATE adf_data_mapping
SET
    sink_dataset = 'ADLS-DELIMITED'
WHERE sink_dataset = 'ADLS';
-- END: Rename the ADLS dataset to ADLS-DELIMITED to avoid confusion

