-- This file includes the queries from the following scripts:
-- * V2024.01.01.0__create_initial_tables
-- * V2024.04.18.0__add_adls_to_adls_support
-- * V2024.05.02.0__update_adls_to_adls_support
-- * V2024.08.25.0__add_conditional_masking_support
-- * V2024.10.24.0__add_checkpointing_and_logging
-- * V2024.12.02.0__add_azuresql_to_azuresql_support
-- * V2024.12.13.0__create_create_constraints_table
-- * V2024.12.26.0__add_adls_to_adls_parquet_support
-- * V2025.01.15.0__separate_algorithm_and_source_metadata
-- * V2025.01.30.0__create_constraints_stored_procedure
-- * V2025.02.04.0__add_azuremi_to_azuremi_support
-- * V2025.02.24.0__add_azuremi_adf_type_mapping
-- * V2025.07.22.0__add_generate_masking_parameters_procedure
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


-- source: V2025.01.15.0__separate_algorithm_and_source_metadata
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

-- source: V2025.01.30.0__create_constraints_stored_procedure

-- Create a table type to store constraint information
-- before dropping the constraints
CREATE TYPE sink_drop_constraint_type AS TABLE
(
    pipeline_run_id NVARCHAR(255),
    dataset NVARCHAR(255),
    specified_database NVARCHAR(255),
    specified_schema NVARCHAR(255),
    identified_parent_table NVARCHAR(255),
    child_table NVARCHAR(255),
    constraint_name NVARCHAR(255),
    children_columns NVARCHAR(MAX),
    parent_columns NVARCHAR(MAX),
    pre_drop_status NVARCHAR(255),
    drop_timestamp DATETIME,
    post_create_status NVARCHAR(255)
);

-- Create a stored procedure to insert constraint information
-- into the capture_constraints table
CREATE PROCEDURE insert_constraints
    @source sink_drop_constraint_type READONLY
AS
BEGIN
    MERGE capture_constraints AS ct
    USING (
        SELECT 
            sc.pipeline_run_id,
            sc.dataset, 
            sc.specified_database, 
            sc.specified_schema, 
            sc.identified_parent_table, 
            sc.child_table, 
            sc.constraint_name, 
            sc.children_columns, 
            sc.parent_columns, 
            sc.pre_drop_status, 
            sc.drop_timestamp, 
            sc.post_create_status,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    sc.pipeline_run_id, 
                    sc.dataset, 
                    sc.specified_database, 
                    sc.specified_schema, 
                    sc.identified_parent_table, 
                    sc.child_table, 
                    sc.constraint_name
                ORDER BY 
                    sc.constraint_name
            ) AS row_num 
        FROM @source sc
        INNER JOIN adf_data_mapping dm
        ON (
            sc.dataset = dm.sink_dataset 
            AND sc.specified_database = dm.sink_database 
            AND sc.specified_schema = dm.sink_schema 
            AND (
                sc.identified_parent_table = dm.sink_table 
                OR sc.child_table = dm.sink_table 
            )
            AND (dm.mapping_complete IS NULL OR dm.mapping_complete = 0)
        )
    ) AS sink_constraints
    ON (
        ct.pipeline_run_id = sink_constraints.pipeline_run_id
        AND ct.dataset = sink_constraints.dataset
        AND ct.specified_database = sink_constraints.specified_database
        AND LOWER(ct.specified_schema) = LOWER(sink_constraints.specified_schema)
        AND ct.child_table = sink_constraints.child_table
        AND ct.identified_parent_table = sink_constraints.identified_parent_table
        AND ct.constraint_name = sink_constraints.constraint_name
    )
    WHEN NOT MATCHED AND sink_constraints.row_num = 1 THEN
        INSERT (
            pipeline_run_id,
            dataset,
            specified_database,
            specified_schema,
            identified_parent_table,
            child_table,
            constraint_name,
            children_columns,
            parent_columns,
            pre_drop_status,
            drop_timestamp,
            post_create_status
        )
        VALUES (
            sink_constraints.pipeline_run_id,
            sink_constraints.dataset,
            sink_constraints.specified_database,
            sink_constraints.specified_schema,
            sink_constraints.identified_parent_table,
            sink_constraints.child_table,
            sink_constraints.constraint_name,
            sink_constraints.children_columns,
            sink_constraints.parent_columns,
            sink_constraints.pre_drop_status,
            sink_constraints.drop_timestamp,
            sink_constraints.post_create_status
        );
END;

-- Create a table type to store constraint information
-- after creating the constraints
CREATE TYPE sink_create_constraint_type AS TABLE
(
    pipeline_run_id NVARCHAR(255),
    dataset NVARCHAR(255),
    specified_database NVARCHAR(255),
    specified_schema NVARCHAR(255),
    identified_parent_table NVARCHAR(255),
    child_table NVARCHAR(255),
    constraint_name NVARCHAR(255),
    children_columns NVARCHAR(MAX),
    parent_columns NVARCHAR(MAX),
    post_create_status NVARCHAR(255),
    create_timestamp DATETIME
);

-- Create a stored procedure to update the status of constraints
-- in the capture_constraints table after they are created
CREATE PROCEDURE update_constraints_status
    @source sink_create_constraint_type READONLY
AS
BEGIN
    MERGE capture_constraints AS ct
    USING @source AS sink_constraints
    ON
    (
        ct.pipeline_run_id = sink_constraints.pipeline_run_id
        AND ct.dataset = sink_constraints.dataset
        AND ct.specified_database = sink_constraints.specified_database
        AND LOWER(ct.specified_schema) = LOWER(sink_constraints.specified_schema)
        AND ct.child_table = sink_constraints.child_table
        AND ct.identified_parent_table = sink_constraints.identified_parent_table
        AND ct.constraint_name = sink_constraints.constraint_name
    )
    WHEN MATCHED THEN
        UPDATE
        SET
            ct.post_create_status = sink_constraints.post_create_status,
            ct.create_timestamp = sink_constraints.create_timestamp;
END;

-- source: V2025.02.04.0__add_azuremi_to_azuremi_support

-- Update ADF type mappings of decimal, numeric and money to double for AzureSQL dataset
UPDATE adf_type_mapping
SET adf_type = 'double'
WHERE dataset = 'AZURESQL' AND dataset_type IN ('decimal', 'numeric', 'money');

-- Update ADF type mapping of smallmoney to float for AzureSQL dataset
UPDATE adf_type_mapping
SET adf_type = 'float'
WHERE dataset = 'AZURESQL' AND dataset_type = 'smallmoney';

-- Copy ADF type mapping from AzureSQL to AzureSQL_MI
INSERT INTO adf_type_mapping(dataset, dataset_type, adf_type)
SELECT 'AZURESQL-MI', dataset_type, adf_type
FROM adf_type_mapping
WHERE dataset = 'AZURESQL';


-- source: V2025.02.24.0__add_azuremi_adf_type_mapping
DELETE from adf_type_mapping where dataset = 'AZURESQL-MI';

INSERT INTO adf_type_mapping(dataset, dataset_type, adf_type)
   VALUES
('AZURESQL-MI', 'tinyint', 'integer'),
('AZURESQL-MI', 'smallint', 'short'),
('AZURESQL-MI', 'int', 'integer'),
('AZURESQL-MI', 'bigint', 'long'),
('AZURESQL-MI', 'bit', 'boolean'),
('AZURESQL-MI', 'decimal', 'double'),
('AZURESQL-MI', 'numeric', 'double'),
('AZURESQL-MI', 'money', 'double'),
('AZURESQL-MI', 'smallmoney', 'float'),
('AZURESQL-MI', 'float', 'double'),
('AZURESQL-MI', 'real', 'float'),
('AZURESQL-MI', 'date', 'date'),
('AZURESQL-MI', 'time', 'timestamp'),
('AZURESQL-MI', 'datetime2', 'timestamp'),
('AZURESQL-MI', 'datetimeoffset', 'string'),
('AZURESQL-MI', 'datetime', 'timestamp'),
('AZURESQL-MI', 'smalldatetime', 'timestamp'),
('AZURESQL-MI', 'char', 'string'),
('AZURESQL-MI', 'varchar', 'string'),
('AZURESQL-MI', 'text', 'string'),
('AZURESQL-MI', 'nchar', 'string'),
('AZURESQL-MI', 'nvarchar', 'string'),
('AZURESQL-MI', 'ntext', 'string'),
('AZURESQL-MI', 'binary', 'binary'),
('AZURESQL-MI', 'varbinary', 'binary'),
('AZURESQL-MI', 'image', 'binary'),
('AZURESQL-MI', 'json', 'string'),
('AZURESQL-MI', 'uniqueidentifier', 'string'),
('AZURESQL-MI', 'xml', 'string')
;


-- source: V2025.07.22.0__add_generate_masking_parameters_procedure
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
CREATE OR ALTER PROCEDURE generate_masking_parameters
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
        FROM discovered_ruleset r
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
        FROM adf_type_mapping t
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

