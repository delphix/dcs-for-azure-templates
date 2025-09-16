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
-- * V2025.08.28.0__add_dataverse_to_dataverse_in_place_support
-- * V2025.09.10.0__add_generate_dataverse_masking_parameters_procedure
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
 * ERROR HANDLING:
 * If no masking rules exist for the specified table and filter alias, an error is raised and
 * no record is returned. This prevents silent failures and ensures pipeline errors are visible.
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
    DECLARE @filter_alias_display NVARCHAR(128);
    SET
        @filter_alias_display = CASE
            WHEN @filter_alias = ''
                THEN 'No filter set'
            ELSE @filter_alias
        END;
    DECLARE @msg NVARCHAR(400);
    SET
        @msg = 'No masking rules found for the specified parameters. Ensure the '
        + 'discovered_ruleset table has valid entries for dataset: %s, '
        + 'database: %s, schema: %s, table: %s, filter: %s.';

    -- Check if the discovered_ruleset table has valid entries for the specified parameters
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
                    AND (LEFT(LTRIM(r.assigned_algorithm), 1) = '[')
                )
                AND (
                    @filter_alias = ''
                    OR filter_check.filter_found = 1
                )
        )
        BEGIN
            RAISERROR (
                @msg,
                16, 1, -- Severity and State
                @dataset,
                @specified_database,
                @specified_schema,
                @identified_table,
                @filter_alias_display
            );
            RETURN;
        END;

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
            AND NOT (
                ISJSON(r.assigned_algorithm) = 1
                AND (LEFT(LTRIM(r.assigned_algorithm), 1) = '[')
            )
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
                    '"'
                    + LOWER(encoded_column_name)
                    + '":"'
                    + REPLACE(final_date_format, '''', '\''')
                    + '"',
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
        FROM base_ruleset_filter
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


-- source: V2025.08.28.0__add_dataverse_to_dataverse_in_place_support
-- Add a default constraint to the 'is_excluded' column in the 'discovered_ruleset' table.
ALTER TABLE discovered_ruleset
ADD is_excluded BIT NOT NULL
CONSTRAINT df_discovered_ruleset_is_excluded DEFAULT (0);

INSERT INTO adf_type_mapping (dataset, dataset_type, adf_type)
VALUES
('DATAVERSE', 'BigInt', 'long'),
('DATAVERSE', 'Boolean', 'boolean'),
('DATAVERSE', 'DateOnly', 'timestamp'),
('DATAVERSE', 'DateTime', 'timestamp'),
('DATAVERSE', 'Decimal', 'double'),
('DATAVERSE', 'Double', 'double'),
('DATAVERSE', 'File', 'binary'),
('DATAVERSE', 'Image', 'binary'),
('DATAVERSE', 'Integer', 'integer'),
('DATAVERSE', 'Memo', 'string'),
('DATAVERSE', 'Money', 'double'),
('DATAVERSE', 'MultiSelectPicklist', 'string'),
('DATAVERSE', 'Picklist', 'string'),
('DATAVERSE', 'String', 'string'),
('DATAVERSE', 'Uniqueidentifier', 'string');


-- source: V2025.09.10.0__add_generate_dataverse_masking_parameters_procedure
ÿþ/ *  
   *   g e n e r a t e _ d a t a v e r s e _ m a s k i n g _ p a r a m e t e r s   -   G e n e r a t e s   m a s k i n g   p a r a m e t e r s   f o r   A z u r e   D a t a   F a c t o r y   d a t a f l o w s .  
   *  
   *   T h i s   s t o r e d   p r o c e d u r e   r e p l a c e s   t h e   A D F   d a t a f l o w s   p r e v i o u s l y   u s e d   f o r   g e n e r a t i n g  
   *   m a s k i n g   p a r a m e t e r s ,   p r o v i d i n g   i m p r o v e d   p e r f o r m a n c e   a n d   m a i n t a i n a b i l i t y .   I t   r e c o n s t r u c t s  
   *   t h e   r u l e s e t   l o g i c   b y   f i l t e r i n g   f o r   t h e   a p p r o p r i a t e   a s s i g n e d   a l g o r i t h m s   a n d  
   *   c o n d i t i o n a l   a s s i g n m e n t s   ( w h e r e   a p p l i c a b l e ) ,   a n d   g e n e r a t e s   a l l   n e c e s s a r y   p a r a m e t e r s  
   *   f o r   m a s k i n g   a   s p e c i f i c   t a b l e   u s i n g   D e l p h i x   C o m p l i a n c e   S e r v i c e s   A P I s   w i t h i n  
   *   A z u r e   D a t a   F a c t o r y   p i p e l i n e s .  
   *  
   *   P A R A M E T E R S :  
   *   @ d a t a s e t   -   D a t a s e t   t y p e   i d e n t i f i e r  
   *   @ s p e c i f i e d _ d a t a b a s e   -   S o u r c e   d a t a b a s e   n a m e  
   *   @ s p e c i f i e d _ s c h e m a   -   S o u r c e   s c h e m a   n a m e  
   *   @ i d e n t i f i e d _ t a b l e   -   S o u r c e   t a b l e   n a m e  
   *   @ c o l u m n _ w i d t h _ e s t i m a t e   -   E s t i m a t e d   c o l u m n   w i d t h   f o r   b a t c h   c a l c u l a t i o n s   ( d e f a u l t :   1 0 0 0 )  
   *   @ f i l t e r _ a l i a s   -   O p t i o n a l   f i l t e r   k e y   f o r   c o n d i t i o n a l   m a s k i n g   ( d e f a u l t :   ' ' )  
   *  
   *   O U T P U T   P A R A M E T E R S :  
   *   -   F i e l d A l g o r i t h m A s s i g n m e n t s :   J S O N   m a p p i n g   o f   e n c o d e d   c o l u m n   n a m e s   t o   m a s k i n g   a l g o r i t h m s  
   *   -   C o l u m n s T o M a s k :   J S O N   a r r a y   o f   c o l u m n   n a m e s   r e q u i r i n g   m a s k i n g  
   *   -   D a t a F a c t o r y T y p e M a p p i n g :   A D F   s c h e m a   d e f i n i t i o n   f o r   A P I   r e s p o n s e   p a r s i n g  
   *   -   N u m b e r O f B a t c h e s :   C a l c u l a t e d   b a t c h   c o u n t   f o r   o p t i m a l   p r o c e s s i n g  
   *   -   T r i m L e n g t h s :   J S O N   a r r a y   o f   c o l u m n   m a x   l e n g t h s   f o r   o u t p u t   t r i m m i n g  
   *   -   D a t e F o r m a t A s s i g n m e n t s :   J S O N   m a p p i n g   o f   c o l u m n s   t o   t h e i r   d a t e   f o r m a t   s t r i n g s  
   *   -   C o l u m n s T o C a s t A s S t r i n g s :   J S O N   a r r a y   o f   c o l u m n s   r e q u i r i n g   s t r i n g   c a s t i n g  
   *   -   C o l u m n s T o C a s t B a c k T o * :   J S O N   a r r a y s   o f   c o l u m n s   t o   c a s t   b a c k   t o   s p e c i f i c   A D F   t y p e s  
   *   -   C o l u m n s _ t o _ r e m o v e :   J S O N   a r r a y   o f   e x c l u d e d   c o l u m n s   ( w h e r e   i s _ e x c l u d e d   =   1 )  
   *   -   D a t e _ o n l y _ c o l u m n s :   J S O N   a r r a y   o f   c o l u m n s   w i t h   t y p e   ' D a t e O n l y '  
   *   -   P r i m a r y _ i d _ c o l u m n s :   C o m m a - s e p a r a t e d   l i s t   o f   p r i m a r y   I D   c o l u m n s   ( I s P r i m a r y I d   =   t r u e ,   I s L o g i c a l   =   f a l s e )  
   *  
   *   D E P E N D E N C I E S :  
   *   -   d i s c o v e r e d _ r u l e s e t   t a b l e :   C o n t a i n s   m a s k i n g   r u l e s ,   m e t a d a t a ,   a n d   a l g o r i t h m   a s s i g n m e n t s  
   *   -   a d f _ t y p e _ m a p p i n g   t a b l e :   M a p s   d a t a b a s e   t y p e s   t o   A D F   t y p e s  
   *   -   S u p p o r t s   c o n d i t i o n a l   m a s k i n g   v i a   J S O N   s t r u c t u r e s   i n   a s s i g n e d _ a l g o r i t h m   c o l u m n  
   *  
   *   E R R O R   H A N D L I N G :  
   *   I f   n o   m a s k i n g   r u l e s   e x i s t   f o r   t h e   s p e c i f i e d   t a b l e   a n d   f i l t e r   a l i a s ,   a n   e r r o r   i s   r a i s e d   a n d  
   *   n o   r e c o r d   i s   r e t u r n e d .   T h i s   p r e v e n t s   s i l e n t   f a i l u r e s   a n d   e n s u r e s   p i p e l i n e   e r r o r s   a r e   v i s i b l e .  
   *  
   *   N O T E S :  
   *   -   E m p t y   J S O N   a r r a y s   a r e   c o n v e r t e d   t o   [ " " ]   f o r   A D F   U I   c o m p a t i b i l i t y  
   *   -   U s e s   h e x - e n c o d e d   c o l u m n   n a m e s   t o   a v o i d   A D F   p i p e l i n e   i s s u e s   w i t h   s p e c i a l   c h a r a c t e r s  
   *   -   S u p p o r t s   t r e a t _ a s _ s t r i n g   f l a g   f o r   t y p e   c a s t i n g   f e a t u r e  
   *   -   I n c l u d e s   e x p l i c i t   h a n d l i n g   f o r   e x c l u d e d   c o l u m n s ,   D a t e O n l y   c o l u m n s ,   a n d   p r i m a r y   I D   c o l u m n s  
   * /  
  
 C R E A T E   O R   A L T E R   P R O C E D U R E   g e n e r a t e _ d a t a v e r s e _ m a s k i n g _ p a r a m e t e r s  
         @ d a t a s e t   N V A R C H A R ( 1 2 8 ) ,  
         @ s p e c i f i e d _ d a t a b a s e   N V A R C H A R ( 1 2 8 ) ,  
         @ s p e c i f i e d _ s c h e m a   N V A R C H A R ( 1 2 8 ) ,  
         @ i d e n t i f i e d _ t a b l e   N V A R C H A R ( 1 2 8 ) ,  
         @ c o l u m n _ w i d t h _ e s t i m a t e   I N T   =   1 0 0 0 ,  
         @ f i l t e r _ a l i a s   N V A R C H A R ( 1 2 8 )   =   ' '     - -   O p t i o n a l   f i l t e r   k e y   f o r   c o n d i t i o n a l   m a s k i n g  
 A S  
 B E G I N  
         S E T   N O C O U N T   O N ;  
         D E C L A R E   @ S t o r e d P r o c e d u r e V e r s i o n   V A R C H A R ( 1 3 )   =   ' V 2 0 2 5 . 0 9 . 0 8 . 0 ' ;  
         D E C L A R E   @ f i l t e r _ a l i a s _ d i s p l a y   N V A R C H A R ( 1 2 8 ) ;  
         S E T  
                 @ f i l t e r _ a l i a s _ d i s p l a y   =   C A S E  
                         W H E N   @ f i l t e r _ a l i a s   =   ' '  
                                 T H E N   ' N o   f i l t e r   s e t '  
                         E L S E   @ f i l t e r _ a l i a s  
                 E N D ;  
         D E C L A R E   @ m s g   N V A R C H A R ( 4 0 0 ) ;  
         S E T  
                 @ m s g   =   ' N o   m a s k i n g   r u l e s   f o u n d   f o r   t h e   s p e c i f i e d   p a r a m e t e r s .   E n s u r e   t h e   '  
                 +   ' d i s c o v e r e d _ r u l e s e t   t a b l e   h a s   v a l i d   e n t r i e s   f o r   d a t a s e t :   % s ,   '  
                 +   ' d a t a b a s e :   % s ,   s c h e m a :   % s ,   t a b l e :   % s ,   f i l t e r :   % s . ' ;  
          
         - -   C h e c k   i f   t h e   d i s c o v e r e d _ r u l e s e t   t a b l e   h a s   v a l i d   e n t r i e s   f o r   t h e   s p e c i f i e d   p a r a m e t e r s  
         I F  
                 N O T   E X I S T S   (  
                         S E L E C T   1  
                         F R O M   d i s c o v e r e d _ r u l e s e t   A S   r  
                         O U T E R   A P P L Y   (  
                                 S E L E C T   1   A S   f i l t e r _ f o u n d  
                                 F R O M  
                                         O P E N J S O N   ( r . a s s i g n e d _ a l g o r i t h m )  
                                         W I T H   (  
                                                 [ k e y _ c o l u m n ]   N V A R C H A R ( 2 5 5 )   ' $ . k e y _ c o l u m n ' ,  
                                                 [ c o n d i t i o n s ]   N V A R C H A R ( M A X )   ' $ . c o n d i t i o n s '   A S   J S O N  
                                         )   A S   a a  
                                 C R O S S   A P P L Y  
                                         O P E N J S O N   ( a a . [ c o n d i t i o n s ] )  
                                         W I T H   (  
                                                 [ c o n d i t i o n _ a l i a s ]   N V A R C H A R ( 2 5 5 )   ' $ . a l i a s ' ,  
                                                 [ a l g o r i t h m ]   N V A R C H A R ( 2 5 5 )   ' $ . a l g o r i t h m '  
                                         )   A S   c o n d  
                                 W H E R E  
                                         r . a s s i g n e d _ a l g o r i t h m   I S   N O T   N U L L  
                                         A N D   r . a s s i g n e d _ a l g o r i t h m   < >   ' '  
                                         A N D   I S J S O N ( r . a s s i g n e d _ a l g o r i t h m )   =   1  
                                         A N D   J S O N _ V A L U E ( r . a s s i g n e d _ a l g o r i t h m ,   ' $ . k e y _ c o l u m n ' )   I S   N O T   N U L L  
                                         A N D   c o n d . [ c o n d i t i o n _ a l i a s ]   I S   N O T   N U L L  
                                         A N D   c o n d . [ c o n d i t i o n _ a l i a s ]   =   @ f i l t e r _ a l i a s  
                         )   A S   f i l t e r _ c h e c k  
                         W H E R E  
                                 r . d a t a s e t   =   @ d a t a s e t  
                                 A N D   r . s p e c i f i e d _ d a t a b a s e   =   @ s p e c i f i e d _ d a t a b a s e  
                                 A N D   r . s p e c i f i e d _ s c h e m a   =   @ s p e c i f i e d _ s c h e m a  
                                 A N D   r . i d e n t i f i e d _ t a b l e   =   @ i d e n t i f i e d _ t a b l e  
                                 A N D   r . a s s i g n e d _ a l g o r i t h m   I S   N O T   N U L L  
                                 A N D   r . a s s i g n e d _ a l g o r i t h m   < >   ' '  
                                 A N D   N O T   (  
                                         I S J S O N ( r . a s s i g n e d _ a l g o r i t h m )   =   1  
                                         A N D   ( L E F T ( L T R I M ( r . a s s i g n e d _ a l g o r i t h m ) ,   1 )   =   ' [ ' )  
                                 )  
                                 A N D   (  
                                         @ f i l t e r _ a l i a s   =   ' '  
                                         O R   f i l t e r _ c h e c k . f i l t e r _ f o u n d   =   1  
                                 )  
                 )  
                 B E G I N  
                         R A I S E R R O R   (  
                                 @ m s g ,  
                                 1 6 ,   1 ,   - -   S e v e r i t y   a n d   S t a t e  
                                 @ d a t a s e t ,  
                                 @ s p e c i f i e d _ d a t a b a s e ,  
                                 @ s p e c i f i e d _ s c h e m a ,  
                                 @ i d e n t i f i e d _ t a b l e ,  
                                 @ f i l t e r _ a l i a s _ d i s p l a y  
                         ) ;  
                         R E T U R N ;  
                 E N D ;  
  
         - -   C T E :   G e t   c o l u m n s   t o   r e m o v e   ( w h e r e   i s _ e x c l u d e d   =   1 )  
         W I T H   c o l u m n s _ t o _ r e m o v e _ c t e   A S   (  
                 S E L E C T  
                         i d e n t i f i e d _ c o l u m n  
                 F R O M   d i s c o v e r e d _ r u l e s e t  
                 W H E R E  
                         d a t a s e t   =   @ d a t a s e t  
                         A N D   s p e c i f i e d _ d a t a b a s e   =   @ s p e c i f i e d _ d a t a b a s e  
                         A N D   s p e c i f i e d _ s c h e m a   =   @ s p e c i f i e d _ s c h e m a  
                         A N D   i d e n t i f i e d _ t a b l e   =   @ i d e n t i f i e d _ t a b l e  
                         A N D   i s _ e x c l u d e d   =   1  
         ) ,  
         - -   C T E :   G e t   D a t e O n l y   c o l u m n s   ( w h e r e   i d e n t i f i e d _ c o l u m n _ t y p e   =   ' D a t e O n l y ' )  
         d a t e _ o n l y _ c o l u m n s _ c t e   A S   (  
                 S E L E C T  
                         i d e n t i f i e d _ c o l u m n  
                 F R O M   d i s c o v e r e d _ r u l e s e t  
                 W H E R E  
                         d a t a s e t   =   @ d a t a s e t  
                         A N D   s p e c i f i e d _ d a t a b a s e   =   @ s p e c i f i e d _ d a t a b a s e  
                         A N D   s p e c i f i e d _ s c h e m a   =   @ s p e c i f i e d _ s c h e m a  
                         A N D   i d e n t i f i e d _ t a b l e   =   @ i d e n t i f i e d _ t a b l e  
                         A N D   i d e n t i f i e d _ c o l u m n _ t y p e   =   ' D a t e O n l y '  
         ) ,  
         - -   C T E :   G e t   P r i m a r y   I D   c o l u m n s   ( w h e r e   I s P r i m a r y I d   =   t r u e   A N D   I s L o g i c a l   =   f a l s e   i n   s o u r c e _ m e t a d a t a   J S O N )  
         p r i m a r y _ i d _ c o l u m n s _ c t e   A S   (  
                 S E L E C T  
                         i d e n t i f i e d _ c o l u m n  
                 F R O M   d i s c o v e r e d _ r u l e s e t  
                 W H E R E  
                         d a t a s e t   =   @ d a t a s e t  
                         A N D   s p e c i f i e d _ d a t a b a s e   =   @ s p e c i f i e d _ d a t a b a s e  
                         A N D   s p e c i f i e d _ s c h e m a   =   @ s p e c i f i e d _ s c h e m a  
                         A N D   i d e n t i f i e d _ t a b l e   =   @ i d e n t i f i e d _ t a b l e  
                         A N D   s o u r c e _ m e t a d a t a   I S   N O T   N U L L  
                         A N D   I S J S O N ( s o u r c e _ m e t a d a t a )   =   1  
                         A N D   J S O N _ V A L U E ( s o u r c e _ m e t a d a t a ,   ' $ . I s P r i m a r y I d ' )   =   ' t r u e '  
                         A N D   J S O N _ V A L U E ( s o u r c e _ m e t a d a t a ,   ' $ . I s L o g i c a l ' )   =   ' f a l s e '  
         ) ,  
         - -   b a s e _ r u l e s e t _ f i l t e r   -   F i l t e r   t o   t a r g e t   t a b l e ,   a d d   A D F   t y p e   m a p p i n g s ,    
         - -   c o m p u t e   m e t a d a t a ,   a n d   e x c l u d e   k e y   c o l u m n s  
         b a s e _ r u l e s e t _ f i l t e r   A S   (  
                 S E L E C T  
                         r . d a t a s e t ,  
                         r . s p e c i f i e d _ d a t a b a s e ,  
                         r . s p e c i f i e d _ s c h e m a ,  
                         r . i d e n t i f i e d _ t a b l e ,  
                         r . i d e n t i f i e d _ c o l u m n ,  
                         r . i d e n t i f i e d _ c o l u m n _ t y p e ,  
                         r . i d e n t i f i e d _ c o l u m n _ m a x _ l e n g t h ,  
                         r . r o w _ c o u n t ,  
                         r . o r d i n a l _ p o s i t i o n ,  
                         r . a s s i g n e d _ a l g o r i t h m ,  
                         r . a l g o r i t h m _ m e t a d a t a ,  
                         t . a d f _ t y p e ,  
                         C O N C A T (  
                                 ' x ' ,  
                                 C O N V E R T (  
                                         V A R C H A R ( M A X ) ,  
                                         C O N V E R T ( V A R B I N A R Y ,   r . i d e n t i f i e d _ c o l u m n ) ,  
                                         2  
                                 )  
                         )   A S   e n c o d e d _ c o l u m n _ n a m e ,  
                         - -   a l g o r i t h m _ m e t a d a t a   i s   a l w a y s   e x p e c t e d   t o   b e   a   J S O N   o b j e c t   ( o r   N U L L ) ,    
                         - -   s o   J S O N _ V A L U E   i s   s a f e   w i t h o u t   I S J S O N / C A S E  
                         J S O N _ V A L U E ( r . a l g o r i t h m _ m e t a d a t a ,   ' $ . d a t e _ f o r m a t ' )   A S   [ d a t e _ f o r m a t ] ,  
                         C O N V E R T ( B I T ,   J S O N _ V A L U E ( r . a l g o r i t h m _ m e t a d a t a ,   ' $ . t r e a t _ a s _ s t r i n g ' ) )   A S   t r e a t _ a s _ s t r i n g ,  
                         - -   A d d   c o l u m n   w i d t h   e s t i m a t e   h e r e  
                         C A S E  
                                 W H E N   r . i d e n t i f i e d _ c o l u m n _ m a x _ l e n g t h   >   0   T H E N   r . i d e n t i f i e d _ c o l u m n _ m a x _ l e n g t h   +   4  
                                 E L S E   @ c o l u m n _ w i d t h _ e s t i m a t e  
                         E N D   A S   c o l u m n _ w i d t h _ e s t i m a t e  
                 F R O M   d i s c o v e r e d _ r u l e s e t   A S   r  
                 I N N E R   J O I N   a d f _ t y p e _ m a p p i n g   A S   t  
                         O N  
                                 r . i d e n t i f i e d _ c o l u m n _ t y p e   =   t . d a t a s e t _ t y p e  
                                 A N D   r . d a t a s e t   =   t . d a t a s e t  
                 W H E R E  
                         r . d a t a s e t   =   @ d a t a s e t  
                         A N D   r . s p e c i f i e d _ d a t a b a s e   =   @ s p e c i f i e d _ d a t a b a s e  
                         A N D   r . s p e c i f i e d _ s c h e m a   =   @ s p e c i f i e d _ s c h e m a  
                         A N D   r . i d e n t i f i e d _ t a b l e   =   @ i d e n t i f i e d _ t a b l e  
                         A N D   r . a s s i g n e d _ a l g o r i t h m   I S   N O T   N U L L  
                         A N D   r . a s s i g n e d _ a l g o r i t h m   < >   ' '  
                         - -   E x c l u d e   k e y   c o l u m n s   f o r   c o n d i t i o n a l   m a s k i n g :   k e y   c o l u m n s   h a v e    
                         - -   a s s i g n e d _ a l g o r i t h m   a s   a   J S O N   a r r a y  
                         A N D   N O T   (  
                                 I S J S O N ( r . a s s i g n e d _ a l g o r i t h m )   =   1  
                                 A N D   ( L E F T ( L T R I M ( r . a s s i g n e d _ a l g o r i t h m ) ,   1 )   =   ' [ ' )  
                         )  
         ) ,  
         - -   c o n d i t i o n a l _ a l g o r i t h m _ e x t r a c t i o n   -   E x t r a c t   c o n d i t i o n a l   a l g o r i t h m s   f o r   m a t c h i n g   f i l t e r   k e y  
         c o n d i t i o n a l _ a l g o r i t h m _ e x t r a c t i o n   A S   (  
                 S E L E C T  
                         b r f . * ,  
                         a a . [ k e y _ c o l u m n ] ,  
                         a a . [ c o n d i t i o n s ] ,  
                         c . [ c o n d i t i o n _ a l i a s ] ,  
                         c . [ a l g o r i t h m ]   A S   r e s o l v e d _ a l g o r i t h m  
                 F R O M   b a s e _ r u l e s e t _ f i l t e r   A S   b r f  
                 C R O S S   A P P L Y  
                         O P E N J S O N   ( b r f . a s s i g n e d _ a l g o r i t h m )  
                         W I T H   (  
                                 [ k e y _ c o l u m n ]   N V A R C H A R ( 2 5 5 )   ' $ . k e y _ c o l u m n ' ,  
                                 [ c o n d i t i o n s ]   N V A R C H A R ( M A X )   ' $ . c o n d i t i o n s '   A S   J S O N  
                         )   A S   a a  
                 C R O S S   A P P L Y  
                         O P E N J S O N   ( a a . [ c o n d i t i o n s ] )  
                         W I T H   (  
                                 [ c o n d i t i o n _ a l i a s ]   N V A R C H A R ( 2 5 5 )   ' $ . a l i a s ' ,  
                                 [ a l g o r i t h m ]   N V A R C H A R ( 2 5 5 )   ' $ . a l g o r i t h m '  
                         )   A S   c  
                 W H E R E  
                         I S J S O N ( b r f . a s s i g n e d _ a l g o r i t h m )   =   1  
                         A N D   J S O N _ V A L U E ( b r f . a s s i g n e d _ a l g o r i t h m ,   ' $ . k e y _ c o l u m n ' )   I S   N O T   N U L L  
                         A N D   c . [ c o n d i t i o n _ a l i a s ]   =   @ f i l t e r _ a l i a s  
                         A N D   @ f i l t e r _ a l i a s   < >   ' '  
         ) ,  
         - -   a l g o r i t h m _ r e s o l u t i o n   -   C o m b i n e   c o n d i t i o n a l   a n d   s t a n d a r d   a l g o r i t h m s  
         a l g o r i t h m _ r e s o l u t i o n   A S   (  
                 S E L E C T  
                         d a t a s e t ,  
                         s p e c i f i e d _ d a t a b a s e ,  
                         s p e c i f i e d _ s c h e m a ,  
                         i d e n t i f i e d _ t a b l e ,  
                         i d e n t i f i e d _ c o l u m n ,  
                         i d e n t i f i e d _ c o l u m n _ t y p e ,  
                         i d e n t i f i e d _ c o l u m n _ m a x _ l e n g t h ,  
                         r o w _ c o u n t ,  
                         o r d i n a l _ p o s i t i o n ,  
                         e n c o d e d _ c o l u m n _ n a m e ,  
                         r e s o l v e d _ a l g o r i t h m   A S   a s s i g n e d _ a l g o r i t h m  
                 F R O M   c o n d i t i o n a l _ a l g o r i t h m _ e x t r a c t i o n  
                 U N I O N   A L L  
                 S E L E C T  
                         d a t a s e t ,  
                         s p e c i f i e d _ d a t a b a s e ,  
                         s p e c i f i e d _ s c h e m a ,  
                         i d e n t i f i e d _ t a b l e ,  
                         i d e n t i f i e d _ c o l u m n ,  
                         i d e n t i f i e d _ c o l u m n _ t y p e ,  
                         i d e n t i f i e d _ c o l u m n _ m a x _ l e n g t h ,  
                         r o w _ c o u n t ,  
                         o r d i n a l _ p o s i t i o n ,  
                         e n c o d e d _ c o l u m n _ n a m e ,  
                         a s s i g n e d _ a l g o r i t h m   A S   r e s o l v e d _ a l g o r i t h m  
                 F R O M   b a s e _ r u l e s e t _ f i l t e r  
                 W H E R E  
                         a s s i g n e d _ a l g o r i t h m   I S   N O T   N U L L  
                         A N D   a s s i g n e d _ a l g o r i t h m   < >   ' '  
                         A N D   I S J S O N ( a s s i g n e d _ a l g o r i t h m )   =   0  
         ) ,  
         - -   g e n e r a t e _ m a s k _ p a r a m e t e r s   -   C r e a t e   c o r e   m a s k i n g   p a r a m e t e r s   w i t h   f a l l b a c k   d e f a u l t s  
         g e n e r a t e _ m a s k _ p a r a m e t e r s   A S   (  
                 S E L E C T  
                         - -   J S O N   m a p p i n g :   e n c o d e d   c o l u m n   n a m e   - >   a l g o r i t h m  
                         C O A L E S C E (  
                                 ' { '  
                                 +   S T R I N G _ A G G (  
                                         ' " '   +   L O W E R ( b r f . e n c o d e d _ c o l u m n _ n a m e )   +   ' " : " '   +   a r . a s s i g n e d _ a l g o r i t h m   +   ' " ' ,  
                                         ' , '  
                                 )  
                                 +   ' } ' ,  
                                 ' { } '  
                         )   A S   [ F i e l d A l g o r i t h m A s s i g n m e n t s ] ,  
                         - -   J S O N   a r r a y   o f   c o l u m n   n a m e s   t o   m a s k  
                         C O A L E S C E (  
                                 J S O N _ Q U E R Y (  
                                         ' [ '  
                                         +   S T R I N G _ A G G ( ' " '   +   b r f . i d e n t i f i e d _ c o l u m n   +   ' " ' ,   ' , ' )  
                                         +   ' ] '  
                                 ) ,  
                                 ' [ ] '  
                         )   A S   [ C o l u m n s T o M a s k ] ,  
                         - -   A D F   d a t a   f l o w   e x p r e s s i o n   f o r   D C S   m a s k i n g   A P I   r e s p o n s e   p a r s i n g  
                         ' ' ' '  
                         +   ' ( t i m e s t a m p   a s   d a t e ,   s t a t u s   a s   s t r i n g ,   m e s s a g e   a s   s t r i n g ,   t r a c e _ i d   a s   s t r i n g ,   '  
                         +   ' i t e m s   a s   ( D E L P H I X _ C O M P L I A N C E _ S E R V I C E _ B A T C H _ I D   a s   l o n g '  
                         +   C O A L E S C E (  
                                 ' ,   '  
                                 +   S T R I N G _ A G G (  
                                         L O W E R (  
                                                 C O N C A T (  
                                                         b r f . e n c o d e d _ c o l u m n _ n a m e ,  
                                                         '   a s   ' ,  
                                                         C A S E   W H E N   b r f . t r e a t _ a s _ s t r i n g   =   1   T H E N   ' s t r i n g '   E L S E   b r f . a d f _ t y p e   E N D  
                                                 )  
                                         ) ,  
                                         ' ,   '  
                                 ) ,  
                                 ' '  
                         )  
                         +   ' ) [ ] ) '  
                         +   ' ' ' '   A S   [ D a t a F a c t o r y T y p e M a p p i n g ] ,  
                         C O A L E S C E (  
                                 C A S E  
                                         W H E N  
                                                 C E I L I N G (  
                                                         M A X ( b r f . r o w _ c o u n t )  
                                                         *   (  
                                                                 S U M ( b r f . c o l u m n _ w i d t h _ e s t i m a t e )  
                                                                 +   L O G 1 0 ( M A X ( b r f . r o w _ c o u n t ) )  
                                                                 +   1  
                                                         )  
                                                         /   ( 2 0 0 0 0 0 0   *   0 . 9 )  
                                                 )   <   1  
                                                 T H E N   1  
                                         E L S E   C E I L I N G (  
                                                 M A X ( b r f . r o w _ c o u n t )  
                                                 *   (  
                                                         S U M ( b r f . c o l u m n _ w i d t h _ e s t i m a t e )  
                                                         +   L O G 1 0 ( M A X ( b r f . r o w _ c o u n t ) )  
                                                         +   1  
                                                 )  
                                                 /   ( 2 0 0 0 0 0 0   *   0 . 9 )  
                                         )  
                                 E N D ,   1  
                         )   A S   [ N u m b e r O f B a t c h e s ] ,       - -   O p t i m a l   b a t c h   c o u n t   ( m i n i m u m   1 )  
                         C O A L E S C E (  
                                 ' [ '   +   S T R I N G _ A G G (  
                                         C O N V E R T ( N V A R C H A R ( 1 0 ) ,   b r f . i d e n t i f i e d _ c o l u m n _ m a x _ l e n g t h ) ,   ' , '  
                                 )   +   ' ] ' ,   ' [ ] '  
                         )   A S   [ T r i m L e n g t h s ]       - -   J S O N   a r r a y   o f   c o l u m n   m a x   l e n g t h s  
                 F R O M   a l g o r i t h m _ r e s o l u t i o n   A S   a r  
                 I N N E R   J O I N   b a s e _ r u l e s e t _ f i l t e r   A S   b r f  
                         O N   a r . i d e n t i f i e d _ c o l u m n   =   b r f . i d e n t i f i e d _ c o l u m n  
         ) ,  
         - -   c o n d i t i o n a l _ d a t e _ f o r m a t s   -   E x t r a c t   c o n d i t i o n a l   d a t e   f o r m a t s   f o r   f i l t e r   k e y  
         c o n d i t i o n a l _ d a t e _ f o r m a t s   A S   (  
                 S E L E C T  
                         b r f . i d e n t i f i e d _ c o l u m n ,  
                         b r f . e n c o d e d _ c o l u m n _ n a m e ,  
                         c . [ c o n d i t i o n _ a l i a s ] ,  
                         c . [ d a t e _ f o r m a t ]   A S   c o n d i t i o n a l _ d a t e _ f o r m a t  
                 F R O M   b a s e _ r u l e s e t _ f i l t e r   A S   b r f  
                 C R O S S   A P P L Y  
                         O P E N J S O N   ( b r f . a l g o r i t h m _ m e t a d a t a ,   ' $ . c o n d i t i o n s ' )  
                         W I T H   (  
                                 [ c o n d i t i o n _ a l i a s ]   N V A R C H A R ( 2 5 5 )   ' $ . a l i a s ' ,  
                                 [ d a t e _ f o r m a t ]   N V A R C H A R ( 2 5 5 )   ' $ . d a t e _ f o r m a t '  
                         )   A S   c  
                 W H E R E  
                         @ f i l t e r _ a l i a s   < >   ' '  
                         A N D   b r f . a l g o r i t h m _ m e t a d a t a   I S   N O T   N U L L  
                         A N D   c . [ c o n d i t i o n _ a l i a s ]   =   @ f i l t e r _ a l i a s  
                         A N D   c . [ d a t e _ f o r m a t ]   I S   N O T   N U L L  
         ) ,  
         - -   d a t e _ f o r m a t _ r e s o l u t i o n   -   M e r g e   c o n d i t i o n a l   a n d   s t a n d a r d   d a t e   f o r m a t s  
         d a t e _ f o r m a t _ r e s o l u t i o n   A S   (  
                 S E L E C T  
                         b r f . i d e n t i f i e d _ c o l u m n ,  
                         b r f . e n c o d e d _ c o l u m n _ n a m e ,  
                         C O A L E S C E ( c d f . c o n d i t i o n a l _ d a t e _ f o r m a t ,   b r f . [ d a t e _ f o r m a t ] )   A S   f i n a l _ d a t e _ f o r m a t  
                 F R O M   b a s e _ r u l e s e t _ f i l t e r   A S   b r f  
                 L E F T   J O I N   c o n d i t i o n a l _ d a t e _ f o r m a t s   A S   c d f  
                         O N   b r f . i d e n t i f i e d _ c o l u m n   =   c d f . i d e n t i f i e d _ c o l u m n  
                 W H E R E  
                         b r f . [ d a t e _ f o r m a t ]   I S   N O T   N U L L  
                         O R   c d f . c o n d i t i o n a l _ d a t e _ f o r m a t   I S   N O T   N U L L  
         ) ,  
         - -   d a t e _ f o r m a t _ p a r a m e t e r s   -   C r e a t e   J S O N   m a p p i n g   w i t h   f a l l b a c k   d e f a u l t  
         d a t e _ f o r m a t _ p a r a m e t e r s   A S   (  
                 S E L E C T  
                         C O A L E S C E (  
                                 ' { '   +   S T R I N G _ A G G (  
                                         ' " '  
                                         +   L O W E R ( e n c o d e d _ c o l u m n _ n a m e )  
                                         +   ' " : " '  
                                         +   R E P L A C E ( f i n a l _ d a t e _ f o r m a t ,   ' ' ' ' ,   ' \ ' ' ' )  
                                         +   ' " ' ,  
                                         ' , '  
                                 )   +   ' } ' ,  
                                 ' { } '  
                         )   A S   [ D a t e F o r m a t A s s i g n m e n t s ]  
                 F R O M   d a t e _ f o r m a t _ r e s o l u t i o n  
         ) ,  
         - -   t y p e _ c a s t i n g _ p a r a m e t e r s   -   C r e a t e   c a s t i n g   a r r a y s   w i t h   f a l l b a c k   d e f a u l t s  
         t y p e _ c a s t i n g _ p a r a m e t e r s   A S   (  
                 S E L E C T  
                         C O A L E S C E (  
                                 ' [ '   +   S T R I N G _ A G G ( ' " '   +   i d e n t i f i e d _ c o l u m n   +   ' " ' ,   ' , ' )   +   ' ] ' ,   ' [ " " ] '  
                         )   A S   [ C o l u m n s T o C a s t A s S t r i n g s ] ,  
                         C O A L E S C E (  
                                 ' [ '   +   S T R I N G _ A G G (  
                                         C A S E   W H E N   a d f _ t y p e   =   ' b i n a r y '   T H E N   ' " '   +   i d e n t i f i e d _ c o l u m n   +   ' " '   E N D ,   ' , '  
                                 )   +   ' ] ' ,   ' [ " " ] '  
                         )   A S   [ C o l u m n s T o C a s t B a c k T o B i n a r y ] ,  
                         C O A L E S C E (  
                                 ' [ '   +   S T R I N G _ A G G (  
                                         C A S E   W H E N   a d f _ t y p e   =   ' b o o l e a n '   T H E N   ' " '   +   i d e n t i f i e d _ c o l u m n   +   ' " '   E N D ,   ' , '  
                                 )   +   ' ] ' ,   ' [ " " ] '  
                         )   A S   [ C o l u m n s T o C a s t B a c k T o B o o l e a n ] ,  
                         C O A L E S C E (  
                                 ' [ '   +   S T R I N G _ A G G (  
                                         C A S E   W H E N   a d f _ t y p e   =   ' d a t e '   T H E N   ' " '   +   i d e n t i f i e d _ c o l u m n   +   ' " '   E N D ,   ' , '  
                                 )   +   ' ] ' ,   ' [ " " ] '  
                         )   A S   [ C o l u m n s T o C a s t B a c k T o D a t e ] ,  
                         C O A L E S C E (  
                                 ' [ '   +   S T R I N G _ A G G (  
                                         C A S E   W H E N   a d f _ t y p e   =   ' d o u b l e '   T H E N   ' " '   +   i d e n t i f i e d _ c o l u m n   +   ' " '   E N D ,   ' , '  
                                 )   +   ' ] ' ,   ' [ " " ] '  
                         )   A S   [ C o l u m n s T o C a s t B a c k T o D o u b l e ] ,  
                         C O A L E S C E (  
                                 ' [ '   +   S T R I N G _ A G G (  
                                         C A S E   W H E N   a d f _ t y p e   =   ' f l o a t '   T H E N   ' " '   +   i d e n t i f i e d _ c o l u m n   +   ' " '   E N D ,   ' , '  
                                 )   +   ' ] ' ,   ' [ " " ] '  
                         )   A S   [ C o l u m n s T o C a s t B a c k T o F l o a t ] ,  
                         C O A L E S C E (  
                                 ' [ '   +   S T R I N G _ A G G (  
                                         C A S E   W H E N   a d f _ t y p e   =   ' i n t e g e r '   T H E N   ' " '   +   i d e n t i f i e d _ c o l u m n   +   ' " '   E N D ,   ' , '  
                                 )   +   ' ] ' ,   ' [ " " ] '  
                         )   A S   [ C o l u m n s T o C a s t B a c k T o I n t e g e r ] ,  
                         C O A L E S C E (  
                                 ' [ '   +   S T R I N G _ A G G (  
                                         C A S E   W H E N   a d f _ t y p e   =   ' l o n g '   T H E N   ' " '   +   i d e n t i f i e d _ c o l u m n   +   ' " '   E N D ,   ' , '  
                                 )   +   ' ] ' ,   ' [ " " ] '  
                         )   A S   [ C o l u m n s T o C a s t B a c k T o L o n g ] ,  
                         C O A L E S C E (  
                                 ' [ '   +   S T R I N G _ A G G (  
                                         C A S E   W H E N   a d f _ t y p e   =   ' t i m e s t a m p '   T H E N   ' " '   +   i d e n t i f i e d _ c o l u m n   +   ' " '   E N D ,   ' , '  
                                 )   +   ' ] ' ,   ' [ " " ] '  
                         )   A S   [ C o l u m n s T o C a s t B a c k T o T i m e s t a m p ]  
                 F R O M   b a s e _ r u l e s e t _ f i l t e r  
                 W H E R E   t r e a t _ a s _ s t r i n g   =   1  
         ) ,  
         - -   G e n e r a t e   t h e   c o l u m n s   t o   r e m o v e   a r r a y  
         c o l u m n s _ t o _ r e m o v e _ p a r a m e t e r s   A S   (  
                 S E L E C T  
                         C O A L E S C E (  
                                 ' [ '   +   S T R I N G _ A G G ( ' " '   +   i d e n t i f i e d _ c o l u m n   +   ' " ' ,   ' , ' )   +   ' ] ' ,  
                                 ' [ ] '  
                         )   A S   [ C o l u m n s _ t o _ r e m o v e ]  
                 F R O M   c o l u m n s _ t o _ r e m o v e _ c t e  
         ) ,  
         - -   G e n e r a t e   t h e   D a t e O n l y   c o l u m n s   a r r a y  
         d a t e _ o n l y _ c o l u m n s _ p a r a m e t e r s   A S   (  
                 S E L E C T  
                         C O A L E S C E (  
                                 ' [ '   +   S T R I N G _ A G G ( ' " '   +   i d e n t i f i e d _ c o l u m n   +   ' " ' ,   ' , ' )   +   ' ] ' ,  
                                 ' [ ] '  
                         )   A S   [ D a t e _ o n l y _ c o l u m n s ]  
                 F R O M   d a t e _ o n l y _ c o l u m n s _ c t e  
         ) ,  
         - -   G e n e r a t e   t h e   P r i m a r y   I D   c o l u m n s   a s   a   s t r i n g   ( c o m m a - s e p a r a t e d )  
         p r i m a r y _ i d _ c o l u m n s _ p a r a m e t e r s   A S   (  
                 S E L E C T  
                         C O A L E S C E (  
                                 S T R I N G _ A G G ( i d e n t i f i e d _ c o l u m n ,   ' , ' ) ,  
                                 ' '  
                         )   A S   [ P r i m a r y _ i d _ c o l u m n s ]  
                 F R O M   p r i m a r y _ i d _ c o l u m n s _ c t e  
         )  
         - -   C o m b i n e   a l l   p a r a m e t e r s  
         S E L E C T  
                 g . [ F i e l d A l g o r i t h m A s s i g n m e n t s ] ,  
                 g . [ C o l u m n s T o M a s k ] ,  
                 g . [ D a t a F a c t o r y T y p e M a p p i n g ] ,  
                 g . [ N u m b e r O f B a t c h e s ] ,  
                 g . [ T r i m L e n g t h s ] ,  
                 d f . [ D a t e F o r m a t A s s i g n m e n t s ] ,  
                 a . [ C o l u m n s T o C a s t A s S t r i n g s ] ,  
                 a . [ C o l u m n s T o C a s t B a c k T o B i n a r y ] ,  
                 a . [ C o l u m n s T o C a s t B a c k T o B o o l e a n ] ,  
                 a . [ C o l u m n s T o C a s t B a c k T o D a t e ] ,  
                 a . [ C o l u m n s T o C a s t B a c k T o D o u b l e ] ,  
                 a . [ C o l u m n s T o C a s t B a c k T o F l o a t ] ,  
                 a . [ C o l u m n s T o C a s t B a c k T o I n t e g e r ] ,  
                 a . [ C o l u m n s T o C a s t B a c k T o L o n g ] ,  
                 a . [ C o l u m n s T o C a s t B a c k T o T i m e s t a m p ] ,  
                 c t r . [ C o l u m n s _ t o _ r e m o v e ] ,  
                 d o c . [ D a t e _ o n l y _ c o l u m n s ] ,  
                 p i c . [ P r i m a r y _ i d _ c o l u m n s ] ,  
                 @ S t o r e d P r o c e d u r e V e r s i o n   A S   [ S t o r e d P r o c e d u r e V e r s i o n ]  
         F R O M   g e n e r a t e _ m a s k _ p a r a m e t e r s   A S   g ,  
                 d a t e _ f o r m a t _ p a r a m e t e r s   A S   d f ,  
                 t y p e _ c a s t i n g _ p a r a m e t e r s   A S   a ,  
                 c o l u m n s _ t o _ r e m o v e _ p a r a m e t e r s   A S   c t r ,  
                 d a t e _ o n l y _ c o l u m n s _ p a r a m e t e r s   A S   d o c ,  
                 p r i m a r y _ i d _ c o l u m n s _ p a r a m e t e r s   A S   p i c ;  
 E N D  
 
