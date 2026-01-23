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
-- * V2025.09.16.0__update_generate_masking_parameters_procedure
-- * V2025.09.19.0__update_generate_masking_parameters_procedure
-- * V2025.12.18.0__update_generate_masking_parameters_procedure
-- * V2026.01.14.0__update_generate_masking_parameters_procedure
-- * V2026.01.20.0__add_azurepostgres_adf_type_mapping
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
/*
 * generate_dataverse_masking_parameters - Generates masking parameters for Azure Data Factory dataflows.
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
 * - ColumnsToRemove: JSON array of excluded columns (where is_excluded = 1)
 * - DateOnlyColumns: JSON array of columns with type 'DateOnly'
 * - PrimaryKey: Comma-separated list of primary ID columns (IsPrimaryId = true, IsLogical = false)
 *
 * DEPENDENCIES:
 * - discovered_ruleset table: Contains masking rules, metadata, and algorithm assignments
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
 * - Includes explicit handling for excluded columns, DateOnly columns, and primary ID columns
 */
 CREATE OR ALTER PROCEDURE generate_dataverse_masking_parameters
    @dataset NVARCHAR(128),
    @specified_database NVARCHAR(128),
    @specified_schema NVARCHAR(128),
    @identified_table NVARCHAR(128),
    @column_width_estimate INT = 1000,
    @filter_alias NVARCHAR(128) = ''  -- Optional filter key for conditional masking
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StoredProcedureVersion VARCHAR(13) = 'V2025.09.11.0';
    DECLARE @filter_alias_display NVARCHAR(128);
    SET @filter_alias_display = CASE
        WHEN @filter_alias = '' THEN 'No filter set'
        ELSE @filter_alias
    END;

    DECLARE @msg NVARCHAR(400);
    SET @msg = 'No masking rules found for the specified parameters. Ensure the '
             + 'discovered_ruleset table has valid entries for dataset: %s, '
             + 'database: %s, schema: %s, table: %s, filter: %s.';

    -- Check if the discovered_ruleset table has valid entries for the specified parameters
    IF NOT EXISTS (
        SELECT 1
        FROM discovered_ruleset AS r
        OUTER APPLY (
            SELECT 1 AS filter_found
            FROM OPENJSON(r.assigned_algorithm)
            WITH (
                [key_column] NVARCHAR(255) '$.key_column',
                [conditions] NVARCHAR(MAX) '$.conditions' AS JSON
            ) AS aa
            CROSS APPLY OPENJSON(aa.[conditions])
            WITH (
                [condition_alias] NVARCHAR(255) '$.alias',
                [algorithm] NVARCHAR(255) '$.algorithm'
            ) AS cond
            WHERE r.assigned_algorithm IS NOT NULL
              AND r.assigned_algorithm <> ''
              AND ISJSON(r.assigned_algorithm) = 1
              AND JSON_VALUE(r.assigned_algorithm, '$.key_column') IS NOT NULL
              AND cond.[condition_alias] IS NOT NULL
              AND cond.[condition_alias] = @filter_alias
        ) AS filter_check
        WHERE r.dataset = @dataset
          AND r.specified_database = @specified_database
          AND r.specified_schema = @specified_schema
          AND r.identified_table = @identified_table
          AND r.assigned_algorithm IS NOT NULL
          AND r.assigned_algorithm <> ''
          AND NOT (ISJSON(r.assigned_algorithm) = 1 AND LEFT(LTRIM(r.assigned_algorithm), 1) = '[')
          AND (@filter_alias = '' OR filter_check.filter_found = 1)
    )
    BEGIN
        RAISERROR (
            @msg,
            16, 1,
            @dataset,
            @specified_database,
            @specified_schema,
            @identified_table,
            @filter_alias_display
        );
        RETURN;
    END;

    -- CTE: Get columns to remove (where is_excluded = 1)
    WITH columns_to_remove_cte AS (
        SELECT identified_column
        FROM discovered_ruleset
        WHERE dataset = @dataset
          AND specified_database = @specified_database
          AND specified_schema = @specified_schema
          AND identified_table = @identified_table
          AND is_excluded = 1
    ),
    -- CTE: Get DateOnly columns
    date_only_columns_cte AS (
        SELECT identified_column
        FROM discovered_ruleset
        WHERE dataset = @dataset
          AND specified_database = @specified_database
          AND specified_schema = @specified_schema
          AND identified_table = @identified_table
          AND identified_column_type = 'DateOnly'
    ),
    -- CTE: Get Primary ID columns
    primary_id_columns_cte AS (
        SELECT identified_column
        FROM discovered_ruleset
        WHERE dataset = @dataset
          AND specified_database = @specified_database
          AND specified_schema = @specified_schema
          AND identified_table = @identified_table
          AND source_metadata IS NOT NULL
          AND ISJSON(source_metadata) = 1
          AND JSON_VALUE(source_metadata, '$.IsPrimaryId') = 'true'
          AND JSON_VALUE(source_metadata, '$.IsLogical') = 'false'
    ),
    -- base_ruleset_filter
    base_ruleset_filter AS (
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
            CONCAT('x', CONVERT(VARCHAR(MAX), CONVERT(VARBINARY, r.identified_column), 2)) AS encoded_column_name,
            JSON_VALUE(r.algorithm_metadata, '$.date_format') AS date_format,
            CONVERT(BIT, JSON_VALUE(r.algorithm_metadata, '$.treat_as_string')) AS treat_as_string,
            CASE
                WHEN r.identified_column_max_length > 0 THEN r.identified_column_max_length + 4
                ELSE @column_width_estimate
            END AS column_width_estimate
        FROM discovered_ruleset AS r
        INNER JOIN adf_type_mapping AS t
            ON r.identified_column_type = t.dataset_type
           AND r.dataset = t.dataset
        WHERE r.dataset = @dataset
          AND r.specified_database = @specified_database
          AND r.specified_schema = @specified_schema
          AND r.identified_table = @identified_table
          AND r.assigned_algorithm IS NOT NULL
          AND r.assigned_algorithm <> ''
          AND NOT (ISJSON(r.assigned_algorithm) = 1 AND LEFT(LTRIM(r.assigned_algorithm), 1) = '[')
    ),
    -- conditional_algorithm_extraction
    conditional_algorithm_extraction AS (
        SELECT
            brf.*,
            aa.[key_column],
            aa.[conditions],
            c.[condition_alias],
            c.[algorithm] AS resolved_algorithm
        FROM base_ruleset_filter AS brf
        CROSS APPLY OPENJSON(brf.assigned_algorithm)
        WITH (
            [key_column] NVARCHAR(255) '$.key_column',
            [conditions] NVARCHAR(MAX) '$.conditions' AS JSON
        ) AS aa
        CROSS APPLY OPENJSON(aa.[conditions])
        WITH (
            [condition_alias] NVARCHAR(255) '$.alias',
            [algorithm] NVARCHAR(255) '$.algorithm'
        ) AS c
        WHERE ISJSON(brf.assigned_algorithm) = 1
          AND JSON_VALUE(brf.assigned_algorithm, '$.key_column') IS NOT NULL
          AND c.[condition_alias] = @filter_alias
          AND @filter_alias <> ''
    ),
    -- algorithm_resolution
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
            assigned_algorithm
        FROM base_ruleset_filter
        WHERE assigned_algorithm IS NOT NULL
          AND assigned_algorithm <> ''
          AND ISJSON(assigned_algorithm) = 0
    ),
    -- generate_mask_parameters
    generate_mask_parameters AS (
        SELECT
            COALESCE(
                '{' + STRING_AGG('"' + LOWER(brf.encoded_column_name) + '":"' + ar.assigned_algorithm + '"', ',') + '}',
                '{}'
            ) AS FieldAlgorithmAssignments,
            COALESCE(
                JSON_QUERY('[' + STRING_AGG('"' + brf.identified_column + '"', ',') + ']'),
                '[]'
            ) AS ColumnsToMask,
            '''(timestamp as date, status as string, message as string, trace_id as string, items as (DELPHIX_COMPLIANCE_SERVICE_BATCH_ID as long'
            + COALESCE(', ' + STRING_AGG(LOWER(CONCAT(brf.encoded_column_name, ' as ',
                CASE WHEN brf.treat_as_string = 1 THEN 'string' ELSE brf.adf_type END)), ', '), '')
            + ')[])''' AS DataFactoryTypeMapping,
            COALESCE(
                CASE
                    WHEN CEILING(MAX(brf.row_count) * (SUM(brf.column_width_estimate) + LOG10(MAX(brf.row_count)) + 1) / (2000000 * 0.9)) < 1
                        THEN 1
                    ELSE CEILING(MAX(brf.row_count) * (SUM(brf.column_width_estimate) + LOG10(MAX(brf.row_count)) + 1) / (2000000 * 0.9))
                END, 1
            ) AS NumberOfBatches,
            COALESCE('[' + STRING_AGG(CONVERT(NVARCHAR(10), brf.identified_column_max_length), ',') + ']', '[]') AS TrimLengths
        FROM algorithm_resolution AS ar
        INNER JOIN base_ruleset_filter AS brf
            ON ar.identified_column = brf.identified_column
    ),
    -- conditional_date_formats
    conditional_date_formats AS (
        SELECT
            brf.identified_column,
            brf.encoded_column_name,
            c.[condition_alias],
            c.[date_format] AS conditional_date_format
        FROM base_ruleset_filter AS brf
        CROSS APPLY OPENJSON(brf.algorithm_metadata, '$.conditions')
        WITH (
            [condition_alias] NVARCHAR(255) '$.alias',
            [date_format] NVARCHAR(255) '$.date_format'
        ) AS c
        WHERE @filter_alias <> ''
          AND brf.algorithm_metadata IS NOT NULL
          AND c.[condition_alias] = @filter_alias
          AND c.[date_format] IS NOT NULL
    ),
    -- date_format_resolution
    date_format_resolution AS (
        SELECT
            brf.identified_column,
            brf.encoded_column_name,
            COALESCE(cdf.conditional_date_format, brf.date_format) AS final_date_format
        FROM base_ruleset_filter AS brf
        LEFT JOIN conditional_date_formats AS cdf
          ON brf.identified_column = cdf.identified_column
        WHERE brf.date_format IS NOT NULL OR cdf.conditional_date_format IS NOT NULL
    ),
    -- date_format_parameters
    date_format_parameters AS (
        SELECT
            COALESCE(
                '{' + STRING_AGG('"' + LOWER(encoded_column_name) + '":"' + REPLACE(final_date_format, '''', '\''') + '"', ',') + '}',
                '{}'
            ) AS DateFormatAssignments
        FROM date_format_resolution
    ),
    -- type_casting_parameters
    type_casting_parameters AS (
        SELECT
            COALESCE('[' + STRING_AGG('"' + identified_column + '"', ',') + ']', '[""]') AS ColumnsToCastAsStrings,
            COALESCE('[' + STRING_AGG(CASE WHEN adf_type = 'binary' THEN '"' + identified_column + '"' END, ',') + ']', '[""]') AS ColumnsToCastBackToBinary,
            COALESCE('[' + STRING_AGG(CASE WHEN adf_type = 'boolean' THEN '"' + identified_column + '"' END, ',') + ']', '[""]') AS ColumnsToCastBackToBoolean,
            COALESCE('[' + STRING_AGG(CASE WHEN adf_type = 'date' THEN '"' + identified_column + '"' END, ',') + ']', '[""]') AS ColumnsToCastBackToDate,
            COALESCE('[' + STRING_AGG(CASE WHEN adf_type = 'double' THEN '"' + identified_column + '"' END, ',') + ']', '[""]') AS ColumnsToCastBackToDouble,
            COALESCE('[' + STRING_AGG(CASE WHEN adf_type = 'float' THEN '"' + identified_column + '"' END, ',') + ']', '[""]') AS ColumnsToCastBackToFloat,
            COALESCE('[' + STRING_AGG(CASE WHEN adf_type = 'integer' THEN '"' + identified_column + '"' END, ',') + ']', '[""]') AS ColumnsToCastBackToInteger,
            COALESCE('[' + STRING_AGG(CASE WHEN adf_type = 'long' THEN '"' + identified_column + '"' END, ',') + ']', '[""]') AS ColumnsToCastBackToLong,
            COALESCE('[' + STRING_AGG(CASE WHEN adf_type = 'timestamp' THEN '"' + identified_column + '"' END, ',') + ']', '[""]') AS ColumnsToCastBackToTimestamp
        FROM base_ruleset_filter
        WHERE treat_as_string = 1
    ),
    -- columns_to_remove_parameters
    columns_to_remove_parameters AS (
        SELECT COALESCE('[' + STRING_AGG('"' + identified_column + '"', ',') + ']', '[]') AS ColumnsToRemove
        FROM columns_to_remove_cte
    ),
    -- date_only_columns_parameters
    date_only_columns_parameters AS (
        SELECT COALESCE('[' + STRING_AGG('"' + identified_column + '"', ',') + ']', '[]') AS DateOnlyColumns
        FROM date_only_columns_cte
    ),
    -- primary_id_columns_parameters
    primary_id_columns_parameters AS (
        SELECT COALESCE(STRING_AGG(identified_column, ','), '') AS PrimaryKey
        FROM primary_id_columns_cte
    )
    -- final SELECT
    SELECT
        g.FieldAlgorithmAssignments,
        g.ColumnsToMask,
        g.DataFactoryTypeMapping,
        g.NumberOfBatches,
        g.TrimLengths,
        df.DateFormatAssignments,
        a.ColumnsToCastAsStrings,
        a.ColumnsToCastBackToBinary,
        a.ColumnsToCastBackToBoolean,
        a.ColumnsToCastBackToDate,
        a.ColumnsToCastBackToDouble,
        a.ColumnsToCastBackToFloat,
        a.ColumnsToCastBackToInteger,
        a.ColumnsToCastBackToLong,
        a.ColumnsToCastBackToTimestamp,
        ctr.ColumnsToRemove,
        doc.DateOnlyColumns,
        pic.PrimaryKey,   -- Dataverse tables have only one Primary ID, this will never be a comma-separated string
        @StoredProcedureVersion AS StoredProcedureVersion
    FROM generate_mask_parameters AS g,
         date_format_parameters AS df,
         type_casting_parameters AS a,
         columns_to_remove_parameters AS ctr,
         date_only_columns_parameters AS doc,
         primary_id_columns_parameters AS pic;
END
GO


-- source: V2025.09.16.0__update_generate_masking_parameters_procedure
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
 * @capped_identified_column_max_length - Optional, additional capping on
 *   identified_column_max_length value (defaults to 1 MiB or 1048576 bytes)
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
ALTER PROCEDURE generate_masking_parameters
    @dataset NVARCHAR(128),
    @specified_database NVARCHAR(128),
    @specified_schema NVARCHAR(128),
    @identified_table NVARCHAR(128),
    @column_width_estimate INT = 1000,
    @filter_alias NVARCHAR(128) = '',  -- Optional filter key for conditional masking
    -- Additional capping on identified column max length (defaults to 1 MiB or 1048576 bytes)
    @capped_identified_column_max_length INT = 1048576
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StoredProcedureVersion VARCHAR(13) = 'V2025.09.16.0';
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
            -- Add column width estimate, if column max length is known, add 4 bytes of overhead
            -- In cases where max length is excessive, provide a cap to improve computation accuracy
            CASE
                WHEN
                    r.identified_column_max_length > 0
                    AND r.identified_column_max_length <= @capped_identified_column_max_length
                    THEN r.identified_column_max_length + 4
                WHEN
                    r.identified_column_max_length > 0
                    AND r.identified_column_max_length > @capped_identified_column_max_length
                    THEN @capped_identified_column_max_length + 4
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
                    ELSE CONVERT(INT, CEILING(
                        MAX(brf.row_count)
                        * (
                            SUM(brf.column_width_estimate)
                            + LOG10(MAX(brf.row_count))
                            + 1
                        )
                        / (2000000 * 0.9)
                    ))
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

-- source: V2025.09.19.0__update_generate_masking_parameters_procedure
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
    DECLARE @StoredProcedureVersion VARCHAR(13) = 'V2025.09.19.0';
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
                    WHEN MAX(brf.row_count) = -1 THEN -1
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
            -- Optimal batch count (minimum 1, or -1 when row_count is unavailable)
            ) AS [NumberOfBatches],
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

-- source: V2025.12.18.0__update_generate_masking_parameters_procedure
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
 * @capped_identified_column_max_length - Optional, additional capping on
 *   identified_column_max_length value (defaults to 1 MiB or 1048576 bytes)
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
    @filter_alias NVARCHAR(128) = '',  -- Optional filter key for conditional masking
    -- Additional capping on identified column max length (defaults to 1 MiB or 1048576 bytes)
    @capped_identified_column_max_length INT = 1048576
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StoredProcedureVersion VARCHAR(13) = 'V2025.12.18.0';
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
            -- Add column width estimate, if column max length is known, add 4 bytes of overhead
            -- In cases where max length is excessive, provide a cap to improve computation accuracy
            CASE
                WHEN
                    r.identified_column_max_length > 0
                    AND r.identified_column_max_length <= @capped_identified_column_max_length
                    THEN r.identified_column_max_length + 4
                WHEN
                    r.identified_column_max_length > 0
                    AND r.identified_column_max_length > @capped_identified_column_max_length
                    THEN @capped_identified_column_max_length + 4
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
                    WHEN MAX(brf.row_count) = -1 THEN -1
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
                    ELSE CONVERT(INT, CEILING(
                        MAX(brf.row_count)
                        * (
                            SUM(brf.column_width_estimate)
                            + LOG10(MAX(brf.row_count))
                            + 1
                        )
                        / (2000000 * 0.9)
                    ))
                END, 1
            -- Optimal batch count (minimum 1, or -1 when row_count is unavailable)
            ) AS [NumberOfBatches],
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


-- source: V2026.01.14.0__update_generate_masking_parameters_procedure
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
 * @capped_identified_column_max_length - Optional, additional capping on
 *   identified_column_max_length value (defaults to 1 MiB or 1048576 bytes)
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
    @filter_alias NVARCHAR(128) = '',  -- Optional filter key for conditional masking
    -- Additional capping on identified column max length (defaults to 1 MiB or 1048576 bytes)
    @capped_identified_column_max_length INT = 1048576
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StoredProcedureVersion VARCHAR(13) = 'V2026.01.14.0';
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
                    CONVERT(VARBINARY(MAX), r.identified_column),
                    2
                )
            ) AS encoded_column_name,
            -- algorithm_metadata is always expected to be a JSON object (or NULL),
            -- so JSON_VALUE is safe without ISJSON/CASE
            JSON_VALUE(r.algorithm_metadata, '$.date_format') AS [date_format],
            CONVERT(BIT, JSON_VALUE(r.algorithm_metadata, '$.treat_as_string')) AS treat_as_string,
            -- Add column width estimate, if column max length is known, add 4 bytes of overhead
            -- In cases where max length is excessive, provide a cap to improve computation accuracy
            CASE
                WHEN
                    r.identified_column_max_length > 0
                    AND r.identified_column_max_length <= @capped_identified_column_max_length
                    THEN r.identified_column_max_length + 4
                WHEN
                    r.identified_column_max_length > 0
                    AND r.identified_column_max_length > @capped_identified_column_max_length
                    THEN @capped_identified_column_max_length + 4
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
                    WHEN MAX(brf.row_count) = -1 THEN -1
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
                    ELSE CONVERT(INT, CEILING(
                        MAX(brf.row_count)
                        * (
                            SUM(brf.column_width_estimate)
                            + LOG10(MAX(brf.row_count))
                            + 1
                        )
                        / (2000000 * 0.9)
                    ))
                END, 1
            -- Optimal batch count (minimum 1, or -1 when row_count is unavailable)
            ) AS [NumberOfBatches],
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


-- source: V2026.01.20.0__add_azurepostgres_adf_type_mapping
DELETE FROM adf_type_mapping
WHERE dataset = 'AZURE_POSTGRES';


INSERT INTO adf_type_mapping (dataset, dataset_type, adf_type)
VALUES
('AZURE_POSTGRES', 'bigint', 'long'),
('AZURE_POSTGRES', 'bigserial', 'long'),
('AZURE_POSTGRES', 'boolean', 'boolean'),
('AZURE_POSTGRES', 'bytea', 'binary'),
('AZURE_POSTGRES', 'char', 'string'),
('AZURE_POSTGRES', 'character', 'string'),
('AZURE_POSTGRES', 'character varying', 'string'),
('AZURE_POSTGRES', 'date', 'date'),
('AZURE_POSTGRES', 'decimal', 'double'),
('AZURE_POSTGRES', 'double precision', 'double'),
('AZURE_POSTGRES', 'integer', 'integer'),
('AZURE_POSTGRES', 'json', 'string'),
('AZURE_POSTGRES', 'jsonb', 'string'),
('AZURE_POSTGRES', 'money', 'double'),
('AZURE_POSTGRES', 'numeric', 'double'),
('AZURE_POSTGRES', 'real', 'float'),
('AZURE_POSTGRES', 'serial', 'integer'),
('AZURE_POSTGRES', 'smallint', 'short'),
('AZURE_POSTGRES', 'text', 'string'),
('AZURE_POSTGRES', 'time', 'timestamp'),
('AZURE_POSTGRES', 'timestamp', 'timestamp'),
-- `timestamp with time zone` (aka `timestamptz`) is time-zone aware in PostgreSQL
-- But `timestamp` is time-zone naive in PostgreSQL.
-- Casting drops timezone context, so we map `timestamptz` to `string` to preserve semantics.
('AZURE_POSTGRES', 'timestamp with time zone', 'string'),
('AZURE_POSTGRES', 'timestamp without time zone', 'timestamp'),
('AZURE_POSTGRES', 'uuid', 'string'),
('AZURE_POSTGRES', 'varchar', 'string'),
('AZURE_POSTGRES', 'xml', 'string');

