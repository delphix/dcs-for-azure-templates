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
