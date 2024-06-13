ALTER TABLE discovered_ruleset add
   is_profiled char(1), 
   last_update_pipeline_id varchar(100);

ALTER TABLE adf_data_mapping add
   is_masked char(1), 
   last_update_pipeline_id varchar(100);

-- ADF/Synapse Execution Log

CREATE TABLE adf_execution_log (
pipeline_name VARCHAR(100), 
pipeline_run_id VARCHAR(100) NOT NULL, 
activity_run_id VARCHAR(100) NOT NULL, 
pipeline_status VARCHAR(20), 
error_message VARCHAR(MAX), 
input_parameters VARCHAR(MAX), 
execution_start_time DATETIME, 
execution_end_time DATETIME, 
src_dataset VARCHAR(255),
src_file_format VARCHAR(255), 
src_db_name VARCHAR(100), 
src_table_name VARCHAR(100), 
src_schema_name VARCHAR(100), 
sink_dataset VARCHAR(255), 
sink_file_format VARCHAR(255), 
sink_db_name VARCHAR(100), 
sink_table_name VARCHAR(100), 
sink_schema_name VARCHAR(100), 
last_inserted DATETIME DEFAULT getdate(), 
CONSTRAINT adf_execution_log_pk PRIMARY KEY (pipeline_run_id, activity_run_id));

-- procedure to capture logs

create or alter procedure capture_adf_execution_sp
(
@pipeline_name varchar(100),
@pipeline_run_id varchar(100),
@activity_run_id varchar(100),
@pipeline_status varchar(20),
@error_message varchar(max),
@input_parameters varchar(max),
@execution_start_time datetime,
@execution_end_time datetime,
@src_dataset varchar(255),
@src_file_format varchar(255),
@src_db_name varchar(100),
@src_table_name varchar(100),
@src_schema_name varchar(100),
@sink_dataset varchar(255),
@sink_file_format varchar(255),
@sink_db_name varchar(100),
@sink_table_name varchar(100),
@sink_schema_name varchar(100)
)
as
begin
insert into adf_execution_log (pipeline_name, pipeline_run_id, activity_run_id, pipeline_status, error_message, input_parameters, src_dataset, 
src_file_format, src_db_name, src_table_name, src_schema_name, sink_dataset, sink_file_format, sink_db_name, sink_table_name, sink_schema_name, execution_start_time, execution_end_time)
values
(@pipeline_name, @pipeline_run_id, @activity_run_id, @pipeline_status, @error_message, @input_parameters, @src_dataset, @src_file_format, @src_db_name,
@src_table_name, @src_schema_name, @sink_dataset, @sink_file_format, @sink_db_name, @sink_table_name, @sink_schema_name, @execution_start_time, @execution_end_time)

---------------------------------------------------------------------------
------------------------------------------- For Masking Pipelines
---------------------------------------------------------------------------

if @pipeline_name like 'dcsazure_%_mask_pl%'

-- for ADLS Parquet

if @src_dataset = 'ADLS' and @src_file_format = 'PARQUET' and @sink_dataset = 'ADLS' and @sink_file_format = 'PARQUET' and @pipeline_status = 'Succeeded'
   update adls_adf_data_mapping set is_masked = 'Y', last_update_pipeline_id = @pipeline_run_id
   where source_dataset = @src_dataset and  source_folder = @src_table_name and source_path = @src_schema_name and source_container = @src_db_name and source_fileformat = @src_file_format
   and sink_dataset = @sink_dataset and  sink_folder = @sink_table_name and sink_path = @sink_schema_name and sink_container = @sink_db_name and sink_fileformat = @sink_file_format;

else if @src_dataset = 'ADLS' and @src_file_format = 'PARQUET' and @sink_dataset = 'ADLS' and @sink_file_format = 'PARQUET' and @pipeline_status = 'Failed'
   update adls_adf_data_mapping set is_masked = 'N', last_update_pipeline_id = @pipeline_run_id
   where source_dataset = @src_dataset and  source_folder = @src_table_name and source_path = @src_schema_name and source_container = @src_db_name and source_fileformat = @src_file_format
   and sink_dataset = @sink_dataset and  sink_folder = @sink_table_name and sink_path = @sink_schema_name and sink_container = @sink_db_name and sink_fileformat = @sink_file_format;

-- for ADLS Delimited

if @src_dataset = 'ADLS' and @src_file_format = 'DELIMITED' and @sink_dataset = 'ADLS' and @sink_file_format = 'DELIMITED' and @pipeline_status = 'Succeeded'
   update adf_data_mapping set is_masked = 'Y', last_update_pipeline_id = @pipeline_run_id
   where source_dataset = @src_dataset and  source_table = @src_table_name and source_schema = @src_schema_name and source_database = @src_db_name
   and sink_dataset = @sink_dataset and  sink_table = @sink_table_name and sink_schema = @sink_schema_name and sink_database = @sink_db_name;

else if @src_dataset = 'ADLS' and @src_file_format = 'DELIMITED' and @sink_dataset = 'ADLS' and @sink_file_format = 'DELIMITED' and @pipeline_status = 'Failed'
   update adf_data_mapping set is_masked = 'N', last_update_pipeline_id = @pipeline_run_id
   where source_dataset = @src_dataset and  source_table = @src_table_name and source_schema = @src_schema_name and source_database = @src_db_name
   and sink_dataset = @sink_dataset and  sink_table = @sink_table_name and sink_schema = @sink_schema_name and sink_database = @sink_db_name;

-- for Snowflake

if @src_dataset = 'Snowflake' and @sink_dataset = 'Snowflake' and @pipeline_status = 'Succeeded'
   update adf_data_mapping set is_masked = 'Y', last_update_pipeline_id = @pipeline_run_id
   where source_dataset = @src_dataset and  source_table = @src_table_name and source_schema = @src_schema_name and source_database = @src_db_name
   and sink_dataset = @sink_dataset and  sink_table = @sink_table_name and sink_schema = @sink_schema_name and sink_database = @sink_db_name;

else if @src_dataset = 'Snowflake' and @sink_dataset = 'Snowflake' and @pipeline_status = 'Failed'
   update adf_data_mapping set is_masked = 'N', last_update_pipeline_id = @pipeline_run_id
   where source_dataset = @src_dataset and  source_table = @src_table_name and source_schema = @src_schema_name and source_database = @src_db_name
   and sink_dataset = @sink_dataset and  sink_table = @sink_table_name and sink_schema = @sink_schema_name and sink_database = @sink_db_name;

-- for SqlServer to ADLS

if @src_dataset = 'SqlServer' and @sink_dataset = 'ADLS' and @pipeline_status = 'Succeeded'
   update adf_data_mapping set is_masked = 'Y', last_update_pipeline_id = @pipeline_run_id
   where source_dataset = @src_dataset and  source_table = @src_table_name and source_schema = @src_schema_name and source_database = @src_db_name
   and sink_dataset = @sink_dataset and  sink_table = @sink_table_name and sink_schema = @sink_schema_name and sink_database = @sink_db_name;

else if @src_dataset = 'SqlServer' and @sink_dataset = 'ADLS' and @pipeline_status = 'Failed'
   update adf_data_mapping set is_masked = 'N', last_update_pipeline_id = @pipeline_run_id
   where source_dataset = @src_dataset and  source_table = @src_table_name and source_schema = @src_schema_name and source_database = @src_db_name
   and sink_dataset = @sink_dataset and  sink_table = @sink_table_name and sink_schema = @sink_schema_name and sink_database = @sink_db_name;

---------------------------------------------------------------------------
------------------------------------------- For Profiling Pipelines
---------------------------------------------------------------------------

else if @pipeline_name like 'dcsazure_%_prof_pl%'

if @src_dataset = 'ADLS' and @src_file_format = 'PARQUET' and @pipeline_status = 'Succeeded'
   update adls_discovered_ruleset set is_profiled = 'Y', last_update_pipeline_id = @pipeline_run_id
   where dataset = @src_dataset and  identified_folder = @src_table_name and specified_path = @src_schema_name and specified_container = @src_db_name and file_format = @src_file_format;

else if @src_dataset = 'ADLS' and @src_file_format = 'PARQUET' and @pipeline_status = 'Failed'
   update adls_discovered_ruleset set is_profiled = 'N', last_update_pipeline_id = @pipeline_run_id
   where dataset = @src_dataset and  identified_folder = @src_table_name and specified_path = @src_schema_name and specified_container = @src_db_name and file_format = @src_file_format;

-- for ADLS Delimited

if @src_dataset = 'ADLS' and @src_file_format = 'DELIMITED' and @pipeline_status = 'Succeeded'
   update discovered_ruleset set is_profiled = 'Y', last_update_pipeline_id = @pipeline_run_id
   where dataset = @src_dataset and  identified_table = @src_table_name and specified_schema = @src_schema_name and specified_database = @src_db_name;

else if @src_dataset = 'ADLS' and @src_file_format = 'DELIMITED' and @sink_dataset = 'ADLS' and @sink_file_format = 'DELIMITED' and @pipeline_status = 'Failed'
   update discovered_ruleset set is_profiled = 'N', last_update_pipeline_id = @pipeline_run_id
where dataset = @src_dataset and  identified_table = @src_table_name and specified_schema = @src_schema_name and specified_database = @src_db_name;

-- for Snowflake

if @src_dataset = 'Snowflake' and @src_file_format = 'N/A' and @pipeline_status = 'Succeeded'
   update discovered_ruleset set is_profiled = 'Y', last_update_pipeline_id = @pipeline_run_id
   where dataset = @src_dataset and  identified_table = @src_table_name and specified_schema = @src_schema_name and specified_database = @src_db_name;

else if @src_dataset = 'Snowflake' and @src_file_format = 'N/A'  and @pipeline_status = 'Failed'
   update discovered_ruleset set is_profiled = 'N', last_update_pipeline_id = @pipeline_run_id
where dataset = @src_dataset and  identified_table = @src_table_name and specified_schema = @src_schema_name and specified_database = @src_db_name;

-- for Snowflake

if @src_dataset = 'SqlServer' and @src_file_format = 'N/A' and @pipeline_status = 'Succeeded'
   update discovered_ruleset set is_profiled = 'Y', last_update_pipeline_id = @pipeline_run_id
   where dataset = @src_dataset and  identified_table = @src_table_name and specified_schema = @src_schema_name and specified_database = @src_db_name;

else if @src_dataset = 'SqlServer' and @src_file_format = 'N/A'  and @pipeline_status = 'Failed'
   update discovered_ruleset set is_profiled = 'N', last_update_pipeline_id = @pipeline_run_id
where dataset = @src_dataset and  identified_table = @src_table_name and specified_schema = @src_schema_name and specified_database = @src_db_name;

end;