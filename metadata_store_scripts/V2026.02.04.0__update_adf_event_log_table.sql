ALTER TABLE adf_events_log
ALTER COLUMN source_schema VARCHAR(255) COLLATE sql_latin1_general_cp1_ci_as NULL;

ALTER TABLE adf_events_log
ALTER COLUMN sink_schema VARCHAR(255) COLLATE sql_latin1_general_cp1_ci_as NULL;
