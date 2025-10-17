-- Constraint to ensure that for source datasets of type 'DATAVERSE',
-- the source and sink schema, table, and database must match exactly.
-- If the source_dataset is not 'DATAVERSE', this check is bypassed.
ALTER TABLE adf_data_mapping
ADD CONSTRAINT [CK_adf_data_mapping_source_sink_match_for_dataverse]
CHECK (
    (
        source_dataset = 'DATAVERSE'
        AND source_table = sink_table
        AND source_schema = sink_schema
        AND source_database = sink_database
    )
    OR (source_dataset <> 'DATAVERSE')
);
GO


-- Constraint to ensure that when a ruleset is marked as excluded (is_excluded = 1),
-- the assigned_algorithm must be NULL. 
-- For all other cases (is_excluded not equal to 1), no restriction is enforced.
ALTER TABLE discovered_ruleset
ADD CONSTRAINT [CK_discovered_ruleset_assigned_algorithm_null_when_excluded]
CHECK (
    (is_excluded = 1 AND assigned_algorithm IS NULL)
    OR (is_excluded <> 1)
);
GO
