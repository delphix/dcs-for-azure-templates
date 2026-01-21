DELETE from adf_type_mapping where dataset = 'AZURE_POSTGRES';


INSERT INTO adf_type_mapping (dataset, dataset_type, adf_type)
VALUES
-- Integer types
('AZURE_POSTGRES','smallint','short'),
('AZURE_POSTGRES','integer','integer'),
('AZURE_POSTGRES','bigint','long'),
('AZURE_POSTGRES','serial','integer'),
('AZURE_POSTGRES','bigserial','long'),

-- Floating / exact numeric
('AZURE_POSTGRES','real','float'),
('AZURE_POSTGRES','double precision','double'),
('AZURE_POSTGRES','numeric','decimal'),
('AZURE_POSTGRES','decimal','decimal'),
('AZURE_POSTGRES','money','decimal'),

-- Boolean
('AZURE_POSTGRES','boolean','boolean'),

-- Character / text
('AZURE_POSTGRES','char','string'),
('AZURE_POSTGRES','character','string'),
('AZURE_POSTGRES','character varying','string'),
('AZURE_POSTGRES','varchar','string'),
('AZURE_POSTGRES','text','string'),
('AZURE_POSTGRES','xml','string'),

-- Date / Time
('AZURE_POSTGRES','date','date'),
('AZURE_POSTGRES','time','string'),
('AZURE_POSTGRES','timestamp','timestamp'),
('AZURE_POSTGRES','timestamp without time zone','timestamp'),
('AZURE_POSTGRES','timestamptz','timestamp'),

-- Binary
('AZURE_POSTGRES','bytea','binary'),

-- Semi-structured
('AZURE_POSTGRES','json','string'),
('AZURE_POSTGRES','jsonb','string'),

-- Other common PG types
('AZURE_POSTGRES','uuid','string'),
('AZURE_POSTGRES','USER-DEFINED','string');
