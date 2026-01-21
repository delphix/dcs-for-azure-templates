DELETE FROM adf_type_mapping WHERE dataset = 'AZURE_POSTGRES';


INSERT INTO adf_type_mapping (dataset, dataset_type, adf_type)
   VALUES
('AZURE_POSTGRES', 'smallint', 'short'),
('AZURE_POSTGRES', 'integer', 'integer'),
('AZURE_POSTGRES', 'bigint', 'long'),
('AZURE_POSTGRES', 'serial', 'integer'),
('AZURE_POSTGRES', 'bigserial', 'long'),
('AZURE_POSTGRES', 'real', 'float'),
('AZURE_POSTGRES', 'double precision', 'double'),
('AZURE_POSTGRES', 'numeric', 'decimal'),
('AZURE_POSTGRES', 'decimal', 'decimal'),
('AZURE_POSTGRES', 'money', 'decimal'),
('AZURE_POSTGRES', 'boolean', 'boolean'),
('AZURE_POSTGRES', 'char', 'string'),
('AZURE_POSTGRES', 'character', 'string'),
('AZURE_POSTGRES', 'character varying', 'string'),
('AZURE_POSTGRES', 'varchar', 'string'),
('AZURE_POSTGRES', 'text', 'string'),
('AZURE_POSTGRES', 'xml', 'string'),
('AZURE_POSTGRES', 'date', 'date'),
('AZURE_POSTGRES', 'time', 'string'),
('AZURE_POSTGRES', 'timestamp', 'timestamp'),
('AZURE_POSTGRES', 'timestamp without time zone', 'timestamp'),
('AZURE_POSTGRES', 'timestamptz', 'timestamp'),
('AZURE_POSTGRES', 'bytea', 'binary'),
('AZURE_POSTGRES', 'json', 'string'),
('AZURE_POSTGRES', 'jsonb', 'string'),
('AZURE_POSTGRES', 'uuid', 'string'),
('AZURE_POSTGRES', 'USER-DEFINED', 'string')
;
