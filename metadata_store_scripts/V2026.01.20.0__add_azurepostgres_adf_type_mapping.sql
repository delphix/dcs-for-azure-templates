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
-- `timestamptz` is time-zone aware, but `timestamp` is time-zone naive in PostgreSQL.
-- Casting drops timezone context, so we map `timestamptz` to `string` to preserve semantics.
('AZURE_POSTGRES', 'timestamp with time zone', 'string'),
('AZURE_POSTGRES', 'timestamp without time zone', 'timestamp'),
('AZURE_POSTGRES', 'uuid', 'string'),
('AZURE_POSTGRES', 'varchar', 'string'),
('AZURE_POSTGRES', 'xml', 'string')
;
