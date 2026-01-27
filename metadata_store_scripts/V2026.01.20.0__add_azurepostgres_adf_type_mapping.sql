DELETE FROM adf_type_mapping
WHERE dataset = 'AZURE_POSTGRES';

INSERT INTO adf_type_mapping (dataset, dataset_type, adf_type)
VALUES
('AZURE_POSTGRES', 'bigint', 'long'),
('AZURE_POSTGRES', 'boolean', 'boolean'),
('AZURE_POSTGRES', 'bytea', 'binary'),
('AZURE_POSTGRES', 'character', 'string'),
('AZURE_POSTGRES', 'character varying', 'string'),
('AZURE_POSTGRES', 'date', 'date'),
('AZURE_POSTGRES', 'double precision', 'double'),
('AZURE_POSTGRES', 'integer', 'integer'),
('AZURE_POSTGRES', 'numeric', 'double'),
('AZURE_POSTGRES', 'real', 'float'),
('AZURE_POSTGRES', 'smallint', 'short'),
('AZURE_POSTGRES', 'text', 'string'),
('AZURE_POSTGRES', 'json', 'string'),
('AZURE_POSTGRES', 'jsonb', 'string'),
('AZURE_POSTGRES', 'money', 'double'),
-- bit-string types (no ADF equivalent)
('AZURE_POSTGRES', 'bit', 'string'),
('AZURE_POSTGRES', 'bit varying', 'string'),
-- interval has no ADF duration type
('AZURE_POSTGRES', 'interval', 'string'),
-- `timestamp with time zone` (aka `timestamptz`) is time-zone aware in PostgreSQL
-- But `timestamp` is time-zone naive in PostgreSQL.
-- Casting drops timezone context, so we map `timestamptz` to `string` to preserve semantics.
('AZURE_POSTGRES', 'timestamp with time zone', 'string'),
('AZURE_POSTGRES', 'timestamp without time zone', 'timestamp'),
('AZURE_POSTGRES', 'uuid', 'string'),
('AZURE_POSTGRES', 'xml', 'string');

/*
 * NOTE ON UNSUPPORTED POSTGRESQL TYPES
 * The adf_type_mapping table intentionally includes only canonical, scalar
 * PostgreSQL data types that are surfaced via information_schema.columns
 * and have a safe, deterministic representation in ADF Mapping Data Flows.
 *
 * The following PostgreSQL types are intentionally excluded:
 *
 * - Geometric types:
 *   box, circle, line, lseg, path, point, polygon
 *
 * - Network / address types:
 *   cidr, inet, macaddr, macaddr8
 *
 * - Full-text search types:
 *   tsquery, tsvector
 *
 * - Pseudo-types:
 *   smallserial
 *
 * These types are excluded because they:
 * - Have no native ADF Mapping Data Flow equivalent
 * - Are not meaningfully maskable
 * - Or resolve to canonical types at creation time (pseudo-types)
 *
 * Columns of these types may be excluded from discovery or treated as
 * unsupported explicitly to avoid silent data loss or unpredictable behavior.
*/
