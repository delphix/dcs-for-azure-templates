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
