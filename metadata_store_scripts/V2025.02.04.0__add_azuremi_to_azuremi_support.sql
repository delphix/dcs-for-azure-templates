
-- Update ADF type mappings of decimal, numeric and money to double for AzureSQL dataset
UPDATE adf_type_mapping
SET adf_type = 'double'
WHERE dataset = 'AZURESQL' AND dataset_type IN ('decimal', 'numeric', 'money');

-- Update ADF type mapping of smallmoney to float for AzureSQL dataset
UPDATE adf_type_mapping
SET adf_type = 'float'
WHERE dataset = 'AZURESQL' AND dataset_type = 'smallmoney';

-- Copy ADF type mapping from AzureSQL to AzureMI
INSERT INTO adf_type_mapping(dataset, dataset_type, adf_type)
SELECT 'AZUREMI', dataset_type, adf_type
FROM adf_type_mapping
WHERE dataset = 'AZURESQL';
