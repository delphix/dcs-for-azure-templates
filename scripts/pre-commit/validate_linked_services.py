#!/usr/bin/env python3
"""
Linked Services Validation Script
Validates linked service names against naming convention rules
"""

import json
import os
import re
import sys
import yaml
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger("validate_linked_services")

# Support type to technology name mapping
SUPPORT_TYPE_TO_TECHNOLOGY = {
    "AzureBlobFS": "AzureDataLakeStorage",
    "AzureSqlDatabase": "AzureSqlDatabase",
    "AzureSqlMI": "AzureSqlMI",
    "SnowflakeV2": "Snowflake",
    "AzureDatabricksDeltaLake": "AzureDatabricksDeltaLake",
    "AzureBlobStorage": "BlobStorage",
    "CommonDataServiceForApps": "Dataverse"
}


class ValidationError(Exception):
    """Custom exception for validation errors"""
    pass


def load_config():
    """Load configuration from pipeline_template_standard_params.yaml"""
    config_path = "pipeline_template_standard_params.yaml"
    
    default_config = {
        'in_place_masking_pipelines': ['dcsazure_Dataverse_to_Dataverse_in_place_mask_pl']
    }
    
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            return config.get('linked_service_conventions', default_config)
    
    return default_config


def validate_linked_service(service_name, support_types, pipeline_name):
    """Validate a single linked service against all rules"""
    support_type = support_types[0] if support_types else None
    
    # Rule 1: Check for mandatory Metadata Datastore
    if service_name == "Metadata Datastore":
        if support_type != "AzureSqlDatabase":
            raise ValidationError(f"Rule 1: 'Metadata Datastore' must have supportType 'AzureSqlDatabase', got '{support_type}'")
        return  # Valid
    
    # Rule 2: Check for mandatory ProdDCSForAzureService
    if service_name == "ProdDCSForAzureService":
        if support_type != "RestService":
            raise ValidationError(f"Rule 2: 'ProdDCSForAzureService' must have supportType 'RestService', got '{support_type}'")
        return  # Valid
    
    # Rule 4: RestService naming convention
    if support_type == "RestService":
        # Must be CamelCase without underscores
        if "_" in service_name:
            raise ValidationError(f"Rule 4: RestService '{service_name}' must not contain underscores")
        if not re.match(r"^[A-Z][a-zA-Z0-9]+$", service_name):
            raise ValidationError(f"Rule 4: RestService '{service_name}' must be CamelCase")
        # Special check: DCS+Azure RestServices must use ProdDCSForAzureService
        if "dcs" in service_name.lower() and "azure" in service_name.lower():
            if service_name != "ProdDCSForAzureService":
                raise ValidationError(f"Rule 4: RestService with 'DCS' and 'Azure' must be named 'ProdDCSForAzureService', got '{service_name}'")
        return  # Valid
    
    # Rule 5: Non-RestService must follow CamelCase_{Source|Sink|Staging}
    if not re.match(r"^[A-Z][a-zA-Z0-9]+_(Source|Sink|Staging)$", service_name):
        raise ValidationError(f"Rule 5: Non-RestService '{service_name}' must follow format 'CamelCase_{{Source|Sink|Staging}}'")
    
    # Verify technology name matches support type
    tech_name = service_name.rsplit('_', 1)[0]  # Extract technology part
    expected_tech = SUPPORT_TYPE_TO_TECHNOLOGY.get(support_type)
    if expected_tech and tech_name != expected_tech:
        raise ValidationError(f"Rule 5: Technology name '{tech_name}' doesn't match expected '{expected_tech}' for supportType '{support_type}'")


def validate_pipeline(manifest_path, config):
    """Validate all linked services in a pipeline"""
    with open(manifest_path, 'r') as f:
        manifest = json.load(f)
    
    pipeline_name = os.path.basename(os.path.dirname(manifest_path))
    linked_services = manifest.get('requires', {}).get('linkedservices', {})
    
    errors = []
    
    # Check mandatory services are present (Rules 1 & 2)
    if "Metadata Datastore" not in linked_services:
        errors.append(f"Rule 1: Missing mandatory 'Metadata Datastore'")
    
    if "ProdDCSForAzureService" not in linked_services:
        errors.append(f"Rule 2: Missing mandatory 'ProdDCSForAzureService'")
    
    # Rule 3: Check Source/Sink requirement
    sources = [name for name in linked_services if name.endswith('_Source')]
    sinks = [name for name in linked_services if name.endswith('_Sink')]
    
    # All pipelines must have at least 1 Source
    if len(sources) < 1:
        errors.append(f"Rule 3: Must have at least 1 _Source linked service")
    
    # Mask pipelines (not discovery, not in-place) must have at least 1 Sink
    if "_discovery_pl" not in pipeline_name:
        in_place_masking = config.get('in_place_masking_pipelines', [])
        if pipeline_name not in in_place_masking:
            if len(sinks) < 1:
                errors.append(f"Rule 3: Mask pipeline must have at least 1 _Sink linked service")
    
    # Validate each linked service (Rules 4 & 5)
    for service_name, service_config in linked_services.items():
        support_types = service_config.get('supportTypes', [])
        try:
            validate_linked_service(service_name, support_types, pipeline_name)
        except ValidationError as e:
            errors.append(str(e))
    
    return errors


def main():
    """Main validation function"""
    
    # Load configuration
    config = load_config()
    
    # Find all manifest.json files
    manifest_files = []
    for root, dirs, files in os.walk('.'):
        # Skip temp and .git directories
        if 'temp' in root or '.git' in root:
            continue
        if 'manifest.json' in files:
            manifest_files.append(os.path.join(root, 'manifest.json'))
    
    total_violations = 0
    failed_pipelines = []
    
    for manifest_path in sorted(manifest_files):
        pipeline_name = os.path.basename(os.path.dirname(manifest_path))
        errors = validate_pipeline(manifest_path, config)

        if errors:
            failed_pipelines.append(pipeline_name)
            total_violations += len(errors)
            error_lines = "\n".join([f"   - {error}" for error in errors])
            error_message = (
                f"❌ {pipeline_name}:\n"
                f"{error_lines}"
            )
            logger.error(error_message)

    if total_violations > 0:
        summary_message = (
            f"\n❌ Validation FAILED\n"
            f"   {len(failed_pipelines)} pipeline(s) with {total_violations} violation(s)"
        )
        logger.error(summary_message)
        return 1
    else:
        return 0


if __name__ == "__main__":
    sys.exit(main())
