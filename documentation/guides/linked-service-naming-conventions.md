# Linked Service Naming Conventions

## Overview

All ADF pipeline templates in this repository follow standardized naming conventions for linked services to ensure consistency, maintainability, and clarity across the codebase.

## The 6 Rules

### Rule 1: Mandatory Metadata Datastore

**All pipelines MUST have a metadata datastore linked service.**

- **Name:** `Metadata Datastore`
- **Support Type:** `AzureSqlDatabase`
- **Rationale:** Centralized metadata storage required across all pipelines

```json
"Metadata Datastore": {
  "supportTypes": ["AzureSqlDatabase"]
}
```

---

### Rule 2: Mandatory DCS Service

**All pipelines MUST have a DCS for Azure REST service.**

- **Name:** `ProdDCSForAzureService`
- **Support Type:** `RestService`
- **Rationale:** Required to access Delphix Compliance Services APIs

```json
"ProdDCSForAzureService": {
  "supportTypes": ["RestService"]
}
```

---

### Rule 3: Source and Sink Requirements

**Source/Sink Requirements:**
1. All pipelines MUST have at least 1 `_Source` linked service
2. Mask pipelines (except in-place masking) MUST have at least 1 `_Sink` linked service

**Exceptions:**
- Discovery pipelines (`*_discovery_pl`) don't require Sink
- In-place masking pipelines don't require Sink

**Examples:**

```json
"linkedservices": {
  "AzureDataLakeStorage_Source": {"supportTypes": ["AzureBlobFS"]},
  "AzureDataLakeStorage_Sink": {"supportTypes": ["AzureBlobFS"]}
}

"linkedservices": {
  "AzureSqlDatabase_Source": {"supportTypes": ["AzureSqlDatabase"]}
}

"linkedservices": {
  "Dataverse_Source": {"supportTypes": ["CommonDataServiceForApps"]}
}
```

---

### Rule 4: RestService Naming Convention

**RestService names MUST use CamelCase without underscores.**

**Format:** `CamelCase` (no underscores)

**Special Rule:** RestService names containing both "DCS" and "Azure" MUST be named `ProdDCSForAzureService`

**Valid Examples:**
- `ProdDCSForAzureService`
- `DataverseService`

**Invalid Examples:**
- `DCS_For_Azure_Prod` (has underscores)
- `dcsForAzure` (not CamelCase)
- `DCSForAzureProd` (should be `ProdDCSForAzureService`)

---

### Rule 5: Non-RestService Naming Convention

**Non-RestService names MUST follow `CamelCase_{Role}` format where the technology name matches the support type.**

**Format:** `{TechnologyName}_{Role}`
- **TechnologyName:** Must match the mapping below
- **Role:** One of `Source`, `Sink`, or `Staging`

**Technology Name Mapping:**

| Support Type | Technology Name |
|-------------|----------------|
| `AzureBlobFS` | `AzureDataLakeStorage` |
| `AzureSqlDatabase` | `AzureSqlDatabase` |
| `AzureSqlMI` | `AzureSqlMI` |
| `SnowflakeV2` | `Snowflake` |
| `AzureDatabricksDeltaLake` | `AzureDatabricksDeltaLake` |
| `AzureBlobStorage` | `BlobStorage` |
| `CommonDataServiceForApps` | `Dataverse` |

**Valid Examples:**
```json
"AzureDataLakeStorage_Source": {"supportTypes": ["AzureBlobFS"]}
"AzureSqlMI_Sink": {"supportTypes": ["AzureSqlMI"]}
"BlobStorage_Staging": {"supportTypes": ["AzureBlobStorage"]}
"Snowflake_Source": {"supportTypes": ["SnowflakeV2"]}
```

**Invalid Examples:**
- `azuresql_source` (lowercase, no technology name)
- `AzureSqlDatabaseSource` (no underscore separator)
- `AzureBlob_Source` (wrong tech name for AzureBlobFS)

---

### Rule 6: No Other Formats Allowed

Only the formats specified in Rules 1-5 are permitted.

**Examples of Prohibited patterns:**
- snake_case: `azure_sql_source`
- Non-REST linked service name with no underscore like `BlobStoreStagingArea` 

---

## Validation

All linked service names are automatically validated through pre-commit hooks. The validation script checks all 6 rules and will block commits that don't comply.

**Run validation manually:**
```bash
python3 scripts/pre-commit/validate_linked_services.py
```

## Configuration

Naming conventions are configured in `pipeline_template_standard_params.yaml`:
- Mandatory service definitions
- In-place masking pipeline list
- Technology name mappings

## Contributing

When creating new pipelines:
1. Follow these 6 naming rules
2. Run validation locally before committing
3. Pre-commit hooks will enforce these rules automatically

For questions or exceptions, please file an issue.
