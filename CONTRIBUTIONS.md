# Contribution
Contributions are welcome! We follow a strict code quality and formatting guideline to keep the repository clean and maintainable.

If you'd like to contribute be sure you're starting with the latest templates (should they exist) before you make
changes. Once you're ready to contribute your changes back, the best way to get the templates ready to add to this repo
is by leveraging the `Export template` option from the tab of the pipeline detail view in Data Factory Studio.
(Tip: before exporting your template make sure variables are set to common-sense default values.)

To make this easier to follow, let's use an example. Suppose you'd like to add `sample_template_pl` to the set of
pipelines that are available in this repository. Your exported template will come from Azure with the name
`sample_template_pl.zip`. Once you unzip this file, it will produce a directory with structure like:
```
sample_template_pl
├── manifest.json
└── sample_template_pl.json
```

These files, since they are meant to be imported directly into Data Factory Studio are not well-suited to version
control (as they are one very long line). To make these files more suited to version control, the following utility
script has been provided [./scripts/format.sh](./scripts/format.sh). To leverage the script, export the template from
the data factory studio, this will result in a zip file downloaded to your local machine, move the template from the
download destination directory to the base directory of the repository, from a terminal with a working directory
of this repository, run `./scripts/format.sh`.

Using the example above, the script will unzip the template, re-format the files leveraging `jq` and then remove the
zip file. Note that this will only unzip the pipelines that we already have support for in this repo. Manual unzip will
need to be performed for new pipelines.

New templates will need to have an associated `README.md` and for them to be built into the `releases` folder, edits
will have to be made to the `docker-compose.yaml`. For this example, you'd have to add a line
`zip sample_template_pl.zip sample_template_pl/* &&` somewhere between `apt-get install -y zip &&`
and `mv *.zip releases/."`.

If Metadata store scripts are needed to support your new templates or new features, please update the
`metadata_store_scripts` directory to include those changes, and update the `README.md` for all impacted templates to
incidate the need for the new DB version. New statements should play nicely with previous statements - isolate the
changes in a particular versioned script, and follow the version naming convention. Versioned scripts follow the naming
convention `V<version_number>__<comment>.sql`, where `<version_number>` is `YYYY.MM.DD.#` and represents the date when
this script is added (with the final digit being used to allow multiple versions to be tagged with the same date).
You can leverage the [./scripts/migrations.sh](./scripts/migrations.sh) to automatically add the new versioned migration
to the [./metadata_store_scripts/bootstrap.sql](./metadata_store_scripts/bootstrap.sql) file.

The templates present in this repository use parameterized linked services for source/sink databases. While modifying
templates, please ensure that the linked services created to be used in the template for source/sink databases
are parameterized as configured in the existing template. If you are adding or removing some parameters of any linked
service, please ensure that the changes are reflected in the
[pipeline_template_standard_params.yaml](pipeline_template_standard_params.yaml) file. Also, linked service parameters of
any new template should be added to the same file.

## Linked Service Naming Conventions

When creating or modifying pipelines, all linked services must follow the repository's naming conventions:

1. **Mandatory Services:** All pipelines must include:
   - `Metadata Datastore` (AzureSqlDatabase)
   - `ProdDCSForAzureService` (RestService)

2. **Source/Sink Requirements:**
   - All pipelines need at least one `_Source`
   - Mask pipelines need at least one `_Sink` (except in-place masking)

3. **Naming Format:**
   - RestService: CamelCase without underscores (e.g., `DataverseService`)
   - Non-RestService: `CamelCase_{Source|Sink|Staging}` (e.g., `AzureSqlDatabase_Source`)

4. **Technology Names:** Must match the support type mapping in the configuration

📖 **Complete guide:** [Linked Service Naming Conventions](./documentation/guides/linked-service-naming-conventions.md)

**Validation:** Pre-commit hooks automatically validate naming conventions. Run manually:
```bash
python3 scripts/pre-commit/validate_linked_services.py
```

## Code Style and Standards
Before committing, you must run the `pre-commit` checks to ensure your changes meet the repository’s standards.

### Pre-commit Hooks

We use `pre-commit` to enforce coding standards like:

1. Directory names containing ADF pipeline templates for different source and sink databases.
2. Naming convention of source and sink databases in the ADF pipeline templates formatting.
3. Parameter and dataflow naming convention in pipeline templates.
4. **Linked service naming conventions.**
5. Version numbering.
6. Validation of `CHANGELOG.md`.
7. `YAML`/`SQL`/`JSON` validation.
8. Count of params used in the source/sink database linked services of the pipeline templates.

### Setup Instructions

#### 1: Install pre-commit
If you don’t have it already, install pre-commit:
```
pip install pre-commit
```
#### 2: Run checks manually (optional)
Before committing, you can run the pre-commit hooks manually:
```
pre-commit run --all-files
```
This is helpful to catch issues early before writing your commit message.

### Commit Message Guidelines
Please use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) format for all commit messages to ensure consistency and automation support. Examples:
```
chore: any non code related change
feat: adding new templates or a feature in an existing template
docs: documentation update
```
