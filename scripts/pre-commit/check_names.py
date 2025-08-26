#
# Copyright (c) 2025 by Delphix. All rights reserved.
#

import logging
import pathlib
import re
import typing as tp
import json

import helpers

log = logging.getLogger("check_names")
log.setLevel(logging.INFO)

PL_NAME = (
    "dcsazure_<source-data-source>_to_<sink-data-source><_optional-identifier_>_{service}_pl"
)
MASKING_PL_NAME = PL_NAME.format(service="mask")
DISCOVERY_PL_NAME = PL_NAME.format(service="discovery")

NAMING_ERROR = f"""
The files do not follow the naming conventions. Use the format below.

{MASKING_PL_NAME}/{MASKING_PL_NAME}.json
{MASKING_PL_NAME}/{helpers.MANIFEST_FILE}
{MASKING_PL_NAME}/{helpers.README_FILE}

{DISCOVERY_PL_NAME}/{DISCOVERY_PL_NAME}.json
{DISCOVERY_PL_NAME}/{helpers.MANIFEST_FILE}
{DISCOVERY_PL_NAME}/{helpers.README_FILE}
"""


def validate_file_and_directory_names(files: tp.Set[pathlib.Path]) -> None:
    """
    Validate the names of all the files being added to repo
    """
    invalid_files = [
        file
        for file in files
        if not is_valid_template_file_name(file)
    ]
    if invalid_files:
        raise helpers.InvalidTemplateNameException(
            "\n".join([file.absolute().as_uri() for file in invalid_files])
        )


def get_name_from_resource(resource_name: str) -> str:
    """
    Extract the name "dcsazure_ADLS_to_ADLS_container_pl" of the resource from
    the below sample string present in template's JSON file
    "[concat(parameters('factoryName'), '/dcsazure_ADLS_to_ADLS_container_pl')]"
    """
    # Split initial 'concat' label and discard it
    name_string = resource_name.split()[1]
    # Separate the single quotes from the name
    name_with_slash = name_string.split("'")[1]
    # Remove leading '/' character
    return name_with_slash[1:]


def get_param_prefix_from_resource_type(resource_type: str) -> str:
    """
    Get the parameter name prefix of the resource type present in template's JSON file
    For e.g. "DS_" for datasets in "Microsoft.DataFactory/factories/datasets"
    """
    # Split the resource type with "/" and pick the last element
    resource = resource_type.split("/")[2]
    # Read the parameter name prefix given in helpers.resource_type_abbr dict
    # as follows:: "datasets": ("DS_", "_ds")
    return helpers.RESOURCE_TYPE_ABBR[resource][0]


def get_type_abbr_from_resource_type(resource_type: str) -> str:
    """
    Get the abbreviation of the resource type present in template's JSON file.
    For e.g. "_pl" for pipelines in "Microsoft.DataFactory/factories/pipelines"
    """
    # Split the resource type with "/" and pick the last element
    resource = resource_type.split("/")[2]
    # Read the resource abbreviation given in helpers.resource_type_abbr dict
    # as follows:: "pipelines": ("P_", "_pl"),
    return helpers.RESOURCE_TYPE_ABBR[resource][1]


def validate_resource_names(file: pathlib.Path, resource_data: tp.List[tp.Any]) -> None:
    json_parent = file.parts[0]
    pipeline = helpers.Pipeline.from_string(json_parent)
    resource_prefix = f"{helpers.TEMPLATES_JSON_PATH_PREFIX}{pipeline.source_db}_to_{pipeline.sink_db}"

    invalid_resource_names = []

    for resource in resource_data:
        resource_name = get_name_from_resource(resource["name"])
        resource_abbr = get_type_abbr_from_resource_type(resource["type"])
        if resource_name == 'default':
            continue
        if not resource_name.startswith(resource_prefix):
            invalid_resource_names.append(resource_name)
            log.error(
                f"Resource name '{resource_name}' in {file.absolute().as_uri()}"
                f" should start with '{resource_prefix}'."
            )
        if not resource_name.endswith(resource_abbr):
            invalid_resource_names.append(resource_name)
            log.error(
                f"Resource name '{resource_name}' of type '{resource['type']}'"
                f" in {file.absolute().as_uri()} should end with '{resource_abbr}'."
            )

    if invalid_resource_names:
        raise helpers.InvalidResourceNameException(
            f"\nERROR - Resource name(s) in {file.absolute().as_uri()} file is(are) not as per convention."
        )


def validate_resource_parameter_names(file: pathlib.Path, resource_data: tp.List[tp.Any]) -> None:

    invalid_parameters = []

    for resource in resource_data:
        resource_name = get_name_from_resource(resource["name"])
        param_prefix = get_param_prefix_from_resource_type(resource["type"])
        if not param_prefix or "parameters" not in resource["properties"]:
            continue

        for param in resource["properties"]["parameters"]:
            if not param.startswith(param_prefix):
                invalid_parameters.append(resource_name)
                log.error(
                    f"The parameter '{param}' of resource name '{resource_name}'"
                    f" of type '{resource['type']}' in {file.absolute().as_uri()}"
                    f" should start with '{param_prefix}'."
                )

    if invalid_parameters:
        raise helpers.InvalidParameterNameException(
            f"\nERROR - Resource parameters in {file.absolute().as_uri()} file do"
            f" not follow the naming conventions."
        )


def validate_resource_activity_names(file: pathlib.Path, resource_data: tp.List[tp.Any]) -> None:

    invalid_activity_names = []

    for resource in resource_data:
        resource_name = get_name_from_resource(resource["name"])
        if "activities" not in resource["properties"]:
            continue

        for activity in resource["properties"]["activities"]:
            if not helpers.is_title(activity["name"]):
                # Activity name should be like: "Until No More Blobs", "Use CSV"
                invalid_activity_names.append(resource_name)
                log.error(
                    f"The activity name '{activity['name']}' of resource name '{resource_name}'"
                    f" in {file.absolute().as_uri()} should be in title case."
                )

    if invalid_activity_names:
        raise helpers.InvalidParameterNameException(
            f"\nERROR - Resource activity name(s) in {file.absolute().as_uri()} file is(are) not as per convention."
        )


def validate_manifest_json_content(file: pathlib.Path) -> None:
    with open(file) as f:
        out = json.load(f)

    json_parent = file.parts[0]
    manifest_name = out["name"]
    if manifest_name != json_parent:
        log.error(
            f"The name field in {file.absolute().as_uri()}:2 file should match the"
            f" template directory name '{json_parent}'."
        )
        raise helpers.InvalidResourceNameException(
            f"\nERROR - The name field in {file.absolute().as_uri()} file is not as per convention."
        )


def validate_code_json_content(file: pathlib.Path) -> None:
    with open(file) as f:
        json_content = json.load(f)

    validate_resource_names(file, json_content['resources'])
    validate_resource_parameter_names(file, json_content['resources'])
    validate_resource_activity_names(file, json_content['resources'])


def validate_pipeline_json_content(files: tp.Set[pathlib.Path]) -> None:
    all_json = filter_json_files(files)
    for file in all_json:
        if is_template_code_json(file):
            validate_code_json_content(file)
        elif is_manifest_json(file):
            validate_manifest_json_content(file)


def filter_json_files(files: tp.Set[pathlib.Path]) -> tp.List[pathlib.Path]:
    json_files = []
    for file in files:
        if (
            len(file.parts) == 2
            and file.parts[0].startswith(helpers.TEMPLATES_JSON_PATH_PREFIX)
        ):
            json_files.append(file)
    return json_files


def is_template_code_json(file: pathlib.Path) -> bool:
    return file.parts[1].startswith(helpers.TEMPLATES_JSON_PATH_PREFIX)


def is_manifest_json(file: pathlib.Path) -> bool:
    return file.parts[1] == helpers.MANIFEST_FILE


def is_valid_template_file_name(filename: pathlib.Path) -> bool:
    if filename.parent.parts:
        top_dir = filename.parent.parts[0]
        # Skip the validations for files added in non template directories
        if top_dir in helpers.NON_TEMPLATES_DIR:
            return True

        # Raise if the pipeline file is created inside nested directories
        if len(filename.parent.parts) > 1:
            return False

        # Raise if the pipeline file's directory name doesn't follow the convention
        if not re.match(helpers.TEMPLATE_DIR_REGEX, top_dir):
            return False

        # Raise if the pipeline JSON filename doesn't follow the convention or if it's
        # not a manifest.json or README.md file
        if filename.name not in [
            (top_dir + helpers.JSON), helpers.MANIFEST_FILE, helpers.README_FILE
        ]:
            return False
        return True
    return True


def main() -> int:
    """
    This is triggered by the pre-commit hook to ensure that the template files
    added for a new pipeline follow the naming conventions of the repo
    :return: system exit status 1|0
    """
    exit_status = 0
    modified_files = helpers.get_all_modified_files()
    deleted_files = helpers.get_staged_deleted_files()

    # Validate the files which are added or modified
    active_modified_files = set(modified_files) - set(deleted_files)
    try:
        validate_file_and_directory_names(active_modified_files)
        validate_pipeline_json_content(active_modified_files)
    except helpers.InvalidTemplateNameException as e:
        log.error(f"{e}\n{NAMING_ERROR}")
        exit_status = 1
    except (helpers.InvalidResourceNameException, helpers.InvalidParameterNameException) as e:
        log.error(e)
        exit_status = 1

    return exit_status


if __name__ == "__main__":
    exit(main())
