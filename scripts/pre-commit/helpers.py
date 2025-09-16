#
# Copyright (c) 2025 by Delphix. All rights reserved.
#

import dataclasses
import pathlib
import re
import subprocess
import typing as tp

from packaging.version import Version

MANIFEST_FILE = "manifest.json"
README_FILE = "README.md"
VERSION_FILE = "VERSION.md"
CHANGELOG_FILE = "CHANGELOG.md"
TEMPLATES_JSON_PATH_PREFIX = "dcsazure_"
METADATA_STORE_PATH = "metadata_store_scripts"
DOCUMENTATION_PATH = "documentation"
DOCKER_COMPOSE_FILE = "docker-compose.yaml"
DOCUMENTATION_FILE = f"{DOCUMENTATION_PATH}/pipelines.md"
PIPELINE_LS_PARAM_YAML_FILE = "pipeline_template_standard_params.yaml"
JSON = ".json"
SQL = ".sql"
MD = ".md"

RESOURCE_TYPE_ABBR = {
    "pipelines": ("P_", "_pl"),
    "datasets": ("DS_", "_ds"),
    "dataflows": (None, "_df"),
    "managedVirtualNetworks": (None, "default"),
}

NON_TEMPLATES_DIR = [
    DOCUMENTATION_PATH, METADATA_STORE_PATH, "releases", "scripts",
]

#
# This regex is used to match the template directory names
# e.g. dcsazure_<source_db>_to_<sink_db>_<service>_pl
# e.g. dcsazure_AzureSQL_to_AzureSQL_discovery_pl
# e.g. dcsazure_AzureSQL_to_AzureSQL_mask_pl
# e.g dcsazure_ADLS_to_ADLS_delimited_discovery_pl
# Captures:
#   1. source_db as group 1 --> ([A-Za-z]+)
#   2. sink_db and optional specifier as group 2 --> ([A-Za-z]+(?:_[A-Za-z]+)*)
#      2.1 (?:_[A-Za-z]+)* allows 0 or more letters, making it optional
#   3. service - mask or discovery as group 3 --> (mask|discovery)
#
TEMPLATE_DIR_REGEX = rf"^{TEMPLATES_JSON_PATH_PREFIX}([A-Za-z0-9]+)_to_([A-Za-z0-9]+(?:_[A-Za-z]+)*)_(mask|discovery)_pl$"


class GitCommand:
    MODIFIED_FILES = [
        "git", "diff", "--no-merges", "--name-only", "--first-parent", "origin/main"
    ]
    FILE_CONTENT = ["git", "show"]
    COMMIT_MESSAGE = ["git", "log", "-1", "--pretty=%s"]
    PROJECT_ROOT = ["git", "rev-parse", "--show-toplevel"]
    ORIGIN_FILES = ["git", "ls-tree", "-r", "origin/main", "--name-only"]


class FileNotFoundException(Exception):
    """Exception to be raised for invalid or non-existent VERSION.md"""
    pass


class VersionParsingException(Exception):
    """Exception to be raised for failures in parsing the version present in VERSION.md"""
    pass


class InvalidVersionException(Exception):
    """Exception to be raised if an incorrect version present in VERSION.md"""
    pass


class InvalidTemplateNameException(Exception):
    """
    Exception to be raised if the ADF pipeline's template file or directory
    names are not per convention.
    """
    pass


class InvalidResourceNameException(Exception):
    """
    Exception to be raised if the resource name in the ADF pipeline's template file
    are not per convention.
    """
    pass


class InvalidParameterNameException(Exception):
    """
    Exception to be raised if the parameter name in the ADF pipeline's template file
    are not per convention.
    """
    pass


class InvalidLinkedServiceParamCountException(Exception):
    """
    Exception to be raised if the LinkedServiceParameters count in the ADF pipeline's template
    file are not as per convention.
    """
    pass

@dataclasses.dataclass
class Pipeline:
    source_db: str
    sink_db: str
    service: str

    @classmethod
    def from_string(cls, input_str: str):
        match = re.match(TEMPLATE_DIR_REGEX, input_str)
        if match:
            return cls(match.group(1), match.group(2), match.group(3))
        else:
            raise ValueError("Input name can't be parsed as a pipeline name.")


def get_cmd_output(cmd: tp.List[str]) -> str:
    """
    Get the subprocess output for the command to be run
    :param cmd: command parameters as list
    :return: Command output
    """
    try:
        return subprocess.check_output(cmd).decode("utf-8")
    except subprocess.CalledProcessError as e:
        raise Exception(f"Could not retrieve command output: {e}")


def get_project_root() -> pathlib.Path:
    return pathlib.Path(get_cmd_output(GitCommand.PROJECT_ROOT).strip())


def get_commit_message() -> str:
    """
    Get the commit message
    """
    return get_cmd_output(GitCommand.COMMIT_MESSAGE).strip()


def get_files_from_origin_main() -> tp.List[pathlib.Path]:
    """
    Get the list of files from the origin/main branch
    """
    origin_pipeline_files = get_cmd_output(GitCommand.ORIGIN_FILES).strip()
    return [pathlib.Path(path) for path in origin_pipeline_files.splitlines()]


def get_staged_deleted_files() -> tp.List[pathlib.Path]:
    """
    Get the list of files deleted in the commit
    """
    try:
        deleted_files = get_cmd_output(GitCommand.MODIFIED_FILES + ["--diff-filter=D"])
        return [pathlib.Path(path) for path in deleted_files.splitlines()]
    except subprocess.CalledProcessError as e:
        raise Exception(f"Could not retrieve git diff: {e}")


def get_all_modified_files() -> tp.List[pathlib.Path]:
    """
    Retrieve the list of files updated from the upstream branch
    """
    try:
        files_updated = get_cmd_output(GitCommand.MODIFIED_FILES)
        return [pathlib.Path(path) for path in files_updated.splitlines()]
    except subprocess.CalledProcessError as e:
        raise Exception(f"Could not retrieve git diff: {e}")


def get_version_from_version_md(version_md: str) -> Version:
    """
    Previous version of a file
    :return: previous version
    """
    try:
        return Version(version_md)
    except Exception as e:
        raise VersionParsingException(f"Invalid version number: {str(e)}")


def get_previous_version() -> Version:
    """
    Parse version from the previous version of the file
    """
    version_md = get_cmd_output(
        GitCommand.FILE_CONTENT + [f"origin/main:{VERSION_FILE}"]
    ).strip()
    return get_version_from_version_md(version_md)


def get_current_version(file_path: str) -> Version:
    """
    Parse version from the current version of the file
    """
    with open(file_path, "r") as f:
        version_md = f.read().strip()
    return get_version_from_version_md(version_md)


def get_next_version(version: Version) -> tp.List[str]:
    """
    Get the valid next versions
    :param version: As present in the VERSION.md file
    :return: List of possible versions
    """
    return [
        f"{version.major}.{version.minor}.{version.micro + 1}",
        f"{version.major}.{version.minor + 1}.0",
        f"{version.major + 1}.0.0",
    ]


def is_title(inp: str) -> bool:
    """
    Check if the input string is in title case format. E.g. Both
    'Check CSV', 'Select Tables' will be considered as title case.
    Python inbuilt `istitle()` returns False for the former.

    :param inp: Input String
    :return: True if input string is in title case
    """
    return all(substr[0].isupper() for substr in inp.strip().split())
