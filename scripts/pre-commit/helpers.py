#
# Copyright (c) 2025 by Delphix. All rights reserved.
#

import pathlib
import subprocess
import typing as tp

from packaging.version import Version

VERSION_FILE = "VERSION.md"
CHANGELOG_FILE = "CHANGELOG.md"
TEMPLATES_JSON_PATH_PREFIX = "dcsazure_"
METADATA_STORE_PATH = "metadata_store_scripts"
JSON = ".json"
SQL = ".sql"
MD = ".md"


class GitCommand:
    MODIFIED_FILES = [
        "git", "diff", "--no-merges", "--name-only", "--first-parent", "origin/main"
    ]
    FILE_CONTENT = ["git", "show"]
    COMMIT_MESSAGE = ["git", "log", "-1", "--pretty=%s"]
    PROJECT_ROOT = ["git", "rev-parse", "--show-toplevel"]


class FileNotFoundException(Exception):
    """Exception to be raised for invalid or non-existent VERSION.md"""
    pass


class VersionParsingException(Exception):
    """Exception to be raised for failures in parsing the version present in VERSION.md"""
    pass


class InvalidVersionException(Exception):
    """Exception to be raised if an incorrect version present in VERSION.md"""
    pass


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
