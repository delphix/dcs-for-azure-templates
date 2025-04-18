#
# Copyright (c) 2025 by Delphix. All rights reserved.
#

from __future__ import annotations

import logging
import pathlib
import subprocess
import typing

from pkg_resources import parse_version

log = logging.getLogger("check_version")
log.setLevel(logging.INFO)

VERSION_FILE = "VERSION.md"
TEMPLATES_JSON_PATH_PREFIX = "dcsazure_"
METADATA_STORE_PATH = "metadata_store_scripts"


class VersionParsingException(Exception):
    """Exception to be raised for failures in parsing the version present in VERSION.md"""
    pass


class InvalidVersionException(Exception):
    """Exception to be raised if an incorrect version present in VERSION.md"""
    pass


class FileNotFoundException(Exception):
    """Exception to be raised for invalid or non-existent VERSION.md"""
    pass


class GitCommand:
    MODIFIED_FILES = [
        "git", "diff", "--no-merges", "--name-only", "--first-parent", "origin/main"
    ]
    FILE_CONTENT = ["git", "show"]
    COMMIT_MESSAGE = ["git", "log", "@{u}..HEAD", "-1", "--pretty=%s"]


def get_cmd_output(cmd: typing.List[str]) -> str:
    """
    Get the subprocess output for the command to be run
    :param cmd: command parameters as list
    :return: Command output
    """
    try:
        return subprocess.check_output(cmd).decode("utf-8")
    except subprocess.CalledProcessError as e:
        raise Exception(f"Could not retrieve command output: {e}")


def get_all_modified_files() -> typing.List[pathlib.Path]:
    """
    Retrieve the list of files updated from the upstream branch
    """
    try:
        files_updated = get_cmd_output(GitCommand.MODIFIED_FILES)
        return [pathlib.Path(path) for path in files_updated.splitlines()]
    except subprocess.CalledProcessError as e:
        raise Exception(f"Could not retrieve git diff: {e}")


def get_modified_files() -> typing.List[pathlib.Path]:
    """
    Get the list of changed pipelines and metadata store scripts
    """
    modified_files = []
    for file_path in get_all_modified_files():
        parent = file_path.parent.name
        filetype = file_path.suffix
        if (
            (parent.startswith(TEMPLATES_JSON_PATH_PREFIX) and filetype == ".json")
            or (parent.startswith(METADATA_STORE_PATH) and filetype == ".sql")
        ):
            modified_files.append(file_path)

    return modified_files


def get_version_from_version_md(version_md: str) -> "packaging.version.Version":
    """
    Previous version of a file
    :return: previous version
    """
    try:
        return parse_version(version_md)
    except ValueError as e:
        raise VersionParsingException(f"Invalid version number: {str(e)}")
    except Exception as e:
        logging.warning(f"Cannot find previous version: {e}")


def get_previous_version() -> str:
    """
    Parse version from the previous version of the file
    """
    version_md = get_cmd_output(
        GitCommand.FILE_CONTENT + [f"origin/main:{VERSION_FILE}"]
    ).strip()
    return get_version_from_version_md(version_md)


def get_current_version() -> str:
    """
    Parse version from the current version of the file
    """
    with open(VERSION_FILE, "r") as f:
        version_md = f.read().strip()
    return get_version_from_version_md(version_md)


def get_commit_message() -> str:
    """
    Get the commit message
    """
    return get_cmd_output(GitCommand.COMMIT_MESSAGE).strip()


def validate() -> None:
    """
    Validate current repo version against the previous version
    """
    version_md = pathlib.Path(VERSION_FILE)
    if not version_md.exists():
        raise FileNotFoundException(f"Error: {VERSION_FILE} file does not exist.")

    current_version = get_current_version()
    previous_version = get_previous_version()

    if current_version <= previous_version:
        raise InvalidVersionException(
            f"{version_md.absolute().as_uri()} {previous_version} -->"
            f" {current_version}. The current version must be greater than the "
            f"previous version."
        )


def main() -> int:
    """
    This script is triggered by pre-commit hook to ensure that the repo
    version is updated.
    :return: system exit status 1|0
    """
    exit_status = 0
    modified_files = get_modified_files()
    if modified_files:
        file_names = "\n".join([str(path) for path in modified_files])
        error_message = (
            "\nThe below pipelines and/or metadata store scripts have been modified:"
            f"\n\n{file_names}"
        )
        try:
            validate()
        except FileNotFoundException as e:
            log.error(e)
            exit_status = 1
        except (InvalidVersionException, VersionParsingException) as e:
            log.error(f"{error_message}\n\n{str(e)}")
            exit_status = 1

        if get_commit_message().startswith("docs:"):
            log.error(
                f"{error_message}\n\nCommit message should start with"
                f" 'feat:' instead of 'docs:'."
            )
            exit_status = 1

    return exit_status


if __name__ == "__main__":
    exit(main())
