#
# Copyright (c) 2025 by Delphix. All rights reserved.
#

import logging
import pathlib
import typing as tp

import helpers

log = logging.getLogger("check_version")
log.setLevel(logging.INFO)


def filter_changed_template_files(
    changed_files: tp.List[pathlib.Path]
) -> tp.List[pathlib.Path]:
    """
    Filter out the changed template and metadata store script files
    """
    modified_files = []
    for file_path in changed_files:
        parent = file_path.parent.name
        filetype = file_path.suffix
        if (
            (
                parent.startswith(helpers.TEMPLATES_JSON_PATH_PREFIX)
                and filetype == helpers.JSON
            )
            or (
                parent.startswith(helpers.METADATA_STORE_PATH)
                and filetype == helpers.SQL
            )
        ):
            modified_files.append(file_path)

    return modified_files


def filter_documentation_files(
    changed_files: tp.List[pathlib.Path]
) -> tp.List[pathlib.Path]:
    """
    Filter out the changed documentation files
    """
    return [
        file_path
        for file_path in changed_files
        if (file_path.suffix == helpers.MD and file_path.name != helpers.VERSION_FILE)
    ]


def validate() -> None:
    """
    Validate current repo version against the previous version
    """
    version_md = pathlib.Path(helpers.VERSION_FILE)
    if not version_md.exists():
        raise helpers.FileNotFoundException(f"Error - {helpers.VERSION_FILE} file does not exist.")

    previous_version = helpers.get_previous_version()
    possible_next_versions = helpers.get_next_version(previous_version)
    current_version = helpers.get_current_version(str(version_md))

    if str(current_version) not in possible_next_versions:
        raise helpers.InvalidVersionException(
            f"{version_md.absolute().as_uri()} {previous_version} -->"
            f" {current_version}.\nERROR - The current version must be greater than the"
            f" previous version and should be one from {possible_next_versions}."
        )


def main() -> int:
    """
    This script is triggered by pre-commit hook to ensure that the repo
    version is updated.
    :return: system exit status 1|0
    """
    exit_status = 0
    all_updated_files = helpers.get_all_modified_files()
    df_files = filter_changed_template_files(all_updated_files)
    commit_message = helpers.get_commit_message()
    if df_files:
        file_names = "\n".join([str(path) for path in df_files])
        error_message = (
            "\nThe below pipelines and/or metadata store scripts have been modified:"
            f"\n\n{file_names}"
        )
        try:
            validate()
        except helpers.FileNotFoundException as e:
            log.error(e)
            exit_status = 1
        except (helpers.InvalidVersionException, helpers.VersionParsingException) as e:
            log.error(f"{error_message}\n\n{str(e)}")
            exit_status = 1

        if commit_message.startswith("docs:"):
            log.error(
                f"{error_message}\n\nCommit message should start with"
                f" 'feat:' instead of 'docs:'."
            )
            exit_status = 1
    else:
        doc_files = filter_documentation_files(all_updated_files)
        if doc_files and commit_message and not commit_message.startswith("docs:"):
            file_names = "\n".join([str(path) for path in doc_files])
            log.error(
                "Below documentation files related to ADF templates have been"
                f" modified:\n\n{file_names}\n\nCommit message should start"
                f" with 'docs:'\n\n{commit_message}"
            )
            exit_status = 1

    return exit_status


if __name__ == "__main__":
    exit(main())
