#
# Copyright (c) 2025 by Delphix. All rights reserved.
#

import subprocess
import logging
import pathlib
import re
import sys
import typing as tp

import helpers

from packaging.version import Version

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger("validate_changelog")


class ValidationError(Exception):
    """Custom exception for validation-related errors."""


def read_file_content(file_path: pathlib.Path) -> str:
    if not file_path.exists():
        raise ValidationError(f"File not found: {file_path}")
    return file_path.read_text().strip()


def read_version_file() -> str:
    return read_file_content(helpers.get_project_root() / helpers.VERSION_FILE)


def read_and_validate_changelog_file() -> tp.Optional[str]:
    changelog_path = helpers.get_project_root() / helpers.CHANGELOG_FILE
    content = changelog_path.read_text().splitlines()

    # Validate the first line
    if not content or content[0].strip() != "# CHANGELOG":
        raise ValidationError(
            f"The first line of {helpers.CHANGELOG_FILE} must be '# CHANGELOG'"
        )

    versions = []
    # Read the next lines to find version headers. Example: "# 0.0.25"
    version_pattern = re.compile(r"^#\s*(\d+\.\d+\.\d+)\s*$")
    for line in content[1:]:
        if match := version_pattern.match(line):
            versions.append(match.group(1))

    if not versions:
        raise ValidationError(
            f"No version header found in {helpers.CHANGELOG_FILE}. Expected format: '# <version>'."
            " Example - '# 0.0.25'"
        )

    # Verify that the version list is in strictly descending order.
    for i in range(len(versions) - 1):
        if Version(versions[i]) <= Version(versions[i + 1]):
            raise ValidationError(
                f"Version {versions[i]} is not greater than {versions[i + 1]}."
                " Please ensure new version should be greater than the previous version."
            )

    return versions[0]


def validate_files() -> None:
    changed_files = helpers.get_all_modified_files()
    staged_code = any(
        file.suffix in {helpers.SQL, helpers.JSON}
        for file in changed_files
    )
    changelog_updated = pathlib.Path(helpers.CHANGELOG_FILE) in changed_files

    if not staged_code and not changelog_updated:
        return

    if staged_code and not changelog_updated:
        raise ValidationError(
            f"Detected code changes, but no corresponding update to {helpers.CHANGELOG_FILE}."
            " Please document your changes to maintain release history."
        )

    if changelog_updated and not staged_code:
        logger.warning(
            f"{helpers.CHANGELOG_FILE} has been modified, but no code files were changed."
            " If this was unintentional, please remove the changelog update."
        )

    version = read_version_file()
    if not version:
        raise ValidationError(f"Could not determine version from {helpers.VERSION_FILE}")

    changelog_version = read_and_validate_changelog_file()
    if not changelog_version:
        raise ValidationError(f"Could not determine version from {helpers.CHANGELOG_FILE}")

    if version != changelog_version:
        raise ValidationError(
            f"Version mismatch:  {helpers.VERSION_FILE}: {version},  {helpers.CHANGELOG_FILE}: {changelog_version}"
        )


def main():
    try:
        validate_files()
        return 0

    except subprocess.CalledProcessError as e:
        logger.error(f"Git command failed: {e}")
    except ValidationError as e:
        logger.error(f"Validation failed: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

    return 1


if __name__ == "__main__":
    sys.exit(main())
