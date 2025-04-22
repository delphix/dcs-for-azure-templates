#
# Copyright (c) 2025 by Delphix. All rights reserved.
#

import subprocess
import logging
import re
import sys
import typing as tp
from pathlib import Path
from packaging.version import Version

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger("validate_changelog")

CODE_EXTENSIONS = {".json", ".sql"}
VERSION_FILE = "VERSION.md"
CHANGELOG_FILE = "CHANGELOG.md"


class ValidationError(Exception):
    """Custom exception for validation-related errors."""


def is_code_file(filepath: str) -> bool:
    return Path(filepath).suffix in CODE_EXTENSIONS


def get_project_root() -> Path:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=True
    )
    return Path(result.stdout.strip())


def read_file_content(file_path: Path) -> str:
    if not file_path.exists():
        raise ValidationError(f"File not found: {file_path}")
    return file_path.read_text().strip()


def read_version_file() -> str:
    return read_file_content(get_project_root() / VERSION_FILE)


def read_and_validate_changelog_file() -> tp.Optional[str]:
    changelog_path = get_project_root() / CHANGELOG_FILE
    content = changelog_path.read_text().splitlines()

    # Validate the first line
    if not content or content[0].strip() != "# CHANGELOG":
        raise ValidationError(
            f"The first line of {CHANGELOG_FILE} must be '# CHANGELOG'"
        )

    versions = []
    # Read the next lines to find version headers. Example: "# 0.0.25"
    version_pattern = re.compile(r"^#\s*(\d+\.\d+\.\d+)\s*$")
    for line in content[1:]:
        if match := version_pattern.match(line):
            versions.append(match.group(1))

    if not versions:
        raise ValidationError(
            f"No version header found in {CHANGELOG_FILE}. Expected format: '# <version>'."
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


def get_changed_files() -> tp.List[str]:
    output = subprocess.check_output(
        [
            "git", "diff", "--no-merges", "--name-only",
            "--first-parent", "origin/main"
        ],
        text=True
    )
    return output.splitlines()


def validate_files(changed_files: tp.List[str]) -> None:
    staged_code = any(is_code_file(file) for file in changed_files)
    changelog_updated = CHANGELOG_FILE in changed_files

    if not staged_code and not changelog_updated:
        return

    if staged_code and not changelog_updated:
        raise ValidationError(
            f"Detected code changes, but no corresponding update to {CHANGELOG_FILE}."
            " Please document your changes to maintain release history."
        )

    if changelog_updated and not staged_code:
        logger.warning(
            f"{CHANGELOG_FILE} has been modified, but no code files were changed."
            " If this was unintentional, please remove the changelog update."
        )

    version = read_version_file()
    if not version:
        raise ValidationError(f"Could not determine version from {VERSION_FILE}")

    changelog_version = read_and_validate_changelog_file()
    if not changelog_version:
        raise ValidationError(f"Could not determine version from {CHANGELOG_FILE}")

    if version != changelog_version:
        raise ValidationError(
            f"Version mismatch:  {VERSION_FILE}: {version},  {CHANGELOG_FILE}: {changelog_version}"
        )


def main():
    try:
        changed_files = get_changed_files()
        validate_files(changed_files)
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
