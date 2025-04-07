import subprocess
import logging
import re
import sys
import typing as tp
from pathlib import Path

logger = logging.getLogger("validate_changelog_update")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

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
    if not file_path.is_file():
        raise ValidationError(f"File not found: {file_path}")
    return file_path.read_text().strip()


def read_version_file() -> str:
    return read_file_content(get_project_root() / VERSION_FILE)


def read_changelog_file() -> tp.Optional[str]:
    changelog_path = get_project_root() / CHANGELOG_FILE
    content = changelog_path.read_text()
    match = re.search(r"^#\s*\[?(\d+\.\d+\.\d+)]?", content, re.MULTILINE)
    return match.group(1) if match else None


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
            " Please document your changes in the changelog to maintain proper release history."
        )

    if changelog_updated and not staged_code:
        raise ValidationError(
            f"{CHANGELOG_FILE} has been modified, but no code files were changed."
            " If this was unintentional, please remove the changelog update."
        )

    version = read_version_file()
    if not version:
        raise ValidationError(f"Could not determine version from {VERSION_FILE}")

    changelog_version = read_changelog_file()
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
