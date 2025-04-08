import subprocess
import sys
import re
import logging
from pathlib import Path
import typing as tp
from datetime import datetime

logger = logging.getLogger("verify_migration_script")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

CODE_EXTENSION = ".sql"
BOOTSTRAP_FILE = "bootstrap.sql"
METADATA_STORE_SCRIPTS_DIR = "metadata_store_scripts"


class MigrationValidationError(Exception):
    """Raised when a migration validation check fails."""


def is_code_file(filepath: str) -> bool:
    return Path(filepath).suffix == CODE_EXTENSION


def get_project_root() -> Path:
    project_root = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=True
    )
    return Path(project_root.stdout.strip())


def parse_date(version_str: str) -> datetime:
    date_str = ".".join(version_str[1:].split(".")[:3])
    return datetime.strptime(date_str, "%Y.%m.%d")


def check_duplicate_migration_versions(new_migration_files: tp.List, old_migration_files: tp.List) -> None:
    """
    Check if there are any duplicate migration versions
    1. Between new and old migration files.
    2. Between new migration files.
    """
    new_versions = [file.split("_")[0] for file in new_migration_files if file != BOOTSTRAP_FILE]
    old_versions = [file.split("_")[0] for file in old_migration_files]

    duplicates = set(new_versions) & set(old_versions)
    if duplicates:
        raise MigrationValidationError(
            f"Duplicate migration versions found: {duplicates}"
        )

    new_migration_files_set = set(new_migration_files)
    duplicates = [file for file in new_migration_files_set if new_versions.count(file.split("_")[0]) > 1]
    if duplicates:
        raise MigrationValidationError(
            f"Duplicate migration versions found in new migration files: {duplicates}"
        )


def check_new_migration_file_format(new_migration_files: tp.List) -> None:
    """
    Check if the new migration files have a date format in their names.
    """
    for file in new_migration_files:
        if file == BOOTSTRAP_FILE:
            continue
        if not re.match(r"^V\d{4}.\d{2}.\d{2}.\d{1,2}__.+\.sql$", file):
            raise MigrationValidationError(
                f"New migration file {file} does not have a valid file name format."
            )


def validate_new_migration_dates(new_migration_files: tp.List, old_migration_files: tp.List) -> None:
    """
    Validate that the new migration files have a date format in their names.
    """
    new_migration_date = [parse_date(file) for file in new_migration_files if file != BOOTSTRAP_FILE]
    latest_migration_date = [parse_date(file) for file in old_migration_files]

    invalid_new_migration_file_date = [
        file_date for file_date in new_migration_date if file_date < max(latest_migration_date)
    ]
    if invalid_new_migration_file_date:
        for file_date in invalid_new_migration_file_date:
            raise MigrationValidationError(
                f"New migration file date: V{file_date.strftime('%Y.%m.%d')} is older than the latest"
                f" available migration version date: V{max(latest_migration_date).strftime('%Y.%m.%d')}")


def validate_if_bootstrap_file_is_updated(new_migration_files: tp.List) -> None:
    """
    Validate if the bootstrap file is updated with the contents of new migration files.
    """
    migration_scripts = [f for f in new_migration_files if f != BOOTSTRAP_FILE]
    if migration_scripts and BOOTSTRAP_FILE not in new_migration_files:
        raise MigrationValidationError(
            f"Bootstrap file [{BOOTSTRAP_FILE}] is not updated with the latest migration scripts."
            " Execute scripts/migrations.sh script to update bootstrap.sql file."
        )

    if BOOTSTRAP_FILE in new_migration_files:
        bootstrap_file_path = get_project_root() / METADATA_STORE_SCRIPTS_DIR / BOOTSTRAP_FILE
        bootstrap_content = Path(bootstrap_file_path).read_text()

        for migration_script in migration_scripts:
            migration_script_path = get_project_root() / METADATA_STORE_SCRIPTS_DIR / migration_script
            migration_script_content = Path(migration_script_path).read_text()

            if migration_script_content not in bootstrap_content:
                raise MigrationValidationError(
                    f"Bootstrap file [{BOOTSTRAP_FILE}] is not updated with the contents of"
                    f" new migration file {migration_script}."
                )


def get_changed_files() -> tp.List[str]:
    """
    Get the list of changed files in the current branch compared to the main branch.
    """
    output = subprocess.check_output([
        "git", "diff", "--no-merges", "--name-only", "--first-parent", "origin/main"
    ])
    return output.decode().splitlines()


def get_existing_migration_files(scripts_dir: Path, new_files: tp.List[str]) -> tp.List[str]:
    """
    Get existing migration files from the metadata store script directory.
    """
    output = subprocess.check_output([
        "git", "ls-tree", "-r", "--name-only", "HEAD", str(scripts_dir)
    ])
    return [
        Path(line).name
        for line in output.decode().splitlines()
        if Path(line).name not in new_files and Path(line).name != BOOTSTRAP_FILE
    ]


def main():
    try:
        changed_files = get_changed_files()
        new_migration_files = [
            Path(file).name for file in changed_files if is_code_file(file)
        ]

        if not new_migration_files:
            return

        metadata_store_scripts_dir = get_project_root() / METADATA_STORE_SCRIPTS_DIR

        old_migration_files = get_existing_migration_files(
            metadata_store_scripts_dir, new_migration_files
        )

        check_new_migration_file_format(new_migration_files)
        check_duplicate_migration_versions(new_migration_files, old_migration_files)
        validate_new_migration_dates(new_migration_files, old_migration_files)
        validate_if_bootstrap_file_is_updated(new_migration_files)
        return 0

    except MigrationValidationError as e:
        logger.error(f"Validation error: {e}")

    except subprocess.CalledProcessError as e:
        logger.error(f"Error while checking for changed files: {e}")

    return 1


if __name__ == "__main__":
    sys.exit(main())
