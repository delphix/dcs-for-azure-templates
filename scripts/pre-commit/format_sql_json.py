#
# Copyright (c) 2025 by Delphix. All rights reserved.
#

import subprocess
import sys
import helpers
from pathlib import Path
import typing as tp
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger("format_sql_json")


def format_sql(modified_sql_files: tp.List[Path]):
    for sql_file in modified_sql_files:
        try:
            subprocess.run(
                ["sqlfluff", "format", str(sql_file)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True,
            )
            # On success, do not print anything
        except subprocess.CalledProcessError as e:
            logger.error(f"Error formatting SQL file {sql_file}:")
            if e.stdout:
                print(e.stdout)
            if e.stderr:
                print(e.stderr)
            
            print(
                f"\nSome errors may not be shown by 'sqlfluff format'.\n"
                f"To see all parsing and linting errors, run:\n"
                f"  sqlfluff parse {sql_file}\n"
                f"  sqlfluff lint {sql_file}\n"
            )
            raise e

def format_json(modified_json_files: tp.List[Path]):
    for json_file in modified_json_files:
        json_path = helpers.get_project_root() / json_file
        temp_json_path = json_path.with_suffix(json_path.suffix + ".fmt")
        try:
            with temp_json_path.open("w") as temp_file:
                subprocess.run(
                    ["jq", ".", str(json_file)],
                    stdout=temp_file,
                    stderr=subprocess.DEVNULL,
                    check=True,
                )
            temp_json_path.replace(json_path)
        except subprocess.CalledProcessError as e:
            logger.error(f"Error formatting JSON file {json_file}: {e}")
            raise e


def main():
    try:
        changed_files = helpers.get_all_modified_files()
        modified_sql_files = [sql_file for sql_file in changed_files if sql_file.suffix == helpers.SQL]
        modified_json_files = [json_file for json_file in changed_files if json_file.suffix == helpers.JSON]
        format_sql(modified_sql_files)
        format_json(modified_json_files)
        return 0
    except subprocess.CalledProcessError:
        return 1
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
