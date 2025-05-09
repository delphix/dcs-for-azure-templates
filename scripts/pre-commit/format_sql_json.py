#
# Copyright (c) 2025 by Delphix. All rights reserved.
#

import subprocess
import sys
import helpers
from pathlib import Path


SCRIPT_DIR = "scripts/pre-commit"
SQL_FORMATTER_SCRIPT = "format_sql.sh"
JSON_FORMATTER_SCRIPT = "format_json.sh"


def run_script(script_path: Path):
    subprocess.run(
        [script_path],
        check=True,
    )


def main():
    try:
        sql_formatter_script = helpers.get_project_root() / SCRIPT_DIR / SQL_FORMATTER_SCRIPT
        run_script(sql_formatter_script)

        json_formatter_script = helpers.get_project_root() / SCRIPT_DIR / JSON_FORMATTER_SCRIPT
        run_script(json_formatter_script)
        return 0
    except subprocess.CalledProcessError as e:
        print(f"Error running script: {e}")
        return 1
    except Exception as e:
        print(f"An error occurred: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
