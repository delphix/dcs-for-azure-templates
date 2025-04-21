#!/bin/bash

# This script formats SQL files with sqlfluff.
BOOTSTRAP_FILE="bootstrap.sql"
DIALECT="tsql"

new_sql_files=$(git diff --name-only origin/main | grep "\.sql$" | grep -v "${BOOTSTRAP_FILE}")
project_root=$(git rev-parse --show-toplevel)

for file in ${new_sql_files}; do
  sqlfluff format "${project_root}"/"${file}" --dialect "${DIALECT}" > /dev/null 2>&1
  if [ $? -ne 0 ]; then
    echo "Error formatting SQL file: ${file}"
    exit 1
  fi
done
