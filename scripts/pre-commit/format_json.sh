#!/bin/bash
#
# Copyright (c) 2025 by Delphix. All rights reserved.
#
# This script formats JSON files with jq.

new_json_files=$(git diff --name-only origin/main | grep "\.json$")
project_root=$(git rev-parse --show-toplevel)

for json_file in ${new_json_files}; do
  echo "Processing ${json_file}"
  jq '.' "${project_root}/${json_file}" > "${project_root}/${json_file}.fmt"
  if [ $? -ne 0 ]; then
    echo "Error formatting JSON file: ${json_file}"
    exit 1
  fi
  mv "${project_root}/${json_file}.fmt" "${project_root}/${json_file}"
done
