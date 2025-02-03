#!/bin/bash

git checkout origin/main -- metadata_store_scripts/bootstrap.sql
for MIGRATION in $(ls metadata_store_scripts/V*.sql)
do
  script_version_and_comment=( $(basename -- "$MIGRATION" .sql ) )
  echo "$script_version_and_comment"
  use_of_dbo=( $(grep "dbo" $MIGRATION | wc -l ) )
  if [[ $use_of_dbo -ne 0 ]]; then
    echo "Please remove reference to dbo in migration script"
    exit -1
  fi
  lines_in_bootstrap=( $(grep "$script_version_and_comment" metadata_store_scripts/bootstrap.sql | wc -l) )
  if [[ $lines_in_bootstrap -eq 2 ]]; then
    # Nothing to do, assume this is correct
    true
  elif [[ $lines_in_bootstrap -eq 0 ]]; then
    sed "s/\-\- The contents of each of those files follows/-- * $script_version_and_comment\n-- The contents of each of those files follows/g" metadata_store_scripts/bootstrap.sql > metadata_store_scripts/bootstrap.sql.tmp
    echo "" >> metadata_store_scripts/bootstrap.sql.tmp
    echo "-- source: $script_version_and_comment" >> metadata_store_scripts/bootstrap.sql.tmp
    cat $MIGRATION >> metadata_store_scripts/bootstrap.sql.tmp
    echo "" >> metadata_store_scripts/bootstrap.sql.tmp
    mv metadata_store_scripts/bootstrap.sql.tmp metadata_store_scripts/bootstrap.sql
    echo "[REMINDER] It appears as though this migration was added, please update the README.md associated with impacted pipelines"
  else
    echo "$script_version_and_comment not properly configured in bootstrap.sql"
    exit -1
  fi
done
