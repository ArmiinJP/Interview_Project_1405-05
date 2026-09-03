#!/bin/bash

SQL_FILE="/docker-entrypoint-initdb.d/901_config.sql_tmp"
OUT_FILE="/docker-entrypoint-initdb.d/901_config.sql"

if [[ "$PASSWORD" == *"'"* ]]; then
  echo "Error: PASSWORD contains single quote (') which is not supported in this script."
  exit 1
fi

sed "s/{{ PASSWORD }}/'$PASSWORD'/g" "$SQL_FILE" > "$OUT_FILE"

clickhouse-client --multiquery < "$OUT_FILE"