#!/bin/bash

CONF_FILE=$1
DUMP_DIR=$2

if [ -z "$CONF_FILE" ]; then
  echo "Config file required"
  exit 1;
fi

if [ ! -d "$DUMP_DIR" ]; then
  echo "Dump directory not provided or does not exist"
  exit 1
fi

declare -A SITES

SITES=(
  ["MIS-E-01"]="MIS-E-01.csv"
  ["MIS-M-01"]="MIS-M-01.csv"
  ["MIS-W-01"]="MIS-W-01.csv"
  ["MOS-M-01"]="MOS-M-01.csv"
  ["WAG-E-01"]="WAG-E-01.csv"
  ["WAG-E-02"]="WAG-E-02.csv"
  ["WAG-E-03"]="WAG-E-03.csv"
  ["WAG-E-05"]="WAG-E-05.csv"
  ["WAG-E-06a"]="WAG-E-06a.csv"
  ["WAG-E-06b"]="WAG-E-06b.csv"
  ["WAG-E-07"]="WAG-E-07.csv"
  ["WAG-M-01"]="WAG-M-01.csv"
  ["WAG-M-02"]="WAG-M-02.csv"
  ["WAG-M-03"]="WAG-M-03.csv"
  ["WAG-W-02a"]="WAG-W-02a.csv"
  ["WAG-W-02b"]="WAG-W-02b.csv"
  ["WAG-W-03"]="WAG-W-03.csv"
)

#../../venv/bin/python3 ./waterrangers-import.py -cfg "$CONF_FILE" WAG-E-01 ~/opt/nssk-data-dumps/waterrangers/WAG-E-01.csv

for SITE in "${!SITES[@]}"; do
  DUMP_FILE="$DUMP_DIR/${SITES[$SITE]}"
  if [[ -f "$DUMP_FILE" ]]; then
    echo "Key: $SITE | File: $DUMP_FILE"

    ../../venv/bin/python3 ./waterrangers-import.py -cfg "$CONF_FILE" "$SITE" "$DUMP_FILE"

    echo "==============================="
  else
    echo "SITE: $SITE | File: $DUMP_FILE Not found"
  fi
done


#