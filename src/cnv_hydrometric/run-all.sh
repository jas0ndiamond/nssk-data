#!/bin/bash

CONF_FILE=$1

if [ -z "$CONF_FILE" ]; then
  echo "Config file required"
  exit 1;
fi

../../venv/bin/python3 ./cnv-hydrometric-import.py -cfg "$CONF_FILE" WaggCreek ~/opt/nssk-data-dumps/cnv-hydrometric/WaggCreek_export_20250206133229.csv
../../venv/bin/python3 ./cnv-hydrometric-import.py -cfg "$CONF_FILE" WaggCreek ~/opt/nssk-data-dumps/cnv-hydrometric/WaggCreek_export_20250206133346.csv
../../venv/bin/python3 ./cnv-hydrometric-import.py -cfg "$CONF_FILE" WaggCreek ~/opt/nssk-data-dumps/cnv-hydrometric/WaggCreek_export_20250206133616.csv
../../venv/bin/python3 ./cnv-hydrometric-import.py -cfg "$CONF_FILE" WaggCreek ~/opt/nssk-data-dumps/cnv-hydrometric/WaggCreek_export_20250206133300.csv
../../venv/bin/python3 ./cnv-hydrometric-import.py -cfg "$CONF_FILE" WaggCreek ~/opt/nssk-data-dumps/cnv-hydrometric/WaggCreek_export_20250206133551.csv
../../venv/bin/python3 ./cnv-hydrometric-import.py -cfg "$CONF_FILE" WaggCreek ~/opt/nssk-data-dumps/cnv-hydrometric/WaggCreek_export_20250206133642.csv
