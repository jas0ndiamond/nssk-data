#!/bin/bash

CONF_FILE=$1

if [ -z "$CONF_FILE" ]; then
  echo "Config file required"
  exit 1;
fi

##############################################
# first batch
../../venv/bin/python3 ./cosmo-solinst-import.py -cfg "$CONF_FILE" WAGG01\
 ~/opt/nssk-data-dumps/solinst/Wagg01\ oct\ 09\ 2024\ download.csv
../../venv/bin/python3 ./cosmo-solinst-import.py -cfg "$CONF_FILE" WAGG03\
 ~/opt/nssk-data-dumps/solinst/1080831_2024_06_11\ Wagg\ 03.csv

# all duplicates as of 05/14/2026
#../../venv/bin/python3 ./cosmo-solinst-import.py -cfg "$CONF_FILE" WAGG03\
# ~/opt/nssk-data-dumps/solinst/Wagg\ 03\ Oct\ 09\ 2024.csv

../../venv/bin/python3 ./cosmo-solinst-import.py -cfg "$CONF_FILE" WAGG03\
 ~/opt/nssk-data-dumps/solinst/1080931_2024_02_19.csv

##############################################
# jan 2025
../../venv/bin/python3 ./cosmo-solinst-import.py -cfg "$CONF_FILE" WAGG01\
 ~/opt/nssk-data-dumps/solinst/Wagg01\ jan06\ 2025\ download.csv
../../venv/bin/python3 ./cosmo-solinst-import.py -cfg "$CONF_FILE" WAGG03\
 ~/opt/nssk-data-dumps/solinst/Wagg\ 03\ Jan\ 06\ 2025\ Download.csv

##############################################
# feb 2025
../../venv/bin/python3 ./cosmo-solinst-import.py -cfg "$CONF_FILE" WAGG01\
 ~/opt/nssk-data-dumps/solinst/Wagg\ 01\ Feb\ 14\ 2025\ download.csv

# all duplicates as of 05/14/2026
#../../venv/bin/python3 ./cosmo-solinst-import.py -cfg "$CONF_FILE" WAGG03\
# ~/opt/nssk-data-dumps/solinst/Wagg\ 03\ Feb\ 14\ 2025\ Download.csv

##############################################
# july 2025
../../venv/bin/python3 ./cosmo-solinst-import.py -cfg "$CONF_FILE" WAGG01\
 ~/opt/nssk-data-dumps/solinst/Wagg01_1091817_2025_07_02.csv
../../venv/bin/python3 ./cosmo-solinst-import.py -cfg "$CONF_FILE" WAGG03\
 ~/opt/nssk-data-dumps/solinst/Wagg03_2130615_2025_07_01.csv

##############################################
# aug 2025
../../venv/bin/python3 ./cosmo-solinst-import.py -cfg "$CONF_FILE" WAGG01\
 ~/opt/nssk-data-dumps/solinst/Wagg01_1091817__2025_08_05.csv
../../venv/bin/python3 ./cosmo-solinst-import.py -cfg "$CONF_FILE" WAGG03\
 ~/opt/nssk-data-dumps/solinst/WAGG03_2130615_2025_08_05.csv

##############################################
# oct 2025
../../venv/bin/python3 ./cosmo-solinst-import.py -cfg "$CONF_FILE" WAGG01\
 ~/opt/nssk-data-dumps/solinst/Wagg\ 01\ Oct\ 15\ 2025\ download.csv

##############################################
# nov 2025
../../venv/bin/python3 ./cosmo-solinst-import.py -cfg "$CONF_FILE" WAGG03\
 ~/opt/nssk-data-dumps/solinst/2130615_WAGG03_2025_11_30.csv

# dec 2025
../../venv/bin/python3 ./cosmo-solinst-import.py -cfg "$CONF_FILE" WAGG03\
 ~/opt/nssk-data-dumps/solinst/2130615_WAGG03_2025_12_06.csv

##############################################
# oct 2025
../../venv/bin/python3 ./cosmo-solinst-import.py -cfg "$CONF_FILE" WAGG01\
 ~/opt/nssk-data-dumps/solinst/1091817_WAGG01_2025_10_15.csv

# feb 2026
../../venv/bin/python3 ./cosmo-solinst-import.py -cfg "$CONF_FILE" WAGG03\
 ~/opt/nssk-data-dumps/solinst/1080850_WAGG03_2026_02_03.csv
