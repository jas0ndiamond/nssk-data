# Correlating Conductivity and Rainfall

---
## Setup
1. Run a fresh import of all datasets
1. Create a database config file from the template in nssk-data/conf for the database that will house the imported data.

---
## Run
`cd src/conductivity-rainfall-correlation`
`../../venv/bin/python3 conductivity-rainfall-correlation.py -cfg ../../conf/conductivity-rainfall-correlation.json`

---
## Notes
* There are gaps in sensor readings, which will create gaps in correlation
* WAGG01 and WAGG03 only for now