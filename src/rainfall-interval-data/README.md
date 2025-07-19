# Rainfall Interval Data

---
## Description
* Compile data in intervals for various data sources

---
## Setup
1. Create a database config file from the template in nssk-data/conf for the database that will house the imported data.
2. Run imports for `cosmo`, `cnv_flowworks`, `dnv_flowworks`, and `cnv_hydrometric`.

---
## Run
`cd src/rainfall-interval-data`
`../../venv/bin/python3 rainfall-interval-data.py -cfg ../../conf/rainfall-interval-data.json`

---
## Notes
* Datasets, especially CoSMo, will have gaps. 
* Rainfall data is summed into 10 minute intervals from 5-minute rainfall measurements.
* Correlation windows between data sets is 5-minutes.
* Interval data for CoSMo sites WAGG01, WAGG03, and consolidated.