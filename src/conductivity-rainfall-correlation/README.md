# Correlating Conductivity and Rainfall

---
## Description
* Correlate CoSMo conductivity readings with area rainfall amounts from the `cnv_flowworks` dataset. 

---
## Setup
1. Create a database config file from the template in nssk-data/conf for the database that will house the imported data.
2. Run imports for `cosmo` and `cnv_flowworks`

---
## Run
`cd src/conductivity-rainfall-correlation`
`../../venv/bin/python3 conductivity-rainfall-correlation.py -cfg ../../conf/conductivity-rainfall-correlation.json`

---
## Notes
* Correlation time window for measurements is 5 minutes. 
* Multiple CoSMo measurements may map to the same rainfall measurement.
* There are gaps in sensor readings, which will create gaps in correlation.
* CoSMo sites WAGG01 and WAGG03 only for now.

---
## TODO
* Enforce start of cnv_flowworks dataset consideration with start date override rather than trusting
  * Reliable CNV Flowworks rainfall measurements begin at 2018-01-01