# Rainfall Event Data

---
## Description
* Collect data during known rainfall events for targeted analysis.

---
## Setup
1. Create a database config file from the template in nssk-data/conf for the database that will house the imported data.
2. Run import for `CoSMo` to accumulate CoSMo measurements to be included in event data.
3. Run import for `cnv_flowworks` to accumulate the source rainfall measurements to be included in event data.
4. Run import for `dnv_flowworks` to accumulate the source flow measurements to be included in event data.
5. Run import for `rainfall-events` to accumulate rainfall events to be included in event data. 

---
## Run
`cd src/rainfall-events-data`
`../../venv/bin/python3 rainfall-event-data.py -cfg ../../conf/rainfall-events.json`

---
## Notes
* The following data describes the schema of a rainfall event datum:
  ```
  CNV_FLOWWORKS_TIMESTAMP_FIELD,
  CNV_FLOWWORKS_RAINFALL_AMOUNT_FIELD,
  CNV_AIR_TEMPERATURE_FIELD,
  COSMO_CONDUCTANCE_RESULT_FIELD,
  DNV_FLOWWORKS_FLOW_READING_FIELD,
  RAINFALL_EVENT_ID_FIELD  
  ```
* CoSMo sites used are `WAGG01` and `WAGG03` only.