# Rainfall Events

---
## Description
* Determine start and end of rainfall events from rainfall measurement data in the `NSSK_CNV_FLOWWORKS` database.

---
## Setup
1. Create a database config file from the template in nssk-data/conf for the database that will house the imported data.
2. Run import for `cnv_flowworks` to accumulate the source rainfall measurements.

---
## Run
`cd src/rainfall-events`
`../../venv/bin/python3 rainfall-events.py -cfg ../../conf/rainfall-events.json`

---
## Notes
* The CNV rainfall dataset may start in the middle of a rain event. In this case, the start of the dataset is used as the start of the rain event.
* The CNV rainfall dataset may end in the middle of a rain event. In this case, the event is discarded. 
  * The expectation is that the next update to the CNV rainfall dataset will have the complete rain event that was previously incomplete. At that point the next run of the Rainfall Event determination process will pick it up as a complete event.
* A rainfall event starts at the first non-zero rainfall amount in the CNV Rainfall dataset, and extends to the first zero rainfall measurement that's followed by 48 hours of zero rainfall. 
* An identifier is determined for a rainfall event, comprised of the 4-digit year of the Rainfall Event start timestamp, followed by a 3-digit counter for the nth event of the year. 
  * EVENT_ID "2022004" would indicate the 4th rainfall event of year 2022.
* If the source rainfall measurement data ends during a rainfall event, the event is not imported.
  * The expectation is that the next update of rainfall measurements will contain the true end of the rainfall event.