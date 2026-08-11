# DNV Flowworks Importer

Imports data from DNV Flowworks JSON data dumps into a MySQL database

---
## Setup

1. Create a database config file (i.e. dnv_flowworks.json) from the template in nssk-data/conf for the database that will house the imported data.
2. Acquire the data dump from the DNV Flowworks website
3. Set the list of sites in `dnv-whitewater-import.py`

---
## Run

`cd src/dnv_flowworks`

`../../venv/bin/python3 dnv-flowworks-import.py -cfg ../../conf/dnv-flowworks.json /path/to/nssk-data-dumps/20240824-174208_dnv_flowworks.json`

---
## Notes
* Logs output to `dnv-flowworks.log`
* The database will enforce uniqueness constraints on `MeasurementTimestamp`. Inserts that fail uniqueness constraints will be dumped to a `duplicates_*.sql` file
* There are occasional `NULL` FlowReading measurements- mostly between 2018-08-11 10:55:00 and 2018-09-07 08:20:00. 