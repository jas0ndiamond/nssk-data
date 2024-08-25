# CNV Importer

Imports data from CNV Rainfall CSV data dumps into a MySQL database

---
## Setup

1. Create a database config file (i.e. cnv-rainfall.json) from the template in nssk-data/conf for the database that will house the imported data.
2. Acquire the data dump from the CNV Flowworks website
3. Set the list of sensors in `cnv-rainfall-import.py`

---
## Run

`cd src/cnv_rainfall`

`../../venv/bin/python3 cnv-rainfall-import.py -cfg ../../conf/cnv-rainfall.json ~/Pub/nssk-data-dumps/North_Vancouver_City_Hall_export_20240824163544.csv`

---
## Notes

Logs output to `cnv-rainfall.log`

The database will enforce uniqueness constraints. Inserts that fail uniqueness constraints will be dumped to a `duplicates_*.sql` file

Runtimes can take ~15 minutes for 1 million inserts for a database on the local network.