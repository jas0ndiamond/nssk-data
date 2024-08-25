# DNV Whitewater Importer

Imports data from DNV Whitewater JSON data dumps into a MySQL database

---
## Setup

1. Create a database config file (i.e. flowworks.json) from the template in nssk-data/conf for the database that will house the imported data.
2. Acquire the data dump from the DNV Whitewater Flowworks website
3. Set the list of sensors in `flowworks-import.py`

---
## Run

`cd src/flowworks`

`../../venv/bin/python3 flowworks-import.py -cfg ../../conf/flowworks.json ~/Pub/nssk-data-dumps/20240824-174208_flowworks.json`

---
## Notes

Logs output to `flowworks.log`

The database will enforce uniqueness constraints. Inserts that fail uniqueness constraints will be dumped to a `duplicates_*.sql` file

Runtimes can take ~15 minutes for 1 million inserts for a database on the local network.