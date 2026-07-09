# CNV Hydrometric

Imports flow data from CNV Hydrometric CSV data dumps into a MySQL database.

Data dumps retrieved manually from CNV.

---
## Setup

1. Create a database config file (i.e. cnv-hydrometric.json) from the template in nssk-data/conf for the database that will house the imported data.
2. Acquire the data dump from the CNV Hydrometric website
3. Set the list of sensors in `cnv-hydrometric-import.py`

---
## Run

`cd src/cnv_hydrometric`

Manually:

`../../venv/bin/python3 cnv-hydrometric-import.py -cfg ../../conf/cnv-hydrometric.json /path/to/nssk-data-dumps/WaggCreek_export_20250206133642.csv`

Batch:

`./run-all.sh -cfg ../../conf/cnv-hydrometric.json`

---
## Notes
* Logs output to `cnv-hydrometric.log`
* Only one site `WaggCreek`
* The database will enforce uniqueness constraints. Inserts that fail uniqueness constraints will be dumped to a `duplicates_*.sql` file
* Some preliminary measurements will be 0 or blank, but revised to coherent numbers in the "revised" stage field.
* Runtimes can take ~15 minutes for 1 million inserts for a database on the local network.

---
## TODO
* Unhardcode data dump directory and file names in `run-all.sh`