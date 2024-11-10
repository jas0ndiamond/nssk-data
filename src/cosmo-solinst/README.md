# CoSMo/Solinst Importer

Imports from Solinst Instrument raw CSV data dumps into a MySQL database

---
## Setup

1. Create a database config file from the template in nssk-data/conf for the database that will house the imported data.
2. Acquire the data dump from whoever has access to the raw data dumps.

---
## Run

`cd src/cosmo-solinst`

`../../venv/bin/python3 cosmo-solinst-import.py -cfg ../../conf/cosmo.json [SITE] [OFFSET] [DUMP_FILE]`

---
## Notes

Typically, this raw data is collected locally and handed off to DFO PSEC Community Stream Monitoring where it 
eventually appears in the CoSMo dataset.

Logs output to `cosmo-solisnt-import.log`

The database will enforce uniqueness constraints. Inserts that fail uniqueness constraints will be dumped 
to a `duplicates_*.sql` file

