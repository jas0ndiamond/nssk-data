# CoSMo Importer

Imports data from CoSMo CSV data dumps into a MySQL database

---
## Setup

1. Create a database config file from the template in nssk-data/conf for the database that will house the imported data.
2. Acquire the data dump from the CoSMo website
3. Set the list of sensors in `FILE`

---
## Run

`venv/bin/python3 cosmo-import.py`
`../../venv/bin/python3 cosmo-import.py -cfg ../../conf/cosmo.json ~/Pub/nssk-data-dumps/doi.org_10.25976_0gvo-9d12.csv`

---
## Notes

Logs output to `cosmo-import.log`

The database will enforce uniqueness constraints. Inserts that fail uniqueness constraints will be dumped to a `duplicates_*.sql` file

Runtimes can take ~15 minutes for 1 million inserts for a database on the local network.