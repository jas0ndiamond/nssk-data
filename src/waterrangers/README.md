# Waterrangers Importer

Imports from NSSK Waterrangers data dumps into a MySQL database

---
## Setup

1. Create a database config file from the template in nssk-data/conf for the database that will house the imported data.

---
## Run

1. Change to the scripts directory `cd nssk-data/src/waterrangers/scripts`
2. Run the retrieval script `./get.sh`
3. Run the import wrapper script `./import-all.sh`

---
## Notes

* Logs output to `waterrangers.log`
* The database will enforce uniqueness constraints. Inserts that fail uniqueness constraints will be dumped 
to a `duplicates_*.sql` file
* May be defunct- no measurements for 2026.