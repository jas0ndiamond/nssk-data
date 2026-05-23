# Waterrangers Importer

Imports from NSSK Waterrangers data dumps into a MySQL database

---
## Setup
1. Create a database config file from the template in nssk-data/conf for the database that will house the imported data.

---
## Run
1. Change to the scripts directory `cd nssk-data/src/waterrangers/scripts`
2. Run the retrieval script `./get.sh` to retrieve csv data dumps from the Waterrangers site.
3. Run the import wrapper script `./import-all.sh`

---
## Notes
* Logs output to `waterrangers.log`
* The database will enforce uniqueness constraints. Inserts that fail uniqueness constraints will be dumped 
to a `duplicates_*.sql` file.
* Currently tracking Waterrangers sites:
  * MIS-M-01
  * MIS-E-01
  * MIS-W-01
  * MOS-M-01
  * WAG-E-01
  * WAG-E-02
  * WAG-E-03
  * WAG-E-05
  * WAG-E-06a
  * WAG-E-06b
  * WAG-E-07
  * WAG-M-01
  * WAG-M-02
  * WAG-M-03
  * WAG-W-02a
  * WAG-W-02b
  * WAG-W-03
* May be defunct: no measurements for 2026.