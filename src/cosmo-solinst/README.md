# CoSMo/Solinst Importer

Imports from Solinst Instrument raw CSV data dumps into a MySQL database.

Plugs gaps in the CoSMo dataset, and provides early usage of raw sensor data destined for the CoSMo ingest process.

---
## Setup

1. Create a database config file from the template in nssk-data/conf for the database that will house the imported data.
2. Acquire the data dump from whoever has access to the raw data dumps.
3. Remove any preamble to the file so that the CSV schema is the first line.
4. Run a CoSMo import

---
## Run

`cd src/cosmo-solinst`

`../../venv/bin/python3 cosmo-solinst-import.py -cfg ../../conf/cosmo.json [SITE] [DUMP_FILE]`

---
## Notes

* Typically, this raw data is collected locally and handed off to DFO PSEC Community Stream Monitoring where it 
eventually appears in the CoSMo dataset.
* Logs output to `cosmo-solisnt-import.log`
* The database will enforce uniqueness constraints. Inserts that fail uniqueness constraints will be dumped 
to a `duplicates_*.sql` file
* A typical import will encounter many duplicates.
* Added measurements add the following annotations to CoSMo fields:
  * `ResultValue`: "NSSK Supplied Actual"
  * `ResultComment`: "Preliminary QC and Specific Conductance by NSSK"
* `Specific Conductivity` is dynamically calculated by formula `entry_specific_conductance = entry_conductivity / (1 + (0.02 * (entry_temperature - 25)))`
* Measurements processed in Solinst dump files are integrated with the [CoSMo](../cosmo) dataset.
* As the CoSMo dataset gets updated, our local CoSMo/Solist updates will register as duplicate measurements and fail database insertion.
  * This requires occasional clean CoSMo dataset imports.