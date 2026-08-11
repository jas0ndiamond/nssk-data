# CoSMo Importer

Imports data from CoSMo CSV data dumps into a MySQL database

---
## Setup

1. Create a database config file from the template in nssk-data/conf for the database that will house the imported data.
2. Acquire the data dump from the CoSMo website
3. Set the list of sensors in `cosmo-import.py`

---
## Run

`cd src/cosmo`

`../../venv/bin/python3 cosmo-import.py -cfg ../../conf/cosmo.json /path/to/nssk-data-dumps/doi.org_10.25976_0gvo-9d12.csv`

---
## Notes
* Logs output to `cosmo-import.log`
* The database will enforce uniqueness constraints. Inserts that fail uniqueness constraints will be dumped to a `duplicates_*.sql` file
* Runtimes can take ~15 minutes for 1 million inserts for a database on the local network.
* Measurements are typically every 5 minutes, but there are stretches (2019-06-12, 2019-07-02, ...) where measurements are made hourly.
* We track the following CoSMo sensor sites:
  * WAGG01
  * WAGG02
  * WAGG03
  * MOSQ01
  * MOSQ02
  * MOSQ03
  * MOSQ04
  * MOSQ05
  * MOSQ06
  * MOSQ07
  * MISS01
  * MACK02
  * MACK03
  * MACK04
  * MACK05
  * HAST01
  * HAST02
  * HAST03
* We pay special attention to `WAGG01` and `WAGG03`
  * `WAGG01` begins logging measurements at 2019-06-12 11:00:00
  * `WAGG03` begins logging measurements at 2022-03-09 00:00:00