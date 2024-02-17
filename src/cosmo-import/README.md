# CoSMo Importer

Imports data from CoSMo CSV data dumps into a MySQL database

# Setup

1. Create a database config file from the template in nssk-data/conf for the database that will house the imported data.
   2. Ensure the user has `INSERT` access to database.
2. Acquire the data dump from the CoSMo website
3. Set the list of sensors in `cosmo-import.py`

# Run

`python3 cosmo-import.py`

# Notes

Logs output to `run.log`

The database will enforce uniqueness constraints. Inserts that fail uniqueness constraints will be dumped to a `duplicates_*.sql` file

Runtimes can be long: ~15 minutes for 1 million inserts for a database on the local network.