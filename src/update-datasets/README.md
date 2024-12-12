# update-datasets

Automation around dataset retrieval and database insertion

---
### Setup

1. Ensure the `nssk-data` database is set up and configured correctly.
2. Create a config file from the template in `conf/config.template.json`.
   1. Ensure credentials and keys are correct.
3. ...
4. Execute `update-datasets.py`

---
### Notes
* Choose an update frequency that is considerate of source bandwidth.
* Any curl commands are (and should continue to be), have bandwidth capped with `--limit-rate`. 
  * Current rate limit is `1m`
---
### Components
* `update-datasets.py` - Master script to initiate and manage dataset retrieval and database importer execution. Intended to run as a cronjob.
* `update-cosmo.sh` - Retrieve the data dump file from the DFO's CoSMo project.
* `update-cnv-rainfall.sh` - Retrieve Flowworks data for a CNV Rainfall known site and channel.
  * Requires username and password.
* `update-dnv-whitewater.sh` - Retrieve Flowworks data for a DNV Whitewater known site and channel.
  * Requires API key.
* `update-waterrangers.sh` - Retrieve Waterrangers data for a known site.