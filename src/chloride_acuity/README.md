# Chloride Acuity

---
## Description
* Collect CoSMo measurements when conductivity readings are high.

---
## Setup
1. Create a database config file from the template in nssk-data/conf for the database that will house the imported data.
2. Run import for `cosmo`.

---
## Run
There is no importer for this. This is run as part of distribution generation.

---
## Notes
* Threshold for measurement collection is set during distribution generation. The initial threshold is `Specific Conductance` in amount greater than `500 uS/cm`.
* CoSMo sites WAGG01 and WAGG03 only for now.