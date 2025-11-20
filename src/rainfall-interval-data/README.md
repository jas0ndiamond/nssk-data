# Rainfall Interval Data

---
## Description
Compile rainfall amounts around correlated measurements from CoSMo, CNV Flowworks, and CNV Hydrometrics measurements.

---
## Setup
1. Create a database config file from the template in nssk-data/conf for the database that will house the imported data.
2. Run imports for `cosmo`, `cnv_flowworks`, and `cnv_hydrometric`.

---
## Run
```
cd src/rainfall-interval-data
../../venv/bin/python3 rainfall-interval-data.py -cfg ../../conf/rainfall-interval-data.json
```

---
## Implementation
* Source measurement datasets can be large, and will continue to grow. Iterate over database tables in time blocks, so we're not putting 500k rows in memory that won't be read frequently enough.


---
## Notes
* Source datasets (especially CoSMo) will have gaps and drift.
* This dataset must be rebuilt completely with every update to any dataset
  * Gaps could be plugged
  * Datasets update at different intervals, and won't be current to the same start/end dates
* Rainfall data is combined into 10 minute intervals from 5-minute rainfall measurements.
* Correlation windows between datasets (CoSMo, Hydrometric) is 2 minutes.
  * We only want the closest (in time) measurement within the correlation window.
* If several CNV Hydrometric measurements correlate to a single cosmo measurement, the earliest measurement is considered the correlated measurement. 
  * This is unlikely given current measurement frequencies
* Interval data for CoSMo sites WAGG01, WAGG03, and consolidated.
* Although CNV Flowworks rainfall measurements are typically in 5-minute intervals, it is not guaranteed that one measurement is followed by another measurements 5 minutes later.
  * Factors include drift, de-calibration, or other availability problems.
  * Site can go offline, then begin recording at a skewed interval.
  * Rainfall measurements may not always have been, or continue to be, in 5-minute intervals.