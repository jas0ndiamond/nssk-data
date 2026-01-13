# Rainfall Interval Data

---
## Description
Compile rainfall amounts at 10-minute intervals from the 5-minute rainfall measurements in the CNV Flowworks dataset, and collect correlated measurements from CoSMo and CNV Hydrometrics datasets.

---
## Notes
* Source datasets (especially CoSMo) will have gaps and drift.
* This dataset must be rebuilt completely with every update to any dataset.
  * Gaps in component datasets could be plugged.
  * Datasets update at different intervals, and won't be current to the same start/end dates.
* Rainfall data is combined into 10 minute intervals from 5-minute rainfall measurements.
  * CNV Rainfall measurements from the CNV Flowworks dataset are the amount of rainfall recorded in the previous 5 minutes.
  * Ex: A 10-minute rainfall tally comprised of two measurements at `2024-10-20 00:00:00` and `2024-10-20 00:05:00`:
    * The actual realtime measured is between `2024-10-19 23:55:00` and `2024-10-20 00:05:00`.
* Correlation windows between datasets CoSMo and CNV Hydrometric is 2 minutes.
  * We only want the closest (in time) measurement within the correlation window.
* Correlation between 10-minute rainfall intervals, and correlated CoSMo/CNV Hydrometric datasets is within 2 minutes of the first rainfall measurement.
  * This rainfall measurement is the middle of the realtime measurement interval. 
* If several CNV Hydrometric measurements correlate to a single cosmo measurement, the earliest CNV Hydrometric measurement is considered the correlated measurement. 
  * This scenario is unlikely given current measurement frequencies.
* Interval data is determined for CoSMo sites WAGG01, WAGG03.
  * Also a consolidated (all) view in xlsx workbooks.
* Although CNV Flowworks rainfall measurements are typically in 5-minute intervals, it is not guaranteed that one measurement is followed by another measurements 5 minutes later.
  * Factors include drift, de-calibration, or other availability problems.
  * Site can go offline, then begin recording at a skewed interval.
  * Rainfall measurements may not always have been, or continue to be, in 5-minute intervals.
* Timestamps from source datasets (CoSMo, CNV Hydrometric) are deliberately included for easier charting.

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
* Tables are truncated at the start of each run, due to divergent updates and gaps in the source datasets.

---
## Outputs
* CSV datasets organized by CoSMo site.
* Xlsx dashboards of site datasets side-by-side.

---
## TODO
* This would benefit from using an intermediate document database rather than collections of objects.
* This would benefit from distributed processing.
* JSON organization of site mappings between datasets.
* Cache rainfall measurements.