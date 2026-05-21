# Wind Turbine Data Processing Pipeline

A PySpark pipeline that runs on Databricks to process raw sensor readings from a wind farm. It handles data cleaning, computes daily summary statistics, flags anomalous turbine behaviour, and writes everything to Delta tables with incremental merge support.

## What it does

The notebook runs through five stages:

1. **Ingestion** — Reads three CSV files (one per turbine group), enforces a typed schema, and unions them into a single Spark DataFrame. On repeat runs it compares incoming data against what's already in the Delta table and only processes new records.

2. **Cleaning** :
   - Drops duplicate readings (same turbine + timestamp).
   - Nulls out values outside physically plausible ranges (e.g. negative power output, wind speed above 100 m/s).
   - Fills gaps using per-turbine forward fill then backward fill via window functions.
   - Caps outliers at 3 standard deviations from each turbine's mean (winsorisation).

3. **Summary statistics** — Groups by turbine and calendar date to produce min, max, and mean power output per day. Also generates an overall per-turbine summary across the full period.

4. **Anomaly detection** — Computes a z-score for each reading against its turbine's daily mean. Anything beyond 2 standard deviations is flagged. A severity summary is also produced per turbine.

5. **Storage** — Writes cleaned readings, daily summaries, and anomaly records to Delta tables using MERGE. This means the notebook can be re-run daily as new data arrives — existing rows are updated and new rows are inserted, with no duplicates.

## Incremental design

The CSVs are expected to grow over time (appended daily with the latest 24 hours of data). Rather than reprocessing everything from scratch, the pipeline:

- Reads the full CSV (Spark handles this efficiently).
- Performs a `left_anti` join against the existing `cleaned_readings` Delta table to isolate only the new records.
- Cleans, summarises, and detects anomalies on the new batch only.
- Merges results into each Delta table using `DeltaTable.merge()` keyed on `(turbine_id, timestamp)` for readings and anomalies, and `(turbine_id, date)` for daily summaries.

If no new records are found, the notebook exits early.

## Assumptions

- Each CSV contains exactly one group of 5 turbines. A turbine always appears in the same file.
- Timestamps are hourly and timezone-naive (UTC assumed).
- Readings are expected every hour. Gaps are treated as sensor malfunctions and filled via interpolation where possible.
- Power output is in MW and should be non-negative. The valid range is 0–15 MW.
- Wind speed is in m/s with a plausible upper bound of 100 m/s.
- Anomaly detection uses a per-turbine, per-day z-score approach rather than a global baseline. This accounts for natural variation in daily wind conditions.

## Project structure

```
wind_turbine_pipeline_databricks.ipynb   # Single notebook with all pipeline logic
data/
    data_group_1.csv                     # Turbines 1-5, hourly readings
    data_group_2.csv                     # Turbines 6-10
    data_group_3.csv                     # Turbines 11-15
README.md
```

## Delta tables produced

| Table | Key | Description |
|-------|-----|-------------|
| `wind_turbine.cleaned_readings` | `turbine_id, timestamp` | Cleaned sensor readings after dedup, range checks, imputation, and outlier capping |
| `wind_turbine.daily_summary` | `turbine_id, date` | Min, max, mean power output per turbine per day |
| `wind_turbine.anomalies` | `turbine_id, timestamp` | Readings flagged as anomalous (z-score > 2), with deviation details |

## Tests

The notebook includes inline unit tests at the bottom (Section 12) that validate each pipeline stage: duplicate removal, range checking, imputation, cleaning, summary statistics, anomaly detection, Delta merge idempotency, and incremental record detection. Run the test cell after the pipeline to verify everything works.
