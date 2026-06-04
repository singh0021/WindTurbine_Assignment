# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — aggregates + anomaly detection (no DLT)
# MAGIC
# MAGIC Builds the business-ready tables from `silver_cleaned_readings`:
# MAGIC
# MAGIC * `gold_daily_summary`   — min/max/mean power per turbine per day
# MAGIC * `gold_overall_summary` — min/max/mean/std power per turbine, whole period
# MAGIC * `gold_anomalies`       — readings >2 std-devs from the turbine's daily mean
# MAGIC * `gold_anomaly_summary` — anomaly counts and severity per turbine
# MAGIC All four are overwritten each run (deterministic recompute from silver).



import sys

dbutils.widgets.text("schema", "")
dbutils.widgets.text("src_path", "")

schema = dbutils.widgets.get("schema")
src_path = dbutils.widgets.get("src_path")

if not all([schema, src_path]):
    raise ValueError("schema and src_path are required")

if src_path not in sys.path:
    sys.path.insert(0, src_path)

from wind_turbine.anomalies import detect_anomalies, summarise_anomalies 
from wind_turbine.summary import compute_daily_summary, compute_overall_summary 


def _write(df, table):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{schema}.{table}")
    )
    print(f"{table}: {spark.table(f'{schema}.{table}').count()} rows")


cleaned = spark.table(f"{schema}.silver_cleaned_readings")

_write(compute_daily_summary(cleaned), "gold_daily_summary")
_write(compute_overall_summary(cleaned), "gold_overall_summary")

anomalies = detect_anomalies(cleaned)
_write(anomalies, "gold_anomalies")
_write(summarise_anomalies(anomalies), "gold_anomaly_summary")
