"""Delta write-path integration test for the no-DLT batch pipeline.
"""

import datetime
from pathlib import Path

from pyspark.sql import functions as F
from wind_turbine.anomalies import detect_anomalies
from wind_turbine.cleaning import clean
from wind_turbine.quality import load_dq_rules, split_on_rules
from wind_turbine.schema import RAW_SCHEMA
from wind_turbine.summary import compute_daily_summary

DQ_RULES_PATH = str(Path(__file__).resolve().parents[2] / "resources" / "dq_rules.csv")


def _overwrite(df, table):
    """Mirror the write call the silver/gold notebooks use."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )


def test_delta_medallion_roundtrip_is_idempotent(spark, make_readings, base_ts):
    schema = "wt_test_roundtrip"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    try:

        rows = [
            (base_ts + datetime.timedelta(hours=h), 1, 10.0, 180.0, 3.0 + (h % 5))
            for h in range(30)
        ]
        rows += [
            (base_ts + datetime.timedelta(hours=h), 2, 9.0, 90.0, 2.0 + (h % 4))
            for h in range(30)
        ]
        rows.append((base_ts, 1, 10.0, 180.0, 3.0))
        rows.append((base_ts, 3, 10.0, 180.0, -5.0))

        # BRONZE (stand-in for Auto Loader: a batch write to the Delta table).
        _overwrite(make_readings(rows), f"{schema}.bronze_readings")

        # SILVER
        rules = load_dq_rules(DQ_RULES_PATH)
        bronze = spark.table(f"{schema}.bronze_readings").select(*RAW_SCHEMA.fieldNames())
        valid, quarantined = split_on_rules(bronze, rules)
        _overwrite(quarantined, f"{schema}.silver_quarantine")
        _overwrite(clean(valid), f"{schema}.silver_cleaned_readings")

        assert spark.table(f"{schema}.silver_quarantine").count() == 1
        cleaned_count = spark.table(f"{schema}.silver_cleaned_readings").count()
        assert cleaned_count == 60
        for col in ["wind_speed", "wind_direction", "power_output"]:
            assert (
                spark.table(f"{schema}.silver_cleaned_readings")
                .filter(F.col(col).isNull())
                .count()
                == 0
            )

        # GOLD writes succeed and are readable.
        cleaned = spark.table(f"{schema}.silver_cleaned_readings")
        _overwrite(compute_daily_summary(cleaned), f"{schema}.gold_daily_summary")
        _overwrite(detect_anomalies(cleaned), f"{schema}.gold_anomalies")
        assert spark.table(f"{schema}.gold_daily_summary").count() >= 1

        # IDEMPOTENCY: re-running silver overwrite yields the same row count.
        valid2, _ = split_on_rules(bronze, rules)
        _overwrite(clean(valid2), f"{schema}.silver_cleaned_readings")
        assert spark.table(f"{schema}.silver_cleaned_readings").count() == cleaned_count
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS {schema} CASCADE")
