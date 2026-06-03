"""Summary-statistic transformations for the gold layer."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def compute_daily_summary(df: DataFrame) -> DataFrame:
    """Min, max and mean power output per turbine per calendar day.

    """
    return (
        df.withColumn("date", F.to_date("timestamp"))
        .groupBy("turbine_id", "date")
        .agg(
            F.min("power_output").alias("min_power"),
            F.max("power_output").alias("max_power"),
            F.round(F.mean("power_output"), 4).alias("mean_power"),
            F.count("power_output").alias("record_count"),
        )
    )


def compute_overall_summary(df: DataFrame) -> DataFrame:
    """Aggregate statistics across the entire reporting period, per turbine."""
    return (
        df.groupBy("turbine_id")
        .agg(
            F.min("power_output").alias("min_power"),
            F.max("power_output").alias("max_power"),
            F.round(F.mean("power_output"), 4).alias("mean_power"),
            F.round(F.stddev("power_output"), 4).alias("std_power"),
            F.count("power_output").alias("total_records"),
        )
    )
