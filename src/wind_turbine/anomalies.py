"""Anomaly-detection transformations for the gold layer."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from wind_turbine.config import ANOMALY_STD_THRESHOLD


def detect_anomalies(
    df: DataFrame, threshold: float = ANOMALY_STD_THRESHOLD
) -> DataFrame:
    """Flag readings deviating more than ``threshold`` std-devs from the daily mean.

    Statistics are computed per turbine *per day* rather than against a global
    baseline, so natural day-to-day variation in wind conditions doesn't drown
    out genuinely anomalous readings. Days where every reading is identical
    (std-dev 0) produce a z-score of 0 and are never flagged.
    """
    df_with_date = df.withColumn("date", F.to_date("timestamp"))

    daily_stats = df_with_date.groupBy("turbine_id", "date").agg(
        F.mean("power_output").alias("daily_mean"),
        F.stddev("power_output").alias("daily_std"),
    )

    scored = df_with_date.join(
        daily_stats, on=["turbine_id", "date"], how="left"
    ).withColumn(
        "z_score",
        F.when(
            F.col("daily_std") > 0,
            F.round(
                (F.col("power_output") - F.col("daily_mean")) / F.col("daily_std"),
                4,
            ),
        ).otherwise(F.lit(0.0)),
    )

    return (
        scored.filter(F.abs(F.col("z_score")) > threshold)
        .withColumn(
            "deviation_mw",
            F.round(F.col("power_output") - F.col("daily_mean"), 4),
        )
        .select(
            "timestamp",
            "turbine_id",
            "power_output",
            "wind_speed",
            F.round("daily_mean", 4).alias("daily_mean"),
            F.round("daily_std", 4).alias("daily_std"),
            "z_score",
            "deviation_mw",
        )
    )


def summarise_anomalies(anomalies: DataFrame) -> DataFrame:
    """Aggregate anomaly counts and severity per turbine."""
    return anomalies.groupBy("turbine_id").agg(
        F.count("z_score").alias("anomaly_count"),
        F.round(F.mean("deviation_mw"), 4).alias("avg_deviation_mw"),
        F.round(F.max(F.abs(F.col("z_score"))), 4).alias("max_abs_z_score"),
    )
