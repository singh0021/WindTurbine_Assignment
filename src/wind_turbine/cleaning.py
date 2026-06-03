"""Data cleaning transformations.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from wind_turbine.config import OUTLIER_STD_THRESHOLD
from wind_turbine.schema import MEASURE_COLUMNS


def remove_duplicates(df: DataFrame) -> DataFrame:
    """Drop duplicate readings for the same turbine and timestamp.
 
    """
    return df.dropDuplicates(["turbine_id", "timestamp"])


def impute_missing(df: DataFrame) -> DataFrame:
    """Fill missing numeric values per turbine using forward then backward fill.
    """
    w_forward = (
        Window.partitionBy("turbine_id")
        .orderBy("timestamp")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    w_backward = (
        Window.partitionBy("turbine_id")
        .orderBy("timestamp")
        .rowsBetween(0, Window.unboundedFollowing)
    )

    for col in MEASURE_COLUMNS:
        df = df.withColumn(col, F.last(col, ignorenulls=True).over(w_forward))
        df = df.withColumn(col, F.first(col, ignorenulls=True).over(w_backward))

    return df


def cap_outliers(df: DataFrame, n_std: float = OUTLIER_STD_THRESHOLD) -> DataFrame:
    """Winsorise ``power_output`` to +/- ``n_std`` std-devs of each turbine's mean.
    """
    stats = df.groupBy("turbine_id").agg(
        F.mean("power_output").alias("_mean"),
        F.stddev("power_output").alias("_std"),
    )

    capped = (
        df.join(stats, on="turbine_id", how="left")
        .withColumn(
            "power_output",
            F.greatest(
                F.col("_mean") - n_std * F.col("_std"),
                F.least(F.col("power_output"), F.col("_mean") + n_std * F.col("_std")),
            ),
        )
        .drop("_mean", "_std")
    )
    return capped


def clean(df: DataFrame) -> DataFrame:
    """Run the full cleaning pipeline: dedup -> impute -> cap outliers."""
    df = remove_duplicates(df)
    df = impute_missing(df)
    df = cap_outliers(df)
    return df
