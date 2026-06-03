"""Schema and column definitions for raw turbine sensor data."""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

# Natural key for a single reading: one turbine at one point in time.
KEY_COLUMNS = ["turbine_id", "timestamp"]

# Numeric sensor measurements (the columns we clean, impute and aggregate).
MEASURE_COLUMNS = ["wind_speed", "wind_direction", "power_output"]


RAW_SCHEMA = StructType(
    [
        StructField("timestamp", TimestampType(), True),
        StructField("turbine_id", IntegerType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_direction", DoubleType(), True),
        StructField("power_output", DoubleType(), True),
    ]
)
