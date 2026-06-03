"""Shared pytest fixtures.

"""

import datetime
import shutil
import tempfile

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from wind_turbine.schema import RAW_SCHEMA


@pytest.fixture(scope="session")
def spark():

    warehouse = tempfile.mkdtemp(prefix="wt-spark-wh-")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("wind-turbine-tests")
        # Small shuffle partition count keeps the local tests fast.
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", warehouse)
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture
def make_readings(spark):
    """Factory building a raw-readings DataFrame from a list of tuples.

    Tuples follow RAW_SCHEMA order:
    ``(timestamp, turbine_id, wind_speed, wind_direction, power_output)``.
    """

    def _make(rows):
        return spark.createDataFrame(rows, schema=RAW_SCHEMA)

    return _make


@pytest.fixture
def base_ts():
    """A fixed base timestamp so tests are deterministic."""
    return datetime.datetime(2022, 3, 1, 0, 0, 0)
