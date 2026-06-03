"""Bronze layer — raw ingestion via Auto Loader.

Incrementally loads CSV files as they land in the configured source directory.
Auto Loader tracks which files it has already processed, so re-running the
pipeline only ingests new files — this replaces the brittle ``left_anti`` join
the original notebook used for incremental detection.

The bronze table is append-only and faithfully preserves the raw data plus
ingestion lineage; no cleaning happens here.
"""

import os
import sys

from pyspark.sql import functions as F

import dlt

# Make the reusable `wind_turbine` package importable when this file runs inside
# the Declarative Pipelines runtime (dlt/pipelines/.. -> repo root -> src).
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_SRC = os.path.join(_REPO_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from wind_turbine.schema import RAW_SCHEMA  # noqa: E402

SOURCE_PATH = spark.conf.get("source_path")  # noqa: F821  (spark is injected by DLT)


@dlt.table(
    name="bronze_readings",
    comment="Raw turbine sensor readings ingested from CSV via Auto Loader.",
    table_properties={"quality": "bronze"},
)
def bronze_readings():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        # Enforce the schema rather than infer it — fail loud on malformed files.
        .schema(RAW_SCHEMA)
        .load(SOURCE_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
