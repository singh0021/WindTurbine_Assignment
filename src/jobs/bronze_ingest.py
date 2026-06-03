# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — raw ingestion (no DLT)
# MAGIC
# MAGIC Incrementally ingests CSVs from the landing zone into the
# MAGIC `bronze_readings` Delta table using **Auto Loader** with an enforced
# MAGIC schema. Auto Loader works in a plain job (it does not require Declarative
# MAGIC Pipelines): it tracks already-processed files via a checkpoint, so each run
# MAGIC only picks up new files — replacing the original notebook's `left_anti` join.
# MAGIC
# MAGIC The bronze table is append-only and preserves the raw data plus ingestion
# MAGIC lineage; no cleaning happens here.



import sys

dbutils.widgets.text("schema", "")
dbutils.widgets.text("landing_path", "")
dbutils.widgets.text("checkpoint_path", "")
dbutils.widgets.text("src_path", "")

schema = dbutils.widgets.get("schema")
landing_path = dbutils.widgets.get("landing_path")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
src_path = dbutils.widgets.get("src_path")

if not all([schema, landing_path, checkpoint_path, src_path]):
    raise ValueError("schema, landing_path, checkpoint_path and src_path are required")


if src_path not in sys.path:
    sys.path.insert(0, src_path)

from pyspark.sql import functions as F  
from wind_turbine.schema import RAW_SCHEMA  



spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")



stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    .schema(RAW_SCHEMA)
    .load(landing_path)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

query = (
    stream.writeStream.format("delta")
    .option("checkpointLocation", f"{checkpoint_path}/bronze")
    .option("mergeSchema", "true")
    # availableNow processes all new files then stops — batch semantics on a job.
    .trigger(availableNow=True)
    .toTable(f"{schema}.bronze_readings")
)
query.awaitTermination()

# COMMAND ----------

count = spark.table(f"{schema}.bronze_readings").count()
print(f"bronze_readings now holds {count} rows")
