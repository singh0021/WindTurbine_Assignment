# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — data-quality gate + cleaning (no DLT)
# MAGIC
# MAGIC Reads the full `bronze_readings` table, applies the data-quality rules from
# MAGIC `dq_rules.csv`, and writes two Delta tables:
# MAGIC
# MAGIC * `silver_quarantine` — rows that failed one or more rules, tagged with the
# MAGIC   rule names, kept for later examination.
# MAGIC * `silver_cleaned_readings` — rows that passed, then de-duplicated,
# MAGIC   gap-filled and outlier-capped.
# MAGIC
# MAGIC Silver is a full recompute from bronze (its window/global statistics need
# MAGIC the complete history), so both tables are overwritten each run — bronze is
# MAGIC the incremental layer, silver/gold are deterministic materialisations.

# COMMAND ----------

import sys

dbutils.widgets.text("schema", "")
dbutils.widgets.text("dq_rules_path", "")
dbutils.widgets.text("src_path", "")

schema = dbutils.widgets.get("schema")
dq_rules_path = dbutils.widgets.get("dq_rules_path")
src_path = dbutils.widgets.get("src_path")

if not all([schema, dq_rules_path, src_path]):
    raise ValueError("schema, dq_rules_path and src_path are required")

if src_path not in sys.path:
    sys.path.insert(0, src_path)

from wind_turbine.cleaning import clean  # noqa: E402
from wind_turbine.quality import load_dq_rules, split_on_rules  # noqa: E402
from wind_turbine.schema import RAW_SCHEMA  # noqa: E402



bronze = spark.table(f"{schema}.bronze_readings")

rules = load_dq_rules(dq_rules_path)
print(f"Loaded {len(rules)} data-quality rules: {[r.name for r in rules]}")


valid, quarantined = split_on_rules(bronze.select(*RAW_SCHEMA.fieldNames()), rules)



(
    quarantined.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{schema}.silver_quarantine")
)
print(f"silver_quarantine: {spark.table(f'{schema}.silver_quarantine').count()} rows")



cleaned = clean(valid)
(
    cleaned.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{schema}.silver_cleaned_readings")
)
print(f"silver_cleaned_readings: {spark.table(f'{schema}.silver_cleaned_readings').count()} rows")
