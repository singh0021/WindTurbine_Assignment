# Declarative Pipelines (DLT) variant — optional

This folder holds an alternative implementation of the same medallion pipeline
built on **Lakeflow Declarative Pipelines** (DLT). It is **not** wired into the
default bundle, because DLT must be enabled in the workspace — if it isn't, you
get:

```
Error: cannot create pipeline: The Delta Pipelines feature is not enabled in your workspace.
```

The **default** deployment instead runs the medallion stages as plain notebook
job tasks (see [`src/jobs/`](../src/jobs) and
[`resources/wind_turbine.job.yml`](../resources/wind_turbine.job.yml)). Both
variants call the exact same transformation library in
[`src/wind_turbine/`](../src/wind_turbine), so they produce identical tables.

## Contents

```
dlt/
├── pipelines/
│   ├── 00_bronze.py   # @dlt.table Auto Loader ingestion
│   ├── 01_silver.py   # @dlt.table DQ quarantine + cleaning
│   └── 02_gold.py     # @dlt.table summaries + anomalies
└── resources/
    └── wind_turbine.pipeline.yml   # pipeline definition (publishes to hive_metastore)
```

## Enabling it later

1. Confirm DLT is available in the workspace (Workspace admin → previews, or ask
   your account team). Note: **serverless** DLT additionally requires Unity
   Catalog; the pipeline here is configured for classic compute against
   `hive_metastore`.

2. Include the pipeline resource in the bundle by adding this folder to the
   `include` list in [`databricks.yml`](../databricks.yml):

   ```yaml
   include:
     - resources/*.yml
     - dlt/resources/*.yml
   ```

3. (Optional) Replace the four notebook tasks in
   `resources/wind_turbine.job.yml` with a single pipeline task so the daily job
   drives the DLT pipeline instead:

   ```yaml
   - task_key: run_pipeline
     depends_on:
       - task_key: land_data
     pipeline_task:
       pipeline_id: ${resources.pipelines.wind_turbine_pipeline.id}
   ```

4. Validate and deploy:

   ```bash
   databricks bundle validate -t dev
   databricks bundle deploy   -t dev
   ```
