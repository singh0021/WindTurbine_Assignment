# Declarative Pipelines (DLT) variant — optional


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

1. Once DLT is available in workspace. Note: **serverless** DLT additionally requires Unity
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

