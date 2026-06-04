# Wind Turbine Data Pipeline

A production-style data pipeline for a wind farm, built on **Databricks** using
**Asset Bundles** for deployment and a **medallion (bronze → silver → gold)**
architecture. It ingests raw turbine sensor CSVs, quarantines bad data against
externally-defined quality rules, cleans the survivors, and publishes daily
summaries and anomaly flags for reporting.


### Two execution modes

The medallion logic lives once in the reusable library at
[`src/wind_turbine/`](src/wind_turbine); two thin orchestration layers call it:

- **Plain notebook jobs (default)** — each layer is a notebook task in a single
  scheduled job ([`src/jobs/`](src/jobs)). Runs on any workspace; **no special
  features required**. Bronze still uses Auto Loader for incremental ingestion.
- **Lakeflow Declarative Pipelines / DLT (optional)** — the same layers as
  `@dlt.table` definitions ([`dlt/`](dlt)). Requires the Declarative Pipelines
  feature to be enabled in the workspace as we don't have DLT enabled in our workspace



In the default (plain-job) mode each layer is its own notebook task, wired as a
DAG (`land_data → bronze → silver → gold`) so any stage can be retried or
inspected on its own. Bronze ingests incrementally via Auto Loader; silver and
gold are deterministic full recomputes from the layer below.

## Repository structure

```
.
├── databricks.yml                     # Asset Bundle: targets (dev/prod), variables
├── resources/
│   ├── wind_turbine.job.yml            # Daily scheduled job: land → bronze → silver → gold
│   └── dq_rules.csv                    # Data-quality rules (editable without code changes)
├── src/
│   ├── jobs/                           # Default pipeline — one notebook task per layer (no DLT)
│   │   ├── bronze_ingest.py            #   Auto Loader ingestion (schema enforced)
│   │   ├── silver_transform.py         #   DQ quarantine + cleaning
│   │   └── gold_aggregate.py           #   summaries + anomaly detection
│   ├── setup/
│   │   └── land_sample_data.py         # Job task: stage CSVs into the landing zone
│   └── wind_turbine/                   # Reusable, framework-agnostic transforms
│       ├── schema.py  config.py
│       ├── quality.py                  #   rule loading + valid/quarantine split
│       ├── cleaning.py                 #   dedup, imputation, outlier capping
│       ├── summary.py  anomalies.py
├── dlt/                                # Optional DLT variant (same logic)
│   ├── pipelines/{00_bronze,01_silver,02_gold}.py
│   └── resources/wind_turbine.pipeline.yml
├── tests/
│   ├── conftest.py                     # local SparkSession fixture
│   ├── unit/                           # one focused test module per transform
│   └── integration/                    # end-to-end medallion flow
├── dashboards/queries/                 # SQL backing the reporting dashboard
├── .github/workflows/ci.yml            # lint → tests → bundle validate → deploy
├── data/                               # sample CSVs (3 groups × 5 turbines)
└── archive/                            # original single-notebook prototype
```

## Deployment


```bash
# 1. Authenticate (creates a profile).
databricks auth login --host https://<your-workspace>.azuredatabricks.net


# 2. Validate and deploy to the dev target.
databricks bundle validate -t dev
databricks bundle deploy   -t dev

# 3. Run the job once (lands the sample data, then runs the pipeline).
databricks bundle run wind_turbine_daily_job -t dev
```

### Configuration

Everything environment-specific is a bundle **variable** (see `databricks.yml`):

| Variable | dev default | Purpose |
|----------|-------------|---------|
| `schema` | `wind_turbine_dev` | hive_metastore schema tables publish to |
| `landing_path` | `dbfs:/FileStore/wind_turbine/dev/landing` | Auto Loader source directory |
| `storage_path` | `dbfs:/FileStore/wind_turbine/dev/storage` | pipeline storage + checkpoints |

## Data quality & the quarantine table

Validity rules live in [`resources/dq_rules.csv`](resources/dq_rules.csv) — one
row per rule, each a SQL boolean expression that must be TRUE for a record to be
valid:

| name | constraint |
|------|------------|
| `valid_timestamp` | `timestamp IS NOT NULL` |
| `valid_turbine_id` | `turbine_id IS NOT NULL AND turbine_id BETWEEN 1 AND 15` |
| `valid_wind_speed` | `wind_speed IS NULL OR wind_speed BETWEEN 0 AND 100` |
| `valid_wind_direction` | `wind_direction IS NULL OR wind_direction BETWEEN 0 AND 360` |
| `valid_power_output` | `power_output IS NULL OR power_output BETWEEN 0 AND 15` |

At the bronze → silver hop every row is checked against these rules. Rows failing
any rule are written to **`silver_quarantine`**, tagged with a `_failed_rules`
array and a `_quarantined_at` timestamp, so bad data can be examined later
instead of being silently dropped. Out-of-range values are quarantined;
genuinely *missing* values (nulls) pass the gate and are imputed during cleaning.

## Tests

Transformation logic lives in `src/wind_turbine/` as pure functions, so the test
suite runs locally with plain PySpark — no Databricks or `dlt` runtime needed.

```bash
pip install -r requirements-dev.txt
pytest tests/unit                       # fast, per-transform unit tests
pytest tests/integration                # end-to-end flow + Delta write path
pytest --cov=wind_turbine --cov-report=term-missing   # with coverage
```

- **Unit tests** are split by concern (cleaning, quality, summary, anomalies) —
  one assertion focus per test — so a failure points straight at the broken
  transform.
- **Integration tests** cover the composed medallion flow over a realistic batch
  and a **Delta round-trip** (`test_delta_roundtrip.py`) that writes the
  silver/gold tables via `overwrite` and asserts a re-run is idempotent.

## Dashboard

The reporting dashboard is built on the gold tables; its queries are
version-controlled under [`dashboards/queries/`](dashboards/queries)

## Design notes

How this structure addresses the goals of a scalable, deployable, testable
pipeline:

- **Modular, not one notebook** — reusable transforms in a library, thin
  orchestration wrappers, separate setup/test/dashboard concerns. The same
  library backs both the plain-job and DLT execution modes.
- **Deployment & orchestration** — Databricks Asset Bundles define the job (and
  the optional DLT pipeline) as code, promotable dev → prod.
- **Incremental ingestion via Auto Loader** — Auto Loader tracks processed files and enforces the schema
  (and works without DLT, in a plain streaming job).
- **Medallion architecture** — bronze (raw) / silver (validated + cleaned) /
  gold (business aggregates).
- **Stage independence** — each layer is its own job task in a retryable DAG
  (`land → bronze → silver → gold`); a failed stage reruns without redoing the
  others.
- **Data-quality quarantine** — rules in a CSV, failures captured in a table.
- **Structured tests** — pytest, fixtures, unit vs integration separation.
- **CI/CD** — GitHub Actions lints, tests, validates the bundle, and deploys.

## Assumptions

- Each CSV holds one group of 5 turbines; a turbine always appears in the same
  file (turbines 1–15 across three groups).
- Timestamps are hourly and timezone-naive (UTC assumed).
- Power output is in MW, valid range 0–15; wind speed in m/s, plausible 0–100;
  wind direction a compass bearing 0–360.
- Missing readings are sensor gaps to be imputed (forward then backward fill per
  turbine); values *outside* the valid ranges are treated as bad data and
  quarantined.
- Anomalies are readings beyond 2 standard deviations of a turbine's **daily**
  mean — a per-turbine, per-day baseline that tolerates natural day-to-day
  variation in wind conditions.
- Outliers are winsorised (capped at 3 std-devs) rather than dropped, so daily
  aggregates aren't skewed while no rows are lost.
```
