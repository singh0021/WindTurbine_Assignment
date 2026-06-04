# Reporting dashboard

The SQL queries in [`queries/`](queries) back a Databricks SQL / Lakeview
dashboard built on the **gold** (and quarantine) tables. They are kept as plain
`.sql` so they are version-controlled and reviewable independently of the
dashboard object.

| Query | Tile | Source table |
|-------|------|--------------|
| `01_fleet_daily_power.sql` | Fleet daily power trend (line) | `gold_daily_summary` |
| `02_power_by_turbine.sql` | Output per turbine (bar) | `gold_overall_summary` |
| `03_anomalies_by_turbine.sql` | Anomaly hotspots (bar) | `gold_anomaly_summary` |
| `04_recent_anomalies.sql` | Anomaly detail (table) | `gold_anomalies` |
| `05_quarantine_overview.sql` | Quarantine by failed rule (bar) | `silver_quarantine` |
