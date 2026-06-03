-- Recent anomalous readings (detail table).
-- Visual: table, sortable by z_score. Operational drill-down for investigation.
SELECT
  timestamp,
  turbine_id,
  power_output,
  daily_mean,
  deviation_mw,
  z_score
FROM hive_metastore.${schema}.gold_anomalies
ORDER BY ABS(z_score) DESC
LIMIT 200;
