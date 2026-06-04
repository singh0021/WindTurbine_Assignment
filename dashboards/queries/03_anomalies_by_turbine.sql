-- Anomaly hotspots.
SELECT
  turbine_id,
  anomaly_count,
  avg_deviation_mw,
  max_abs_z_score
FROM hive_metastore.${schema}.gold_anomaly_summary
ORDER BY anomaly_count DESC, max_abs_z_score DESC;
