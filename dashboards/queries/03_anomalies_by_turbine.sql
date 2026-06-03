-- Anomaly hotspots.
-- Visual: bar chart, X = turbine_id, Y = anomaly_count, colour = max_abs_z_score.
-- Highlights turbines deviating most often / most severely from their daily norm.
SELECT
  turbine_id,
  anomaly_count,
  avg_deviation_mw,
  max_abs_z_score
FROM hive_metastore.${schema}.gold_anomaly_summary
ORDER BY anomaly_count DESC, max_abs_z_score DESC;
