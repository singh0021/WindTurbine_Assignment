-- Per-turbine output overview.
-- Visual: bar chart, X = turbine_id, Y = mean_power (with min/max as range).
-- Spot under- or over-performing turbines across the whole period.
SELECT
  turbine_id,
  min_power,
  max_power,
  mean_power,
  std_power,
  total_records
FROM hive_metastore.${schema}.gold_overall_summary
ORDER BY turbine_id;
