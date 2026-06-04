-- Per-turbine output overview.
SELECT
  turbine_id,
  min_power,
  max_power,
  mean_power,
  std_power,
  total_records
FROM hive_metastore.${schema}.gold_overall_summary
ORDER BY turbine_id;
