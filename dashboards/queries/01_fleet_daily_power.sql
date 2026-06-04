-- Fleet daily power trend.
SELECT
  date,
  ROUND(AVG(mean_power), 3) AS avg_mean_power,
  ROUND(MIN(min_power), 3)  AS fleet_min_power,
  ROUND(MAX(max_power), 3)  AS fleet_max_power,
  SUM(record_count)         AS total_readings
FROM hive_metastore.${schema}.gold_daily_summary
GROUP BY date
ORDER BY date;
