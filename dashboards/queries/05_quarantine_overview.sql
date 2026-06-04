-- Data-quality quarantine overview.
SELECT
  failed_rule,
  COUNT(*) AS rows_quarantined
FROM hive_metastore.${schema}.silver_quarantine
LATERAL VIEW explode(_failed_rules) AS failed_rule
GROUP BY failed_rule
ORDER BY rows_quarantined DESC;
