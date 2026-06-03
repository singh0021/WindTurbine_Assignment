-- Data-quality quarantine overview.
-- Visual: bar chart, X = failed_rule, Y = rows_quarantined.
-- Operational health check on the inbound feed — which rules trip most often.
SELECT
  failed_rule,
  COUNT(*) AS rows_quarantined
FROM hive_metastore.${schema}.silver_quarantine
LATERAL VIEW explode(_failed_rules) AS failed_rule
GROUP BY failed_rule
ORDER BY rows_quarantined DESC;
