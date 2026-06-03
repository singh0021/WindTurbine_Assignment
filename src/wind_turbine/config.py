"""Tunable pipeline parameters.

Data-quality *range* checks live in ``resources/dq_rules.csv`` (so they can be
edited without touching code). The constants here are the statistical thresholds
and windowing parameters that drive cleaning, summarisation and anomaly
detection.
"""

# Winsorisation threshold for outlier capping during cleaning: power readings are
# clamped to +/- this many standard deviations of each turbine's own mean.
OUTLIER_STD_THRESHOLD = 3.0

# A reading is flagged anomalous if its power output is more than this many
# standard deviations from its turbine's *daily* mean.
ANOMALY_STD_THRESHOLD = 2.0

# Expected readings per turbine per day. Daily summaries with fewer rows than
# this are flagged as incomplete (likely sensor gaps).
EXPECTED_READINGS_PER_DAY = 24
