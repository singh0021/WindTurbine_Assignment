"""Gold layer — business-ready aggregates and anomaly flags.

Built from ``silver_cleaned_readings``. Four tables back the reporting dashboard:

* ``gold_daily_summary``    — min/max/mean power per turbine per day
* ``gold_overall_summary``  — min/max/mean/std power per turbine, whole period
* ``gold_anomalies``        — individual readings >2 std-devs from the daily mean
* ``gold_anomaly_summary``  — anomaly counts and severity per turbine
"""

import os
import sys

import dlt

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_SRC = os.path.join(_REPO_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from wind_turbine.anomalies import detect_anomalies, summarise_anomalies  # noqa: E402
from wind_turbine.summary import (  # noqa: E402
    compute_daily_summary,
    compute_overall_summary,
)


@dlt.table(
    name="gold_daily_summary",
    comment="Min, max and mean power output per turbine per calendar day.",
    table_properties={"quality": "gold"},
)
def gold_daily_summary():
    return compute_daily_summary(dlt.read("silver_cleaned_readings"))


@dlt.table(
    name="gold_overall_summary",
    comment="Aggregate power statistics per turbine across the full reporting period.",
    table_properties={"quality": "gold"},
)
def gold_overall_summary():
    return compute_overall_summary(dlt.read("silver_cleaned_readings"))


@dlt.table(
    name="gold_anomalies",
    comment="Readings whose power output deviates >2 std-devs from the turbine's daily mean.",
    table_properties={"quality": "gold"},
)
def gold_anomalies():
    return detect_anomalies(dlt.read("silver_cleaned_readings"))


@dlt.table(
    name="gold_anomaly_summary",
    comment="Anomaly counts and severity per turbine.",
    table_properties={"quality": "gold"},
)
def gold_anomaly_summary():
    return summarise_anomalies(dlt.read("gold_anomalies"))
