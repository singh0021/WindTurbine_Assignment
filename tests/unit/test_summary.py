"""Unit tests for wind_turbine.summary."""

import datetime

from pyspark.sql import functions as F
from wind_turbine.summary import compute_daily_summary, compute_overall_summary


def test_daily_summary_min_le_mean_le_max(make_readings, base_ts):
    rows = [
        (base_ts + datetime.timedelta(hours=h), 1, 10.0, 180.0, float(h % 5))
        for h in range(24)
    ]
    df = make_readings(rows)

    summary = compute_daily_summary(df)
    violations = summary.filter(
        (F.col("min_power") > F.col("mean_power"))
        | (F.col("mean_power") > F.col("max_power"))
    ).count()

    assert violations == 0
    assert summary.count() == 1  # one turbine, one day


def test_daily_summary_record_count(make_readings, base_ts):
    rows = [
        (base_ts + datetime.timedelta(hours=h), 1, 10.0, 180.0, 3.0)
        for h in range(24)
    ]
    df = make_readings(rows)

    row = compute_daily_summary(df).first()
    assert row["record_count"] == 24


def test_overall_summary_one_row_per_turbine(make_readings, base_ts):
    rows = [
        (base_ts + datetime.timedelta(hours=h), tid, 10.0, 180.0, 3.0 + h)
        for tid in (1, 2, 3)
        for h in range(5)
    ]
    df = make_readings(rows)

    summary = compute_overall_summary(df)

    assert summary.count() == 3
    assert {r["turbine_id"] for r in summary.collect()} == {1, 2, 3}
