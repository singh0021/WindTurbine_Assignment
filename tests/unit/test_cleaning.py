"""Unit tests for wind_turbine.cleaning."""

import datetime

from pyspark.sql import functions as F
from wind_turbine.cleaning import (
    cap_outliers,
    clean,
    impute_missing,
    remove_duplicates,
)


def test_remove_duplicates_drops_same_turbine_timestamp(make_readings, base_ts):
    row = (base_ts, 1, 10.0, 180.0, 3.0)
    df = make_readings([row, row, (base_ts, 2, 9.0, 90.0, 2.5)])

    result = remove_duplicates(df)

    assert result.count() == 2


def test_impute_missing_forward_then_backward_fill(make_readings, base_ts):
    rows = [
        (base_ts, 1, None, 180.0, None),  # leading nulls -> backward filled
        (base_ts + datetime.timedelta(hours=1), 1, 10.0, 190.0, 2.0),
        (base_ts + datetime.timedelta(hours=2), 1, None, 200.0, None),  # forward filled
    ]
    df = make_readings(rows)

    result = impute_missing(df).orderBy("timestamp")
    values = [(r["wind_speed"], r["power_output"]) for r in result.collect()]

    # Row 0 backward-filled from row 1; row 2 forward-filled from row 1.
    assert values == [(10.0, 2.0), (10.0, 2.0), (10.0, 2.0)]


def test_cap_outliers_clamps_extreme_power(make_readings, base_ts):
    # 23 readings at ~3.0 MW and one massive spike; the spike must be clamped.
    rows = [
        (base_ts + datetime.timedelta(hours=h), 1, 10.0, 180.0, 3.0)
        for h in range(23)
    ]
    rows.append((base_ts + datetime.timedelta(hours=23), 1, 10.0, 180.0, 50.0))
    df = make_readings(rows)

    result = cap_outliers(df, n_std=3.0)
    max_power = result.agg(F.max("power_output")).first()[0]

    assert max_power < 50.0


def test_clean_leaves_no_nulls(make_readings, base_ts):
    rows = [
        (base_ts, 1, 10.0, 180.0, 2.0),
        (base_ts + datetime.timedelta(hours=1), 1, None, None, None),
        (base_ts + datetime.timedelta(hours=2), 1, 12.0, 200.0, 4.0),
    ]
    df = make_readings(rows)

    result = clean(df)

    for col in ["wind_speed", "wind_direction", "power_output"]:
        assert result.filter(F.col(col).isNull()).count() == 0, f"nulls left in {col}"
