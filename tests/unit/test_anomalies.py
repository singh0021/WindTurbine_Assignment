"""Unit tests for wind_turbine.anomalies."""

import datetime

from wind_turbine.anomalies import detect_anomalies, summarise_anomalies


def test_spike_is_flagged(make_readings, base_ts):
    # 23 steady readings and one clear spike on the same day.
    rows = [
        (base_ts + datetime.timedelta(hours=h), 1, 10.0, 180.0, 3.0 if h < 23 else 12.0)
        for h in range(24)
    ]
    df = make_readings(rows)

    anomalies = detect_anomalies(df, threshold=2.0)

    assert anomalies.count() >= 1
    assert anomalies.first()["turbine_id"] == 1


def test_uniform_data_has_no_anomalies(make_readings, base_ts):
    rows = [
        (base_ts + datetime.timedelta(hours=h), 1, 10.0, 180.0, 3.0)
        for h in range(24)
    ]
    df = make_readings(rows)

    # std-dev is 0 -> z-score forced to 0 -> nothing flagged.
    assert detect_anomalies(df, threshold=2.0).count() == 0


def test_summarise_anomalies_aggregates_per_turbine(make_readings, base_ts):
    rows = [
        (base_ts + datetime.timedelta(hours=h), 1, 10.0, 180.0, 3.0 if h < 23 else 12.0)
        for h in range(24)
    ]
    df = make_readings(rows)

    anomalies = detect_anomalies(df, threshold=2.0)
    summary = summarise_anomalies(anomalies)

    row = summary.first()
    assert row["turbine_id"] == 1
    assert row["anomaly_count"] >= 1
    assert row["max_abs_z_score"] > 2.0
