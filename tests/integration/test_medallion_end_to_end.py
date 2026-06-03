"""End-to-end integration test for the medallion flow.
"""

import datetime
from pathlib import Path

import numpy as np
import pytest
from pyspark.sql import functions as F
from wind_turbine.anomalies import detect_anomalies
from wind_turbine.cleaning import clean
from wind_turbine.quality import load_dq_rules, split_on_rules
from wind_turbine.summary import compute_daily_summary

REPO_ROOT = Path(__file__).resolve().parents[2]
DQ_RULES_PATH = REPO_ROOT / "resources" / "dq_rules.csv"


@pytest.fixture
def sample_batch(make_readings, base_ts):
    """Two turbines, 48 hourly readings each, with injected quality problems."""
    rng = np.random.default_rng(42)
    rows = []
    for tid in (1, 2):
        for h in range(48):
            rows.append(
                (
                    base_ts + datetime.timedelta(hours=h),
                    tid,
                    round(float(9 + 6 * rng.random()), 1),
                    float(rng.integers(0, 360)),
                    round(float(1.5 + 3 * rng.random()), 1),
                )
            )

    # Inject defects:
    rows[5] = (rows[5][0], rows[5][1], rows[5][2], rows[5][3], -999.0)  # bad power
    rows[10] = (rows[10][0], 99, rows[10][2], rows[10][3], rows[10][4])  # bad turbine
    rows[15] = (rows[15][0], rows[15][1], None, rows[15][3], None)  # gaps (valid)
    rows.append(rows[0])  # exact duplicate

    return make_readings(rows)


def test_quality_gate_quarantines_bad_rows(sample_batch):
    rules = load_dq_rules(str(DQ_RULES_PATH))

    valid, quarantined = split_on_rules(sample_batch, rules)

    # The negative power and the unknown turbine id must be quarantined.
    assert quarantined.count() == 2
    failed_rules = {
        r for row in quarantined.collect() for r in row["_failed_rules"]
    }
    assert "valid_power_output" in failed_rules
    assert "valid_turbine_id" in failed_rules

    # Rows with nulls (sensor gaps) are still valid — they get imputed later.
    assert valid.filter(F.col("wind_speed").isNull()).count() >= 1


def test_full_flow_produces_clean_gold_tables(sample_batch):
    rules = load_dq_rules(str(DQ_RULES_PATH))

    # silver
    valid, _ = split_on_rules(sample_batch, rules)
    cleaned = clean(valid)

    # No nulls and no duplicates survive cleaning.
    for col in ["wind_speed", "wind_direction", "power_output"]:
        assert cleaned.filter(F.col(col).isNull()).count() == 0
    assert cleaned.count() == cleaned.dropDuplicates(["turbine_id", "timestamp"]).count()

    # gold: daily summary invariants hold and cover both turbines x two days.
    daily = compute_daily_summary(cleaned)
    assert daily.count() == 4  # 2 turbines x 2 calendar days
    assert (
        daily.filter(
            (F.col("min_power") > F.col("mean_power"))
            | (F.col("mean_power") > F.col("max_power"))
        ).count()
        == 0
    )

    # gold: anomaly detection runs and stays within the cleaned id space.
    anomalies = detect_anomalies(cleaned)
    assert anomalies.filter(~F.col("turbine_id").isin([1, 2])).count() == 0
