"""Unit tests for wind_turbine.quality (DQ rule loading + quarantine split)."""


import pytest
from pyspark.sql import functions as F
from wind_turbine.quality import (
    FAILED_RULES_COL,
    DQRule,
    load_dq_rules,
    split_on_rules,
)

RULES_CSV = """name,constraint,description
valid_turbine_id,turbine_id BETWEEN 1 AND 15,known turbine
valid_power_output,power_output IS NULL OR power_output BETWEEN 0 AND 15,plausible power
# a comment row that should be skipped,,
,,blank name skipped
"""


def test_load_dq_rules_parses_and_skips_noise(tmp_path):
    path = tmp_path / "rules.csv"
    path.write_text(RULES_CSV, encoding="utf-8")

    rules = load_dq_rules(str(path))

    assert [r.name for r in rules] == ["valid_turbine_id", "valid_power_output"]
    assert rules[0].description == "known turbine"


def test_load_dq_rules_raises_when_empty(tmp_path):
    path = tmp_path / "empty.csv"
    path.write_text("name,constraint,description\n", encoding="utf-8")

    with pytest.raises(ValueError):
        load_dq_rules(str(path))


def test_split_on_rules_routes_valid_and_quarantine(make_readings, base_ts):
    rules = [
        DQRule("valid_power_output", "power_output BETWEEN 0 AND 15"),
        DQRule("valid_turbine_id", "turbine_id BETWEEN 1 AND 15"),
    ]
    rows = [
        (base_ts, 1, 10.0, 180.0, 3.0),  # valid
        (base_ts, 2, 10.0, 180.0, -5.0),  # bad power -> quarantine
        (base_ts, 99, 10.0, 180.0, 3.0),  # bad turbine -> quarantine
    ]
    df = make_readings(rows)

    valid, quarantined = split_on_rules(df, rules)

    assert valid.count() == 1
    assert quarantined.count() == 2

    # The quarantined rows must record which rule they violated.
    bad_power = quarantined.filter(F.col("turbine_id") == 2).first()
    assert bad_power[FAILED_RULES_COL] == ["valid_power_output"]


def test_split_on_rules_flags_multiple_failures(make_readings, base_ts):
    rules = [
        DQRule("valid_power_output", "power_output BETWEEN 0 AND 15"),
        DQRule("valid_turbine_id", "turbine_id BETWEEN 1 AND 15"),
    ]
    df = make_readings([(base_ts, 99, 10.0, 180.0, -5.0)])

    _, quarantined = split_on_rules(df, rules)
    failed = quarantined.first()[FAILED_RULES_COL]

    assert set(failed) == {"valid_power_output", "valid_turbine_id"}
