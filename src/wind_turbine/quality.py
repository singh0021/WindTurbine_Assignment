"""Data-quality gate driven by externally-defined rules.


Rows that fail one or more rules are routed to a *quarantine* table, tagged with
the names of the rules they violated, so they can be examined later instead of
being silently dropped.
"""

import csv
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

FAILED_RULES_COL = "_failed_rules"
QUARANTINED_AT_COL = "_quarantined_at"


@dataclass(frozen=True)
class DQRule:
    """A single data-quality rule loaded from the rules CSV."""

    name: str
    constraint: str
    description: str = ""


def load_dq_rules(path: str) -> list[DQRule]:
    """Read data-quality rules from a CSV file.
    """
    rules: list[DQRule] = []
    with open(path, newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            name = (row.get("name") or "").strip()
            constraint = (row.get("constraint") or "").strip()
            if not name or name.startswith("#") or not constraint:
                continue
            rules.append(
                DQRule(
                    name=name,
                    constraint=constraint,
                    description=(row.get("description") or "").strip(),
                )
            )
    if not rules:
        raise ValueError(f"No valid data-quality rules found in {path!r}")
    return rules


def _failed_rules_column(rules: list[DQRule]) -> Column:
    """Build an array column listing the names of every rule a row violates."""
    flags = [
        F.when(~F.expr(rule.constraint), F.lit(rule.name)) for rule in rules
    ]

    return F.array_compact(F.array(*flags))


def split_on_rules(
    df: DataFrame, rules: list[DQRule]
) -> tuple[DataFrame, DataFrame]:
    """Partition ``df`` into (valid, quarantined) DataFrames against ``rules``.

    Valid rows satisfy every rule. Quarantined rows fail at least one and carry a
    ``_failed_rules`` array plus a ``_quarantined_at`` timestamp for triage.
    """
    tagged = df.withColumn(FAILED_RULES_COL, _failed_rules_column(rules))

    valid = tagged.filter(F.size(FAILED_RULES_COL) == 0).drop(FAILED_RULES_COL)

    quarantined = tagged.filter(F.size(FAILED_RULES_COL) > 0).withColumn(
        QUARANTINED_AT_COL, F.current_timestamp()
    )

    return valid, quarantined
