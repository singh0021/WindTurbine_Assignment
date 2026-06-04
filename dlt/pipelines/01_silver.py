"""Silver layer — data-quality gate + cleaning.

Two tables are published:

* ``silver_quarantine``        — rows that failed one or more data-quality rules,
                                 tagged with the rules they violated, kept for triage.
* ``silver_cleaned_readings``  — rows that passed every rule, then de-duplicated,
                                 gap-filled and outlier-capped.

"""

import os
import sys

import dlt

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_SRC = os.path.join(_REPO_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from wind_turbine.cleaning import clean  
from wind_turbine.quality import load_dq_rules, split_on_rules  

DQ_RULES = load_dq_rules(spark.conf.get("dq_rules_path")) 


@dlt.table(
    name="silver_quarantine",
    comment="Readings that failed data-quality rules, tagged with the failed rule names.",
    table_properties={"quality": "silver"},
)
def silver_quarantine():
    
    _, quarantined = split_on_rules(dlt.read("bronze_readings"), DQ_RULES)
    return quarantined


@dlt.table(
    name="silver_cleaned_readings",
    comment="Validated readings after de-duplication, imputation and outlier capping.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("power_output_present", "power_output IS NOT NULL")
def silver_cleaned_readings():
    valid, _ = split_on_rules(dlt.read("bronze_readings"), DQ_RULES)
    return clean(valid)
