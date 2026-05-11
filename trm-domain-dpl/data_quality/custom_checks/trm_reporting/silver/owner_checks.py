# /data_quality/custom_checks/trm_reporting/silver/owner_checks.py
# from data_quality.utils.dqx_compat import make_condition
from utils.dqx_compat import make_condition
import pyspark.sql.functions as F
from pyspark.sql.window import Window


def exactly_one_current_owner_per_case() -> F.Column:
    """
    Ensure exactly one owner has owner_num = 1 per ser_num.
    This defines the current legal owner for each application.
    """
    w = Window.partitionBy("ser_num")
    current_owner_count = F.sum(
        F.when(F.col("owner_num") == 1, F.lit(1)).otherwise(F.lit(0))
    ).over(w)

    condition = current_owner_count != 1

    return make_condition(
        condition,
        "Exactly one owner must have owner_num = 1 per ser_num",
        "missing_or_duplicate_current_owner"
    )


def current_owner_name_matches_legacy() -> F.Column:
    """
    When owner_num = 1, current_owner must match name (case-insensitive, trimmed).
    This keeps the legacy current_owner field in sync with the canonical name.
    """
    condition = (
        (F.col("owner_num") == 1) &
        (
            F.trim(F.upper(F.col("current_owner"))) !=
            F.trim(F.upper(F.col("name")))
        )
    )

    return make_condition(
        condition,
        "current_owner must match name when owner_num = 1",
        "current_owner_name_mismatch"
    )