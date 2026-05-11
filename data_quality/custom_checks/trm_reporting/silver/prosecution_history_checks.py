# /data_quality/custom_checks/trm_reporting/silver/prosecution_history_checks.py
from utils.dqx_compat import make_condition
import pyspark.sql.functions as F
from pyspark.sql.window import Window


def ph_action_sequence_is_valid() -> F.Column:
    """
    ph_action_number must be contiguous per serial_number:
      - no gaps between min and max
      - no duplicates
      - no null/non-numeric values

    This version avoids orderBy/lag windows (no per-key sorting), which is the main
    expensive part and usually removes the need for explicit repartition tuning.
    """
    num = F.col("ph_action_number").cast("int")
    w = Window.partitionBy("serial_number")

    # All are group-level metrics (same value for every row in the serial_number group)
    min_n = F.min(num).over(w)
    max_n = F.max(num).over(w)
    cnt   = F.count(num).over(w)               # non-null numeric count
    cd    = F.countDistinct(num).over(w)       # distinct numeric count

    # Contiguity rule:
    # - duplicates -> cd < cnt
    # - gaps -> cd < (max-min+1)
    # - null/non-numeric -> num is null (fail)
    expected_span = (max_n - min_n + F.lit(1))

    condition = (
        num.isNull() |
        (cd != cnt) |
        (cd != expected_span)
    )

    return make_condition(
        condition,
        "ph_action_number must be contiguous per serial_number (no gaps, no duplicates)",
        "ph_action_sequence_not_contiguous"
    )


def ph_dates_are_chronological() -> F.Column:
    """
    cm_sys_dt must be non-decreasing per serial_number (never goes backwards).

    Notes:
    - Casts to date to avoid string comparison pitfalls.
    - Null cm_sys_dt fails for any row after the first.
    """
    cm_date = F.to_date(F.col("cm_sys_dt"))
    w = Window.partitionBy("serial_number").orderBy(cm_date.asc_nulls_last(), F.col("ph_action_number").cast("int").asc_nulls_last())
    prev_date = F.lag(cm_date).over(w)

    condition = (
        prev_date.isNotNull() &
        (cm_date.isNull() | (cm_date < prev_date))
    )

    return make_condition(
        condition,
        "cm_sys_dt must be chronological (non-decreasing) per serial_number",
        "ph_date_regression"
    )


def notification_within_mailing_standard() -> F.Column:
    """
    For key Office Actions, ri_notif_dt must be within 0..3 days of cm_sys_dt.
    Non-office-action rows are always OK.

    Fixes:
    - If either date is NULL for an office action, FAIL.
    - Negative diffs (notif before event) FAIL.
    """
    office_action_codes = ["CTNF", "CTFR", "CTRE", "EXPT", "PREX"]

    cm_date = F.to_date(F.col("cm_sys_dt"))
    notif_date = F.to_date(F.col("ri_notif_dt"))
    diff_days = F.datediff(notif_date, cm_date)

    is_office_action = F.col("ph_action_code").isin(office_action_codes)

    # Fail office actions when:
    # - either date missing
    # - diff < 0 (notification before event)
    # - diff > 3
    condition = (
        is_office_action &
        (
            cm_date.isNull() |
            notif_date.isNull() |
            diff_days.isNull() |
            (diff_days < 0) |
            (diff_days > 3)
        )
    )

    return make_condition(
        condition,
        "Office Action notification must be within 0–3 days of cm_sys_dt",
        "notification_delay_violation"
    )