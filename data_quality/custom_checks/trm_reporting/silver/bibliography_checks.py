# /data_quality/custom_checks/trm_reporting/silver/bibliography_checks.py
# from data_quality.utils.dqx_compat import make_condition
from utils.dqx_compat import make_condition
import pyspark.sql.functions as F


def registration_number_format_when_present() -> F.Column:
    """
    If REGISTRATION_NUMBER is present, it must be a 7-8 digit number.
    Does not enforce that it must be present (use is_not_null in YAML for that).
    """
    cond_ok = (
        F.col("REGISTRATION_NUMBER").isNull() |
        F.col("REGISTRATION_NUMBER").rlike(r"^[0-9]{7,8}$")
    )
    condition = ~cond_ok

    return make_condition(
        condition,
        "REGISTRATION_NUMBER must be 7-8 digits when present",
        "REGISTRATION_NUMBER_invalid_format"
    )


def paper_flag_consistent_with_filing_method() -> F.Column:
    """
    FLG_PAPER_FIL = 1 must imply FILING_METHOD_FILED = 'PAPER'.
    If FLG_PAPER_FIL = 0, no constraint on method.
    """
    cond_ok = F.when(
        F.col("FLG_PAPER_FIL") == 1,
        F.upper(F.trim(F.col("FILING_METHOD_FILED"))) == F.lit("PAPER")
    ).otherwise(F.lit(True))

    condition = ~cond_ok

    return make_condition(
        condition,
        "When FLG_PAPER_FIL = 1, FILING_METHOD_FILED must be 'PAPER'",
        "paper_flag_inconsistent_with_method"
    )


def teas_plus_flag_consistent_with_method() -> F.Column:
    """
    AM_FLG_TEASPL_FIL = 1 must imply an electronic TEAS Plus/TEAS filing method.
    We allow several common variants and normalize to uppercase for comparison.
    """
    filing_method_upper = F.upper(F.trim(F.col("FILING_METHOD_FILED")))

    cond_ok = F.when(
        F.col("AM_FLG_TEASPL_FIL") == 1,
        filing_method_upper.isin("TEAS PLUS", "TEAS", "TEAS STANDARD")
    ).otherwise(F.lit(True))

    condition = ~cond_ok

    return make_condition(
        condition,
        "When AM_FLG_TEASPL_FIL = 1, FILING_METHOD_FILED must be an electronic TEAS/TEAS PLUS method",
        "teas_plus_flag_inconsistent_with_method"
    )