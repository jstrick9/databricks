# /data_quality/custom_checks/trm_reporting/silver/class_checks.py
# from data_quality.utils.dqx_compat import make_condition
from utils.dqx_compat import make_condition
import pyspark.sql.functions as F
from pyspark.sql.window import Window

def valid_international_class() -> F.Column:
    """Nice class must be 001–045, zero-padded 3 digits"""
    condition = ~(
        F.col("class").rlike("^[0-9]{3}$") &
        (F.col("class").cast("int") >= 1) &
        (F.col("class").cast("int") <= 45)
    )
    return make_condition(
        condition,
        "class must be valid 3-digit Nice international class (001-045)",
        "invalid_nice_class"
    )

def no_duplicate_classes_per_application() -> F.Column:
    """
    No duplicate class values within the same ser_num.

    Allows multiple different classes per ser_num (typical trademark behavior),
    but flags duplicates of the same class.
    """
    w = Window.partitionBy("ser_num", "class")
    dup_count = F.count("*").over(w)
    condition = dup_count > 1

    return make_condition(
        condition,
        "Duplicate international class numbers within same ser_num",
        "duplicate_class_per_application"
    )

def valid_gs_format() -> F.Column:
    col = F.lower(F.trim(F.col("goods_and_services_desc").cast("string")))

    invalid_patterns = [
        r"^goods and services$",
        r"^see specimen$",
        r"^n/a$",
        r"^unknown$",
        r"^tbd$",
        r"^international class",
        r"^class \d",
        r"^\s*$"
    ]
    regex_pattern = "|".join(invalid_patterns)

    condition = col.isNull() | col.rlike(regex_pattern) | (F.length(col) < 10)

    return make_condition(
        condition,
        "goods_and_services_desc must be proper legal description (not placeholder)",
        "invalid_or_placeholder_gs"
    )

def teas_plus_compliant_gs() -> F.Column:
    """For TEAS Plus filings, G&S must be from pre-approved ID Manual list"""
    # This would ideally join to dim_id_manual table
    # For now, flag suspiciously short or generic entries
    condition = F.length(F.trim(F.col("goods_and_services_desc"))) < 20
    return make_condition(
        condition,
        "TEAS Plus requires pre-approved ID Manual entry – this G&S is suspiciously short",
        "teas_plus_gs_too_short"
    )