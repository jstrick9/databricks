# /data_quality/custom_checks/common_checks.py
# from data_quality.utils.dqx_compat import make_condition
from utils.dqx_compat import make_condition
import pyspark.sql.functions as F
from pyspark.sql.window import Window
# from data_quality.utils.path_utils import load_yaml
from utils.path_utils import load_yaml

# Load allowed values once
PH_ACTION_CODES = load_yaml("ph_action_codes.yml")
US_STATES = load_yaml("us_state_codes.yml")                  
COUNTRY_MAPPING = load_yaml("country_codes_canonical.yml")   # dict: raw -> canonical
REGEX_PATTERNS = load_yaml("regex.yml")                      # regex pattern catalog


def is_not_null(col_name: str):
    """
    Column must not be NULL.
    """
    condition = F.col(col_name).isNull()
    return make_condition(
        condition,
        f"{col_name} must not be null",
        f"{col_name}_null"
    )


def is_unique(col_name: str):
    """
    Column must be unique within the dataset.
    """
    w = Window.partitionBy(col_name)
    dup_count = F.count("*").over(w)
    condition = dup_count > 1
    return make_condition(
        condition,
        f"{col_name} must be unique",
        f"{col_name}_not_unique"
    )


def regex_match(col_name: str, regex: str | None = None, regex_name: str | None = None):
    """
    Column must match either:
      - a named pattern from regex.yml (regex_name), or
      - a raw regex string (regex).

    Examples in checks YAML:

      - criticality: error
        check:
          function: regex_match
          arguments:
            col_name: SER_NUM
            regex_name: SERIAL_8_DIGITS

      - criticality: error
        check:
          function: regex_match
          arguments:
            col_name: SER_NUM
            regex: "^[0-9]{8}$"
    """
    pattern = None

    # Prefer named pattern if provided
    if regex_name:
        pattern = REGEX_PATTERNS.get(regex_name)
        if pattern is None:
            condition = F.lit(True)
            return make_condition(
                condition,
                f"{col_name} regex_match misconfigured: unknown regex_name '{regex_name}'",
                f"{col_name}_regex_unknown_pattern"
            )

    # Fall back to direct regex argument
    if pattern is None:
        pattern = regex

    # If still no pattern, treat as configuration error: fail all rows
    if not pattern:
        condition = F.lit(True)
        return make_condition(
            condition,
            f"{col_name} regex_match misconfigured: no regex or regex_name provided",
            f"{col_name}_regex_missing_pattern"
        )

    # Normal operation: check value against pattern
    condition = ~F.col(col_name).rlike(pattern)
    return make_condition(
        condition,
        f"{col_name} must match regex '{pattern}'",
        f"{col_name}_regex_fail"
    )


def values_in_0_or_1(col_name: str):
    """
    Column values must be 0 or 1.
    """
    condition = ~F.col(col_name).cast("int").isin(0, 1)
    return make_condition(
        condition,
        f"{col_name} must be 0 or 1",
        f"{col_name}_invalid_flag"
    )


def all_caps(col_name: str):
    """
    Column must be ALL CAPS.
    """
    condition = F.col(col_name) != F.upper(F.col(col_name))
    return make_condition(
        condition,
        f"{col_name} must be ALL CAPS",
        f"{col_name}_not_caps"
    )


def valid_ph_action_code(col_name: str = "ph_action_code"):
    """
    Column must be a valid prosecution action code from ph_action_codes.yml.
    """
    valid_codes = list(PH_ACTION_CODES.keys())
    condition = ~F.col(col_name).isin(valid_codes)
    return make_condition(
        condition,
        f"{col_name} invalid prosecution code",
        "invalid_ph_code"
    )


def valid_iso_country_code(col_name: str):
    """
    Column must be a valid 2-character ISO country code (canonical form).
    Uses country_codes_canonical.yml values as the allowed set.
    """
    # COUNTRY_MAPPING is from country_codes_canonical.yml
    raw_values = set(COUNTRY_MAPPING.values())
    
    # Coerce everything to uppercase STRING and drop Nones
    valid_codes = [str(v).upper() for v in raw_values if v is not None]

    # Use *valid_codes so PySpark sees them as separate STRING arguments
    condition_ok = F.col(col_name).isin(*valid_codes) | F.col(col_name).isNull()
    
    return make_condition(
        ~condition_ok,
        f"{col_name} must be valid 2-character ISO country code",
        f"{col_name}_invalid_country"
    )


def valid_email_format(col_name: str):
    """
    Column must be a valid email format or NULL.
    """
    email_regex = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    condition_ok = F.col(col_name).rlike(email_regex) | F.col(col_name).isNull()
    return make_condition(
        ~condition_ok,
        f"{col_name} must be valid email format",
        f"{col_name}_invalid_email"
    )


def fiscal_year_matches_date(fiscal_year_col: str, date_col: str):
    """
    Generic fiscal year vs date check.
    Ensures that fiscal_year_col equals the USPTO fiscal year derived from date_col.
    Fiscal year: Oct 1 - Sep 30 (e.g., 2024-10-01 → FY2025).
    """
    fy_from_date = F.when(
        F.month(F.col(date_col)) >= 10,
        F.year(F.col(date_col)) + 1
    ).otherwise(F.year(F.col(date_col)))

    condition = F.col(fiscal_year_col).cast("int") != fy_from_date

    return make_condition(
        condition,
        f"{fiscal_year_col} must match the fiscal year of {date_col} (Oct–Sep fiscal year)",
        f"{fiscal_year_col}_{date_col}_fiscal_year_mismatch"
    )


def is_unique_and_immutable(col_name: str):
    """
    Generic uniqueness check for IDs that must never duplicate.
    True immutability across runs should be enforced with Delta constraints / lineage,
    but this checks within the current dataset.
    """
    w = Window.partitionBy(col_name)
    dup_count = F.count("*").over(w)
    condition = dup_count > 1

    return make_condition(
        condition,
        f"{col_name} must be globally unique and immutable",
        f"{col_name}_not_unique_immutable"
    )


def created_before_last_modified(create_col: str = "create_ts", modified_col: str = "last_mod_ts"):
    """
    Generic audit sanity check:
    <create_col> must be less than or equal to <modified_col>.
    """
    condition = F.col(create_col) > F.col(modified_col)
    return make_condition(
        condition,
        f"{create_col} must be less than or equal to {modified_col}",
        f"{create_col}_{modified_col}_invalid_order"
    )