from __future__ import annotations

import threading
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType
from utils.path_utils import load_yaml

# ================================================================
# Spark session helper (portable)
# ================================================================
_SPARK_LOCK = threading.Lock()
_SPARK = None

def _get_spark() -> SparkSession:
    """
    Get active Spark session when running in Databricks.
    Fallback to local master for non-Databricks execution contexts.
    """
    global _SPARK
    if _SPARK is not None:
        return _SPARK

    with _SPARK_LOCK:
        if _SPARK is not None:
            return _SPARK

        active = SparkSession.getActiveSession()
        if active is not None:
            _SPARK = active
            return _SPARK

        # Fallback for local / CI contexts (won't be used in Databricks)
        _SPARK = SparkSession.builder.master("local[*]").appName("dq_common_transforms").getOrCreate()
        return _SPARK


# ================================================================
# Allowed values (fail fast if missing)
# ================================================================
US_STATES = load_yaml("us_state_codes.yml")
COUNTRY_MAPPING = load_yaml("country_codes_canonical.yml")
OFFICIAL_NAMES = load_yaml("country_official_names.yml")
FPEP_CATEGORIES = load_yaml("fpep_categories.yml")
PH_ACTION_CODES = load_yaml("ph_action_codes.yml")

# ================================================================
# PRE-BUILT BROADCAST DATAFRAMES (thread-safe lazy singletons)
# ================================================================
_MAP_LOCK = threading.Lock()
_COUNTRY_MAP_DF = None
_COUNTRY_NAMES_DF = None
_FPEP_MAP_DF = None
_PH_ACTION_MAP_DF = None

def _get_country_map_df():
    global _COUNTRY_MAP_DF
    if _COUNTRY_MAP_DF is not None:
        return _COUNTRY_MAP_DF
    if not COUNTRY_MAPPING:
        return None
    with _MAP_LOCK:
        if _COUNTRY_MAP_DF is None:
            spark = _get_spark()
            data = [(k.strip().upper(), v) for k, v in COUNTRY_MAPPING.items()]
            _COUNTRY_MAP_DF = F.broadcast(
                spark.createDataFrame(data, ["__map_raw__", "__map_canonical__"])
            )
    return _COUNTRY_MAP_DF

def _get_country_names_df():
    global _COUNTRY_NAMES_DF
    if _COUNTRY_NAMES_DF is not None:
        return _COUNTRY_NAMES_DF
    if not OFFICIAL_NAMES:
        return None
    with _MAP_LOCK:
        if _COUNTRY_NAMES_DF is None:
            spark = _get_spark()
            data = [(k, v) for k, v in OFFICIAL_NAMES.items()]
            _COUNTRY_NAMES_DF = F.broadcast(
                spark.createDataFrame(data, ["__map_cd_key__", "__map_official_name__"])
            )
    return _COUNTRY_NAMES_DF

def _get_fpep_map_df():
    global _FPEP_MAP_DF
    if _FPEP_MAP_DF is not None:
        return _FPEP_MAP_DF
    if not FPEP_CATEGORIES:
        return None
    with _MAP_LOCK:
        if _FPEP_MAP_DF is None:
            spark = _get_spark()
            data = [(k.strip().upper(), v) for k, v in FPEP_CATEGORIES.items()]
            _FPEP_MAP_DF = F.broadcast(
                spark.createDataFrame(data, ["__map_raw__", "__map_canonical__"])
            )
    return _FPEP_MAP_DF

def _get_ph_action_map_df():
    global _PH_ACTION_MAP_DF
    if _PH_ACTION_MAP_DF is not None:
        return _PH_ACTION_MAP_DF
    if not PH_ACTION_CODES:
        return None
    with _MAP_LOCK:
        if _PH_ACTION_MAP_DF is None:
            spark = _get_spark()
            data = [(k.strip().upper(), v) for k, v in PH_ACTION_CODES.items()]
            _PH_ACTION_MAP_DF = F.broadcast(
                spark.createDataFrame(data, ["__map_raw__", "__map_canonical__"])
            )
    return _PH_ACTION_MAP_DF


# ===================================================================
# TEXT & BASIC STANDARDIZATION
# ===================================================================
def uppercase_and_trim(df: DataFrame, col_name: str) -> DataFrame:
    return df.withColumn(col_name, F.trim(F.upper(F.col(col_name))))

def clean_email(df: DataFrame, col_name: str) -> DataFrame:
    return df.withColumn(col_name, F.lower(F.trim(F.col(col_name))))


# ===================================================================
# SERIAL NUMBER (Safe — no truncation, preserves NULL)
# ===================================================================
def canonicalize_serial_number(df: DataFrame, col_name: str = "SER_NUM") -> DataFrame:
    return df.withColumn(
        col_name,
        F.when(F.col(col_name).isNull(), F.lit(None))
         .when(F.length(F.col(col_name).cast("string")) < 8,
               F.lpad(F.col(col_name).cast("string"), 8, "0"))
         .otherwise(F.col(col_name).cast("string"))
    )


# ===================================================================
# COUNTRY CODE + NAME CANONICALIZATION (Cached broadcast joins)
# ===================================================================
def apply_country_canonicalization(
    df: DataFrame,
    code_col: str = "ctry_cd",
    name_col: str | None = "ctry_nm"
) -> DataFrame:
    # Guard missing code column (prevents AnalysisException)
    if code_col not in df.columns:
        return df

    mapping_df = _get_country_map_df()
    if mapping_df is None:
        # mapping not available; do minimal cleanup only
        return df.withColumn(code_col, F.upper(F.trim(F.col(code_col))))

    df = (
        df.join(mapping_df, F.upper(F.trim(F.col(code_col))) == F.col("__map_raw__"), "left")
          .withColumn(code_col, F.coalesce(F.col("__map_canonical__"), F.upper(F.trim(F.col(code_col)))))
          .drop("__map_raw__", "__map_canonical__")
    )

    # Name canonicalization only if requested and name column exists
    if name_col is not None and name_col in df.columns:
        names_df = _get_country_names_df()
        if names_df is not None:
            df = (
                df.join(names_df, F.col(code_col) == F.col("__map_cd_key__"), "left")
                  .withColumn(name_col, F.coalesce(F.col("__map_official_name__"), F.upper(F.trim(F.col(name_col)))))
                  .drop("__map_cd_key__", "__map_official_name__")
            )
        else:
            df = df.withColumn(name_col, F.upper(F.trim(F.col(name_col))))

    return df


# ===================================================================
# STREET ADDRESS STANDARDIZATION (USPS PUB 28)
# ===================================================================
STREET_ABBREV_MAPPING = {
    "STREET": "ST", "STR": "ST", "AVENUE": "AVE", "AV": "AVE",
    "ROAD": "RD", "BOULEVARD": "BLVD", "CIRCLE": "CIR", "LANE": "LN",
    "DRIVE": "DR", "COURT": "CT", "PLACE": "PL", "SQUARE": "SQ",
    "PARKWAY": "PKWY", "HIGHWAY": "HWY", "FLOOR": "FL", "SUITE": "STE",
    "APARTMENT": "APT", "BUILDING": "BLDG", "DEPARTMENT": "DEPT", "ROOM": "RM"
}

def standardize_street_address(df: DataFrame, col_name: str) -> DataFrame:
    col = F.upper(F.trim(F.col(col_name)))
    for full, abbr in STREET_ABBREV_MAPPING.items():
        col = F.regexp_replace(col, f"\\b{full}\\b", abbr)
    col = F.regexp_replace(col, "\\s+", " ")
    col = F.regexp_replace(col, "^\\s+|\\s+$", "")
    return df.withColumn(col_name, col)


# ===================================================================
# EMPTY STRINGS → NULL (Vectorized)
# ===================================================================
def empty_string_to_null(df: DataFrame) -> DataFrame:
    """
    Convert empty strings or whitespace-only to null — all string columns in one pass.
    """
    return df.select([
        F.when(F.trim(F.col(field.name)) == "", F.lit(None)).otherwise(F.col(field.name)).alias(field.name)
        if isinstance(field.dataType, StringType)
        else F.col(field.name)
        for field in df.schema.fields
    ])


# ===================================================================
# DATE HELPERS
# ===================================================================
def safe_yyyymmdd_to_date(df: DataFrame, col_name: str) -> DataFrame:
    return df.withColumn(
        col_name,
        F.when(F.col(col_name).rlike("^[0-9]{8}$"), F.to_date(F.col(col_name), "yyyyMMdd"))
         .otherwise(F.lit(None))
    )

def canonicalize_dates(df: DataFrame, col_name: str) -> DataFrame:
    """
    Convert known formats to DateType. Includes a guarded epoch fallback.

    Epoch rules:
      - seconds:  100000000 .. 9999999999   (roughly 1973..2286)
      - millis:   100000000000 .. 9999999999999 (same range, ms)
    Anything outside these ranges becomes NULL (avoids 0 -> 1970-01-01).
    """
    col = F.col(col_name)
    col_str = col.cast("string")

    date_parsed = F.to_date(col_str, "yyyy-MM-dd")
    date_parsed = F.coalesce(date_parsed, F.to_date(col_str, "yyyyMMdd"))
    date_parsed = F.coalesce(date_parsed, F.to_date(col_str, "yyyy/MM/dd"))
    date_parsed = F.coalesce(date_parsed, F.to_date(col_str, "MM/dd/yyyy"))

    as_long = col.cast("long")

    # Guard epoch seconds
    epoch_seconds = F.when(
        (as_long >= F.lit(100000000)) & (as_long <= F.lit(9999999999)),
        as_long
    )

    # Guard epoch milliseconds
    epoch_millis = F.when(
        (as_long >= F.lit(100000000000)) & (as_long <= F.lit(9999999999999)),
        (as_long / F.lit(1000)).cast("long")
    )

    epoch_any = F.coalesce(epoch_seconds, epoch_millis)

    date_parsed = F.coalesce(
        date_parsed,
        F.when(epoch_any.isNotNull(), F.to_date(F.from_unixtime(epoch_any)))
    )

    return df.withColumn(col_name, date_parsed.cast(DateType()))


# ===================================================================
# FPEP CATEGORIES (Cached broadcast join)
# ===================================================================
def canonicalize_fpep_category(df: DataFrame, col_name: str = "CATEGORY") -> DataFrame:
    if col_name not in df.columns:
        return df

    mapping_df = _get_fpep_map_df()
    if mapping_df is None:
        return df.withColumn(col_name, F.upper(F.trim(F.col(col_name))))

    return (
        df.join(mapping_df, F.upper(F.trim(F.col(col_name))) == F.col("__map_raw__"), "left")
          .withColumn(col_name, F.coalesce(F.col("__map_canonical__"), F.upper(F.trim(F.col(col_name)))))
          .drop("__map_raw__", "__map_canonical__")
    )


# ===================================================================
# PH ACTION CODES (Cached broadcast join)
# ===================================================================
def canonicalize_ph_action_code(df: DataFrame, col_name: str = "ph_action_code") -> DataFrame:
    if col_name not in df.columns:
        return df

    mapping_df = _get_ph_action_map_df()
    if mapping_df is None:
        return df.withColumn(col_name, F.upper(F.trim(F.col(col_name))))

    return (
        df.join(mapping_df, F.upper(F.trim(F.col(col_name))) == F.col("__map_raw__"), "left")
          .withColumn(col_name, F.coalesce(F.col("__map_canonical__"), F.upper(F.trim(F.col(col_name)))))
          .drop("__map_raw__", "__map_canonical__")
    )