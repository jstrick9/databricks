# /data_quality/transforms/trm_reporting/silver/prosecution_history_canonical.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transforms.common_transforms import (
    uppercase_and_trim,
    canonicalize_serial_number,
    canonicalize_ph_action_code,
    canonicalize_dates,
    empty_string_to_null
)

def canonicalize_prosecution_history(df: DataFrame) -> DataFrame:
    """
    Canonicalization for prosecution_history.
    Format-only standardization (Medallion-aligned):
      - serial_number: 8-digit padding (no truncation)
      - ph_action_code: canonical mapping
      - date columns: normalized to DateType where possible
      - ph_action_number: cast to int for numeric ordering in window checks
      - tm_worker_eid: uppercase/trim if present
      - empty strings -> null across all string columns
    """

    # Serial number (identifier)
    if "serial_number" in df.columns:
        df = canonicalize_serial_number(df, "serial_number")

    # Action code canonicalization
    if "ph_action_code" in df.columns:
        df = canonicalize_ph_action_code(df, "ph_action_code")

    # Numeric ordering for window checks
    if "ph_action_number" in df.columns:
        df = df.withColumn("ph_action_number", F.col("ph_action_number").cast("int"))

    # Date normalization
    date_cols = ["cm_sys_dt", "ph_action_date", "ri_notif_dt", "last_modified_date"]
    for col in date_cols:
        if col in df.columns:
            df = canonicalize_dates(df, col)

    # Worker ID formatting
    if "tm_worker_eid" in df.columns:
        df = uppercase_and_trim(df, "tm_worker_eid")

    # Final cleanup
    df = empty_string_to_null(df)
    return df