# /data_quality/transforms/trm_reporting/silver/bibliography_canonical.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
# from data_quality.transforms.common_transforms import (
#     canonicalize_serial_number,
#     uppercase_and_trim,
#     apply_country_canonicalization,
#     canonicalize_dates
# )
from transforms.common_transforms import (
    canonicalize_serial_number,
    uppercase_and_trim,
    apply_country_canonicalization,
    canonicalize_dates
)

def canonicalize_bibliography(df: DataFrame) -> DataFrame:
    """
    Canonicalizes bibliography table using centralized common transforms.
    This is the gold standard — used by PayPal, Roche, etc.
    """
    
    # 1. Serial Number — sacred 8-digit
    df = canonicalize_serial_number(df, "SER_NUM")
    
    # 2. Mark names — TESS-searchable format
    df = df.withColumn(
        "MARK_NM_SHORT",
        F.trim(F.upper(F.regexp_replace(F.col("MARK_NM_SHORT"), "[^A-Z0-9]", "")))
    )
    df = uppercase_and_trim(df, "MARK_NM")
    
    # 3. Filing bases — canonical 
    df = uppercase_and_trim(df, "FILING_BASIS_CUR")
    df = uppercase_and_trim(df, "FILING_BASIS_FIL")
    df = uppercase_and_trim(df, "FILING_METHOD_FILED")
    
    # 4. Law office — canonical
    df = uppercase_and_trim(df, "LAW_OFFICE")
    
    # 5. Country fields (if exist in bibliography)
    if "ctry_cd" in df.columns:
        df = apply_country_canonicalization(df, "ctry_cd", "ctry_nm")
    
    # 6. Email fields — lowercase
    if "create_user_id" in df.columns:
        df = df.withColumn("create_user_id", F.lower(F.trim(F.col("create_user_id"))))
    if "update_user_id" in df.columns:
        df = df.withColumn("update_user_id", F.lower(F.trim(F.col("update_user_id"))))

    # 7. All date fields — perfectly canonical
    date_cols = ["STATUS_DT", "LAST_MODIFIED_DATE", "create_ts", "update_ts"]
    for col in date_cols:
        if col in df.columns:
            df = canonicalize_dates(df, col)
    
    return df