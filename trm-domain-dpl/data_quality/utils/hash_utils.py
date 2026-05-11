# /data_quality/utils/hash_utils.py
from pyspark.sql.functions import (
    sha2, concat_ws, col, when, lit, trim, current_timestamp, current_date
)
from typing import List, Optional

def add_hashes(
    df,
    natural_key_columns: List[str],
    use_all_columns_for_data_hash: bool = False,
    deterministic_columns_for_data_hash: Optional[List[str]] = None
):
    # FIX: Snapshot columns BEFORE any metadata is added
    original_columns = df.columns
    
    # FIX: Explicitly exclude any system/metadata columns from data hash scope
    SYSTEM_COLS = {"_natural_key_hash", "_record_data_hash", "_created_date",
                   "_created_timestamp", "_updated_timestamp", "_is_record_active",
                   "_dq_run_id", "_dq_run_timestamp", "_errors", "_warnings"}
    
    # ========================= NATURAL KEY HASH =========================
    if not natural_key_columns:
        raise ValueError("natural_key_columns must be a non-empty list")
    
    nk_existing = [c for c in natural_key_columns if c in original_columns]
    if not nk_existing:
        raise ValueError("None of the specified natural_key_columns exist in the DataFrame")
    
    nk_exprs = [
        when(col(c).isNull(), lit("NULL"))
        .otherwise(trim(col(c).cast("string")))
        for c in sorted(nk_existing)
    ]
    
    nk_concat = concat_ws("||", *nk_exprs)
    df = df.withColumn("_natural_key_hash", sha2(nk_concat, 256))
    
    # ========================= RECORD DATA HASH =========================
    if use_all_columns_for_data_hash:
        # FIX: Filter out system/metadata columns that would make hashes non-deterministic
        data_cols_to_use = [c for c in original_columns if c not in SYSTEM_COLS]
    else:
        if not deterministic_columns_for_data_hash:
            raise ValueError(
                "deterministic_columns_for_data_hash is required when use_all_columns_for_data_hash=False"
            )
        data_cols_to_use = deterministic_columns_for_data_hash
    
    data_existing = [c for c in data_cols_to_use if c in original_columns]
    if not data_existing:
        raise ValueError("No valid columns found for _record_data_hash")
    
    data_exprs = [
        when(col(c).isNull(), lit("NULL"))
        .otherwise(trim(col(c).cast("string")))
        for c in sorted(data_existing)
    ]
    
    data_concat = concat_ws("||", *data_exprs)
    df = df.withColumn("_record_data_hash", sha2(data_concat, 256))
    
    # ========================= METADATA =========================
    # FIX: Capture timestamp once as a literal so all rows get the exact same value
    merge_ts = current_timestamp()
    
    df = (df
          .withColumn("_created_date", current_date())
          .withColumn("_created_timestamp", merge_ts)
          .withColumn("_updated_timestamp", merge_ts)
          .withColumn("_is_record_active", lit(True))
          )
    
    return df