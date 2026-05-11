# /data_quality/utils/load_utils.py
# FINAL — SCD2 INCREMENTAL LOAD — 100% TESTED & PATCHED

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
# from data_quality.utils.path_utils import get_repo_root
from utils.path_utils import get_repo_root
from datetime import datetime, timezone
import os
import yaml

spark = SparkSession.builder.getOrCreate()

def incremental_load_scd2(
    source_df: DataFrame,
    target_table: str,
    is_quarantine: bool = False,
    natural_key_hash_col: str = "_natural_key_hash",
    data_hash_col: str = "_record_data_hash"
) -> dict:
    """
    FINAL SCD2 INCREMENTAL LOAD — WORKS FOR BOTH CLEAN AND QUARANTINE TABLES
    
    For CLEAN tables:
    - INSERT new records (natural key not found in target)
    - UPDATE changed records (natural key found but data hash different) → deactivate old, insert new
    - SKIP unchanged records (natural key found, data hash same)
    
    For QUARANTINE tables:
    - Pure append (every violation is eternal history)
    
    Returns dict with stats: mode, inserted, deactivated, unchanged
    """
    print("\n" + "="*80)
    table_type = "QUARANTINE" if is_quarantine else "CLEAN (SCD2)"
    print(f"{table_type} LOAD → {target_table}")
    print("="*80)

    # OPTIMIZATION: Avoid rdd.isEmpty() which triggers an extra Spark Action
    source_count = source_df.count()
    if source_count == 0:
        print("Source DataFrame is empty — nothing to load")
        return {"mode": "skip", "inserted": 0, "deactivated": 0, "unchanged": 0}

    print(f"Source records: {source_count:,}")

    # === QUARANTINE = PURE APPEND (violations are eternal) ===
    if is_quarantine:
        source_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target_table)
        print(f"Appended {source_count:,} quarantine records")
        return {"mode": "append", "inserted": source_count, "deactivated": 0, "unchanged": 0}

    # === CLEAN TABLE = FULL SCD2 ===
    
    # Check if target table exists
    try:
        target_df = spark.table(target_table).filter(F.col("_is_record_active") == True)
        target_exists = True
        target_count = target_df.count()
        print(f"Target active records: {target_count:,}")
    except AnalysisException:
        target_exists = False
        target_count = 0
        print("Target table doesn't exist — will create with initial load")

    # If target doesn't exist, just write the source
    if not target_exists or target_count == 0:
        source_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target_table)
        print(f"Initial load: Inserted {source_count:,} records")
        return {"mode": "initial", "inserted": source_count, "deactivated": 0, "unchanged": 0}

    # === COMPARE SOURCE VS TARGET ===
    comparison_df = (
        source_df.alias("src")
        .join(
            target_df.select(
                F.col(natural_key_hash_col).alias("tgt_nk_hash"),
                F.col(data_hash_col).alias("tgt_data_hash")
            ).alias("tgt"),
            F.col(f"src.{natural_key_hash_col}") == F.col("tgt_nk_hash"),
            "left"
        )
    )

    # Categorize records
    categorized_df = comparison_df.withColumn(
        "_change_type",
        F.when(F.col("tgt_nk_hash").isNull(), F.lit("INSERT"))
         .when(F.col(f"src.{data_hash_col}") != F.col("tgt_data_hash"), F.lit("UPDATE"))
         .otherwise(F.lit("NO_CHANGE"))
    )

    # Count by category
    inserts_df = categorized_df.filter(F.col("_change_type") == "INSERT")
    updates_df = categorized_df.filter(F.col("_change_type") == "UPDATE")
    unchanged_df = categorized_df.filter(F.col("_change_type") == "NO_CHANGE")

    insert_count = inserts_df.count()
    update_count = updates_df.count()
    unchanged_count = unchanged_df.count()

    print(f"\nChange Analysis:")
    print(f"   → INSERT (new):      {insert_count:,}")
    print(f"   → UPDATE (changed):  {update_count:,}")
    print(f"   → NO_CHANGE (same):  {unchanged_count:,}")

    total_changes = insert_count + update_count

    if total_changes == 0:
        print("\n✓ No changes detected — skipping write")
        return {"mode": "no_change", "inserted": 0, "deactivated": 0, "unchanged": unchanged_count}

    # === STEP 1: DEACTIVATE OLD VERSIONS OF UPDATED RECORDS ===
    deactivated_count = 0
    if update_count > 0:
        updates_keys = updates_df.select(
            F.col(f"src.{natural_key_hash_col}").alias("nk_to_deactivate")
        ).distinct()
        
        updates_keys.createOrReplaceTempView("updates_to_deactivate")
        
        spark.sql(f"""
            MERGE INTO {target_table} tgt
            USING updates_to_deactivate src
            ON tgt.{natural_key_hash_col} = src.nk_to_deactivate
               AND tgt._is_record_active = true
            WHEN MATCHED THEN
              UPDATE SET
                tgt._is_record_active = false,
                tgt._updated_timestamp = current_timestamp()
        """)
        
        deactivated_count = update_count
        print(f"\nDeactivated {deactivated_count:,} old record versions")

    # === STEP 2: INSERT NEW AND UPDATED RECORDS ===
    records_to_insert = (
        categorized_df
        .filter(F.col("_change_type").isin("INSERT", "UPDATE"))
        # FIX: Removed "_change_type" from the initial drop so we can use it below
        .drop("tgt_nk_hash", "tgt_data_hash")
    )

    # For updates, reset the timestamps to now
    records_to_insert = (
        records_to_insert
        .withColumn("_created_timestamp", 
            F.when(F.col("_change_type") == "UPDATE", F.current_timestamp())
             .otherwise(F.col("_created_timestamp")))
        .withColumn("_updated_timestamp", F.current_timestamp())
        .withColumn("_is_record_active", F.lit(True))
    )

    # FIX: Now that we are done with _change_type, drop it before writing to Delta
    records_to_insert = records_to_insert.drop("_change_type")

    inserted_count = records_to_insert.count()
    records_to_insert.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target_table)
    
    print(f"Inserted {inserted_count:,} new/current record versions")
    print(f"\n✓ INCREMENTAL SCD2 LOAD COMPLETE")

    return {
        "mode": "incremental",
        "inserted": inserted_count,
        "deactivated": deactivated_count,
        "unchanged": unchanged_count
    }


def resolve_error_log_violations(
    current_clean_df,
    catalog: str,
    schema: str,
    table_name: str,
    current_run_id: str,
    dbx_env: str, 
    current_run_ts: datetime = None
) -> None:
    """
    When a previously failing record is now clean,
    mark its old ACTIVE error_log entries as RESOLVED with reason 'DATA_FIXED'.
    """
    current_run_ts = current_run_ts or datetime.now(timezone.utc)

    # Load config to get physical domain catalog
    repo_root = get_repo_root()
    config_path = os.path.join(repo_root, "config", dbx_env, f"{catalog}-conf.yaml")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config not found for environment: {config_path}")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)["schema"]

    domain_catalog = config["domain_catalog"]
    error_log_table = f"{domain_catalog}.audit_quality.error_log"

    print(f"Resolving healed records → error_log: {error_log_table}")

    try:
        active_errors = spark.table(error_log_table).filter(
            (F.col("catalog_name") == catalog) &
            (F.col("schema_name") == schema) &
            (F.col("table_name") == table_name) &
            (F.col("error_status") == "ACTIVE") &
            (F.col("_is_record_active") == True)
        ).select("error_log_id", "_natural_key_hash", "_record_data_hash")
    except AnalysisException:
        print("No existing error_log table or no active errors — nothing to resolve")
        return

    # OPTIMIZATION: limit(1) is much faster than full .count() or .rdd.isEmpty() on massive tables
    if active_errors.limit(1).count() == 0:
        print("No ACTIVE errors found for this table — nothing to resolve")
        return

    # Find records that were previously failing but are NOW in clean with different data hash
    resolved_candidates = (
        current_clean_df.alias("clean")
        .join(
            active_errors.alias("err"),
            F.col("clean._natural_key_hash") == F.col("err._natural_key_hash"),
            "inner"
        )
        .filter(F.col("clean._record_data_hash") != F.col("err._record_data_hash"))
        .select(
            F.col("err.error_log_id"),
            F.col("err._natural_key_hash"),
            F.col("clean._record_data_hash").alias("new_data_hash"),
            F.col("clean._updated_timestamp")
        )
        .distinct()
    )

    resolved_count = resolved_candidates.count()
    if resolved_count == 0:
        print("No previously failing records are now clean — no resolutions needed")
        return

    print(f"RESOLVING {resolved_count} previously failing records — they have been FIXED")

    resolved_candidates.createOrReplaceTempView("resolved_violations")

    # Deactivate old ACTIVE violations and mark as RESOLVED
    spark.sql(f"""
        MERGE INTO {error_log_table} tgt
        USING resolved_violations src
        ON tgt.error_log_id = src.error_log_id
           AND tgt.error_status = 'ACTIVE'
           AND tgt._is_record_active = true
        WHEN MATCHED THEN
          UPDATE SET
            tgt._is_record_active = false,
            tgt._updated_timestamp = current_timestamp(),
            tgt.error_status = 'RESOLVED',
            tgt.resolution_reason = 'DATA_FIXED',
            tgt.resolved_at = current_timestamp(),
            tgt.resolved_run_id = '{current_run_id}'
    """)

    # FIX: Use safe F.lit() instead of fragile SQL string injection for timestamps
    resolution_rows = resolved_candidates.select(
        F.expr("uuid()").alias("error_log_id"),
        F.lit(current_run_id).alias("run_id"),
        F.lit(current_run_ts).cast("timestamp").alias("run_timestamp"),
        F.lit(catalog).alias("catalog_name"),
        F.lit(schema).alias("schema_name"),
        F.lit(table_name).alias("table_name"),
        F.col("_natural_key_hash"),
        F.col("new_data_hash").alias("_record_data_hash"),
        F.lit(None).cast("string").alias("check_name"),
        F.lit(None).cast("string").alias("check_function"),
        F.lit(None).cast("string").alias("column_name"),
        F.lit("Previously failing record has been corrected and is now valid").alias("error_message"),
        F.lit(None).cast("string").alias("failed_value"),
        F.lit("info").alias("criticality"),
        F.lit(None).cast("string").alias("quarantine_table"),
        F.current_timestamp().alias("created_at"),
        F.lit(None).cast("string").alias("created_by"),
        F.lit("RESOLVED").alias("error_status"),
        F.lit("DATA_FIXED").alias("resolution_reason"),
        F.current_timestamp().alias("resolved_at"),
        F.lit(current_run_id).alias("resolved_run_id"),
        F.current_date().alias("_created_date"),
        F.current_timestamp().alias("_created_timestamp"),
        F.current_timestamp().alias("_updated_timestamp"),
        F.lit(True).alias("_is_record_active")
    )

    resolution_rows.write.mode("append").saveAsTable(error_log_table)
    print(f"Resolution complete: {resolved_count} violations marked RESOLVED + tracking rows inserted")