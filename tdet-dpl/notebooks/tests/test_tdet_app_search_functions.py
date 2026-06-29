# Databricks notebook source
"""
Testable ETL functions for TDET silver layer.
Extracted from ntb_tdet_app_search for unit testing.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, coalesce, sha2, concat_ws,
    current_date, current_timestamp, when,
    max, first, array_join, collect_set,
    trim, concat
)
from typing import Dict, Tuple
import hashlib

# COMMAND ----------

# ============================================================================
# HASH FUNCTIONS
# ============================================================================

def add_hashes(df: DataFrame, natural_key_col: str) -> DataFrame:
    """
    Add hash columns for natural key and record data.
    
    Args:
        df: Input DataFrame
        natural_key_col: Column to use as natural key (e.g., 'serial_number')
    
    Returns:
        DataFrame with _natural_key_hash and _record_data_hash columns
    """
    merge_timestamp = current_timestamp()
    
    # Natural key hash
    df = df.withColumn(
        "_natural_key_hash",
        sha2(trim(col(natural_key_col).cast("string")), 256)
    )
    
    # Deterministic columns for data hash (exclude aggregated fields)
    deterministic_cols = [
        "serial_number", "mark_tx", "filing_date", "registration_number",
        "registration_date", "examiner_number", "examiner_name", "docket_number",
        "law_office", "status", "status_date",
        "international_registration_number", "international_us_reference_number"
    ]
    
    # Build hash expression only from deterministic columns that exist
    existing_cols = [c for c in deterministic_cols if c in df.columns]
    
    hash_expr = concat_ws("||", *[
        when(col(c).isNull(), lit("NULL"))
        .otherwise(trim(col(c).cast("string")))
        for c in sorted(existing_cols)
    ])
    
    df = df.withColumn("_record_data_hash", sha2(hash_expr, 256))
    
    # Add metadata columns
    df = (df
        .withColumn("_created_date", current_date())
        .withColumn("_created_timestamp", merge_timestamp)
        .withColumn("_updated_timestamp", merge_timestamp)
        .withColumn("_is_record_active", lit(True))
    )
    
    return df

# COMMAND ----------

# ============================================================================
# AGGREGATION FUNCTIONS
# ============================================================================

def aggregate_class_list(class_df: DataFrame) -> DataFrame:
    """
    Aggregate class codes into semicolon-separated list per serial number.
    
    Args:
        class_df: DataFrame with ser_num and class columns
    
    Returns:
        DataFrame with ser_num and class_list columns
    """
    return (
        class_df
        .groupBy("ser_num")
        .agg(array_join(collect_set("class"), ";").alias("class_list"))
    )


def aggregate_current_party_names(
    party_role_df: DataFrame,
    interested_party_df: DataFrame
) -> DataFrame:
    """
    Aggregate current party names by role (OWNER, AT, COR, DR).
    
    Args:
        party_role_df: tm_party_role DataFrame
        interested_party_df: interested_party DataFrame
    
    Returns:
        DataFrame with aggregated party names by trademark_gid
    """
    # Join party roles with names
    party_with_names = (
        party_role_df.alias("tpr")
        .filter(col("fk_tm_party_role_cd").isin("OWNER", "AT", "COR", "DR"))
        .join(
            interested_party_df.alias("ip"),
            col("tpr.fk_interested_party_gid") == col("ip.interested_party_gid"),
            "left"
        )
        .select(
            col("tpr.fk_trademark_gid"),
            col("tpr.fk_tm_party_role_cd"),
            col("tpr.bar_information_tx"),
            trim(col("ip.interested_party_nm")).alias("party_name"),
            col("ip.country_cd")
        )
    )
    
    # Aggregate by role
    return (
        party_with_names
        .groupBy("fk_trademark_gid")
        .agg(
            first(when(col("fk_tm_party_role_cd") == "OWNER", col("party_name")), ignorenulls=True).alias("owner_name"),
            first(when(col("fk_tm_party_role_cd") == "OWNER", col("country_cd")), ignorenulls=True).alias("owner_country"),
            first(when(col("fk_tm_party_role_cd") == "AT", col("party_name")), ignorenulls=True).alias("attorney_name"),
            first(when(col("fk_tm_party_role_cd") == "AT", col("bar_information_tx")), ignorenulls=True).alias("attorney_membership_number"),
            first(when(col("fk_tm_party_role_cd") == "COR", col("party_name")), ignorenulls=True).alias("correspondent_name"),
            first(when(col("fk_tm_party_role_cd") == "DR", col("party_name")), ignorenulls=True).alias("domestic_representative_name")
        )
    )


def aggregate_historical_party_names(
    party_role_h_df: DataFrame,
    interested_party_h_df: DataFrame
) -> DataFrame:
    """
    Aggregate historical party names into semicolon-separated lists.
    
    Args:
        party_role_h_df: tm_party_role_h DataFrame
        interested_party_h_df: interested_party_h DataFrame
    
    Returns:
        DataFrame with historical party names aggregated
    """
    historic_party_base = (
        party_role_h_df.alias("tpr")
        .filter(
            (col("tpr.action_ct") != "D") &
            col("tpr.fk_tm_party_role_cd").isin("OWNER", "AT", "COR", "DR")
        )
        .join(
            interested_party_h_df.filter(col("action_ct") != "D").alias("iph"),
            col("tpr.fk_interested_party_gid") == col("iph.interested_party_gid"),
            "left"
        )
        .select(
            col("tpr.fk_trademark_gid"),
            col("tpr.fk_tm_party_role_cd"),
            trim(col("iph.interested_party_nm")).alias("party_name")
        )
    )
    
    return (
        historic_party_base
        .groupBy("fk_trademark_gid")
        .agg(
            array_join(collect_set(when(col("fk_tm_party_role_cd") == "OWNER", col("party_name"))), ";").alias("hist_owner_nm"),
            array_join(collect_set(when(col("fk_tm_party_role_cd") == "AT", col("party_name"))), ";").alias("hist_attorney_nm"),
            array_join(collect_set(when(col("fk_tm_party_role_cd") == "COR", col("party_name"))), ";").alias("hist_cr_nm"),
            array_join(collect_set(when(col("fk_tm_party_role_cd") == "DR", col("party_name"))), ";").alias("hist_dr_nm")
        )
    )

# COMMAND ----------

# ============================================================================
# SCD TYPE 2 LOGIC
# ============================================================================

def detect_changes(
    source_df: DataFrame,
    target_df: DataFrame,
    natural_key: str = "serial_number"
) -> DataFrame:
    """
    Compare source and target DataFrames to detect changes.
    
    Args:
        source_df: New data with _natural_key_hash and _record_data_hash
        target_df: Existing data (active records only)
        natural_key: Column to join on
    
    Returns:
        DataFrame with _change_type column (INSERT, UPDATE, NO_CHANGE)
    """
    comparison_df = (
        source_df.alias("src")
        .join(
            target_df.alias("tgt"),
            col(f"src.{natural_key}") == col(f"tgt.{natural_key}"),
            "left"
        )
        .select(
            col("src.*"),
            col(f"tgt.{natural_key}").alias("tgt_serial"),
            col("tgt._natural_key_hash").alias("tgt_natural_hash"),
            col("tgt._record_data_hash").alias("tgt_data_hash")
        )
    )
    
    changes_df = comparison_df.withColumn(
        "_change_type",
        when(col("tgt_serial").isNull(), lit("INSERT"))
        .when(col("_record_data_hash") != col("tgt_data_hash"), lit("UPDATE"))
        .otherwise(lit("NO_CHANGE"))
    )
    
    return changes_df


def categorize_changes(changes_df: DataFrame) -> Dict[str, int]:
    """
    Count records by change type.
    
    Args:
        changes_df: DataFrame with _change_type column
    
    Returns:
        Dictionary with counts: {"INSERT": X, "UPDATE": Y, "NO_CHANGE": Z}
    """
    change_summary = changes_df.groupBy("_change_type").count().collect()
    
    result = {"INSERT": 0, "UPDATE": 0, "NO_CHANGE": 0}
    for row in change_summary:
        result[row["_change_type"]] = row["count"]
    
    return result

# COMMAND ----------

# ============================================================================
# DATA QUALITY CHECKS
# ============================================================================

def check_duplicate_active_records(df: DataFrame, key_col: str = "serial_number") -> Tuple[bool, DataFrame]:
    """
    Check for duplicate active records (data quality issue).
    
    Args:
        df: DataFrame to check
        key_col: Column that should be unique where _is_record_active = true
    
    Returns:
        Tuple of (is_valid, duplicates_df)
        - is_valid: True if no duplicates found
        - duplicates_df: DataFrame of duplicate records (empty if none)
    """
    duplicates = (
        df
        .filter(col("_is_record_active") == True)
        .groupBy(key_col)
        .count()
        .filter(col("count") > 1)
    )
    
    duplicate_count = duplicates.count()
    
    return (duplicate_count == 0, duplicates)


def validate_required_columns(df: DataFrame, required_cols: list) -> Tuple[bool, list]:
    """
    Validate that all required columns exist in DataFrame.
    
    Args:
        df: DataFrame to validate
        required_cols: List of required column names
    
    Returns:
        Tuple of (is_valid, missing_columns)
    """
    existing_cols = df.columns
    missing = [col for col in required_cols if col not in existing_cols]
    
    return (len(missing) == 0, missing)


def validate_data_types(df: DataFrame, expected_types: Dict[str, str]) -> Tuple[bool, list]:
    """
    Validate column data types.
    
    Args:
        df: DataFrame to validate
        expected_types: Dict of {column_name: expected_type}
    
    Returns:
        Tuple of (is_valid, mismatches)
        - mismatches: List of tuples (column, expected, actual)
    """
    mismatches = []
    
    for col_name, expected_type in expected_types.items():
        if col_name in df.columns:
            actual_type = dict(df.dtypes)[col_name]
            if actual_type != expected_type:
                mismatches.append((col_name, expected_type, actual_type))
    
    return (len(mismatches) == 0, mismatches)


def validate_no_nulls(df: DataFrame, non_null_cols: list) -> Tuple[bool, Dict[str, int]]:
    """
    Validate that specified columns have no null values.
    
    Args:
        df: DataFrame to validate
        non_null_cols: List of columns that should not contain nulls
    
    Returns:
        Tuple of (is_valid, null_counts)
        - null_counts: Dict of {column: count_of_nulls}
    """
    null_counts = {}
    
    for col_name in non_null_cols:
        if col_name in df.columns:
            count = df.filter(F.col(col_name).isNull()).count()
            if count > 0:
                null_counts[col_name] = count
    
    return (len(null_counts) == 0, null_counts)