# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: Data Quality Validation   
# MAGIC ---
# MAGIC ## Description:     
# MAGIC (Output Table)Validates data quality on the silver output (or adapt to gold app tables).    
# MAGIC
# MAGIC ---
# MAGIC **Created By:** Joshua Strickland  
# MAGIC **Created Date:** 2025-10-20     
# MAGIC **Last Updated By:** Joshua Strickland  
# MAGIC **Last Updated Date:** 2025-10-20

# COMMAND ----------

# MAGIC %run ./../test_tdet_app_search_functions

# COMMAND ----------

from pyspark.sql.functions import col, count, countDistinct, max as fmax

print(f"Spark Version: {spark.version}")

CATALOG = "tdet_dev"   
# adjust for env 
SILVER_TABLE = f"{CATALOG}.silver.tdet_app_search"
print(f"Validating table: {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 1: Table existence and row count (active records)

# COMMAND ----------

try:
    df = spark.table(SILVER_TABLE)
    total = df.count()
    active = df.filter("_is_record_active").count()

    print(f"Total rows: {total}")
    print(f"Active rows: {active}")
    print("✅ PASS" if active > 0 else "❌ FAIL: No active rows")
except Exception as e:
    print(f"❌ FAIL: Unable to read table {SILVER_TABLE}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 2: Uniqueness of active serial_number (no duplicates)

# COMMAND ----------

try:
    active_df = spark.table(SILVER_TABLE).filter(col("_is_record_active"))
    dup = active_df.groupBy("serial_number").count().filter(col("count") > 1)
    dup_cnt = dup.count()

    print(f"Active duplicate serials: {dup_cnt}")
    print("✅ PASS" if dup_cnt == 0 else "❌ FAIL")

    if dup_cnt > 0:
        display(dup.limit(20))
except Exception as e:
    print(f"❌ FAIL: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 3: Required columns present

# COMMAND ----------

required_cols = [
    "serial_number",
    "mark_tx",
    "filing_date",
    "filed_bases",
    "current_bases",
    "registration_number",
    "registration_date",
    "owner_name",
    "class_list",
    "status",
    "status_date",
    "_is_record_active",
    "_created_date",
    "_natural_key_hash",
    "_record_data_hash",
]

try:
    cols = set(spark.table(SILVER_TABLE).columns)
    missing = [c for c in required_cols if c not in cols]
    print("✅ PASS" if len(missing) == 0 else f"❌ FAIL: missing {missing}")
except Exception as e:
    print(f"❌ FAIL: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 4: Freshness (by _created_date)

# COMMAND ----------

try:
    latest = (
        spark.table(SILVER_TABLE)
        .agg(fmax("_created_date").alias("max_dt"))
        .collect()[0]["max_dt"]
    )
    print(f"Latest created_date: {latest}")
    print("ℹ️ Validate freshness relative to your SLA (e.g., daily)")
except Exception as e:
    print(f"❌ FAIL: {e}")