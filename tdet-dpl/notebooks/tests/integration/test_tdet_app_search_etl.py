# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Tests: ETL End-to-End  
# MAGIC ---  
# MAGIC **Purpose:** Test complete ETL pipeline flow for TDET  
# MAGIC **Test Coverage:**
# MAGIC - Initial load (full table creation)  
# MAGIC - Incremental load (SCD Type 2)- Change detection and versioning- Data persistence and retrieval**    
# MAGIC ⚠️ WARNING:** These tests create temporary tables and may take several minutes to run.
# MAGIC ---
# MAGIC **Created By:** Joshua Strickland  
# MAGIC **Created Date:** 2025-10-20     
# MAGIC **Last Updated By:** Joshua Strickland  
# MAGIC **Last Updated Date:** 2025-10-20

# COMMAND ----------

# MAGIC %run ./../test_tdet_app_search_functions

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, when, desc

print(f"Spark Version: {spark.version}")

TEST_CATALOG = "tdet_dev"
# adjust as needed
TEST_SCHEMA = "test_integration"
TEST_TABLE = f"{TEST_CATALOG}.{TEST_SCHEMA}.test_tdet_app_search"
print(f"Test Table: {TEST_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Clean Test Environment

# COMMAND ----------

try:
    spark.sql(f"DROP SCHEMA IF EXISTS {TEST_CATALOG}.{TEST_SCHEMA} CASCADE")
    print(f"✓ Dropped existing schema: {TEST_CATALOG}.{TEST_SCHEMA}")
except Exception as e:
    print(f"No existing schema to drop or error: {e}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TEST_CATALOG}.{TEST_SCHEMA}")
print(f"✓ Created schema: {TEST_CATALOG}.{TEST_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: Initial Load - Full Table Creation

# COMMAND ----------

trademark_schema = StructType(
    [
        StructField("serial_number", IntegerType(), False),
        StructField("mark_tx", StringType(), True),
        StructField("registration_number", IntegerType(), True),
        StructField("status", StringType(), True),
    ]
)

source = spark.createDataFrame(
    [
        (87654321, "TEST MARK 1", 1234567, "LIVE"),
        (87654322, "TEST MARK 2", 1234568, "LIVE"),
        (87654323, "TEST MARK 3", None, "PENDING"),
    ],
    trademark_schema,
)

initial = add_hashes(source, "serial_number")

initial.write.mode("overwrite").saveAsTable(TEST_TABLE)

t = spark.table(TEST_TABLE)
total = t.count()
active = t.filter("_is_record_active").count()
inactive = t.filter("NOT _is_record_active").count()

print(f"Initial -> Total={total}, Active={active}, Inactive={inactive}")
print("✅ PASS" if (total == 3 and active == 3 and inactive == 0) else "❌ FAIL")
display(t)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: Incremental - SCD Type 2 (1 UPDATE, 1 INSERT, 2 NO_CHANGE)

# COMMAND ----------

new_src = spark.createDataFrame(
    [
        (87654321, "TEST MARK 1", 1234567, "DEAD"),  # UPDATE
        (87654322, "TEST MARK 2", 1234568, "LIVE"),  # NO_CHANGE
        (87654323, "TEST MARK 3", None, "PENDING"),  # NO_CHANGE
        (87654324, "TEST MARK 4", 8888888, "LIVE"),  # INSERT
    ],
    trademark_schema,
)

src_h = add_hashes(new_src, "serial_number")
tgt_active = spark.table(TEST_TABLE).filter("_is_record_active")
changes = detect_changes(src_h, tgt_active)
summary = categorize_changes(changes)

print(summary)
ok = summary["INSERT"] == 1 and summary["UPDATE"] == 1 and summary["NO_CHANGE"] == 2
print("✅ PASS" if ok else "❌ FAIL")

display(changes.select("serial_number", "mark_tx", "status", "_change_type"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply SCD2 changes: deactivate old, append new/updated

# COMMAND ----------

updates = changes.filter("_change_type='UPDATE'")
update_serials = [r.serial_number for r in updates.select("serial_number").collect()]
cur = spark.table(TEST_TABLE)

deactivated = cur.withColumn(
    "_is_record_active",
    when(
        (col("serial_number").isin(update_serials)) & col("_is_record_active"),
        lit(False),
    ).otherwise(col("_is_record_active")),
)

new_rows = changes.filter("_change_type!='NO_CHANGE'").drop(
    "_change_type", "tgt_serial", "tgt_natural_hash", "tgt_data_hash"
)

final_df = deactivated.unionByName(new_rows, allowMissingColumns=True)
final_df.write.mode("overwrite").saveAsTable(TEST_TABLE)

f = spark.table(TEST_TABLE)

print(
    f"Final total={f.count()}, active={f.filter('_is_record_active').count()}, inactive={f.filter('NOT _is_record_active').count()}"
)
versions_21 = f.filter("serial_number=87654321").count()
print("✅ PASS" if versions_21 == 2 else f"❌ FAIL: versions of 87654321={versions_21}")

display(f.orderBy("serial_number", desc("_is_record_active")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"Integration test completed using table {TEST_TABLE}.")