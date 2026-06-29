# Databricks notebook source
# MAGIC %md
# MAGIC # Unit Tests: Hash Functions
# MAGIC ---
# MAGIC **Purpose:** Test hash generation and change detection logic for TDET ETL
# MAGIC
# MAGIC **Test Coverage:**
# MAGIC - Natural key hash generation
# MAGIC - Record data hash generation
# MAGIC - Change detection (INSERT, UPDATE, NO_CHANGE)
# MAGIC - SCD Type 2 logic
# MAGIC
# MAGIC ---
# MAGIC **Created By:** Joshua Strickland  
# MAGIC **Created Date:** 2025-10-20     
# MAGIC **Last Updated By:** Joshua Strickland  
# MAGIC **Last Updated Date:** 2025-10-20

# COMMAND ----------

# MAGIC %run ./../test_tdet_app_search_functions

# COMMAND ----------

# Imports
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
from datetime import date
import sys

# COMMAND ----------

# Test configuration
TEST_CATALOG = "tdet_dev"
TEST_SCHEMA = "test_silver"

print(f"Test Catalog: {TEST_CATALOG}")
print(f"Test Schema: {TEST_SCHEMA}")
print(f"Spark Version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: Hash Functions Create Required Columns

# COMMAND ----------

# Test: add_hashes creates all required metadata columns

# Arrange
schema = StructType(
    [
        StructField("serial_number", IntegerType(), False),
        StructField("mark_tx", StringType(), True),
        StructField("filing_date", DateType(), True),
    ]
)

data = [
    (87654321, "TEST MARK", date(2020, 1, 1)),
    (87654322, "ANOTHER MARK", date(2020, 2, 1)),
]

test_df = spark.createDataFrame(data, schema)

# Act
result_df = add_hashes(test_df, "serial_number")

# Assert
required_columns = [
    "_natural_key_hash",
    "_record_data_hash",
    "_created_date",
    "_created_timestamp",
    "_updated_timestamp",
    "_is_record_active",
]

missing_columns = [col for col in required_columns if col not in result_df.columns]

if len(missing_columns) == 0:
    print("✅ TEST PASSED: All required columns created")
    display(result_df.limit(2))
else:
    print(f"❌ TEST FAILED: Missing columns: {missing_columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: Natural Key Hash Consistency

# COMMAND ----------

# Test: Same serial number produces same natural key hash

# Arrange
schema = StructType([
    StructField("serial_number", IntegerType(), False),
    StructField("mark_tx", StringType(), True)
])

data = [
    (87654321, "MARK 1"),
    (87654321, "MARK 2"),  # Same serial, different mark
]

test_df = spark.createDataFrame(data, schema)

# Act
result_df = add_hashes(test_df, "serial_number")
hashes = result_df.select("serial_number", "_natural_key_hash").collect()

# Assert
hash1 = hashes[0]["_natural_key_hash"]
hash2 = hashes[1]["_natural_key_hash"]

if hash1 == hash2:
    print("✅ TEST PASSED: Same serial number produces consistent natural key hash")
    print(f"   Hash: {hash1}")
else:
    print(f"❌ TEST FAILED: Hashes differ")
    print(f"   Hash 1: {hash1}")
    print(f"   Hash 2: {hash2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: Data Hash Changes With Data

# COMMAND ----------

# Test: Data hash changes when data changes

# Arrange
schema = StructType(
    [
        StructField("serial_number", IntegerType(), False),
        StructField("mark_tx", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

data = [
    (87654321, "TEST MARK", "LIVE"),
    (87654321, "TEST MARK", "DEAD"),  # Status changed
]

test_df = spark.createDataFrame(data, schema)

# Act
result_df = add_hashes(test_df, "serial_number")
hashes = result_df.select("serial_number", "status", "_record_data_hash").collect()

# Assert
hash1 = hashes[0]["_record_data_hash"]
hash2 = hashes[1]["_record_data_hash"]

if hash1 != hash2:
    print("✅ TEST PASSED: Different data produces different data hash")
    print(f"   LIVE hash: {hash1[:16]}...")
    print(f"   DEAD hash: {hash2[:16]}...")
else:
    print(f"❌ TEST FAILED: Hashes are identical when they should differ")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: All New Records Marked Active

# COMMAND ----------

# Test: All new records are marked as active

# Arrange
schema = StructType(
    [
        StructField("serial_number", IntegerType(), False),
        StructField("mark_tx", StringType(), True),
    ]
)

data = [(i, f"MARK {i}") for i in range(1, 101)]
test_df = spark.createDataFrame(data, schema)

# Act
result_df = add_hashes(test_df, "serial_number")

# Assert
total_count = result_df.count()
active_count = result_df.filter("_is_record_active = true").count()
inactive_count = result_df.filter("_is_record_active = false").count()

if total_count == 100 and active_count == 100 and inactive_count == 0:
    print("✅ TEST PASSED: All 100 records marked as active")
    print(
        f"   Total: {total_count}, Active: {active_count}, Inactive: {inactive_count}"
    )
else:
    print(f"❌ TEST FAILED: Unexpected active/inactive counts")
    print(
        f"   Total: {total_count}, Active: {active_count}, Inactive: {inactive_count}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: Change Detection - INSERT

# COMMAND ----------

# Test: New records identified as INSERT

# Arrange
schema = StructType(
    [
        StructField("serial_number", IntegerType(), False),
        StructField("mark_tx", StringType(), True),
        StructField("_natural_key_hash", StringType(), False),
        StructField("_record_data_hash", StringType(), False),
    ]
)

# Source has new records
source_data = [
    (87654321, "NEW MARK", "hash1", "datahash1"),
    (87654322, "ANOTHER NEW", "hash2", "datahash2"),
]
source_df = spark.createDataFrame(source_data, schema)

# Target is empty
target_df = spark.createDataFrame([], schema)

# Act
result_df = detect_changes(source_df, target_df)

# Assert
insert_count = result_df.filter("_change_type = 'INSERT'").count()
update_count = result_df.filter("_change_type = 'UPDATE'").count()
no_change_count = result_df.filter("_change_type = 'NO_CHANGE'").count()

if insert_count == 2 and update_count == 0 and no_change_count == 0:
    print("✅ TEST PASSED: New records identified as INSERT")
    print(
        f"   INSERT: {insert_count}, UPDATE: {update_count}, NO_CHANGE: {no_change_count}"
    )
    display(result_df.select("serial_number", "mark_tx", "_change_type"))
else:
    print(f"❌ TEST FAILED: Unexpected change type counts")
    print(
        f"   INSERT: {insert_count}, UPDATE: {update_count}, NO_CHANGE: {no_change_count}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6: Change Detection - UPDATE

# COMMAND ----------

# Test: Changed records identified as UPDATE

# Arrange
schema = StructType(
    [
        StructField("serial_number", IntegerType(), False),
        StructField("mark_tx", StringType(), True),
        StructField("_natural_key_hash", StringType(), False),
        StructField("_record_data_hash", StringType(), False),
    ]
)

# Source has updated record
source_data = [(87654321, "UPDATED MARK", "hash1", "datahash_new")]
source_df = spark.createDataFrame(source_data, schema)

# Target has old version
target_data = [(87654321, "OLD MARK", "hash1", "datahash_old")]
target_df = spark.createDataFrame(target_data, schema)

# Act
result_df = detect_changes(source_df, target_df)

# Assert
update_count = result_df.filter("_change_type = 'UPDATE'").count()
insert_count = result_df.filter("_change_type = 'INSERT'").count()

if update_count == 1 and insert_count == 0:
    print("✅ TEST PASSED: Changed record identified as UPDATE")
    display(
        result_df.select(
            "serial_number",
            "mark_tx",
            "_change_type",
            "_record_data_hash",
            "tgt_data_hash",
        )
    )
else:
    print(f"❌ TEST FAILED: Expected 1 UPDATE, got {update_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 7: Change Detection - NO_CHANGE

# COMMAND ----------

# Test: Unchanged records identified as NO_CHANGE

# Arrange
schema = StructType(
    [
        StructField("serial_number", IntegerType(), False),
        StructField("mark_tx", StringType(), True),
        StructField("_natural_key_hash", StringType(), False),
        StructField("_record_data_hash", StringType(), False),
    ]
)

# Source and target identical
data = [(87654321, "SAME MARK", "hash1", "datahash1")]
source_df = spark.createDataFrame(data, schema)
target_df = spark.createDataFrame(data, schema)

# Act
result_df = detect_changes(source_df, target_df)

# Assert
no_change_count = result_df.filter("_change_type = 'NO_CHANGE'").count()

if no_change_count == 1:
    print("✅ TEST PASSED: Unchanged record identified as NO_CHANGE")
    display(result_df.select("serial_number", "mark_tx", "_change_type"))
else:
    print(f"❌ TEST FAILED: Expected 1 NO_CHANGE, got {no_change_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Summary

# COMMAND ----------

# Summary of all tests
print("="*60)
print("HASH FUNCTIONS TEST SUITE SUMMARY")
print("="*60)
print("")
print("Tests Completed:")
print("  ✓ Test 1: Hash functions create required columns")
print("  ✓ Test 2: Natural key hash consistency")
print("  ✓ Test 3: Data hash changes with data")
print("  ✓ Test 4: All new records marked active")
print("  ✓ Test 5: Change detection - INSERT")
print("  ✓ Test 6: Change detection - UPDATE")
print("  ✓ Test 7: Change detection - NO_CHANGE")
print("")
print("Review results above for any failures.")
print("="*60)