# Databricks notebook source
# MAGIC %md
# MAGIC # Unit Tests: Data Quality Checks
# MAGIC ---
# MAGIC **Purpose:** Test data quality validation functions for TDET ETL
# MAGIC
# MAGIC **Test Coverage:**
# MAGIC - Duplicate active records check
# MAGIC - Required columns validation
# MAGIC - Data type validation
# MAGIC - NULL value validation
# MAGIC - SCD Type 2 integrity
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: Duplicate Check - No Duplicates (PASS)

# COMMAND ----------

# Test: Unique active records should pass duplicate check

# Arrange
schema = StructType(
    [
        StructField("serial_number", IntegerType(), False),
        StructField("mark_tx", StringType(), True),
        StructField("_is_record_active", BooleanType(), False),
    ]
)

data = [
    (87654321, "MARK 1", True),
    (87654322, "MARK 2", True),
    (87654323, "MARK 3", True),
]

test_df = spark.createDataFrame(data, schema)

# Act
is_valid, duplicates_df = check_duplicate_active_records(test_df)

# Assert
if is_valid and duplicates_df.count() == 0:
    print("✅ TEST PASSED: No duplicates found (as expected)")
    print(f"   Valid: {is_valid}, Duplicate count: {duplicates_df.count()}")
else:
    print("❌ TEST FAILED: Unexpected duplicates detected")
    display(duplicates_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: Duplicate Check - With Duplicates (FAIL)

# COMMAND ----------

# Test: Duplicate active records should fail duplicate check

# Arrange
schema = StructType(
    [
        StructField("serial_number", IntegerType(), False),
        StructField("mark_tx", StringType(), True),
        StructField("_is_record_active", BooleanType(), False),
    ]
)

# Serial 87654321 appears twice as active (DATA QUALITY ISSUE!)
data = [
    (87654321, "MARK 1 V1", True),
    (87654321, "MARK 1 V2", True),  # Duplicate!
    (87654322, "MARK 2", True),
]

test_df = spark.createDataFrame(data, schema)

# Act
is_valid, duplicates_df = check_duplicate_active_records(test_df)

# Assert
if not is_valid and duplicates_df.count() == 1:
    dup_serial = duplicates_df.collect()[0]["serial_number"]
    if dup_serial == 87654321:
        print("✅ TEST PASSED: Duplicate detected correctly")
        print(f"   Valid: {is_valid}")
        display(duplicates_df)
    else:
        print(f"❌ TEST FAILED: Wrong duplicate serial: {dup_serial}")
else:
    print(f"❌ TEST FAILED: Expected duplicate not detected")
    print(f"   Valid: {is_valid}, Duplicate count: {duplicates_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: Duplicate Check - Inactive Duplicates Allowed (SCD Type 2)

# COMMAND ----------

# Test: Inactive duplicates should be allowed (SCD Type 2 historical records)

# Arrange
schema = StructType(
    [
        StructField("serial_number", IntegerType(), False),
        StructField("mark_tx", StringType(), True),
        StructField("_is_record_active", BooleanType(), False),
    ]
)

# Serial 87654321 appears twice, but only one active
data = [
    (87654321, "MARK 1 OLD", False),  # Inactive (historical)
    (87654321, "MARK 1 NEW", True),  # Active (current)
    (87654322, "MARK 2", True),
]

test_df = spark.createDataFrame(data, schema)

# Act
is_valid, duplicates_df = check_duplicate_active_records(test_df)

# Assert
if is_valid:
    print("✅ TEST PASSED: Inactive duplicates allowed (SCD Type 2)")
    print(f"   Valid: {is_valid}")
    print(
        "   This is correct - historical versions should not be flagged as duplicates"
    )
else:
    print("❌ TEST FAILED: SCD Type 2 pattern incorrectly flagged as duplicate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: Required Columns - All Present (PASS)

# COMMAND ----------

# Test: All required columns present

# Arrange
schema = StructType(
    [
        StructField("serial_number", IntegerType(), False),
        StructField("mark_tx", StringType(), True),
        StructField("_is_record_active", BooleanType(), False),
    ]
)

test_df = spark.createDataFrame([], schema)
required = ["serial_number", "mark_tx", "_is_record_active"]

# Act
is_valid, missing = validate_required_columns(test_df, required)

# Assert
if is_valid and len(missing) == 0:
    print("✅ TEST PASSED: All required columns present")
    print(f"   Valid: {is_valid}")
    print(f"   Columns: {test_df.columns}")
else:
    print(f"❌ TEST FAILED: Missing columns: {missing}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: Required Columns - Missing Columns (FAIL)

# COMMAND ----------

# Test: Missing required columns detected

# Arrange
schema = StructType(
    [
        StructField("serial_number", IntegerType(), False),
        StructField("mark_tx", StringType(), True),
    ]
)

test_df = spark.createDataFrame([], schema)
required = ["serial_number", "mark_tx", "_is_record_active", "_created_date"]

# Act
is_valid, missing = validate_required_columns(test_df, required)

# Assert
expected_missing = {"_is_record_active", "_created_date"}

if not is_valid and set(missing) == expected_missing:
    print("✅ TEST PASSED: Missing columns detected correctly")
    print(f"   Valid: {is_valid}")
    print(f"   Missing columns: {missing}")
else:
    print(f"❌ TEST FAILED: Incorrect missing column detection")
    print(f"   Expected missing: {expected_missing}")
    print(f"   Actual missing: {set(missing)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6: Data Types - Correct Types (PASS)

# COMMAND ----------

# Test: Correct data types validation

# Arrange
schema = StructType([
    StructField("serial_number", IntegerType(), False),
    StructField("mark_tx", StringType(), True),
    StructField("_is_record_active", BooleanType(), False)
])

test_df = spark.createDataFrame([], schema)
expected_types = {
    "serial_number": "int",
    "mark_tx": "string",
    "_is_record_active": "boolean"
}

# Act
is_valid, mismatches = validate_data_types(test_df, expected_types)

# Assert
if is_valid and len(mismatches) == 0:
    print("✅ TEST PASSED: All data types correct")
    print(f"   Valid: {is_valid}")
    for col, dtype in expected_types.items():
        print(f"   {col}: {dtype} ✓")
else:
    print(f"❌ TEST FAILED: Type mismatches: {mismatches}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 7: Data Types - Incorrect Types (FAIL)

# COMMAND ----------

# Test: Incorrect data types detected

# Arrange
schema = StructType(
    [
        StructField("serial_number", StringType(), False),  # Should be int!
        StructField("mark_tx", StringType(), True),
    ]
)

test_df = spark.createDataFrame([], schema)
expected_types = {"serial_number": "int", "mark_tx": "string"}

# Act
is_valid, mismatches = validate_data_types(test_df, expected_types)

# Assert
if not is_valid and len(mismatches) == 1:
    col, expected, actual = mismatches[0]
    if col == "serial_number" and expected == "int" and actual == "string":
        print("✅ TEST PASSED: Type mismatch detected correctly")
        print(f"   Column: {col}")
        print(f"   Expected: {expected}")
        print(f"   Actual: {actual}")
    else:
        print(f"❌ TEST FAILED: Wrong mismatch details: {mismatches[0]}")
else:
    print(f"❌ TEST FAILED: Expected 1 mismatch, got {len(mismatches)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 8: NULL Validation - No NULLs (PASS)

# COMMAND ----------

# Test: No NULL values in required columns

# Arrange
schema = StructType(
    [
        StructField("serial_number", IntegerType(), False),
        StructField("mark_tx", StringType(), False),
    ]
)

data = [(87654321, "MARK 1"), (87654322, "MARK 2")]

test_df = spark.createDataFrame(data, schema)

# Act
is_valid, null_counts = validate_no_nulls(test_df, ["serial_number", "mark_tx"])

# Assert
if is_valid and len(null_counts) == 0:
    print("✅ TEST PASSED: No NULL values found")
    print(f"   Valid: {is_valid}")
else:
    print(f"❌ TEST FAILED: Unexpected NULLs: {null_counts}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 9: NULL Validation - With NULLs (FAIL)

# COMMAND ----------

# Test: NULL values detected in required columns

# Arrange
schema = StructType([
    StructField("serial_number", IntegerType(), False),
    StructField("mark_tx", StringType(), True)
])

data = [
    (87654321, "MARK 1"),
    (87654322, None),  # NULL mark_tx
    (87654323, None)   # Another NULL
]

test_df = spark.createDataFrame(data, schema)

# Act
is_valid, null_counts = validate_no_nulls(test_df, ["serial_number", "mark_tx"])

# Assert
if not is_valid and null_counts.get("mark_tx") == 2:
    print("✅ TEST PASSED: NULL values detected correctly")
    print(f"   Valid: {is_valid}")
    print(f"   NULL counts: {null_counts}")
else:
    print(f"❌ TEST FAILED: Incorrect NULL detection")
    print(f"   Expected 2 NULLs in mark_tx, got: {null_counts}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Summary

# COMMAND ----------

# Summary
print("="*60)
print("DATA QUALITY TEST SUITE SUMMARY")
print("="*60)
print("")
print("Tests Completed:")
print("  ✓ Test 1: Duplicate check - no duplicates (pass)")
print("  ✓ Test 2: Duplicate check - with duplicates (fail)")
print("  ✓ Test 3: Duplicate check - inactive allowed (SCD Type 2)")
print("  ✓ Test 4: Required columns - all present (pass)")
print("  ✓ Test 5: Required columns - missing (fail)")
print("  ✓ Test 6: Data types - correct (pass)")
print("  ✓ Test 7: Data types - incorrect (fail)")
print("  ✓ Test 8: NULL validation - no NULLs (pass)")
print("  ✓ Test 9: NULL validation - with NULLs (fail)")
print("")
print("Review results above for any failures.")
print("="*60)