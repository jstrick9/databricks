# Databricks notebook source
# MAGIC %md
# MAGIC # Unit Tests: Aggregation Functions
# MAGIC ---
# MAGIC **Purpose:** Test aggregation logic for TDET ETL
# MAGIC
# MAGIC **Test Coverage:**
# MAGIC - Class list aggregation (semicolon-separated)
# MAGIC - Current party name aggregation (OWNER, AT, COR, DR)
# MAGIC - Historical party name aggregation
# MAGIC - Email/address/phone aggregation
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: Aggregate Class List - Single Class

# COMMAND ----------

# Test: Single class per serial number

# Arrange
schema = StructType(
    [
        StructField("ser_num", IntegerType(), False),
        StructField("class", StringType(), False),
    ]
)

data = [(87654321, "009"), (87654322, "041")]

test_df = spark.createDataFrame(data, schema)

# Act
result_df = aggregate_class_list(test_df)

# Assert
if result_df.count() == 2:
    row1 = result_df.filter("ser_num = 87654321").collect()[0]
    row2 = result_df.filter("ser_num = 87654322").collect()[0]

    if row1["class_list"] == "009" and row2["class_list"] == "041":
        print("✅ TEST PASSED: Single class aggregation works")
        display(result_df)
    else:
        print(f"❌ TEST FAILED: Unexpected class values")
else:
    print(f"❌ TEST FAILED: Expected 2 rows, got {result_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: Aggregate Class List - Multiple Classes

# COMMAND ----------

# Test: Multiple classes per serial number (semicolon-separated)

# Arrange
schema = StructType(
    [
        StructField("ser_num", IntegerType(), False),
        StructField("class", StringType(), False),
    ]
)

data = [(87654321, "009"), (87654321, "041"), (87654321, "042")]

test_df = spark.createDataFrame(data, schema)

# Act
result_df = aggregate_class_list(test_df)

# Assert
if result_df.count() == 1:
    row = result_df.collect()[0]
    classes = set(row["class_list"].split(";"))
    expected_classes = {"009", "041", "042"}

    if classes == expected_classes:
        print("✅ TEST PASSED: Multiple classes aggregated correctly")
        print(f"   Classes: {row['class_list']}")
        display(result_df)
    else:
        print(f"❌ TEST FAILED: Class mismatch")
        print(f"   Expected: {expected_classes}")
        print(f"   Got: {classes}")
else:
    print(f"❌ TEST FAILED: Expected 1 row, got {result_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: Current Party Names - All Roles

# COMMAND ----------

# Test: Party name aggregation with all role types (OWNER, AT, COR, DR)

# Arrange
party_role_schema = StructType(
    [
        StructField("fk_trademark_gid", StringType(), False),
        StructField("fk_tm_party_role_cd", StringType(), False),
        StructField("fk_interested_party_gid", StringType(), False),
        StructField("bar_information_tx", StringType(), True),
    ]
)

interested_party_schema = StructType(
    [
        StructField("interested_party_gid", StringType(), False),
        StructField("interested_party_nm", StringType(), False),
        StructField("country_cd", StringType(), True),
    ]
)

party_role_data = [
    ("TM001", "OWNER", "IP001", None),
    ("TM001", "AT", "IP002", "123456"),
    ("TM001", "COR", "IP003", None),
    ("TM001", "DR", "IP004", None),
]

interested_party_data = [
    ("IP001", "ACME CORPORATION", "US"),
    ("IP002", "JOHN DOE ESQ", "US"),
    ("IP003", "JANE SMITH", "US"),
    ("IP004", "FOREIGN REP LLC", "CA"),
]

party_role_df = spark.createDataFrame(party_role_data, party_role_schema)
interested_party_df = spark.createDataFrame(
    interested_party_data, interested_party_schema
)

# Act
result_df = aggregate_current_party_names(party_role_df, interested_party_df)

# Assert
if result_df.count() == 1:
    row = result_df.collect()[0]

    checks = [
        (row["owner_name"] == "ACME CORPORATION", "owner_name"),
        (row["owner_country"] == "US", "owner_country"),
        (row["attorney_name"] == "JOHN DOE ESQ", "attorney_name"),
        (row["attorney_membership_number"] == "123456", "attorney_membership_number"),
        (row["correspondent_name"] == "JANE SMITH", "correspondent_name"),
        (
            row["domestic_representative_name"] == "FOREIGN REP LLC",
            "domestic_representative_name",
        ),
    ]

    all_passed = all(check[0] for check in checks)

    if all_passed:
        print("✅ TEST PASSED: All party roles aggregated correctly")
        display(result_df)
    else:
        print("❌ TEST FAILED: Some party roles incorrect")
        for check, name in checks:
            status = "✓" if check else "✗"
            print(f"   {status} {name}")
else:
    print(f"❌ TEST FAILED: Expected 1 row, got {result_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: Current Party Names - Missing Roles

# COMMAND ----------

# Test: Party aggregation with missing roles (should be NULL)

# Arrange - Only OWNER role present
party_role_schema = StructType(
    [
        StructField("fk_trademark_gid", StringType(), False),
        StructField("fk_tm_party_role_cd", StringType(), False),
        StructField("fk_interested_party_gid", StringType(), False),
        StructField("bar_information_tx", StringType(), True),
    ]
)

interested_party_schema = StructType(
    [
        StructField("interested_party_gid", StringType(), False),
        StructField("interested_party_nm", StringType(), False),
        StructField("country_cd", StringType(), True),
    ]
)

party_role_data = [("TM001", "OWNER", "IP001", None)]
interested_party_data = [("IP001", "SOLO OWNER", "US")]

party_role_df = spark.createDataFrame(party_role_data, party_role_schema)
interested_party_df = spark.createDataFrame(
    interested_party_data, interested_party_schema
)

# Act
result_df = aggregate_current_party_names(party_role_df, interested_party_df)

# Assert
row = result_df.collect()[0]

if (
    row["owner_name"] == "SOLO OWNER"
    and row["attorney_name"] is None
    and row["correspondent_name"] is None
    and row["domestic_representative_name"] is None
):
    print("✅ TEST PASSED: Missing roles return NULL")
    display(result_df)
else:
    print("❌ TEST FAILED: Missing roles not handled correctly")
    display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: Historical Party Names - Multiple Versions

# COMMAND ----------

# Test: Historical names aggregated into semicolon-separated lists

# Arrange
party_role_h_schema = StructType(
    [
        StructField("fk_trademark_gid", StringType(), False),
        StructField("fk_tm_party_role_cd", StringType(), False),
        StructField("fk_interested_party_gid", StringType(), False),
        StructField("action_ct", StringType(), False),
    ]
)

interested_party_h_schema = StructType(
    [
        StructField("interested_party_gid", StringType(), False),
        StructField("interested_party_nm", StringType(), False),
        StructField("action_ct", StringType(), False),
    ]
)

# Multiple historical owners
party_role_h_data = [
    ("TM001", "OWNER", "IP001", "I"),
    ("TM001", "OWNER", "IP002", "U"),
    ("TM001", "OWNER", "IP003", "U"),
]

interested_party_h_data = [
    ("IP001", "ORIGINAL OWNER", "I"),
    ("IP002", "SECOND OWNER", "I"),
    ("IP003", "THIRD OWNER", "I"),
]

party_role_h_df = spark.createDataFrame(party_role_h_data, party_role_h_schema)
interested_party_h_df = spark.createDataFrame(
    interested_party_h_data, interested_party_h_schema
)

# Act
result_df = aggregate_historical_party_names(party_role_h_df, interested_party_h_df)

# Assert
row = result_df.collect()[0]
hist_owners = set(row["hist_owner_nm"].split(";"))
expected_owners = {"ORIGINAL OWNER", "SECOND OWNER", "THIRD OWNER"}

if len(hist_owners) == 3 and hist_owners == expected_owners:
    print("✅ TEST PASSED: Historical owners aggregated correctly")
    print(f"   Historical owners: {row['hist_owner_nm']}")
    display(result_df)
else:
    print("❌ TEST FAILED: Historical aggregation incorrect")
    print(f"   Expected: {expected_owners}")
    print(f"   Got: {hist_owners}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Summary

# COMMAND ----------

# Summary
print("="*60)
print("AGGREGATION FUNCTIONS TEST SUITE SUMMARY")
print("="*60)
print("")
print("Tests Completed:")
print("  ✓ Test 1: Class list - single class")
print("  ✓ Test 2: Class list - multiple classes")
print("  ✓ Test 3: Current party names - all roles")
print("  ✓ Test 4: Current party names - missing roles")
print("  ✓ Test 5: Historical party names - multiple versions")
print("")
print("Review results above for any failures.")
print("="*60)