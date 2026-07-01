# Databricks notebook source
# DBTITLE 1,Load Libraries
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType
from functools import reduce
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Parameters and Configs
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=},{dbx_env=}")

# COMMAND ----------

# DBTITLE 1,Execute common function ntbk
# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,Environment Parameter Values
common_configs = read_yaml(config_file)
reporting_catalog = common_configs["schema"]["reporting_catalog"]
trgt_catalog = common_configs["schema"]["trgt_catalog"]
tmprodvty_catalog = common_configs["schema"]["tmprodvty_catalog"]

# COMMAND ----------

# DBTITLE 1,Print values
print(f"{reporting_catalog=},{trgt_catalog=}")

# COMMAND ----------

# DBTITLE 1,Start Job Control
job_name = "Annual Workload Historical Tables"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# DBTITLE 0,Phase Gates
from datetime import datetime

# Get current date
current_date = datetime.now()
current_year = current_date.year
current_month = current_date.month

# US Federal Fiscal Year: October 1 - September 30
# If current month >= October (10), we're in FY current_year + 1
# If current month < October, we're in FY current_year
if current_month >= 10:
    fiscal_year = current_year + 1
else:
    fiscal_year = current_year

print(f"Current Date: {current_date.strftime('%Y-%m-%d')}")
print(f"Current Fiscal Year: {fiscal_year}")

# COMMAND ----------

# DBTITLE 1,Extract Source Data
# First, check if any data exists for the current fiscal year in both source tables
check_query = f"""
SELECT
    (SELECT COUNT(*) FROM `{reporting_catalog}`.gold.process_production_staffing_report_rolling
     WHERE year = {fiscal_year} AND fy_month_int BETWEEN 1 AND 12) as rolling_rows,
    (SELECT COUNT(*) FROM `{reporting_catalog}`.gold.process_production_staffing_report_non_rolling
     WHERE year = '{fiscal_year}' AND fy_month_int BETWEEN 1 AND 12) as non_rolling_rows
"""

check_result = spark.sql(check_query).first()
rolling_rows = check_result['rolling_rows']
non_rolling_rows = check_result['non_rolling_rows']

print(f"Fiscal Year Validation Check:")
print(f"  - Target Fiscal Year: {fiscal_year}")
print(f"  - Rows in rolling source for FY {fiscal_year}: {rolling_rows}")
print(f"  - Rows in non-rolling source for FY {fiscal_year}: {non_rolling_rows}")
print("\n" + "="*80 + "\n")

# Query to get the latest data by joining rolling and non-rolling tables
source_query = f"""
SELECT 
    {fiscal_year} as fiscal_year,
    r.total_applications_filed_classes_fy_actual as registration_filed_count,
    nr.section_9_applications_filed as renewal_filed_count,
    nr.affidavits_under_section_8_15_71_combinations_filed_fy as s8_affidavit_filed_count,
    nr.certificates_of_registration_issued_cases_fy as certs_reg_issued_count,
    nr.registrations_renewed_fy as renewed_count,
    NULL as pub_12c_count,
    nr.registrations_including_classes_fy as reg_inc_class_count,
    r.fy_month_int
FROM `{reporting_catalog}`.gold.process_production_staffing_report_rolling r
JOIN `{reporting_catalog}`.gold.process_production_staffing_report_non_rolling nr
    ON CAST(r.year AS STRING) = nr.year AND r.fy_month_int = nr.fy_month_int
WHERE r.year = {fiscal_year}
    AND r.fy_month_int BETWEEN 1 AND 12
ORDER BY r.fy_month_int DESC
LIMIT 1
"""

# Execute query
source_df = spark.sql(source_query)
row_count = source_df.count()

if row_count == 0:
    # No data found - create a row with NULLs
    print(f"No data found in source tables for fiscal year {fiscal_year}")
    
    if rolling_rows == 0:
        print(f"   Reason: No rows in rolling table for year = {fiscal_year}")
    elif non_rolling_rows == 0:
        print(f"   Reason: No rows in non-rolling table for year = {fiscal_year}")
    else:
        print(f"   Reason: Rows exist in both tables but join produced no matches")
    
    print(f"\n   Creating row for FY {fiscal_year} with NULL values for all metrics")
    print("   This row will be updated when source data becomes available.\n")
    
    # Create a DataFrame with NULL values for all metrics
    
    schema = StructType([
        StructField("fiscal_year", IntegerType(), False),
        StructField("registration_filed_count", IntegerType(), True),
        StructField("renewal_filed_count", IntegerType(), True),
        StructField("s8_affidavit_filed_count", IntegerType(), True),
        StructField("certs_reg_issued_count", IntegerType(), True),
        StructField("renewed_count", IntegerType(), True),
        StructField("pub_12c_count", IntegerType(), True),
        StructField("reg_inc_class_count", IntegerType(), True),
        StructField("fy_month_int", IntegerType(), True),
        StructField("last_update_ts", TimestampType(), True)
    ])
    
    source_df = spark.createDataFrame(
        [(fiscal_year, None, None, None, None, None, None, None, None, None)],
        schema=schema
    )
    
    print("Created DataFrame with NULL values:")
    display(source_df)
else:
    # Data found
    row_data = source_df.first()
    max_month = row_data['fy_month_int']
    display(source_df)

# Create temporary view for MERGE operation
source_df.createOrReplaceTempView("source_data")

# COMMAND ----------

# DBTITLE 1,Row Count
before_count_query = f"""
SELECT COUNT(*) as row_count
FROM `{reporting_catalog}`.gold.annual_workload_table_16_18
"""

before_count = spark.sql(before_count_query).first()[0]
print(f"Row count before MERGE: {before_count}")

# Check if current fiscal year already exists
existing_row_query = f"""
SELECT *
FROM `{reporting_catalog}`.gold.annual_workload_table_16_18
WHERE fiscal_year = {fiscal_year}
"""

existing_row_df = spark.sql(existing_row_query)
existing_row_count = existing_row_df.count()

if existing_row_count > 0:
    print(f"\nFiscal year {fiscal_year} already exists in target table - will UPDATE")
    print("\nExisting row:")
    display(existing_row_df)
else:
    print(f"\nFiscal year {fiscal_year} does not exist in target table - will INSERT")

# COMMAND ----------

# DBTITLE 1,Merge to Table
target_table_name = f"{trgt_catalog}.gold.annual_workload_table_16_18"
merge_query = f"""
MERGE INTO {target_table_name} AS target
USING (
    SELECT 
        fiscal_year,
        registration_filed_count,
        renewal_filed_count,
        s8_affidavit_filed_count,
        certs_reg_issued_count,
        renewed_count,
        pub_12c_count,
        reg_inc_class_count
    FROM source_data
) AS source
ON target.fiscal_year = source.fiscal_year
WHEN MATCHED THEN
    UPDATE SET
        target.registration_filed_count = source.registration_filed_count,
        target.renewal_filed_count = source.renewal_filed_count,
        target.s8_affidavit_filed_count = source.s8_affidavit_filed_count,
        target.certs_reg_issued_count = source.certs_reg_issued_count,
        target.renewed_count = source.renewed_count,
        target.pub_12c_count = source.pub_12c_count,
        target.reg_inc_class_count = source.reg_inc_class_count
WHEN NOT MATCHED THEN
    INSERT (
        fiscal_year,
        registration_filed_count,
        renewal_filed_count,
        s8_affidavit_filed_count,
        certs_reg_issued_count,
        renewed_count,
        pub_12c_count,
        reg_inc_class_count
    )
    VALUES (
        source.fiscal_year,
        source.registration_filed_count,
        source.renewal_filed_count,
        source.s8_affidavit_filed_count,
        source.certs_reg_issued_count,
        source.renewed_count,
        source.pub_12c_count,
        source.reg_inc_class_count
    )
"""

print("Executing MERGE operation...")
print("\n" + "="*80 + "\n")

# Execute the MERGE
spark.sql(merge_query)

print(f"MERGE operation completed successfully for fiscal year {fiscal_year}")

# COMMAND ----------

# DBTITLE 1,End Job Control
recs_count = spark.table(target_table_name).count()

end_job_cntl(
    f"{reporting_catalog}.silver",
    job_name,
    job_start_ts,
    "completed",
    recs_count,
    "job completed successfully",
)
dbutils.notebook.exit(f"Completed Loading Annual Workload Historical Tables")