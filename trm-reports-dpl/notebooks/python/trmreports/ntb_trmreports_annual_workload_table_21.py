# Databricks notebook source
# DBTITLE 1,Load Libraries
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
job_name = "Annual Workload Table 21 Update"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# DBTITLE 1,Calculate Current Fiscal Year
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

# DBTITLE 1,Query Current FY Filing Counts by Country
# Query filings_dashboard to count filings by country for current fiscal year
# Maps 'Unknown' to 'Other¹' and excludes 'United States of America'
# Note that 'Other¹' is specified by the Workload tables and has been tested as valid in the database.

current_fy_query = f"""
SELECT 
    CASE 
        WHEN country_or_area_name = 'Unknown' THEN 'Other¹'
        ELSE country_or_area_name
    END AS residence,
    {fiscal_year} AS fiscal_year,
    COUNT(ser_num) AS application_count
FROM `{reporting_catalog}`.gold.filings_dashboard
WHERE filing_fy = {fiscal_year}
    AND country_or_area_name IS NOT NULL
    AND country_or_area_name <> 'United States of America'
GROUP BY 
    CASE 
        WHEN country_or_area_name = 'Unknown' THEN 'Other¹'
        ELSE country_or_area_name
    END
ORDER BY residence
"""


# Execute query
current_fy_df = spark.sql(current_fy_query)
current_fy_df.createOrReplaceTempView("current_fy_data")

# COMMAND ----------

# DBTITLE 1,MERGE Current Fiscal Year Data
merge_query = f"""
MERGE INTO `{reporting_catalog}`.gold.annual_workload_table_21 AS target
USING (
    SELECT 
        residence,
        fiscal_year,
        application_count
    FROM current_fy_data
) AS source
ON target.fiscal_year = source.fiscal_year AND target.residence = source.residence
WHEN MATCHED THEN
    UPDATE SET
        target.application_count = source.application_count
WHEN NOT MATCHED THEN
    INSERT (residence, fiscal_year, application_count)
    VALUES (source.residence, source.fiscal_year, source.application_count)
"""

# Execute the MERGE
spark.sql(merge_query)

# COMMAND ----------

# DBTITLE 1,End Job Control
target_table_name = f"`{reporting_catalog}`.gold.annual_workload_table_21"
recs_count = spark.sql(f"SELECT COUNT(*) FROM {target_table_name}").first()[0]

end_job_cntl(
    f"{reporting_catalog}.silver",
    job_name,
    job_start_ts,
    "completed",
    recs_count,
    "job completed successfully",
)
dbutils.notebook.exit(f"Completed updating Annual Workload Table 21 for FY {fiscal_year}")
