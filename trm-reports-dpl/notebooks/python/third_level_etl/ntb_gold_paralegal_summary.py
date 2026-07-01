# Databricks notebook source
# MAGIC %md
# MAGIC # Silver to Gold Aggregation
# MAGIC
# MAGIC **Source:** `silver.ttab_paralegal_daily_snapshot_staging_clean` (Validated Data)
# MAGIC **Target:** `gold.ttab_paralegal_daily_summary`
# MAGIC
# MAGIC **Logic:**
# MAGIC 1. Filter for active records (`_is_record_active = true`)
# MAGIC 2. Aggregate metrics by Paralegal + Date
# MAGIC 3. Calculate "TOTAL" row for entire office
# MAGIC 4. Merge into Gold table (Update existing, Insert new)

# COMMAND ----------

import os
import yaml
from datetime import date

# COMMAND ----------

# Config Setup
dbutils.widgets.dropdown("dbx_env", "dev", ["dev", "prod"])
dbutils.widgets.text("summary_date", "", "Summary Date (YYYY-MM-DD)")

env = dbutils.widgets.get("dbx_env")
summary_date_str = dbutils.widgets.get("summary_date")

# Determine Date
if summary_date_str:
    target_date = f"DATE('{summary_date_str}')"
else:
    target_date = "CURRENT_DATE()"

# COMMAND ----------

# Load Config
# Note: In real deployment, use centralized config loader
config_path = f"../../config/{env}/trm_reporting-conf.yaml"
# Fallback for notebook execution path
if not os.path.exists(config_path):
    config_path = f"domain_dq_process/config/{env}/trm_reporting-conf.yaml"

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

catalog = config["schema"]["trgt_catalog"]
print(f"Environment: {env} | Catalog: {catalog} | Date: {target_date}")

# Tables
CLEAN_TABLE = f"{catalog}.silver.ttab_paralegal_daily_snapshot_staging_clean"
GOLD_TABLE = f"{catalog}.gold.ttab_paralegal_daily_summary"
EMPLOYEE_TABLE = f"{catalog}.bronze.employee"

# COMMAND ----------

# Aggregation Logic
summary_sql = f"""
WITH active_data AS (
    SELECT * 
    FROM {CLEAN_TABLE}
    WHERE snapshot_date = {target_date}
      AND _is_record_active = true
),

-- Individual Paralegal Metrics
paralegal_metrics AS (
    SELECT
        snapshot_date AS summary_date,
        paralegal_employee_id AS paralegal_id,
        MAX(paralegal_employee_no) AS paralegal_employee_no,
        
        -- Counts
        SUM(CASE WHEN item_class = 'Document' THEN 1 ELSE 0 END) as document_count,
        SUM(CASE WHEN item_class = 'Folder' THEN 1 ELSE 0 END) as folder_count,
        COUNT(*) as total_records,
        
        -- Assignment Breakdown
        SUM(CASE WHEN assignment_method = 'DIRECT' THEN 1 ELSE 0 END) as count_method_direct,
        SUM(CASE WHEN assignment_method = 'PROCEEDING' THEN 1 ELSE 0 END) as count_method_proceeding,
        SUM(CASE WHEN assignment_method = 'RANGE' THEN 1 ELSE 0 END) as count_method_range,
        
        -- Type Breakdown
        SUM(CASE WHEN object_type = 'OPP' THEN 1 ELSE 0 END) as count_opp,
        SUM(CASE WHEN object_type = 'CAN' THEN 1 ELSE 0 END) as count_can,
        SUM(CASE WHEN object_type NOT IN ('OPP', 'CAN') THEN 1 ELSE 0 END) as count_other,
        
        -- SLA
        SUM(CASE WHEN is_over_7_days = false THEN 1 ELSE 0 END) as records_lte_7_days,
        SUM(CASE WHEN is_over_7_days = true THEN 1 ELSE 0 END) as records_gt_7_days,
        
        -- Age Stats
        SUM(CASE WHEN is_over_7_days = false THEN date_diff_business_days ELSE 0 END) as total_days_lte_7,
        SUM(CASE WHEN is_over_7_days = true THEN date_diff_business_days ELSE 0 END) as total_days_gt_7,
        MAX(date_diff_business_days) as max_age,
        MIN(date_diff_business_days) as min_age
        
    FROM active_data
    GROUP BY 1, 2
),

-- Total Office Metrics (Aggregated)
total_metrics AS (
    SELECT
        summary_date,
        'TOTAL' as paralegal_id,
        NULL as paralegal_employee_no,
        SUM(document_count) as document_count,
        SUM(folder_count) as folder_count,
        SUM(total_records) as total_records,
        SUM(count_method_direct) as count_method_direct,
        SUM(count_method_proceeding) as count_method_proceeding,
        SUM(count_method_range) as count_method_range,
        SUM(count_opp) as count_opp,
        SUM(count_can) as count_can,
        SUM(count_other) as count_other,
        SUM(records_lte_7_days) as records_lte_7_days,
        SUM(records_gt_7_days) as records_gt_7_days,
        SUM(total_days_lte_7) as total_days_lte_7,
        SUM(total_days_gt_7) as total_days_gt_7,
        MAX(max_age) as max_age,
        MIN(min_age) as min_age
    FROM paralegal_metrics
    GROUP BY 1
),

-- Combined
all_metrics AS (
    SELECT * FROM paralegal_metrics
    UNION ALL
    SELECT * FROM total_metrics
)

-- Final Projection with Calculated Fields
SELECT
    m.*,
    
    -- Names
    CASE 
        WHEN m.paralegal_id = 'TOTAL' THEN 'ALL PARALEGALS'
        ELSE COALESCE(CONCAT(e.given_name, ' ', e.family_name), m.paralegal_id) 
    END as paralegal_name,
    
    -- Percentages & Averages
    ROUND(m.records_gt_7_days * 100.0 / NULLIF(m.total_records, 0), 2) as pct_gt_7_days,
    
    ROUND((m.total_days_lte_7 + m.total_days_gt_7) / NULLIF(m.total_records, 0), 2) as avg_age,
    ROUND(m.total_days_lte_7 / NULLIF(m.records_lte_7_days, 0), 2) as avg_age_lte_7,
    ROUND(m.total_days_gt_7 / NULLIF(m.records_gt_7_days, 0), 2) as avg_age_gt_7,
    
    -- Metadata
    {target_date} as source_snapshot_date,
    m.total_records as source_record_count,
    true as dq_validated,
    current_timestamp() as created_at

FROM all_metrics m
LEFT JOIN {EMPLOYEE_TABLE} e ON m.paralegal_employee_no = e.number0
"""

# Check for data before running
cnt = spark.sql(f"SELECT count(*) FROM {CLEAN_TABLE} WHERE snapshot_date = {target_date}").collect()[0][0]
if cnt == 0:
    print(f"⚠️ No clean data found for {target_date}. Skipping Gold aggregation.")
    dbutils.notebook.exit("NO_DATA")

df_summary = spark.sql(summary_sql)
df_summary.createOrReplaceTempView("source_summary")

print(f"✅ Generated summary for {cnt} records")



# COMMAND ----------

# 3. Merge to Gold
spark.sql(f"""
    MERGE INTO {GOLD_TABLE} tgt
    USING source_summary src
    ON tgt.summary_date = src.summary_date 
       AND tgt.paralegal_employee_id = src.paralegal_id
    
    WHEN MATCHED THEN UPDATE SET
        tgt.paralegal_name = src.paralegal_name,
        tgt.paralegal_employee_no = src.paralegal_employee_no,
        tgt.document_count = src.document_count,
        tgt.folder_count = src.folder_count,
        tgt.total_records = src.total_records,
        tgt.count_method_direct = src.count_method_direct,
        tgt.count_method_proceeding = src.count_method_proceeding,
        tgt.count_method_range = src.count_method_range,
        tgt.count_opp = src.count_opp,
        tgt.count_can = src.count_can,
        tgt.count_other = src.count_other,
        tgt.records_lte_7_days = src.records_lte_7_days,
        tgt.records_gt_7_days = src.records_gt_7_days,
        tgt.pct_gt_7_days = src.pct_gt_7_days,
        tgt.total_days_lte_7 = src.total_days_lte_7,
        tgt.total_days_gt_7 = src.total_days_gt_7,
        tgt.avg_age = src.avg_age,
        tgt.avg_age_lte_7 = src.avg_age_lte_7,
        tgt.avg_age_gt_7 = src.avg_age_gt_7,
        tgt.max_age = src.max_age,
        tgt.min_age = src.min_age,
        tgt.created_at = current_timestamp()

    WHEN NOT MATCHED THEN INSERT (
        summary_date, paralegal_employee_id, paralegal_name, paralegal_employee_no,
        document_count, folder_count, total_records,
        count_method_direct, count_method_proceeding, count_method_range,
        count_opp, count_can, count_other,
        records_lte_7_days, records_gt_7_days, pct_gt_7_days,
        total_days_lte_7, total_days_gt_7,
        avg_age, avg_age_lte_7, avg_age_gt_7, max_age, min_age,
        source_snapshot_date, source_record_count, dq_validated, created_at
    ) VALUES (
        src.summary_date, src.paralegal_id, src.paralegal_name, src.paralegal_employee_no,
        src.document_count, src.folder_count, src.total_records,
        src.count_method_direct, src.count_method_proceeding, src.count_method_range,
        src.count_opp, src.count_can, src.count_other,
        src.records_lte_7_days, src.records_gt_7_days, src.pct_gt_7_days,
        src.total_days_lte_7, src.total_days_gt_7,
        src.avg_age, src.avg_age_lte_7, src.avg_age_gt_7, src.max_age, src.min_age,
        src.source_snapshot_date, src.source_record_count, src.dq_validated, current_timestamp()
    )
""")

print(f"✅ Gold table updated: {GOLD_TABLE}")