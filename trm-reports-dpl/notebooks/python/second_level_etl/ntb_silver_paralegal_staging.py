# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze to Silver Staging
# MAGIC **Purpose:** Materialize daily snapshot by applying 3-tier assignment logic.
# MAGIC **Output:** `silver.ttab_paralegal_daily_snapshot_staging`

# COMMAND ----------

import sys
import os
import yaml

# COMMAND ----------

def register_domain_dq_path():
    """
    Finds 'domain_dq_process' folder relative to this notebook.
    Notebook location: notebooks/python/silver/ntb_paralegal_staging
    Target location:   notebooks/python/data_quality/domain_dq_process
    """
    # 1. Deployment Env Var
    if "REPO_ROOT" in os.environ:
        path = os.path.join(os.environ["REPO_ROOT"], "domain_dq_process")
        if os.path.exists(path):
            if path not in sys.path: sys.path.insert(0, path)
            print(f"✅ Found via REPO_ROOT: {path}")
            return

    current_dir = os.getcwd()
    
    # 2. Relative Search (Up one level, then into data_quality)
    # Expected relative path: ../data_quality/domain_dq_process
    candidates = [
        os.path.abspath(os.path.join(current_dir, "../data_quality/domain_dq_process")), # From notebooks/python/silver
        os.path.abspath(os.path.join(current_dir, "domain_dq_process")),                 # If colocated
        os.path.abspath(os.path.join(current_dir, "../../data_quality/domain_dq_process")),
        "/Workspace/Shared/trm-reports-dpl-bundle/notebooks/python/data_quality/domain_dq_process"
    ]

    for candidate in candidates:
        if os.path.exists(candidate):
            if candidate not in sys.path:
                sys.path.insert(0, candidate)
            print(f"✅ Found path: {candidate}")
            return

    raise FileNotFoundError("Could not find 'domain_dq_process' directory. Please set REPO_ROOT or check directory structure.")

register_domain_dq_path()

# COMMAND ----------

from utils.hash_utils import add_hashes
from utils.path_utils import get_repo_root

# COMMAND ----------

# Parameters
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env").strip().lower()

repo_root = get_repo_root()
config_path = os.path.join(repo_root, "config", dbx_env, "trm_reporting-conf.yaml")

if not os.path.exists(config_path):
    raise FileNotFoundError(f"Config not found at: {config_path}")

with open(config_path, "r") as f:
    cfg = yaml.safe_load(f)

PHYSICAL_CATALOG = cfg["schema"]["trgt_catalog"]
BRONZE = f"{PHYSICAL_CATALOG}.bronze"
SILVER = f"{PHYSICAL_CATALOG}.silver"
STAGING_TABLE = f"{SILVER}.ttab_paralegal_daily_snapshot_staging"
SILVER_VIEW_LOGICAL = f"{PHYSICAL_CATALOG}.silver" 

print(f"Environment: {dbx_env}")
print(f"Bronze:      {BRONZE}")
print(f"Staging:     {STAGING_TABLE}")

# COMMAND ----------

# DBTITLE 1,Execute Snapshot Logic (Overwrite today's partition in STAGING)
query = f"""
WITH base_queue AS (
    -- Main Paralegal Queue
    SELECT * FROM {BRONZE}.queues WHERE queue = 'Paralegal'
    
    EXCEPT  -- Equivalent to MINUS in Spark SQL
    
    -- Removing folders in paralegal team queue
    SELECT * FROM {BRONZE}.queues WHERE TYPE = 3 AND INTVAR2 = 2 AND queue = 'Paralegal'
    
    EXCEPT  -- Equivalent to MINUS in Spark SQL
    
    -- Removing documents in paralegal team queue
    SELECT * FROM {BRONZE}.queues WHERE TYPE = 2 AND INTVAR3 = 2 AND queue = 'Paralegal'
)

SELECT
    CURRENT_DATE() AS snapshot_date,
    CURRENT_TIMESTAMP() AS snapshot_timestamp,
    
    -- Assignment Logic (Matches Original: Direct STRVAR14 or Range Rule)
    COALESCE(q.STRVAR14, b.user_id) AS paralegal_employee_id,
    COALESCE(tu.fk_employeenumber0, b.fk_employeenumber0) AS paralegal_employee_no,
    
    -- Metadata
    CASE 
        WHEN q.STRVAR14 IS NOT NULL THEN 'DIRECT'
        ELSE 'RANGE'
    END AS assignment_method,
    
    -- Identifiers
    q.id AS object_id,
    q.NAME AS reference_number,
    -- We assume NAME holds the reference number based on your query
    -- Original query mapped: a.NAME as reference_number
    
    -- Note: Your original query didn't select serial_prod_num explicitly from attributes table
    -- but usually you need it for tracking. I will map it from NAME if it looks like one,
    -- or leave NULL if you want strict adherence to the original query's output columns.
    q.NAME AS serial_prod_num, 
    NULL AS registration_number, -- Not in original query
    
    -- Classification
    CASE 
        WHEN q.TYPE = 2 THEN 'Document' 
        WHEN q.TYPE = 3 THEN 'Folder' 
        ELSE 'Unknown' 
    END AS item_class,
    
    q.TYPE AS object_type, -- Mapped from original: a.TYPE as object_type
    q.STRVAR2 AS proceedingtype, -- Mapped from: a.STRVAR2 as proceeding_type
    q.STRVAR3 AS documenttype,   -- Mapped from: a.STRVAR3 as document_type
    
    -- Dates
    CAST(q.TIME_STAMP AS TIMESTAMP) AS received_timestamp,
    CAST(q.TIME_STAMP AS DATE) AS received_date,
    
    -- Aging Calculation
    {SILVER_VIEW_LOGICAL}.business_days_between(CAST(q.TIME_STAMP AS DATE), CURRENT_DATE()) AS date_diff_business_days,
    
    CASE 
        WHEN {SILVER_VIEW_LOGICAL}.business_days_between(CAST(q.TIME_STAMP AS DATE), CURRENT_DATE()) > 7 THEN TRUE 
        ELSE FALSE 
    END AS is_over_7_days,
    
    q.queue AS queue_name,
    q.id AS source_object_id,
    
    -- Placeholders for DQ
    CAST(NULL AS STRING) as _natural_key_hash,
    CAST(NULL AS STRING) as _record_data_hash,
    CURRENT_TIMESTAMP() as _created_timestamp,
    CAST(NULL AS STRING) as _dq_run_id

FROM base_queue q
LEFT JOIN {BRONZE}.ttab_user tu ON q.STRVAR14 = RTRIM(tu.user_id)
JOIN (
    SELECT tu_inner.user_id, tu_inner.fk_employeenumber0, par.BEGIN_RANGE_NO, par.END_RANGE_NO 
    FROM {BRONZE}.ttab_user tu_inner
    JOIN {BRONZE}.paralegal_assignment_rule par 
      ON tu_inner.fk_employeenumber0 = par.fk_paralegal_employee_no 
) b ON CAST(q.INTVAR1 AS INT) >= b.BEGIN_RANGE_NO 
   AND CAST(q.INTVAR1 AS INT) <= b.END_RANGE_NO

WHERE CAST(q.INTVAR1 AS INT) >= 0
"""

print("Executing Snapshot Query...")
df = spark.sql(query)

display(df)

# COMMAND ----------

# Load Hash Config to ensure consistency with DQ process
hash_config_path = os.path.join(repo_root, "hash_configs", "trm_reporting", "silver", "ttab_paralegal_daily_snapshot_staging_hash_config.yml")

if os.path.exists(hash_config_path):
    with open(hash_config_path, "r") as f:
        hash_cfg = yaml.safe_load(f)
    
    nk_cols = hash_cfg["natural_key_columns"]
    data_cols = hash_cfg.get("data_hash_columns")
else:
    # Fallback if config file missing (Hardcoded safety net)
    print("⚠️ Warning: Hash config file not found. Using defaults.")
    nk_cols = ["snapshot_date", "object_id"]
    data_cols = ["paralegal_employee_id", "serial_prod_num", "item_class"]

df_hashed = add_hashes(
    df, 
    natural_key_columns=nk_cols,
    data_hash_columns=data_cols, 
    hash_algorithm="sha256"
)

# COMMAND ----------

# =========================================================================
# 5. Write to Staging
# =========================================================================
print(f"Writing to {STAGING_TABLE}...")

# Ensure partition overwrites are safe
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# SCD2 columns (_created_date, _updated_timestamp, _is_record_active) belong in the _clean table.
cols_to_drop = ["_created_date", "_updated_timestamp", "_is_record_active"]
df_to_write = df_hashed.drop(*cols_to_drop)

target_columns = spark.table(STAGING_TABLE).columns
df_to_write = df_to_write.select(target_columns)

df_to_write.write \
    .mode("overwrite") \
    .format("delta") \
    .insertInto(STAGING_TABLE)

print(f"✅ Successfully staged {df_to_write.count()} records.")