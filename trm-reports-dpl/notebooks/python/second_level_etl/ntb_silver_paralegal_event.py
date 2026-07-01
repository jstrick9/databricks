# Databricks notebook source
import sys
import os
import yaml
from utils.path_utils import get_repo_root

# COMMAND ----------

dbutils.widgets.dropdown("dbx_env", "dev", ["dev", "prod"])
dbutils.widgets.text("event_date", "", "Event Date (YYYY-MM-DD)")

env = dbutils.widgets.get("dbx_env")
event_date_str = dbutils.widgets.get("event_date")

# Load Config
config_path = f"../../config/{env}/trm_reporting-conf.yaml"
if not os.path.exists(config_path):
    config_path = f"domain_dq_process/config/{env}/trm_reporting-conf.yaml"

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

catalog = config["schema"]["trgt_catalog"]
print(f"Environment: {env} | Catalog: {catalog}")

CLEAN_TABLE = f"{catalog}.silver.ttab_paralegal_daily_snapshot_staging_clean"
EVENT_TABLE = f"{catalog}.silver.ttab_item_event_log"

# COMMAND ----------

if event_date_str:
    curr_date = f"DATE('{event_date_str}')"
else:
    curr_date = "CURRENT_DATE()"

# Find the most recent snapshot BEFORE the current date
# This handles weekends or failed runs gracefully
prev_date_row = spark.sql(f"""
    SELECT MAX(snapshot_date) 
    FROM {CLEAN_TABLE} 
    WHERE snapshot_date < {curr_date}
      AND _is_record_active = true
""").collect()[0][0]

if not prev_date_row:
    print("⚠️ No previous snapshot found. This appears to be the first run.")
    print("   All records will be marked as NEW_ARRIVAL.")
    prev_date = "DATE('1900-01-01')" # Dummy date
else:
    prev_date = f"DATE('{prev_date_row}')"

print(f"Current Snapshot:  {curr_date}")
print(f"Previous Snapshot: {prev_date}")

# COMMAND ----------

event_sql = f"""
WITH curr AS (
    SELECT * FROM {CLEAN_TABLE} WHERE snapshot_date = {curr_date} AND _is_record_active = true
),
prev AS (
    SELECT * FROM {CLEAN_TABLE} WHERE snapshot_date = {prev_date} AND _is_record_active = true
),
changes AS (
    SELECT
        -- Identifiers (Coalesce to get ID from whichever side exists)
        COALESCE(curr.object_id, prev.object_id) as object_id,
        COALESCE(curr.reference_number, prev.reference_number) as reference_number,
        COALESCE(curr.serial_prod_num, prev.serial_prod_num) as serial_prod_num,
        COALESCE(curr.item_class, prev.item_class) as item_class,
        
        -- State Info
        prev.paralegal_employee_id as prev_paralegal_id,
        curr.paralegal_employee_id as curr_paralegal_id,
        prev.received_date as prev_received_date,
        
        -- Logic
        CASE
            WHEN prev.object_id IS NULL THEN 'NEW_ARRIVAL'
            WHEN curr.object_id IS NULL THEN 'COMPLETED'
            WHEN prev.paralegal_employee_id != curr.paralegal_employee_id THEN 'REASSIGNED'
            ELSE 'NO_CHANGE'
        END as event_type
        
    FROM curr
    FULL OUTER JOIN prev ON curr.object_id = prev.object_id
)

SELECT
    uuid() as event_id,
    {curr_date} as event_date,
    event_type,
    object_id,
    reference_number,
    serial_prod_num,
    item_class,
    prev_paralegal_id,
    curr_paralegal_id,
    
    -- Calc days in queue for completed items
    CASE 
        WHEN event_type = 'COMPLETED' THEN DATEDIFF({curr_date}, prev_received_date)
        ELSE NULL
    END as days_since_arrival,
    
    current_timestamp() as created_at

FROM changes
WHERE event_type != 'NO_CHANGE'
"""

df_events = spark.sql(event_sql)
count = df_events.count()

print(f"Generated {count:,} events.")

# COMMAND ----------

# Write to Event Log
# We overwrite the partition for idempotency (in case of re-runs for same day)

if count > 0:
    print(f"Writing to {EVENT_TABLE}...")
    
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    df_events.write \
        .format("delta") \
        .mode("overwrite") \
        .insertInto(EVENT_TABLE)
        
    print("✅ Event log updated.")
    
    # Display Stats
    display(df_events.groupBy("event_type").count())
else:
    print("✅ No events detected (Queue state is identical to previous run).")