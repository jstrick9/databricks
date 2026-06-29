# Databricks notebook source
# DBTITLE 1,Imports
import pytz
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("dbx_env", "dev", "Database Environment")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file = f"../../config/{dbx_env}/tdet-conf.yaml"

job_name = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
    .split("/")[-1]
)
job_start_ts = datetime.now(pytz.timezone('US/Eastern'))
print(f"{config_file=}\n\n{job_name=}\n\n{job_start_ts=}")

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,Read Configuration
configs = read_yaml(config_file)
tdet_catalog = configs["schema"]["trgt_catalog"]

print(f"Catalog: {tdet_catalog}")

# COMMAND ----------

# DBTITLE 1,Create Gold Schema (if not exists)
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {tdet_catalog}.gold
    COMMENT 'TDET Gold Layer - Business Unit accessible views'
""")
print(f"✅ Schema {tdet_catalog}.gold verified.")

# COMMAND ----------

# DBTITLE 1,Create TDET App Search Gold View
view_exists = spark.catalog.tableExists(f"{tdet_catalog}.gold.tdet_app_search_vw")

if not view_exists:
    spark.sql(f"""
        CREATE VIEW {tdet_catalog}.gold.tdet_app_search_vw AS
        SELECT
            serial_number,
            mark_tx,
            filing_date,
            filed_bases,
            current_bases,
            registration_number,
            registration_date,
            owner_name,
            owner_name_historical,
            owner_address,
            owner_country,
            owner_email,
            owner_email_historical,
            owner_phone,
            attorney_membership_number,
            attorney_name,
            attorney_name_historical,
            attorney_address,
            attorney_email,
            attorney_email_historical,
            attorney_phone,
            correspondent_name,
            correspondent_name_historical,
            correspondent_address,
            correspondent_email,
            correspondent_email_secondary,
            correspondent_email_historical,
            correspondent_phone,
            domestic_representative_name,
            domestic_representative_name_historical,
            domestic_representative_email,
            domestic_representative_email_historical,
            domestic_representative_phone,
            examiner_number,
            examiner_name,
            docket_number,
            firm_name,
            law_office,
            class_list,
            status,
            status_date,
            og_issue_date,
            og_status,
            og_category,
            international_registration_number,
            international_us_reference_number,
            specimen_url,
            _created_date AS data_as_of_date
        FROM {tdet_catalog}.silver.tdet_app_search
        WHERE _is_record_active = true
    """)
    print(f"✅ {tdet_catalog}.gold.tdet_app_search_vw CREATED.")
else:
    print(f"✅ {tdet_catalog}.gold.tdet_app_search_vw already exists (no recreation needed).")

# COMMAND ----------

# DBTITLE 1,Create TDET PH Events Gold View
view_exists = spark.catalog.tableExists(f"{tdet_catalog}.gold.tdet_app_ph_events_vw")

if not view_exists:
    spark.sql(f"""
        CREATE VIEW {tdet_catalog}.gold.tdet_app_ph_events_vw AS
        SELECT
            serial_number,
            ph_action_code,
            ph_action_date,
            ph_action_description,
            ph_action_title,
            object_gid,
            business_event_reason_id,
            event_created_ts,
            event_updated_ts,
            _created_date AS data_as_of_date
        FROM {tdet_catalog}.silver.tdet_app_ph_events
    """)
    print(f"✅ {tdet_catalog}.gold.tdet_app_ph_events_vw CREATED.")
else:
    print(f"✅ {tdet_catalog}.gold.tdet_app_ph_events_vw already exists (no recreation needed).")

# COMMAND ----------

# DBTITLE 1,Lightweight Verification
print("Verifying gold views...\n")

# Use SELECT serial_number + .head(1) (returns row directly, no separate count job)
search_ok = len(spark.sql(f"SELECT serial_number FROM {tdet_catalog}.gold.tdet_app_search_vw LIMIT 1").head(1)) > 0
ph_ok = len(spark.sql(f"SELECT serial_number FROM {tdet_catalog}.gold.tdet_app_ph_events_vw LIMIT 1").head(1)) > 0

print(f"  tdet_app_search_vw   : {'✅ OK' if search_ok else '❌ EMPTY'}")
print(f"  tdet_app_ph_events_vw: {'✅ OK' if ph_ok else '❌ EMPTY'}")

# COMMAND ----------

# DBTITLE 1,Summary
end_time = datetime.datetime.now(pytz.timezone('US/Eastern'))
total_seconds = (end_time - job_start_ts).total_seconds()
minutes = int(total_seconds // 60)
seconds = int(total_seconds % 60)

print("=" * 60)
print("GOLD LAYER VIEWS SUMMARY")
print("=" * 60)
print(f"")
print(f"  Completed at {end_time}")
print(f"  Total time: {minutes} minutes and {seconds} seconds")
print("=" * 60)
