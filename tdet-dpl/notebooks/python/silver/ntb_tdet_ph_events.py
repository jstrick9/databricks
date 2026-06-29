# Databricks notebook source
# DBTITLE 1,Imports
import pytz
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, when, trim, current_date,
    current_timestamp, sha2, concat_ws, broadcast
)

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("dbx_env", "dev", "Database Environment")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

dbutils.widgets.text("load_method", "", "Load Method (Initial or Incremental)")
load_method = dbutils.widgets.get("load_method").rstrip()

dbutils.widgets.text("merge_lookback_days", "90", "MERGE Lookback Days (target partition pruning)")
merge_lookback_days = int(dbutils.widgets.get("merge_lookback_days").rstrip())

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
print(f"{merge_lookback_days=}")

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,Read Configuration
configs = read_yaml(config_file)
tmngpdb_catalog = configs["schema"]["source_tmngpdb_catalog"]
tdet_catalog = configs["schema"]["trgt_catalog"]

target_ph_table = f"{tdet_catalog}.silver.tdet_app_ph_events"

print(f"Source PH Catalog  : {tmngpdb_catalog}")
print(f"Target TDET Catalog: {tdet_catalog}")
print(f"Target PH Table    : {target_ph_table}")

# COMMAND ----------

# DBTITLE 1,Determine Load Method
table_exists = spark.catalog.tableExists(target_ph_table)

if not table_exists:
    load_method = 'Initial'
elif load_method == '':
    load_method = 'Incremental'

print(f"\033[1mTable exists:\033[0m {table_exists}")
print(f"\033[1mLoad method:\033[0m {load_method}")

# COMMAND ----------

# DBTITLE 1,Spark Configuration
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "auto")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

print("Spark configuration set.")

# COMMAND ----------

# DBTITLE 1,Step 1 - Read Active Serial Numbers
print("Step 1: Reading active serial numbers from tdet_app_search...\n")

active_serials_df = (
    spark.table(f"{tdet_catalog}.silver.tdet_app_search")
    .filter(col("_is_record_active") == True)
    .select(
        col("serial_number").cast("int").alias("serial_number")
    )
    .distinct()
)

# COMMAND ----------

# DBTITLE 1,Step 2 - Load and Cache Lookup Table
print("Step 2: Loading lookup table...\n")

stnd_reason_df = (
    spark.table(f"{tmngpdb_catalog}.bronze.stnd_business_event_reason")
    .select(
        col("business_event_reason_id"),
        col("business_event_reason_cd").alias("ph_action_code"),
        col("description_tx").alias("ph_action_description"),
        col("title_tx").alias("ph_action_title")
    )
    .cache()
)

reason_count = stnd_reason_df.count()
print(f"  stnd_business_event_reason cached: {reason_count:,} rows")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {tdet_catalog}.silver.tdet_watermarks (
        table_name STRING,
        last_run_ts TIMESTAMP,
        last_run_dt DATE,
        rows_processed LONG,
        load_method STRING
    )
    USING DELTA
""")

# Read watermark at start of run
def get_watermark(table_name):
    result = spark.sql(f"""
        SELECT MAX(last_run_ts) AS max_ts
        FROM {tdet_catalog}.silver.tdet_watermarks
        WHERE table_name = '{table_name}'
    """).collect()[0]["max_ts"]
    return result

# Write watermark at end of successful run
def save_watermark(table_name, rows_processed, load_method):
    spark.sql(f"""
        INSERT INTO {tdet_catalog}.silver.tdet_watermarks
        VALUES (
            '{table_name}',
            current_timestamp(),
            current_date(),
            {rows_processed},
            '{load_method}'
        )
    """)

# COMMAND ----------

# DBTITLE 1,Step 3 - Determine Watermark for Incremental
if load_method == 'Incremental':
    watermark_ts = get_watermark("tdet_app_ph_events")
    
    if watermark_ts is None:
        print(" ⚠️ No existing data found — switching to Initial load.")
        load_method = 'Initial'
    else:
        print(f"  Watermark (last loaded event): {watermark_ts}")
        print(f"  Only processing events modified after this timestamp.")
else:
    watermark_ts = None
    print("  Initial load — processing ALL events.")

# COMMAND ----------

# DBTITLE 1,Step 4 - Read Business Events (Full or Incremental)
print(f"Step 4: Reading business_event ({load_method})...\n")

# Base read with pre-extracted serial number
business_event_base = (
    spark.table(f"{tmngpdb_catalog}.bronze.business_event")
    .withColumn(
        "serial_number_int",
        F.expr("TRY_CAST(TRIM(SPLIT(cfk_object_gid, ':')[2]) AS INT)")
    )
    .filter(col("serial_number_int").isNotNull())
)

# Apply watermark filter for incremental
if load_method == 'Incremental':
    business_event_clean_df = (
        business_event_base
        .filter(col("last_mod_ts") > lit(watermark_ts))
    )
    be_count = business_event_clean_df.count()
    print(f"  New/modified business events since watermark: {be_count:,}")
    
    if be_count == 0:
        print("\n✅ No new events to process. Incremental load complete.")
        save_watermark("tdet_app_ph_events", 0, load_method)
        print(f"  Watermark saved (0 rows processed).")
        
        # Release cached DataFrames
        stnd_reason_df.unpersist()
        dbutils.notebook.exit("No new events to process.")
else:
    business_event_clean_df = business_event_base
    print("  Reading ALL business events (Initial load).")

# Select only needed columns
business_event_clean_df = business_event_clean_df.select(
    col("business_event_id"),
    col("cfk_object_gid"),
    col("fk_business_event_reason_id"),
    col("effective_ts"),
    col("create_ts").alias("event_created_ts"),
    col("last_mod_ts").alias("event_updated_ts"),
    col("serial_number_int")
)

# COMMAND ----------

# DBTITLE 1,Step 5 - Join Tables
print("Step 5: Joining tables...\n")

ph_df = (
    business_event_clean_df
    # Join 1: Broadcast the small lookup table
    .join(
        broadcast(stnd_reason_df),
        col("fk_business_event_reason_id") == col("business_event_reason_id"),
        "inner"
    )
    # Join 2: Filter to only TDET active serials
    .join(
        active_serials_df,
        col("serial_number_int") == col("serial_number"),
        "inner"
    )
    .select(
        col("serial_number_int").cast("string").alias("serial_number"),
        col("ph_action_code"),
        F.to_date(col("effective_ts")).alias("ph_action_date"),
        col("cfk_object_gid").alias("object_gid"),
        col("fk_business_event_reason_id").alias("business_event_reason_id"),
        col("ph_action_description"),
        col("ph_action_title"),
        col("event_created_ts"),
        col("event_updated_ts")
    )
    .dropDuplicates(["serial_number", "ph_action_code", "ph_action_date", "object_gid"])
)

ph_count = ph_df.count()
print(f"  PH Events to process: {ph_count:,}")

# COMMAND ----------

# DBTITLE 1,Step 6 - Add Metadata and Write
print(f"Step 6: Writing ({load_method})...\n")

ph_enriched_df = (
    ph_df
    .withColumn("_created_date", current_date())
    .withColumn("_created_timestamp", current_timestamp())
    .withColumn(
        "_row_hash",
        sha2(
            concat_ws("||",
                col("serial_number"),
                col("ph_action_code"),
                col("ph_action_date"),
                col("object_gid")
            ),
            256
        )
    )
)

if load_method == 'Initial':
    (
        ph_enriched_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "false")
        .saveAsTable(target_ph_table)
    )
    print(f"  Initial load complete: {ph_count:,} rows written.")

    save_watermark("tdet_app_ph_events", ph_count, load_method)
    print(f"  Watermark saved.")

else:
    ph_enriched_df.createOrReplaceTempView("ph_updates")
    
    print(f"  MERGE lookback: {merge_lookback_days} days")
    print(f"  Only scanning target partitions from last {merge_lookback_days} days.")
    
    merge_sql = f"""
    MERGE INTO {target_ph_table} AS target
    USING ph_updates AS source
    ON target._row_hash = source._row_hash
    AND target.ph_action_date >= DATE_SUB(CURRENT_DATE(), {merge_lookback_days})
    WHEN MATCHED THEN UPDATE SET
        target.ph_action_code        = source.ph_action_code,
        target.ph_action_date        = source.ph_action_date,
        target.ph_action_description = source.ph_action_description,
        target.ph_action_title       = source.ph_action_title,
        target.event_created_ts      = source.event_created_ts,
        target.event_updated_ts      = source.event_updated_ts,
        target._created_date         = source._created_date,
        target._created_timestamp    = source._created_timestamp
    WHEN NOT MATCHED THEN INSERT *
    """
    
    spark.sql(merge_sql)
    print(f"  Incremental MERGE complete: {ph_count:,} rows processed.")

    save_watermark("tdet_app_ph_events", ph_count, load_method)
    print(f"  Watermark saved.")

stnd_reason_df.unpersist()

# COMMAND ----------

# DBTITLE 1,Step 7 - Optimize
print("Step 7: Optimizing table...\n")

if load_method == 'Initial':
    # Full Z-ORDER on initial load
    spark.sql(f"""
        OPTIMIZE {target_ph_table}
        ZORDER BY (serial_number, ph_action_date, ph_action_code)
    """)
    print(f"  ✅ Full OPTIMIZE + ZORDER complete.")
else:
    # Lightweight optimize on incremental (just compact new files)
    spark.sql(f"OPTIMIZE {target_ph_table}")
    print(f"  ✅ Incremental OPTIMIZE complete.")

# COMMAND ----------

# DBTITLE 1,Step 8 - Verify
print("Step 8: Verification...\n")

# Execution time
end_time = datetime.datetime.now(pytz.timezone('US/Eastern'))
total_seconds = (end_time - job_start_ts).total_seconds()
minutes = int(total_seconds // 60)
seconds = int(total_seconds % 60)

print(f"\n{'=' * 60}")
print(f"  Load Method:  {load_method}")
print(f"  Rows Processed: {ph_count:,}")
print(f"  Completed at: {end_time}")
print(f"  Total time:   {minutes} minutes and {seconds} seconds")
print(f"{'=' * 60}")

# COMMAND ----------


