# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import date_trunc

# COMMAND ----------

# DBTITLE 1,Environment
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name
print(f"{config_file=}")

# COMMAND ----------

# DBTITLE 1,Common Functions
# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,Config
common_configs = read_yaml(config_file)
reporting_catalog = common_configs["schema"]["trgt_catalog"]
tmngpdb_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
tmproceeding_catalog = common_configs["schema"]["tmproceeding_catalog"]
tmintltm_catalog = common_configs["schema"]["tmintltm_src_catalog"]
schema_bronze = "bronze"
schema_silver = "silver"
table_silver = "prosecution_history"

print(
f"""{schema_bronze=},
{schema_silver=},
{table_silver=}, {reporting_catalog=},
{tmngpdb_catalog=},
{tmproceeding_catalog=},
{tmintltm_catalog=}
"""
)

# COMMAND ----------

# Broadcastable tables (row counts):
#   stnd_business_event_reason (1,307), proceeding_event_reason (136),
#   proceeding (26,660), proceeding_mark (26,660),
#   proceeding_event (110,774), international_reg_tm (439,435)
# Non-broadcastable: business_event (307M), trademark (14M)

business_events = spark.sql(f"""
with business_events_base as (
  select /*+ BROADCAST(sber) */
    be.cfk_object_gid,
    tm.trademark_gid,
    tm.filing_dt,
    sber.title_tx,
    nvl(be.cfk_proceeding_no, case when be.create_user_id like '333333%' then '0' else be.create_user_id end) as cm_prcd_num,
    substr(sber.business_event_reason_cd, -1) as cm_ent_type,
    be.order_no as cm_ent_num,
    be.effective_ts,
    cast(to_date(be.effective_ts) as timestamp) as cm_ent_dt,
    substr(sber.business_event_reason_cd, 1, 4) as cm_ent_cd,
    case
      when
        replace(substr(be.create_ts, 12, 8), ':', '') != '000000'
      then
        cast(to_date(be.create_ts) as timestamp)
      else null
    end as cm_sys_dt,
    cast(replace(substr(be.create_ts, 12, 8), ':', '') as integer) as cm_sys_ti,
    case when be.paper_in = 'Y' then 1 when be.paper_in = 'N' then 0 end as cm_flg_paper,
    be.last_mod_ts as last_modified_date,
    null as oracle_apply_time
  from
    {tmngpdb_catalog}.bronze.business_event be
      inner join {tmngpdb_catalog}.bronze.stnd_business_event_reason sber
        on be.fk_business_event_reason_id = sber.business_event_reason_id
      inner join {tmngpdb_catalog}.bronze.trademark tm
        on tm.trademark_gid = be.cfk_object_gid
  where
    sber.prosecution_history_in != 'N'
),
petitions as (
  select
    proceeding_gid,
    proceeding_no
  from
    {tmproceeding_catalog}.bronze.proceeding
  where
    cfk_proceeding_type_cd = 'PET'
),
proceedings as (
  select /*+ BROADCAST(prcd, per) */
    pm.cfk_trademark_gid,
    pm.fk_proceeding_gid,
    prcd.proceeding_no,
    cast(split(pm.fk_proceeding_gid, ':')[2] as integer) as cm_ent_num,
    cast(substr(split(prcd.proceeding_no, '-')[1], 1, 6) as integer) as cm_prcd_num,
    pe.order_no,
    substr(proceeding_event_reason_cd, 1, 4) as cm_ent_cd,
    substr(proceeding_event_reason_cd, -1) as cm_ent_type,
    pe.effective_ts
  from
    {tmproceeding_catalog}.bronze.proceeding_mark pm
      inner join petitions prcd
        on pm.fk_proceeding_gid = prcd.proceeding_gid
      inner join {tmproceeding_catalog}.bronze.proceeding_event pe
        on pm.fk_proceeding_gid = pe.fk_proceeding_gid
      inner join {tmproceeding_catalog}.bronze.proceeding_event_reason per
        on pe.fk_prcdng_event_reason_id = per.proceeding_event_reason_id
)
select /*+ BROADCAST(prcd, ri) */
  cast(split(be.cfk_object_gid, ':')[2] as integer) as serial_number,
  nvl(prcd.cm_prcd_num, be.cm_prcd_num) as cm_prcd_num,
  be.cm_ent_type fifth_char_cm_type,
  be.title_tx cm_desc,
  be.cm_ent_num ph_action_number,
  be.cm_ent_dt ph_action_date,
  be.cm_ent_cd ph_action_code,
  be.cm_sys_dt,
  be.cm_sys_ti,
  be.last_modified_date,
  be.oracle_apply_time,
  be.filing_dt as am_dt_fil,
  cast(split(be.trademark_gid, ':')[2] as integer) as am_ser_num,
  ri.notification_dt as ri_notif_dt,
  be.cm_flg_paper
from
  business_events_base be
    left join proceedings prcd
      on (
        be.cfk_object_gid = prcd.cfk_trademark_gid
        and be.cm_ent_type = prcd.cm_ent_type
        and be.cm_ent_cd = prcd.cm_ent_cd
        and be.cm_ent_dt = prcd.effective_ts
        and be.cm_ent_num = prcd.cm_ent_num
      )
    left join {tmintltm_catalog}.bronze.international_reg_tm ri
      on be.trademark_gid = ri.cfk_trademark_gid
""")
business_events.printSchema()

# COMMAND ----------

# DBTITLE 1,Transformation: Add TTAB
business_events_with_ttab = business_events.withColumn(
    "ttab_tracking_num",
    expr("case when length(cm_prcd_num) = 6 then cm_prcd_num else null end"),
).withColumn(
    "tm_worker_eid",
    expr("case when length(cm_prcd_num) = 5 then cm_prcd_num else null end"),
)
business_events_with_ttab.printSchema()

# COMMAND ----------

# Splitting the chunks to 512 before shuffle 
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "auto")
spark.conf.set("spark.sql.shuffle.partitions", "512")

# COMMAND ----------

# DBTITLE 1,Transformation: Trim, Type, and Add Metadata
cleansed = (
    business_events_with_ttab.select(
        trim(col("serial_number")).cast(IntegerType()).alias("serial_number"),
        trim(col("ph_action_number")).cast(IntegerType()).alias("ph_action_number"),
        trim(col("ph_action_code")).alias("ph_action_code"),
        trim(col("cm_sys_dt")).cast(DateType()).alias("cm_sys_dt"),
        trim(col("ph_action_date")).cast(DateType()).alias("ph_action_date"),
        date_trunc(
            "second", trim(col("last_modified_date")).cast(TimestampType())
        ).alias("last_modified_date"),
        trim(col("oracle_apply_time")).cast(TimestampType()).alias("oracle_apply_time"),
        trim(col("cm_prcd_num")).alias("cm_prcd_num"),
        trim(col("ri_notif_dt")).cast(TimestampType()).alias("ri_notif_dt"),
        trim(col("cm_desc")).alias("cm_desc"),
        trim(col("fifth_char_cm_type")).alias("fifth_char_cm_type"),
        trim(col("cm_flg_paper")).cast(IntegerType()).alias("cm_flg_paper"),
        trim(col("ttab_tracking_num")).alias("ttab_tracking_num"),
        trim(col("tm_worker_eid")).alias("tm_worker_eid"),
    )
    .withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit("-1"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit("-1"))
).distinct()
cleansed.printSchema()

# COMMAND ----------

# 1. Turn off the 7-day retention safety check for this session
spark.conf.set("spark.databricks.delta.stats.skipping", "true")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")

# 2. Run the vacuum using your variables
target_table = f"{reporting_catalog}.{schema_silver}.{table_silver}"
spark.sql(f"VACUUM {target_table} RETAIN 0 HOURS")


# COMMAND ----------

# DBTITLE 0,Overwrite: Prosecution History
cleansed.write.mode("overwrite") \
    .format("delta") \
    .saveAsTable(f"{reporting_catalog}.{schema_silver}.{table_silver}")

# COMMAND ----------

# DBTITLE 1,Base DQ Check: Duplicates on Composite
display(
    spark.sql(f"""
        select
            count(1),
            serial_number,
            ph_action_code,
            fifth_char_cm_type,
            ph_action_number,
            cm_prcd_num
        from
            {reporting_catalog}.silver.prosecution_history
        group by
            all
        having
            count(1) > 1
    """)
)

# COMMAND ----------

from pyspark.sql import Window
import pyspark.sql.functions as F

spark.conf.set("spark.sql.shuffle.partitions", "512")

composite_keys = ["serial_number", "ph_action_code", "fifth_char_cm_type", "ph_action_number", "cm_prcd_num"]
target_table = f"{reporting_catalog}.{schema_silver}.{table_silver}"

df = spark.table(target_table)
non_composite_keys = [c for c in df.columns if c not in composite_keys and c not in ["update_ts", "update_user_id"]]

# 1. Open a window over the keys to isolate distinct values in parallel
window_spec = Window.partitionBy(composite_keys)

# 2. Translate your exact "CASE WHEN count(distinct) > 1" into an optimized plan
causal_columns = []
for c in non_composite_keys:
    # F.size(F.collect_set) finds unique values per group without a heavy GroupBy shuffle
    distinct_count = F.size(F.collect_set(F.col(c)).over(window_spec))
    condition = F.when(distinct_count > 1, F.lit(c)).otherwise(F.lit(None))
    causal_columns.append(condition)

# 3. Concatenate the columns and filter out clean records instantly
final_df = (df
    .withColumn("duplicate_cause", F.concat_ws(";", *causal_columns))
    .filter(F.length(F.col("duplicate_cause")) > 1)
    .groupBy(composite_keys)
    .agg(F.first("duplicate_cause").alias("duplicate_cause")))

display(final_df)

