# Databricks notebook source
#Purpose: This notebook loads data from 'event_inventory_stage' silver table to 'event_inventory' gold table
#Author: Pawanpreet Sangari

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT dbx_env DEFAULT "dev"

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file = "../../../notebooks/config/"+dbutils.widgets.get("dbx_env").rstrip()+"/tqr-conf.yaml"
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

common_configs = read_yaml(config_file)

tqr_catalog = common_configs['schema']['tqr_catalog']
dm_tqr_db = tqr_catalog+'.gold'
stg_tqr_db = tqr_catalog+'.silver'

#Job variables
job_name = 'ntb_tqr_gold_event_inventory'
trgt_tbl_name = 'event_inventory'

#job start timestamp
job_start_ts = datetime.datetime.now()

print(f'{stg_tqr_db=},{dm_tqr_db=},{job_start_ts=}')
spark.sql(f"set dm_tqr_db = {dm_tqr_db}")
spark.sql(f"set stg_tqr_db = {stg_tqr_db}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start Job Control

# COMMAND ----------

# DBTITLE 1,Create entry in job log table and get max dt from job control table
control_dt = begin_job_cntl(stg_tqr_db, job_name, job_start_ts)
if control_dt == 'None':
    control_dt = '1900-01-01'
print(f'{control_dt=}')
spark.sql("set control_dt =" + str(control_dt))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Load ETL Query
# MAGIC <pre>
# MAGIC Data load rules
# MAGIC 1. Silver table has only appends The data is not deleted or updated
# MAGIC 2. Join silver table with gold table on 3 PK columns on serial_num_tx, source_event_dt, review_type_cd
# MAGIC 3. The Insert and Update timestamp in gold table will be the gold table insert and update timestamp?? (instead of silver table insert and update ts)
# MAGIC 4. Every time the gold table successfully loaded the job start timestamp is captured in job control table
# MAGIC 5. For subsequent load compare the max job start ts (from job control table) to last modified ts from silver table to find the newly added records
# MAGIC 6. Insert Logic: Case when record not present in trgt but present in stg then Insert
# MAGIC
# MAGIC </pre>

# COMMAND ----------

try:
    df_tqr_gold_event_inventory = spark.sql("""
    SELECT 
      src.review_type_cd,
      src.serial_num_tx,
      src.source_system_nm,
      src.search_present_in,
      src.source_event_dt,
      src.docket_in,
      src.mark_literal_element_tx,
      src.mark_drawing_type_cd,
      src.mark_drawing_type_title_tx,
      src.mark_description_tx,
      src.examiner_employee_no,
      src.organization_cd,
      src.event_json_doc,
      src.inventory_create_ts,
      src.lock_control_no,
      from_utc_timestamp(current_timestamp(),'America/New_York') AS create_ts,
      'etl' AS create_user_id,
      from_utc_timestamp(current_timestamp(),'America/New_York') AS last_mod_ts,
      'etl' AS last_mod_user_id,
      src.is_tm_exam
    FROM (SELECT * 
            FROM ${stg_tqr_db}.event_inventory_stage 
            WHERE last_mod_ts > CAST('${control_dt}' as timestamp)
          ) src --filter smaller dataset appended after last scucessful data load into gold table
    WHERE NOT EXISTS(
        SELECT 1 FROM ${dm_tqr_db}.event_inventory TRGT
        WHERE CONCAT_WS(',',TRGT.review_type_cd,TRGT.serial_num_tx,TRGT.source_event_dt) = CONCAT_WS(',',SRC.review_type_cd,SRC.serial_num_tx,SRC.source_event_dt)
        )
    """)
    df_tqr_gold_event_inventory.display()
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(stg_tqr_db, job_name, job_start_ts,'failed',0,e)
    raise

# COMMAND ----------

try:
    df_count = df_tqr_gold_event_inventory.count()
    df_tqr_gold_event_inventory.write.mode("append").format("delta").insertInto(f'{dm_tqr_db}.{trgt_tbl_name}')
    end_job_cntl(stg_tqr_db, job_name, job_start_ts,'completed', df_count,"job completed successfully")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(stg_tqr_db, job_name, job_start_ts,'failed',0,e)
    raise

# COMMAND ----------

dbutils.notebook.exit(f"Completed Loading {dm_tqr_db}.{trgt_tbl_name}. Number of records appended: {df_count} ")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unit test cells below

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tqr.silver.job_log where job_nm = 'ntb_tqr_gold_event_inventory' 
# MAGIC order by start_ts desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tqr.silver.job_control where job_nm = 'ntb_tqr_gold_event_inventory' 
# MAGIC order by load_ts desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from tqr.gold.event_inventory 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tqr.gold.event_inventory

# COMMAND ----------

# MAGIC %sql
# MAGIC describe tqr.gold.event_inventory
