# Databricks notebook source
#Purpose: This notebook loads data from 'event_inventory' gold table to 'event_inventory' mysql table in bdr database
#Author: Pawanpreet Sangari

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT dbx_env DEFAULT "dev"

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file = "../../../notebooks/config/"+dbutils.widgets.get("dbx_env").rstrip()+"/tqr-conf.yaml"
print(f'{config_file=}')
config_file_path = config_file

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

common_configs = read_yaml(config_file)

tqr_catalog = common_configs['schema']['tqr_catalog']
mysql_tqr_db = common_configs['schema']['mysql_tqr_db']
dm_tqr_db = tqr_catalog+'.gold'
stg_tqr_db = tqr_catalog+'.silver'

#Job variables
job_name = 'ntb_mysql_bdr_event_inventory_load'
trgt_tbl_name = 'event_inventory'

#job start timestamp
job_start_ts = datetime.datetime.now()

print(f'{stg_tqr_db=},{dm_tqr_db=},{job_start_ts=},{mysql_tqr_db=}')
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

# COMMAND ----------

# DBTITLE 1,Read data from mysql bdr target table into databricks temp table
try:
    pushdown_query = f"""(select fk_review_type_id,serial_no,source_event_dt from event_inventory_pool)"""
    df_read_bdr_event_inventory = read_data_from_mysql_conn_dsu(pushdown_query, mysql_tqr_db)
    df_read_bdr_event_inventory.createOrReplaceTempView("bdr_event_inventory_temp")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(stg_tqr_db, job_name, job_start_ts,'failed',0,e)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ###Remove duplicate Data

# COMMAND ----------

spark.sql("""
insert
  overwrite ${dm_tqr_db}.event_inventory
select
  review_type_cd,
  serial_num_tx,
  source_system_nm,
  search_present_in,
  source_event_dt,
  docket_in,
  mark_literal_element_tx,
  mark_drawing_type_cd,
  mark_drawing_type_title_tx,
  mark_description_tx,
  examiner_employee_no,
  organization_cd,
  event_json_doc,
  inventory_create_ts,
  lock_control_no,
  create_ts,
  create_user_id,
  last_mod_ts,
  last_mod_user_id,
  is_tm_exam
from
  (
    select
      src.*,
      row_number() over (
        partition by SRC.review_type_cd,
        SRC.serial_num_tx,
        to_date(SRC.source_event_dt, 'yyyy-MM-dd')
        order by
          SRC.last_mod_ts
      ) as r_num
    from
      ${dm_tqr_db}.event_inventory src where (organization_cd between 100 and 150 or organization_cd between 300 and 315 )
  )
where
  r_num = 1 """)

# COMMAND ----------

# DBTITLE 1,Query data from gold table not present in mysql table
try:
    df_load_bdr_event_inventory = spark.sql("""
    SELECT  
        SRC.review_type_cd as fk_review_type_id,
        SRC.serial_num_tx as serial_no,
        SRC.source_system_nm as source_system_nm,
        SRC.search_present_in as search_present_in,
        --SRC.source_event_dt as source_event_dt,
        date_trunc("second",SRC.source_event_dt)as source_event_dt,
        SRC.docket_in as docket_in,
        SRC.mark_literal_element_tx as mark_literal_element_tx,
        SRC.mark_drawing_type_cd as mark_drawing_type_cd,
        SRC.mark_drawing_type_title_tx as mark_drawing_type_title_tx,
        SRC.mark_description_tx as mark_description_tx,
        SRC.examiner_employee_no as examiner_employee_no,
        SRC.organization_cd as organization_cd,
        SRC.event_json_doc as event_json_doc,
        SRC.inventory_create_ts as inventory_create_ts,
        SRC.create_ts as create_ts,
        SRC.create_user_id as create_user_id,
        SRC.last_mod_ts as last_mod_ts,
        SRC.last_mod_user_id as last_mod_user_id,
        SRC.lock_control_no as lock_control_no
    FROM (SELECT * 
            FROM ${dm_tqr_db}.event_inventory
           WHERE last_mod_ts > CAST('${control_dt}' as timestamp)
           and organization_cd is not null
          ) SRC --filter smaller dataset appended after last scucessful data load into mysql bdr table
    WHERE NOT EXISTS(
        SELECT 1 FROM bdr_event_inventory_temp TRGT
        WHERE CONCAT_WS(',',TRGT.fk_review_type_id ,TRGT.serial_no,TRGT.source_event_dt) = CONCAT_WS(',',SRC.review_type_cd,SRC.serial_num_tx,date_trunc("second",SRC.source_event_dt))
        )
    """)
    #df_load_bdr_event_inventory.display()
    
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(stg_tqr_db, job_name, job_start_ts,'failed',0,e)
    raise

# COMMAND ----------

# DBTITLE 1,Load data into mysql table
try:
    df_count = load_mysql_table_dsu(df_load_bdr_event_inventory, mysql_tqr_db,"event_inventory_pool","append")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(stg_tqr_db, job_name, job_start_ts,'failed',0,e)
    raise

# COMMAND ----------

# DBTITLE 1,End Job Control
end_job_cntl(stg_tqr_db, job_name, job_start_ts,'completed', df_count,"job completed successfully")

# COMMAND ----------

dbutils.notebook.exit(f"Completed Loading {dm_tqr_db}.{trgt_tbl_name}. Number of records appended: {df_count} ")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Unit test cells below

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tqr.silver.job_log where job_nm = 'ntb_mysql_bdr_event_inventory_load' 
# MAGIC order by start_ts desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tqr.silver.job_control where job_nm = 'ntb_mysql_bdr_event_inventory_load' 
# MAGIC --order by load_ts desc
