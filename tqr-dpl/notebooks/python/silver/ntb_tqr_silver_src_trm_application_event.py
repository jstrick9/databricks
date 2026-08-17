# Databricks notebook source
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
trm_catalog = common_configs['schema']['trm_catalog']
src_tqr_db = trm_catalog+'.bronze'
stg_tqr_db = tqr_catalog+'.silver'

#Job variables
job_name = 'ntb_tqr_silver_src_trm_application_event'
config_file_path = config_file_path = config_file
trgt_tbl_name = 'src_trm_application_event'

#job start timestamp
#import datetime
job_start_ts = datetime.datetime.now()

print(f'{src_tqr_db=},{stg_tqr_db=},{job_start_ts=}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start Job Control

# COMMAND ----------

# DBTITLE 1,Create entry in job log table and get max dt from job control table
control_dt = begin_job_cntl(stg_tqr_db, job_name, job_start_ts)
print(f'{control_dt=}')

# COMMAND ----------

# DBTITLE 1,Get dataload date from config file
configs = read_yaml(config_file_path)
dataload_dt = configs['schema']['dataload_dt']
print(f'{dataload_dt=}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Load

# COMMAND ----------

# DBTITLE 1,Where clause query
if control_dt == "None":
    where_clause = f" and {{tbl_alias}}.create_ts >  to_date('{dataload_dt}')"
else:
    where_clause = f" and {{tbl_alias}}.last_mod_ts > to_date('{control_dt}')  "
print(where_clause)


# COMMAND ----------

# DBTITLE 1,ETL logic
trm_application_event_query = f""" SELECT *,
from_utc_timestamp(current_timestamp(),'America/New_York') as create_ts,
'etl' as create_user_id,
from_utc_timestamp(current_timestamp(),'America/New_York') as last_mod_ts,
'etl' as last_mod_user_id 
FROM (
SELECT DISTINCT
TM.TRADEMARK_GID as cfk_trademark_gid, 
cast(BE.BUSINESS_EVENT_ID as  Decimal(10,0)) BUSINESS_EVENT_ID, 
TM.SERIAL_NUM_TX, 
cast(ST.BUSINESS_EVENT_REASON_ID  as Decimal(10,0)) as ckf_business_event_reason_id, 
ST.business_event_Reason_cd as business_event_reason_cd, 
BE.EFFECTIVE_TS
from {src_tqr_db}.TRADEMARK TM, 
{src_tqr_db}.BUSINESS_EVENT BE, 
{src_tqr_db}.STND_BUSINESS_EVENT_REASON ST
where TM.TRADEMARK_GID = BE.CFK_OBJECT_GID 
and BE.FK_BUSINESS_EVENT_REASON_ID = st.BUSINESS_EVENT_REASON_ID
""" +where_clause.format_map({'tbl_alias':'BE'})+')'

print(trm_application_event_query )

# COMMAND ----------

# DBTITLE 1,Create table or Append data into table if exists
try:
    df_application_event = spark.sql(trm_application_event_query)
    df_count = df_application_event.count()
    df_application_event.write.mode("append").format("delta").saveAsTable(f'{stg_tqr_db}.{trgt_tbl_name}')
    end_job_cntl(stg_tqr_db, job_name, job_start_ts,'completed', df_count,"job completed successfully")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(stg_tqr_db, job_name, job_start_ts,'failed',0,e)
    raise

# COMMAND ----------

dbutils.notebook.exit(f"Completed Loading {stg_tqr_db}.{trgt_tbl_name}. Number of records appended: {df_count} ")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Unit test cells below

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tqr.silver.job_log where job_nm = 'ntb_tqr_silver_src_trm_application_event' order by start_ts desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tqr.silver.job_control where job_nm = 'ntb_tqr_silver_src_trm_application_event' order by load_ts desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from tqr.silver.src_trm_application_event

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tqr.silver.src_trm_application_event
