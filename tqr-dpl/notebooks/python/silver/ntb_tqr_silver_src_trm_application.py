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
job_name = 'ntb_tqr_silver_src_trm_application'
config_file_path = config_file
trgt_tbl_name = 'src_trm_application'

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
    where_clause = f" WHERE {{tbl_alias}}.create_ts >  to_date('{dataload_dt}')"
else:
    where_clause = f" WHERE {{tbl_alias}}.create_ts > to_date('{control_dt}')  "
print(where_clause)

# COMMAND ----------

# DBTITLE 1,ETL logic
trm_application_query = f""" 
SELECT *, 
from_utc_timestamp(current_timestamp(),'America/New_York') as create_ts,
'etl' as create_user_id,
from_utc_timestamp(current_timestamp(),'America/New_York') as last_mod_ts,
'etl' as last_mod_user_id
FROM(
SELECT DISTINCT                             
TM.TRADEMARK_GID cfk_trademark_gid,
TM.SERIAL_NUM_TX serial_num_tx,
TM.FILING_DT filing_dt ,
substr(regexp_replace(TML.LITERAL_ELEMENT_TX, '( ){2,}',' '),1,10000)  literal_element_tx,
substr(regexp_replace(tm.standard_character_tx, '( ){2,}',' '),1,10000) standard_character_tx,
substr(regexp_replace(TM.MARK_DESCRIPTION_TX , '( ){2,}',' '),1,10000)  mark_description_tx,
substr(regexp_replace(SMDT.MARK_DRAWING_TYPE_CD , '( ){2,}',' '),1,10000) mark_drawing_type_cd,
substr(regexp_replace(SMDT.TITLE_TX  , '( ){2,}',' '),1,10000) as mark_drawing_type_title_tx,
EM.CFK_EMPLOYEE_NO examiner_employee_no,
FT.TITLE_TX as source_system_nm
FROM {src_tqr_db}.TRADEMARK TM 
INNER JOIN {src_tqr_db}.TM_EMPLOYEE_ASSIGNMENT EM ON TM.TRADEMARK_GID = EM.FK_TRADEMARK_GID
INNER JOIN {src_tqr_db}.STND_MARK_DRAWING_TYPE SMDT ON SMDT.MARK_DRAWING_TYPE_CD = TM.FK_MARK_DRAWING_TYPE_CD
INNER JOIN {src_tqr_db}.STND_FEE_PROCESS_TYPE FT ON TM.FK_FEE_PROCESS_TYPE_CD = FT.FEE_PROCESS_TYPE_CD
LEFT OUTER JOIN {src_tqr_db}.TM_LITERAL TML ON TML.FK_TRADEMARK_GID = TM.TRADEMARK_GID  
INNER JOIN {stg_tqr_db}.src_trm_application_event ST ON TM.serial_num_tx = ST.serial_num_tx
""" +where_clause.format_map({'tbl_alias':'ST'}) +')'

print(trm_application_query)

# COMMAND ----------

# DBTITLE 1,Create table or Append data into table if exists
try:
    df_trm_application = spark.sql(trm_application_query)
    df_count = df_trm_application.count()
    df_trm_application.write.mode("append").format("delta").saveAsTable(f'{stg_tqr_db}.{trgt_tbl_name}')
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
# MAGIC select * from tqr.silver.job_log where job_nm = 'ntb_tqr_silver_src_trm_application' order by start_ts desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tqr.silver.job_control where job_nm = 'ntb_tqr_silver_src_trm_application' order by load_ts desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from tqr.silver.src_trm_application

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tqr.silver.src_trm_application
