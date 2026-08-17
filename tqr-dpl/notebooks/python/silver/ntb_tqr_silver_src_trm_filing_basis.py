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
job_name = 'ntb_tqr_silver_src_trm_filing_basis'
config_file_path = config_file
trgt_tbl_name = 'src_trm_filing_basis'

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
    where_clause = f" WHERE {{tbl_alias}}.create_ts >=  to_date('{dataload_dt}')"
else:
    where_clause = f" WHERE {{tbl_alias}}.last_mod_ts > to_date('{control_dt}')  "
print(where_clause)

# COMMAND ----------

# DBTITLE 1,ETL logic
filing_basis_query = f""" SELECT *,
from_utc_timestamp(current_timestamp(),'America/New_York') as create_ts,
'etl' as create_user_id,
from_utc_timestamp(current_timestamp(),'America/New_York') as last_mod_ts,
'etl' as last_mod_user_id 
FROM (
SELECT DISTINCT
TM.TRADEMARK_GID as cfk_trademark_gid,
TM.SERIAL_NUM_TX as serial_num_tx,
TMFB.FK_FILING_BASIS_CD AS filing_basis_cd ,
TMFB.current_in,
TMFB.amended_in as amend_in,
TMFB.filed_in as file_in
FROM {src_tqr_db}.TRADEMARK TM 
JOIN {src_tqr_db}.TM_FILING_BASIS TMFB 
ON TMFB.FK_TRADEMARK_GID = TM.TRADEMARK_GID 
""" +where_clause.format_map({'tbl_alias':'TMFB'})+')'

print(filing_basis_query)

# COMMAND ----------

# DBTITLE 1,Create table or Append data into table if exists
try:
    df_filling_basis = spark.sql(filing_basis_query)
    df_count = df_filling_basis.count()
    df_filling_basis.write.mode("append").format("delta").saveAsTable(f'{stg_tqr_db}.{trgt_tbl_name}')
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
# MAGIC select * from tqr.silver.job_log where job_nm = 'ntb_tqr_silver_src_trm_filing_basis' order by start_ts desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tqr.silver.job_control where job_nm = 'ntb_tqr_silver_src_trm_filing_basis' order by load_ts desc
