# Databricks notebook source
# MAGIC %md
# MAGIC ##Query10

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")
dbutils.widgets.text("rundate","")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME").rstrip()
src_name = SRC_SYS_NAME.lower()
config_file_name = src_name+"-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name

import pytz
from pytz import timezone
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

from datetime import date

rundate = dbutils.widgets.get("rundate")
if rundate == '':
    #rdate = date.today()
    rdate = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).date()
    #rdate = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).date()- timedelta(days=1) # unit test

else:
    rdate = rundate
    import datetime
    rdate = datetime.datetime.strptime(rundate, '%Y-%m-%d').date() 

print(rdate)
spark.conf.set('conf.rdate', str(rdate))

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
foreign_oracle_catalog = common_configs['schema']['foreign_oracle_catalog']
foreign_oracle_db = common_configs['schema']['src_db_name']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
src_db_name = common_configs['schema']['src_db_name'].upper()

spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.trgt_catalog', trgt_catalog.lower()) 
spark.conf.set('config.dbx_env', dbx_env.lower())

if trgt_catalog.count("_") == 1:
    env = ""
else:
    env = "_"+trgt_catalog.split("_",2)[-1]

print(f'{src_db_name=},{trgt_catalog=}, {data_quality_catalog=},{dbx_env=},{env=}')
from pyspark.sql.functions import col, lit

# COMMAND ----------

job_name = 'ntb_silver_bdss_case_events_stgload'

start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
print(f'{start_ts=}')
control_dt = begin_job_cntl(f'{data_quality_catalog}',f'{trgt_catalog}.silver',job_name,start_ts)

# COMMAND ----------

df_case_events_stg = spark.sql(
  f"""
    SELECT
      CAST(split(BE.CFK_OBJECT_GID, ':')[2] AS INTEGER) AS CM_SER_NUM,
      CAST(BE.ORDER_NO AS INT) AS CM_ENT_NUM,
      BER.LEGACY_CM_ENT_CD AS CM_ENT_CD,
      BER.title_tx AS tt_text_1,
      BER.LEGACY_CM_ENT_TYPE_CD AS CM_ENT_TYPE,
      date_format((BE.EFFECTIVE_TS),'yyyyMMdd') AS ent_dt,
      from_utc_timestamp(current_timestamp(),'America/New_York') AS create_ts,
      'etl' AS create_user_id
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.BUSINESS_EVENT BE
    LEFT JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.STND_BUSINESS_EVENT_REASON BER
    ON BE.FK_BUSINESS_EVENT_REASON_ID = BER.BUSINESS_EVENT_REASON_ID
    WHERE BE.ORDER_NO > 0 AND fk_object_type_cd = "Trademark"
    AND CAST(split(BE.CFK_OBJECT_GID, ':')[2] AS INTEGER)
    IN
    (
      SELECT DISTINCT sernum
      FROM {trgt_catalog}.silver.tmappl_daily_consolidated_vw
    )
  """
)

# COMMAND ----------

try:
    df_case_events_stg.write.mode("overwrite").format("delta").saveAsTable(f'{trgt_catalog}.silver.bdss_case_events_daily_stg')
    recs_count = df_case_events_stg.count()
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'completed',0,recs_count,"job completed successfully")
    dbutils.notebook.exit(f"Completed Loading {recs_count} records into bdss_case_events_daily_stg Table ")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'failed',0,0,e)
    raise
dbutils.notebook.exit(f"Completed loading bdss_case_events_daily_stg Table ")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trm_tmngpdb_dev.silver.bdss_case_events_daily_stg
