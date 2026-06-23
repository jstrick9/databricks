# Databricks notebook source
# MAGIC %md
# MAGIC ##Query8

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

tmintltm_src = "tmintltm"+"-conf.yaml"
tmintltm_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+tmintltm_src

import pytz
from pytz import timezone
print(f'{config_file=},{dbx_env=}')
print(f'{tmintltm_file=},{dbx_env=}')

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
    rdate = datetime.datetime.strptime(rundate, '%Y-%m-%d')

print(rdate)
spark.conf.set('conf.rdate', str(rdate))

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
foreign_oracle_catalog = common_configs['schema']['foreign_oracle_catalog']

data_quality_catalog = common_configs['schema']['data_quality_catalog']
src_db_name = common_configs['schema']['src_db_name'].upper()

foreign_oracle_db = read_yaml(tmintltm_file)['schema']['src_db_name']

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

job_name = 'ntb_silver_bdss_madrid_and_history_data_stgload'

start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
print(f'{start_ts=}')
control_dt = begin_job_cntl(f'{data_quality_catalog}',f'{trgt_catalog}.silver',job_name,start_ts)

# COMMAND ----------

df_madrid_and_history_data_stg = spark.sql(
  f"""

    WITH history AS (
      SELECT DISTINCT
        ia.international_us_ref_no mhi_ctl_num,
        iaer.international_appl_evnt_rsn_cd mhi_action,
        date_format(cast(iae.effective_ts AS DATE), 'yyyyMMdd') ent_dt,
        iaer.title_tx tt_text_1,
        row_number() OVER (PARTITION BY ia.international_us_ref_no ORDER BY iae.effective_ts) hRow
      FROM {foreign_oracle_catalog}.{foreign_oracle_db}.international_appl_event iae
      LEFT JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.international_appl_evnt_rsn iaer ON iae.international_appl_evnt_rsn_id = iaer.international_appl_evnt_rsn_id
      LEFT JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.international_application ia ON ia.international_application_gid = iae.fk_international_appl_gid
      WHERE iaer.international_appl_evnt_rsn_cd NOT IN ('IRREP', 'IRRRJ', 'CRCRM', 'ADDCH', 'SYNC1', 'NCEDN', 'RENWL') 
      ORDER BY ent_dt
    ),

    madrid AS (
      SELECT 
        INTERNATIONAL_US_REF_NO mas_ctl_num,
        coalesce(date_format(ORIGINAL_FILING_DT, 'yyyyMMdd'), 0 ) orig_fil_dt,
        trim(FK_INTERNATIONAL_REG_NO) intl_reg_num,
        coalesce(date_format(INTERNATIONAL_REG_DT, 'yyyyMMdd'), 0) intl_reg_dt,
        trim(to_char(CFK_STATUS_CD, '000')) stat,
        coalesce(date_format(BASE_APPL_INTL_REG.STATUS_DT, 'yyyyMMdd'), 0) stat_dt,
        coalesce(date_format(REPLY_BY_DT, 'yyyyMMdd'), 0) reply_by_dt,
        coalesce(date_format(IB_RENEWAL_DT, 'yyyyMMdd'), 0) rnwl_dt,
        cast(split(BASE_APPLICATION.cfk_trademark_gid, ':')[2] AS INTEGER) sernum,
        row_number() OVER(ORDER BY INTERNATIONAL_US_REF_NO) mRow
      FROM {foreign_oracle_catalog}.{foreign_oracle_db}.BASE_APPLICATION
      LEFT JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.INTERNATIONAL_APPLICATION ON INTERNATIONAL_APPLICATION.INTERNATIONAL_APPLICATION_GID = BASE_APPLICATION.FK_INTERNATIONAL_APPL_GID
      LEFT JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.BASE_APPL_INTL_REG ON BASE_APPL_INTL_REG.FK_INTERNATIONAL_APPL_GID = BASE_APPLICATION.FK_INTERNATIONAL_APPL_GID AND BASE_APPL_INTL_REG.CFK_TRADEMARK_GID = BASE_APPLICATION.CFK_TRADEMARK_GID
      LEFT OUTER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.INTERNATIONAL_REGISTRATION ON INTERNATIONAL_REGISTRATION.INTERNATIONAL_REG_GID = BASE_APPL_INTL_REG.FK_INTERNATIONAL_REG_GID 
      LEFT OUTER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.INTERNATIONAL_TM ON INTERNATIONAL_REGISTRATION.FK_INTERNATIONAL_REG_NO  = INTERNATIONAL_TM.INTERNATIONAL_REG_NO
      WHERE cast(split(BASE_APPLICATION.cfk_trademark_gid, ':')[2] AS INTEGER)
      IN 
      (
        SELECT DISTINCT sernum
        FROM {trgt_catalog}.silver.tmappl_daily_consolidated_vw
      )
    )

    SELECT *
    FROM history, madrid
    WHERE history.mhi_ctl_num = madrid.mas_ctl_num
  """
)

# COMMAND ----------

try:
    df_madrid_and_history_data_stg.write.mode("overwrite").format("delta").saveAsTable(f'{trgt_catalog}.silver.bdss_madrid_and_history_data_daily_stg')
    recs_count = df_madrid_and_history_data_stg.count()
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'completed',0,recs_count,"job completed successfully")
    dbutils.notebook.exit(f"Completed Loading {recs_count} records into bdss_madrid_and_history_data_daily_stg Table ")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'failed',0,0,e)
    raise
dbutils.notebook.exit(f"Completed loading  bdss_madrid_and_history_data_daily_stg Table ")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trm_tmngpdb_dev.silver.bdss_madrid_and_history_data_daily_stg
