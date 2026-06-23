# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")
#TMBUSCALENDAR,TMINTLTM,TMNGPDB,DATABRIDGE,EFOIAP,EOGADMIN,JBTEASPS,PROCEEDING,TMPRODVTY,TMREVIEWS,TRMWORKER, TMNGFPEPP, TMNGIDMP

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME").rstrip()
src_name = SRC_SYS_NAME.lower()

config_file_name = src_name+"-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

common_configs = read_yaml(config_file)
data_quality_catalog = common_configs['schema']['data_quality_catalog']
trgt_catalog = common_configs['schema']['trgt_catalog']
print(f'{data_quality_catalog=} ')

spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.sql(f"set data_quality_db = data_quality_catalog.lower()")
spark.conf.set('config.src_sys_name', SRC_SYS_NAME)
spark.sql(f"set src_sys_name = SRC_SYS_NAME")
spark.conf.set('config.config_file_name', config_file_name)
spark.sql(f"set config_file_name = config_file_name")

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC WHERE SRC_SYS_NAME='TMNGPDB' AND PROC_CTGRY_CD = 'BRNZ_TO_SLVR' AND PROC_NAME LIKE 'ntb_silver_%'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_silver_tmapplser_inc_load',
# MAGIC 'Process to load tmapplser data in tmapplser table',
# MAGIC 'BRNZ_TO_SLVR',
# MAGIC 'Bronze to silver layer load',
# MAGIC 'tmngpdb-conf.yaml',
# MAGIC 'trm_tmngpdb.bronze',
# MAGIC 'trm_tmngpdb.silver.tmapplser',
# MAGIC 'TMNGPDB',
# MAGIC 'ETL',
# MAGIC from_utc_timestamp(current_timestamp(),'America/New_York'),
# MAGIC 'ETL',
# MAGIC from_utc_timestamp(current_timestamp(),'America/New_York')
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_silver_bdss_class_load',
# MAGIC 'Process to load class data in bdss_class table',
# MAGIC 'BRNZ_TO_SLVR',
# MAGIC 'Bronze to silver layer load',
# MAGIC 'tmngpdb-conf.yaml',
# MAGIC 'trm_tmngpdb.bronze',
# MAGIC 'trm_tmngpdb.silver.bdss_class',
# MAGIC 'TMNGPDB',
# MAGIC 'ETL',
# MAGIC from_utc_timestamp(current_timestamp(),'America/New_York'),
# MAGIC 'ETL',
# MAGIC from_utc_timestamp(current_timestamp(),'America/New_York')
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC WHERE SRC_SYS_NAME='TMNGPDB' AND PROC_CTGRY_CD='BRNZ_TO_SLVR'

# COMMAND ----------


