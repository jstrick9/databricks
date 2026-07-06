# Databricks notebook source
# DBTITLE 1,Define widgets
dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")
#TMBUSCALENDAR,TMINTLTM,TMNGPDB,DATABRIDGE,EFOIAP,EOGADMIN,JBTEASPS,PROCEEDING,TMPRODVTY,TMREVIEWS,TRMWORKER, TMNGFPEPP, TMNGIDMP

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME").rstrip()
src_name = SRC_SYS_NAME.lower()


config_file_name = src_name+"-conf.yaml"
proc_name = 'ntb_'+src_name+'_dq_data_vrfctn_frmwrk'
catalog_proc_name = 'ntb_'+src_name+'_dq_catalog_vrfctn_frmwrk'
catalog_ddl_proc_name = 'ntb_'+src_name+'_dq_catalog_ddl_vrfctn_frmwrk'


config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

common_configs = read_yaml(config_file)
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{data_quality_catalog=} ')

spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.src_sys_name', SRC_SYS_NAME)
spark.conf.set('config.proc_name', proc_name)
spark.conf.set('config.catalog_proc_name', catalog_proc_name)
spark.conf.set('config.catalog_ddl_proc_name', catalog_ddl_proc_name)
spark.conf.set('config.config_file_name', config_file_name)


# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC WHERE SRC_SYS_NAME='${config.src_sys_name}' AND PROC_NAME LIKE '%_dq_data_vrfctn_frmwrk' and PROC_CTGRY_CD='SRC_TO_BRNZ'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC '${config.proc_name}',
# MAGIC 'Process to verify count match between oracle source tables and bronze layer',
# MAGIC 'SRC_TO_BRNZ',
# MAGIC 'Source to Bronze layer load',
# MAGIC '${config.config_file_name}',
# MAGIC '',
# MAGIC '',
# MAGIC '${config.src_sys_name}',
# MAGIC 'ETL',
# MAGIC current_timestamp(),
# MAGIC 'ETL',
# MAGIC current_timestamp()
# MAGIC )

# COMMAND ----------

dbutils.notebook.exit(f"Completed Loading {data_quality_catalog}.SILVER.CMN_PROC_DEFN_RFRNC. ")

# COMMAND ----------


