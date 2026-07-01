# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
SRC_SYS_NAME = 'TRM_REPORTS'
src_name = SRC_SYS_NAME.lower()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

common_configs = read_yaml(config_file)
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{data_quality_catalog=} ')

spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.config_file_name', config_file_name)
spark.conf.set('config.src_sys_name', SRC_SYS_NAME)

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC WHERE  SRC_SYS_NAME='${config.src_sys_name}' AND PROC_NAME LIKE 'ntb_trm_reports_first_level_data_vrfctn_frmwrk' and PROC_CTGRY_CD='FIRST_LEVEL'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_first_level_data_vrfctn_frmwrk',
# MAGIC 'Process to verify count match between trm_reports databricks first level tables and legacy calgary files',
# MAGIC 'FIRST_LEVEL',
# MAGIC 'first level data verification',
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

# DBTITLE 1,Post Reg Dashboard
# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_sec_third_level_post_reg_data_vrfctn_frmwrk',
# MAGIC 'Process to verify count match between trm_reports databricks second and third level post reg tables and legacy calgary files',
# MAGIC 'SEC_THIRD_LEVEL',
# MAGIC 'Second and Third level data verification',
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

# DBTITLE 1,filings_goods_services Dashboard
# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_sec_third_level_filings_goods_services_data_vrfctn_frmwrk',
# MAGIC 'Process to verify count match between trm_reports databricks second and third level filings_goods_services tables and legacy calgary files',
# MAGIC 'SEC_THIRD_LEVEL',
# MAGIC 'Second and Third level data verification',
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

# DBTITLE 1,form_paragraph Dashboard
# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_sec_third_level_form_paragraph_data_vrfctn_frmwrk',
# MAGIC 'Process to verify count match between trm_reports databricks second and third level form_paragraph tables and legacy calgary files',
# MAGIC 'SEC_THIRD_LEVEL',
# MAGIC 'Second and Third level data verification',
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

# DBTITLE 1,pendency_and_inventory Dashboard
# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_sec_third_level_pendency_and_inventory_data_vrfctn_frmwrk',
# MAGIC 'Process to verify count match between trm_reports databricks second and third level pendency_and_inventory tables and legacy calgary files',
# MAGIC 'SEC_THIRD_LEVEL',
# MAGIC 'Second and Third level data verification',
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

# DBTITLE 1,tm_quality Dashboard
# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_sec_third_level_tm_quality_data_vrfctn_frmwrk',
# MAGIC 'Process to verify count match between trm_reports databricks second and third level tm_quality tables and legacy calgary files',
# MAGIC 'SEC_THIRD_LEVEL',
# MAGIC 'Second and Third level data verification',
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

# DBTITLE 1,TTAB Dashboard
# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_sec_third_level_ttab_data_vrfctn_frmwrk',
# MAGIC 'Process to verify count match between trm_reports databricks second and third level ttab tables and legacy calgary files',
# MAGIC 'SEC_THIRD_LEVEL',
# MAGIC 'Second and Third level data verification',
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


