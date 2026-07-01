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
# MAGIC DELETE FROM ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC WHERE SRC_SYS_NAME='${config.src_sys_name}'

# COMMAND ----------

# MAGIC %md
# MAGIC #### First Level

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_milestone_dq',
# MAGIC 'Data match between trm_reports databricks milestone table and alteryx output table',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_bibliography_dq',
# MAGIC 'Data match between trm_reports databricks bibliography table and alteryx output table',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_class_dq',
# MAGIC 'Data match between trm_reports databricks class table and alteryx output table',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_owner_dq',
# MAGIC 'Data match between trm_reports databricks owner table and alteryx output table',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_correspondence_dq',
# MAGIC 'Data match between trm_reports databricks corrsepondence table and alteryx output table',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_prosecution_history_dq',
# MAGIC 'Data match between trm_reports databricks prosecution_history table and alteryx output table',
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

# MAGIC %md
# MAGIC #### Second & Third Level

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_on_hold_dq',
# MAGIC 'Data match between trm_reports databricks on_hold table and alteryx output table',
# MAGIC 'SECOND_THIRD_LEVEL',
# MAGIC 'Second level data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_fixed_filings_dq',
# MAGIC 'Data match between trm_reports databricks fixed_class_counts table and alteryx output table',
# MAGIC 'SECOND_THIRD_LEVEL',
# MAGIC 'Second level data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_filings_dq',
# MAGIC 'Data match between trm_reports databricks filings table and alteryx output table',
# MAGIC 'SECOND_THIRD_LEVEL',
# MAGIC 'Third level data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_pendency_dq',
# MAGIC 'Data match between trm_reports databricks pendency table and alteryx output table',
# MAGIC 'SECOND_THIRD_LEVEL',
# MAGIC 'Third level data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_inventory_dq',
# MAGIC 'Data match between trm_reports databricks inventory dashboard tables and alteryx output tables',
# MAGIC 'SECOND_THIRD_LEVEL',
# MAGIC 'Third level data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_form_paragraph_dq',
# MAGIC 'Data match between trm_reports databricks form_paragraph table and alteryx output table',
# MAGIC 'SECOND_THIRD_LEVEL',
# MAGIC 'Third level data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_post_registration_dq',
# MAGIC 'Data match between trm_reports databricks post registration tables and alteryx output tables',
# MAGIC 'SECOND_THIRD_LEVEL',
# MAGIC 'Third level data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_goods_services_dq',
# MAGIC 'Data match between trm_reports databricks goods_services table and alteryx output table',
# MAGIC 'SECOND_THIRD_LEVEL',
# MAGIC 'Third level data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_quality_dq',
# MAGIC 'Data match between trm_reports databricks quality table and alteryx output table',
# MAGIC 'SECOND_THIRD_LEVEL',
# MAGIC 'Third level data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trm_reports_ttab_dq',
# MAGIC 'Data match between trm_reports databricks TTAB tables and alteryx output tables',
# MAGIC 'SECOND_THIRD_LEVEL',
# MAGIC 'Third level data verification',
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

# MAGIC %md
# MAGIC #### Reports

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_tqr_email_report',
# MAGIC 'Data match between trm_reports databricks TTAB tables and alteryx output tables',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_sn_status_count',
# MAGIC 'Data match between trm_reports databricks and alteryx output sn_status table',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_unpaid_fee_alert',
# MAGIC 'Data match between trm_reports databricks and unpaid_fees_alert_history ',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# DBTITLE 1,exaimer_ppa
# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_examiner_ppa_report',
# MAGIC 'Data match between trm_reports databricks and ppa_report ',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# DBTITLE 1,tm_fee_code_daily_income
# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_tm_fee_code_daily_income',
# MAGIC 'Data match between trm_reports databricks and tm_fee_code_daily_income ',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_kingpin',
# MAGIC 'Data match between trm_reports databricks and alteryx output kingpin table',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# DBTITLE 1,TMINTL_AUTO
# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_tmintl_auto_protect',
# MAGIC 'Data match between trm_reports databricks and alteryx output tmintl_auto_project table',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_prima_fascia_new_fps_report',
# MAGIC 'Data match between trm_reports databricks and alteryx output prima_fascia_form_paragraph_usage_report table',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_status_no_hold_report',
# MAGIC 'Data match between trm_reports databricks and alteryx output sn_status_hold table',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_new_fee_code_alert',
# MAGIC 'Data match between trm_reports databricks and alteryx output new fee code alerts',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_ttab_oppsition_response_report',
# MAGIC 'Data match between trm_reports databricks and alteryx output ttab_opposition_response table',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_tm_expired_registrations',
# MAGIC 'Data match between trm_reports databricks and alteryx output tm_expired_registrations table',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_employee_grade_etl',
# MAGIC 'Data match between trm_reports databricks and alteryx output employee_grade table',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_efy_ap116_renewal_report',
# MAGIC 'Data match between trm_reports databricks and alteryx output efy_ap116_renewal_report table',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_fee_discrepancy_report',
# MAGIC 'Data match between trm_reports databricks and alteryx output fee_discrepancy table',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_first_action_report_v1',
# MAGIC 'Data match between trm_reports databricks and alteryx output pendency_dashboard table',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_goods_and_services_report',
# MAGIC 'Data match between trm_reports databricks and alteryx output goods_and_services_report table',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_og_issue_registration_report',
# MAGIC 'Data match between trm_reports databricks and alteryx output Registrations by OG Issue Data table',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_first_action',
# MAGIC 'Data match between trm_reports databricks and alteryx output first_actions_summary table',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC     'ntb_trmreports_630_638_overdue',
# MAGIC     'Report Logic: 1. Calculate rolling 30 day first action pendency for madrid and non-madrid.2. Flag 630 and 638 cases with no first action that are 2+ months older than the 30 day rolling first action pendency (separately for madrid and non madrid).',
# MAGIC     'REPORTS',
# MAGIC     'Report output data verification',
# MAGIC     '${config.config_file_name}',
# MAGIC     '',
# MAGIC     '',
# MAGIC     '${config.src_sys_name}',
# MAGIC     'ETL',
# MAGIC     current_timestamp(),
# MAGIC     'ETL',
# MAGIC     current_timestamp()
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_email_address_for_cx_survey',
# MAGIC 'Data match between trm_reports databricks and alteryx output email_address_for_cx_survey_trm_efile table',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_attorney_outlier_growth_foreign_applicants',
# MAGIC 'Data match between trm_reports databricks and alteryx output for attroney_history',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_currently_processing_first_actions_with_controls',
# MAGIC 'Data match between trm_reports databricks and alteryx output for currently processing',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_preexam_fee_checker',
# MAGIC 'Data match between trm_reports databricks and alteryx output for preexam fee checker',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_tm_category_case_counts',
# MAGIC 'Data match between trm_reports databricks and alteryx output for tm category case counts',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_trmreports_tranen_tranex_with_limgr',
# MAGIC 'Data match between trm_reports databricks and tranen_tranex_with_limgr ',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
# MAGIC VALUES('0',
# MAGIC 'ntb_third_level_tm_opb_data_portal',
# MAGIC 'Data match between trm_reports databricks and Alteryx for TM OPB Metrics data.',
# MAGIC 'REPORTS',
# MAGIC 'Report output data verification',
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


