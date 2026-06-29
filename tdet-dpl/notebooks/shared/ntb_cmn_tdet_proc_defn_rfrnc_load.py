# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file_name = "tdet-conf.yaml"
config_file = f"../config/{dbx_env}/{config_file_name}"
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

configs = read_yaml(config_file)
data_quality_catalog = configs["schema"]["data_quality_catalog"]
tdet_catalog = configs["schema"]["trgt_catalog"]
src_sys_name = "TDET_SEARCH"
procedure_category_code = "ZERO_COUNT_CHECK"

spark.conf.set("config.data_quality_catalog", data_quality_catalog)
spark.conf.set("config.tdet_catalog", tdet_catalog)
spark.conf.set("config.procedure_category_code", procedure_category_code)
spark.conf.set("config.config_file_name", config_file_name)
spark.conf.set("config.src_sys_name", src_sys_name)

print(f"{data_quality_catalog=} {src_sys_name=} {procedure_category_code=}")

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from
# MAGIC   ${config.data_quality_catalog}.silver.cmn_proc_defn_rfrnc
# MAGIC where
# MAGIC   src_sys_name = '${config.src_sys_name}'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   TABLE ${config.data_quality_catalog}.silver.cmn_proc_defn_rfrnc (
# MAGIC     PRNT_PROC_ID,
# MAGIC     PROC_NAME,
# MAGIC     PROC_DESC,
# MAGIC     PROC_CTGRY_CD,
# MAGIC     PROC_CTGRY_DESC,
# MAGIC     PROC_CNFG_FILE_PATH,
# MAGIC     SRC_TBL_NAME,
# MAGIC     TRGT_TBL_NAME,
# MAGIC     SRC_SYS_NAME,
# MAGIC     AUDT_INSRT_ID,
# MAGIC     AUDT_INSRT_TS,
# MAGIC     AUDT_UPDT_ID,
# MAGIC     AUDT_UPDT_TS
# MAGIC   )
# MAGIC VALUES
# MAGIC   (
# MAGIC     '0',
# MAGIC     'ntb_tdet_gold_search_cnt_non_owner_data_vrfctn',
# MAGIC     'Process to verify count has not exceeded threshold for non-owner party roles',
# MAGIC     '${config.procedure_category_code}',
# MAGIC     'Zero Duplicate Counts Data Verification',
# MAGIC     '${config.config_file_name}',
# MAGIC     '',
# MAGIC     '${config.tdet_catalog}.gold.search',
# MAGIC     '${config.src_sys_name}',
# MAGIC     'ETL',
# MAGIC     current_timestamp(),
# MAGIC     'ETL',
# MAGIC     current_timestamp()
# MAGIC   ),
# MAGIC   (
# MAGIC     '0',
# MAGIC     'ntb_tdet_gold_search_cnt_phone_data_vrfctn',
# MAGIC     'Process to verify count has not exceeded threshold for multiple phones per single party role, per party role number',
# MAGIC     '${config.procedure_category_code}',
# MAGIC     'Zero Duplicate Counts Data Verification',
# MAGIC     '${config.config_file_name}',
# MAGIC     '',
# MAGIC     '${config.tdet_catalog}.gold.search',
# MAGIC     '${config.src_sys_name}',
# MAGIC     'ETL',
# MAGIC     current_timestamp(),
# MAGIC     'ETL',
# MAGIC     current_timestamp()
# MAGIC   ),
# MAGIC   (
# MAGIC     '0',
# MAGIC     'ntb_tdet_gold_search_cnt_email_data_vrfctn',
# MAGIC     'Process to verify count has not exceeded threshold for multiple emails per single party role, per party role number',
# MAGIC     '${config.procedure_category_code}',
# MAGIC     'Zero Duplicate Counts Data Verification',
# MAGIC     '${config.config_file_name}',
# MAGIC     '',
# MAGIC     '${config.tdet_catalog}.gold.search',
# MAGIC     '${config.src_sys_name}',
# MAGIC     'ETL',
# MAGIC     current_timestamp(),
# MAGIC     'ETL',
# MAGIC     current_timestamp()
# MAGIC   ),
# MAGIC   (
# MAGIC     '0',
# MAGIC     'ntb_tdet_gold_search_cnt_addr_data_vrfctn',
# MAGIC     'Process to verify count has not exceeded threshold for multiple addresses per single party role, per party role number',
# MAGIC     '${config.procedure_category_code}',
# MAGIC     'Zero Duplicate Counts Data Verification',
# MAGIC     '${config.config_file_name}',
# MAGIC     '',
# MAGIC     '${config.tdet_catalog}.gold.search',
# MAGIC     '${config.src_sys_name}',
# MAGIC     'ETL',
# MAGIC     current_timestamp(),
# MAGIC     'ETL',
# MAGIC     current_timestamp()
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   ${config.data_quality_catalog}.silver.cmn_proc_defn_rfrnc
# MAGIC where
# MAGIC   src_sys_name = '${config.src_sys_name}'

# COMMAND ----------

dbutils.notebook.exit(f"Completed Loading {data_quality_catalog}.silver.cmn_proc_defn_rfrnc.")
