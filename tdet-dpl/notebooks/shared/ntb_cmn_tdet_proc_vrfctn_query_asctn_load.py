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
tmngpdb_catalog = configs["schema"]["source_tmngpdb_catalog"]
src_sys_name = "TDET_SEARCH"

spark.conf.set("config.data_quality_catalog", data_quality_catalog)
spark.conf.set("config.tmngpdb_catalog", tmngpdb_catalog)
spark.conf.set("config.src_sys_name", src_sys_name)

print(f"{data_quality_catalog=} {src_sys_name=} {tmngpdb_catalog=}")

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from
# MAGIC   ${config.data_quality_catalog}.silver.cmn_proc_vrfctn_query_asctn
# MAGIC where
# MAGIC   src_sys_name = '${config.src_sys_name}'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   TABLE ${config.data_quality_catalog}.silver.cmn_proc_vrfctn_query_asctn (
# MAGIC     PROC_NAME,
# MAGIC     QUERY_SET_ID,
# MAGIC     QUERY_DQ_CD,
# MAGIC     TRGT_QUERY_NAME,
# MAGIC     SRC_QUERY_NAME,
# MAGIC     QUERY_SET_DESC,
# MAGIC     ERR_THRSHLD_PCT,
# MAGIC     SRC_SYS_NAME
# MAGIC   )
# MAGIC VALUES
# MAGIC   (
# MAGIC     'ntb_tdet_gold_search_cnt_non_owner_data_vrfctn',
# MAGIC     '1',
# MAGIC     'RM',
# MAGIC     'TDET_TRGT_GOLD_ZERO_COUNT_CHECK_MULTIPLE_NON_OWNER_PR',
# MAGIC     '',
# MAGIC     'Checks for duplicate target counts for non-owner party roles',
# MAGIC     '0',
# MAGIC     '${config.src_sys_name}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'ntb_tdet_gold_search_cnt_email_data_vrfctn',
# MAGIC     '2',
# MAGIC     'RM',
# MAGIC     'TDET_TRGT_GOLD_ZERO_COUNT_CHECK_MULTIPLE_EMAILS_PER_PR_PER_PRN',
# MAGIC     '',
# MAGIC     'Checks for duplicate target counts for emails per single party role, per party role number',
# MAGIC     '0',
# MAGIC     '${config.src_sys_name}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'ntb_tdet_gold_search_cnt_phone_data_vrfctn',
# MAGIC     '3',
# MAGIC     'RM',
# MAGIC     'TDET_TRGT_GOLD_ZERO_COUNT_CHECK_MULTIPLE_PHONES_PER_PR_PER_PRN',
# MAGIC     '',
# MAGIC     'Checks for duplicate target counts for phones per single party role, per party role number',
# MAGIC     '0',
# MAGIC     '${config.src_sys_name}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'ntb_tdet_gold_search_cnt_addr_data_vrfctn',
# MAGIC     '4',
# MAGIC     'RM',
# MAGIC     'TDET_TRGT_GOLD_ZERO_COUNT_CHECK_MULTIPLE_ADDR_PER_PR_PER_PRN',
# MAGIC     '',
# MAGIC     'Checks for duplicate target counts for addresses per single party role, per party role number',
# MAGIC     '0',
# MAGIC     '${config.src_sys_name}'
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   ${config.data_quality_catalog}.silver.cmn_proc_vrfctn_query_asctn
# MAGIC where
# MAGIC   src_sys_name = '${config.src_sys_name}'

# COMMAND ----------

dbutils.notebook.exit(f"Completed Loading {data_quality_catalog}.silver.cmn_proc_vrfctn_query_asctn.")
