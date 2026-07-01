# Databricks notebook source
# DBTITLE 1,Setting up the env
#dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

# DBTITLE 1,config file widget
#dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ./../first_level_etl/ntb_comm_imports_altx $config_file = config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
run_env = common_configs['schema']['tmngpdb_src_catalog']
edw_scope = common_configs['secrets']['edw_scope']

print(reporting_catalog)

# COMMAND ----------

# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,inputs 
# EDW Inputs

edw_query1="Select * From FORECAST.VW_TM_SALE_TRAN"
ip1_df= read_data_from_oracle_conn_dsu_cmn(edw_query1,edw_scope)
edw_query2="Select * From FORECAST.VW_SALE_TRAN_PRE_FY2010"
ip2_df = read_data_from_oracle_conn_dsu_cmn(edw_query2,edw_scope)

# TRM Silver level table inputs
ip3_query = f'''select * from {reporting_catalog}.silver.milestone'''
ip3_df= spark.sql(ip3_query)


ip4_query = f'''select * from {reporting_catalog}.silver.bibliography'''
ip4_df= spark.sql(ip4_query)

ip5_query = f'''select * from {reporting_catalog}.silver.class'''
ip5_df= spark.sql(ip5_query)


ip6_query = f'''select * from {reporting_catalog}.silver.fixed_class_counts''' #Review Comment:fixed_class_counts table should be created in trm_reporting_dev.silver schema 
ip6_df= spark.sql(ip6_query)
