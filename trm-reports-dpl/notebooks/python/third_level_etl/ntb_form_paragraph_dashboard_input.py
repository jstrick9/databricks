# Databricks notebook source
# DBTITLE 1,Setting environment
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

# DBTITLE 1,config file widget
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
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

print(reporting_catalog,run_env)

# COMMAND ----------

# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

## edw query 1

edw_query1= "Select * from FP_WEBPAGE_LINK weblink"
df_ip_weblink = read_data_from_oracle_conn_dsu_cmn(edw_query1,edw_scope)

df_ip_fpep = spark.sql(f"select * from {reporting_catalog}.silver.fpep_fact")

ip1_df = df_ip_fpep.join(df_ip_weblink, "FP_ID", "left")

## Milestone datafrme
ip2_query = f'''select * from {reporting_catalog}.silver.milestone'''
ip2_df= spark.sql(ip2_query)

## bibliography dataframe
ip3_query = f'''select * from {reporting_catalog}.silver.bibliography'''
ip3_df= spark.sql(ip3_query)

## edw query 2
edw_query2  = "Select * From EMP_GRADE"
ip4_df = read_data_from_oracle_conn_dsu_cmn(edw_query2,edw_scope)

## owner dataframe
ip5_query = f'''select * from {reporting_catalog}.silver.owner'''
ip5_df= spark.sql(ip5_query)

## form_paragraph_counts dataframe
ip6_query = f'''select * from {reporting_catalog}.silver.form_paragraph_counts''' #Review Comment:form_paragraph_dashboard_s32 table should be created in trm_reporting_dev.silver schema 
ip6_df= spark.sql(ip6_query)
