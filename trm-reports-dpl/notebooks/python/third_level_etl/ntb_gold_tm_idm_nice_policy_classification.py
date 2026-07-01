# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

# DBTITLE 1,Define env parameter
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Run common functions and param ntbk
# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
#src_catalog = common_configs['schema']['tmngpdb_src_catalog']
print(f"{trgt_catalog=}")
spark.conf.set('conf.catalog', trgt_catalog)
#spark.conf.set('conf.src_catalog', src_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

df_nice_implementation = spark.sql(f"""
select idml.* ,
substr(ncl_version,-5,4) as year, 
substr(ncl_version,2,2) as edition, 
regexp_replace(ncl_version, '"', '') as ncl_version_op,
count(*) over (partition by substr(ncl_version,-5,4)) as yearly_count,
add.date as additions_date,
add.public_notes as additions_public_note,
add.employee_notes as additions_employee_note,
add.class as additions_class,
add.codes as additions_code,
trim(upper(add.initials)) as additions_initials,
add.internal_comments as additions_internal_comments,
add.descriptions as add_descriptions,
case when add.descriptions is not null then 1 else 0 end as additions_ind,
sum(case when add.descriptions is not null then 1 else 0 end) over (partition by substr(ncl_version,-5,4)) as yearly_count_additions,
current_timestamp() as update_ts,
"etl" as update_user_id
from {trgt_catalog}.bronze.nice_idmanual_csv_file_data idml
left outer join {trgt_catalog}.bronze.nice_additions_xlsm_file_data add
on lower(nvl(idml.description,'')) = lower(nvl(add.descriptions,''))
and idml.class = add.class
 """)

#df_nice_implementation.display()

# COMMAND ----------

# DBTITLE 1,Write data to gold table
df_nice_implementation.write.format("delta") \
    .option("path", f"s3://bdr-databricks-app-{dbx_env}/eds/delta_tables/{trgt_catalog}/gold/tm_idm_nice_policy_classification") .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{trgt_catalog}.gold.tm_idm_nice_policy_classification")

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit(f"Completed loading tm_idm_nice_policy_classification Table ")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from  trm_reporting_dev.gold.tm_idm_nice_policy_classification
# MAGIC --where lower(description) like lower('%Surveying apparatus and instruments%')

# COMMAND ----------


