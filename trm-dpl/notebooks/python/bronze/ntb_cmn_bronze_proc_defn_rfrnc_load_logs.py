# Databricks notebook source
# DBTITLE 1,Define widgets
dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")
#TMBUSCALENDAR,TMINTLTM,TMNGPDB,DATABRIDGE,EFOIAP,EOGADMIN,JBTEASPS,PROCEEDING,TMPRODVTY,TMREVIEWS,TRMWORKER, TMNGFPEPP, TMNGIDMP

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME").rstrip()
src_name = SRC_SYS_NAME.lower()

database = 'bronze'
control_table = 'cdc_batch_job_control'

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
trgt_catalog = common_configs['schema']['trgt_catalog']
print(f'{data_quality_catalog=} ')

spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.sql(f"set data_quality_db = data_quality_catalog.lower()")
spark.conf.set('config.src_sys_name', SRC_SYS_NAME)
spark.sql(f"set src_sys_name = SRC_SYS_NAME")
spark.conf.set('config.proc_name', proc_name)
spark.sql(f"set proc_name = proc_name")
spark.conf.set('config.catalog_proc_name', catalog_proc_name)
spark.sql(f"set catalog_proc_name = catalog_proc_name")
spark.conf.set('config.catalog_ddl_proc_name', catalog_ddl_proc_name)
spark.sql(f"set catalog_ddl_proc_name = catalog_ddl_proc_name")
spark.conf.set('config.config_file_name', config_file_name)
spark.sql(f"set config_file_name = config_file_name")


# COMMAND ----------

control_df = spark.sql(f"select * from {trgt_catalog}.{database}.{control_table} " )
dms_full_load_jobs_parameter_list = []

jobs_control_parameters = control_df.collect()
for autoloader_parameters_row in jobs_control_parameters:
    dms_full_load_jobs_parameter_list.append(
        (
            autoloader_parameters_row['src_folder'], 
            autoloader_parameters_row['catalog_name'],
            autoloader_parameters_row['database_name'], 
            autoloader_parameters_row['table_name']
        )
    )
print(dms_full_load_jobs_parameter_list)

# COMMAND ----------

spark.sql(f"""DELETE FROM {data_quality_catalog}.SILVER.CMN_PROC_DEFN_RFRNC WHERE SRC_SYS_NAME='{SRC_SYS_NAME}' AND PROC_NAME LIKE '%_brnz_load' and PROC_CTGRY_CD='SRC_TO_BRNZ'""")

# COMMAND ----------

for table_name in dms_full_load_jobs_parameter_list: 
        try:
            print("\n")
            print(f"Performing insert for {table_name[3]}:")
            
            proc_defn_rfrnc_insert_query =  f"""INSERT INTO TABLE {data_quality_catalog}.SILVER.CMN_PROC_DEFN_RFRNC (PRNT_PROC_ID,PROC_NAME,PROC_DESC,PROC_CTGRY_CD,PROC_CTGRY_DESC,PROC_CNFG_FILE_PATH,SRC_TBL_NAME,TRGT_TBL_NAME,SRC_SYS_NAME,    AUDT_INSRT_ID,AUDT_INSRT_TS,AUDT_UPDT_ID,AUDT_UPDT_TS)
            VALUES('0',
            'ntb_{src_name}_{table_name[3]}_brnz_load',
            'Process to verify count match between oracle source tables and bronze layer',
            'SRC_TO_BRNZ',
            'Source to Bronze layer load',
            '{config_file_name}',
            '{table_name[3]}',
            '{table_name[3]}',
            '{SRC_SYS_NAME}',
            'ETL',
            current_timestamp(),
            'ETL',
            current_timestamp()
            )"""
            
            spark.sql(proc_defn_rfrnc_insert_query)
        except Exception as e:
            
            print("Exception message: {}".format(e))

# COMMAND ----------

dbutils.notebook.exit(f"Completed Loading {data_quality_catalog}.SILVER.CMN_PROC_DEFN_RFRNC. ")

# COMMAND ----------

# MAGIC %sql
# MAGIC --DELETE from trm_tmintltm.bronze.cdc_batch_job_control
# MAGIC --where lower(source_table_name) not in ('international_appl_event' ,'international_appl_evnt_rsn','base_appl_intl_reg' )

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from data_quality_dev.SILVER.CMN_PROC_DEFN_RFRNC
# MAGIC --where src_sys_name = 'TMINTLTM'

# COMMAND ----------


