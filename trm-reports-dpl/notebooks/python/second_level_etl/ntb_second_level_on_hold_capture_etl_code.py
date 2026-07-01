# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will gives us the overview for On Hold Capture ETL. Which contain Input and output dataframes.
# MAGIC Subsequent Notebook will provide the psudo code.
# MAGIC

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name

print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# MAGIC %run ./../first_level_etl/ntb_comm_imports_altx $config_file = config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
run_env = common_configs['schema']['tmngpdb_src_catalog']
print(reporting_catalog)

# COMMAND ----------

# DBTITLE 1,Start Job Control
job_name = 'ntb_second_level_on_hold_capture_etl_code'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,job_start_ts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input's 

# COMMAND ----------

ip1_query = f'''select CAST(REGEXP_SUBSTR(WI.CFK_OBJECT_GID, '[^:]+$') AS INTEGER) AS ATH_SER_NUM,
TRIM(AH.PLACED_ON_HOLD_DT) AS ATH_CREATE_DT,
cast(concat(hour(AH.PLACED_ON_HOLD_DT),minute(AH.PLACED_ON_HOLD_DT),second(AH.PLACED_ON_HOLD_DT)) as int) as ATH_CREATE_TI,
CAST(REGEXP_SUBSTR(AH.CFK_HOLD_WORKER_GID, '[^:]+$') AS INTEGER) AS ATH_EMP_NUM,
TRIM(AH.LAST_ACTION_DT) ATH_LAST_UPD_DT,
cast(concat(hour(AH.LAST_ACTION_DT),minute(AH.LAST_ACTION_DT),second(AH.LAST_ACTION_DT)) as int) AS ATH_LAST_UPD_TI,
CAST(REGEXP_SUBSTR(AH.CFK_LAST_ACTION_WORKER_GID, '[^:]+$') AS INTEGER) AS ATH_LAST_EMP_NUM,
CASE WHEN CFK_HOLD_STATUS_CD = 'ON_HOLD' THEN 0 WHEN CFK_HOLD_STATUS_CD = 'ASSIGNED_EXAMINER' THEN 1 WHEN CFK_HOLD_STATUS_CD = 'RETURNED_UNASSIGNED' THEN 2 END AS ATH_HOLD_STATUS,
CASE WHEN AH.CFK_HOLD_STATUS_CD = 'ON_HOLD' THEN 1 ELSE 0 END AS ATH_ACTIVE_STATUS,
AH.HOLD_DOCKET_NO AS ATH_HOLD_DOCKET,
AH.LAST_ACTION_DT AS LAST_MODIFIED_DT,
AH.LAST_ACTION_DT AS ORACLE_APPLY_TIME
from {run_env}.bronze.attorney_hold ah left join {run_env}.bronze.work_item_object wi on WI.FK_WORK_ITEM_GID = AH.FK_WORK_ITEM_GID'''

ip1_df= spark.sql(ip1_query)
#display(ip1_df)


ip2_query = f'''select * from {reporting_catalog}.silver.on_hold'''
ip2_df= spark.sql(ip2_query)
#display(ip2_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Selecting columns for 1st intput

# COMMAND ----------

ip1_df = ip1_df.select(col("ATH_SER_NUM"),
col("ATH_CREATE_DT"),
col("ATH_CREATE_TI"),
col("ATH_EMP_NUM"),
col("ATH_LAST_UPD_DT"),
col("ATH_LAST_UPD_TI"),
col("ATH_LAST_EMP_NUM"),
col("ATH_HOLD_STATUS"),
col("ATH_ACTIVE_STATUS"),
col("ATH_HOLD_DOCKET"),
col("LAST_MODIFIED_DT"),
col("ORACLE_APPLY_TIME")
).withColumn(
    "CREATE_TS", current_timestamp()
).withColumn(
    "CREATE_USER_ID", lit("-1")
).withColumn(
    "UPDATE_TS", current_timestamp()
).withColumn(
    "UPDATE_USER_ID", lit("-1")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Joining two dataframe

# COMMAND ----------

df_final = ip1_df.alias("ip1_df").join(
        ip2_df.alias("ip2_df"), on="ATH_SER_NUM", how="outer"
        ).select(expr("nvl(ip1_df.ATH_SER_NUM, ip2_df.ATH_SER_NUM) as ATH_SER_NUM"),
             expr("nvl(ip1_df.ATH_CREATE_DT, ip2_df.ATH_CREATE_DT) as ATH_CREATE_DT"),
             expr("nvl(ip1_df.ATH_CREATE_TI, ip2_df.ATH_CREATE_TI) as ATH_CREATE_TI"),
             expr("nvl(ip1_df.ATH_EMP_NUM, ip2_df.ATH_EMP_NUM) as ATH_EMP_NUM"),
             expr("nvl(ip1_df.ATH_LAST_UPD_DT, ip2_df.ATH_LAST_UPD_DT) as ATH_LAST_UPD_DT"),
             expr("nvl(ip1_df.ATH_LAST_UPD_TI, ip2_df.ATH_LAST_UPD_TI) as ATH_LAST_UPD_TI"),
             expr("nvl(ip1_df.ATH_LAST_EMP_NUM, ip2_df.ATH_LAST_EMP_NUM) as ATH_LAST_EMP_NUM"),
             expr("nvl(ip1_df.ATH_HOLD_STATUS, ip2_df.ATH_HOLD_STATUS) as ATH_HOLD_STATUS"),
             expr("nvl(ip1_df.ATH_ACTIVE_STATUS, ip2_df.ATH_ACTIVE_STATUS) as ATH_ACTIVE_STATUS"),
             expr("nvl(ip1_df.ATH_HOLD_DOCKET, ip2_df.ATH_HOLD_DOCKET) as ATH_HOLD_DOCKET"),
             expr("nvl(ip1_df.LAST_MODIFIED_DT, ip2_df.LAST_MODIFIED_DT) as LAST_MODIFIED_DT"),
             expr("nvl(ip1_df.ORACLE_APPLY_TIME, ip2_df.ORACLE_APPLY_TIME) as ORACLE_APPLY_TIME"),
             expr("nvl(ip1_df.CREATE_TS, ip2_df.CREATE_TS) as CREATE_TS"),
             expr("nvl(ip1_df.CREATE_USER_ID, ip2_df.CREATE_USER_ID) as CREATE_USER_ID"),
             expr("nvl(ip1_df.UPDATE_TS, ip2_df.UPDATE_TS) as UPDATE_TS"),
             expr("nvl(ip1_df.UPDATE_USER_ID, ip2_df.UPDATE_USER_ID) as UPDATE_USER_ID")
        )

# COMMAND ----------

# set column ordering
df_final = df_final.select('ath_ser_num',
 'ath_create_dt',
 'ath_create_ti',
 'ath_emp_num',
 'ath_last_upd_dt',
 'ath_last_upd_ti',
 'ath_last_emp_num',
 'ath_hold_status',
 'ath_active_status',
 'ath_hold_docket',
 'last_modified_dt',
 'oracle_apply_time',
 'create_ts',
 'create_user_id',
 'update_ts',
 'update_user_id')

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing it to table

# COMMAND ----------

try:
    #final_df_onhold.write.saveAsTable(f"{reporting_catalog}.silver.on_hold",mode="overwrite",partitionBy="ATH_HOLD_STATUS")
    df_final.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.silver.on_hold')
    recs_count = df_final.count()
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts,'completed', recs_count,"job completed successfully")
    dbutils.notebook.exit(f"Completed Loading on_hold Table ")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts,'failed',0,e)
    raise
    dbutils.notebook.exit(f"Failed Loading on_hold Table ")
