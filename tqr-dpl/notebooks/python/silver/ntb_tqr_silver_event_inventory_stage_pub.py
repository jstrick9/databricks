# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE WIDGET TEXT dbx_env DEFAULT "dev"

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file = "../../../notebooks/config/"+dbutils.widgets.get("dbx_env").rstrip()+"/tqr-conf.yaml"
print(f'{config_file=}')
config_file_path = config_file

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

common_configs = read_yaml(config_file)

tqr_catalog = common_configs['schema']['tqr_catalog']
trm_catalog = common_configs['schema']['trm_catalog']
src_tqr_db = trm_catalog+'.bronze'
stg_tqr_db = tqr_catalog+'.silver'

#Job variables
job_name = 'ntb_tqr_silver_event_inventory_stage_pub' 
trgt_tbl_name = 'event_inventory_stage'

#job start timestamp
#import datetime
job_start_ts = datetime.datetime.now()

print(f'{src_tqr_db=},{stg_tqr_db=},{job_start_ts=}')
spark.sql(f"set src_tqr_db = {src_tqr_db}")
spark.sql(f"set stg_tqr_db = {stg_tqr_db}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start Job Control

# COMMAND ----------

# DBTITLE 1,Create entry in job log table and get max dt from job control table
control_dt = begin_job_cntl(stg_tqr_db, job_name, job_start_ts)
print(f'{control_dt=}')

# COMMAND ----------

# DBTITLE 1,Get dataload date from config file
configs = read_yaml(config_file_path)
dataload_dt = configs['schema']['dataload_dt']
print(f'{dataload_dt=}')
spark.sql(f"set dataload_dt = {dataload_dt}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Load ETL Query

# COMMAND ----------

try:
    df_event_inventory_stage_pub = spark.sql("""

    WITH 
    filingbasis AS
    (
         SELECT   serial_num_tx ,
                  concat('{"filingBasis":',concat('[',concat_ws(',',collect_set(concat('"',filing_basis_cd,'"'))) ,']')) AS filing_basis_json
         FROM     ${stg_tqr_db}.src_trm_filing_basis
         WHERE    current_in ='Y'
         GROUP BY serial_num_tx )
 
    SELECT SRC.* 
    FROM
    (
           SELECT DISTINCT '102'        AS review_type_cd,
                  app.serial_num_tx     AS serial_num_tx,
                  app.source_system_nm  AS source_system_nm,
                  NULL                  AS search_present_in,
                  pub.effective_ts AS source_event_dt,
                  0                     AS docket_in,
                  CASE
                      WHEN app.literal_element_tx IS NULL THEN app.standard_character_tx
                      ELSE app.literal_element_tx
                  END                    AS mark_literal_element_tx ,
                  app.mark_drawing_type_cd AS mark_drawing_type_cd,
                  app.mark_drawing_type_title_tx AS  mark_drawing_type_title_tx,
                  app.mark_description_tx        AS mark_description_tx,
                  app.examiner_employee_no       AS examiner_employee_no,
                  CASE
                      WHEN emp.status_ct = 'error' THEN 'ZZZ'
                      ELSE emp.organization_cd
                   END                              AS organization_cd,
                   concat(fb.filing_basis_json,'}') AS event_json_doc,
                   from_utc_timestamp(current_timestamp(),'America/New_York')   AS inventory_create_ts,
                   0                   AS lock_control_no,
                   from_utc_timestamp(current_timestamp(),'America/New_York')   AS create_ts,
                   'etl'               AS create_user_id,
                   from_utc_timestamp(current_timestamp(),'America/New_York')   AS last_mod_ts,
                   'etl'               AS last_mod_user_id,
                   cast('0' AS BOOLEAN) AS is_tm_exam
             FROM            ${stg_tqr_db}.src_trm_application app
             join            ${stg_tqr_db}.src_trm_application_event pub
             ON              app.serial_num_tx=pub.serial_num_tx
             join            filingbasis fb
             ON              app.serial_num_tx=fb.serial_num_tx
             left join       ${stg_tqr_db}.employee_organization emp
             ON              app.examiner_employee_no=emp.employee_no
             WHERE           TRIM(pub.business_event_reason_cd) in ('CNSAO','CNSAP')
             AND             pub.effective_ts <= date_sub(from_utc_timestamp(current_timestamp(),'America/New_York'),2)
             AND             pub.effective_ts >= to_date('${dataload_dt}')
            )SRC
    WHERE NOT EXISTS(
        SELECT 1 FROM ${stg_tqr_db}.event_inventory_stage TRGT
        WHERE CONCAT_WS(',',TRGT.review_type_cd,TRGT.serial_num_tx,TRGT.source_event_dt) = CONCAT_WS(',',SRC.review_type_cd,SRC.serial_num_tx,SRC.source_event_dt)
        )
        ;

    """)
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(stg_tqr_db, job_name, job_start_ts,'failed',0,e)
    raise

# COMMAND ----------

try:
    df_count = df_event_inventory_stage_pub.count()
    df_event_inventory_stage_pub.write.mode("append").format("delta").insertInto(f'{stg_tqr_db}.{trgt_tbl_name}')
    end_job_cntl(stg_tqr_db, job_name, job_start_ts,'completed', df_count,"job completed successfully")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(stg_tqr_db, job_name, job_start_ts,'failed',0,e)
    raise

# COMMAND ----------

dbutils.notebook.exit(f"Completed Loading {stg_tqr_db}.{trgt_tbl_name}. Number of records appended: {df_count} ")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Unit test cells below

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tqr.silver.job_log where job_nm = 'ntb_tqr_silver_event_inventory_stage_pub' order by start_ts desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tqr.silver.job_control where job_nm = 'ntb_tqr_silver_event_inventory_stage_pub' order by load_ts desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from tqr.silver.event_inventory_stage where review_type_cd = 102

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tqr.silver.event_inventory_stage where review_type_cd = 102

# COMMAND ----------

# MAGIC %sql
# MAGIC describe tqr.silver.event_inventory_stage
