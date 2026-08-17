# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE WIDGET TEXT dbx_env DEFAULT "dev"

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file = "../../../notebooks/config/"+dbutils.widgets.get("dbx_env").rstrip()+"/tqr-conf.yaml"
print(f'{config_file=}')
config_file_path = config_file

# COMMAND ----------

# MAGIC %md
# MAGIC ###Enhancements:
# MAGIC <pre>
# MAGIC 1. In cell 8 (Data Load ETL Query) on line no 85        AND first_tm.effective_ts >='2021-10-01 00:00:00'  -- This value has to come from property file. #DONE
# MAGIC
# MAGIC 2. Can we change job_control and job_log to have single record for a given workflow execution ? For example , when a job started,start_ts will have current timestamp, end_ts will have null value. and at end of job end_ts will be updated to current timestamp. status_ct will be changed to success or error depends on the job execution status.# DONE
# MAGIC
# MAGIC 3. Also add a new column called Job_log_id for job_log table and job_control_id for job_control table. This is going to be an identity column and id will be inserted automatically.#DONE
# MAGIC
# MAGIC
# MAGIC 4. Before inserting record into event_inventory_stage table, we should check for a given combination of
# MAGIC review_type_cd, serial_num_tx, and source_event_dt  record shouldn't be present in it. We should insert only records which are not present in target table (event_inventory_stage )#DONE
# MAGIC
# MAGIC
# MAGIC </pre>

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

common_configs = read_yaml(config_file)

tqr_catalog = common_configs['schema']['tqr_catalog']
trm_catalog = common_configs['schema']['trm_catalog']
src_tqr_db = trm_catalog+'.bronze'
stg_tqr_db = tqr_catalog+'.silver'
#Job variables
job_name = 'ntb_tqr_silver_event_inventory_stage_first_tm_exam'
config_file_path = config_file
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
    df_event_inventory_stage_first = spark.sql("""

    WITH 
    sub_src_trm_application_event AS
    (
       SELECT *
       FROM   ${stg_tqr_db}.src_trm_application_event
       WHERE  trim(business_event_reason_cd) IN ('CEAPF','GEAPF','CPEAF','CNRTF','CPRAF','GPRAF','GNPAF','GNPEF','GNRTF' ) ),

    intervening7daysfirst AS
    (
       SELECT *
       FROM   ${stg_tqr_db}.src_trm_application_event
       WHERE  trim(business_event_reason_cd) IN ('CEAPF','GEAPF','CPEAF','CNRTF','CPRAF','GPRAF','GNPAF','GNPEF','GNRTF' ) 
       AND    cast(effective_ts AS DATE) BETWEEN date_sub(from_utc_timestamp(current_timestamp(),'America/New_York'),7) AND date_sub(from_utc_timestamp(current_timestamp(),'America/New_York'),0) ),
 
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
    SELECT review_type_cd, 
        serial_num_tx,
        source_system_nm,
        search_present_in,
        source_event_dt,
        docket_in,
        mark_literal_element_tx,
        mark_drawing_type_cd,
        mark_drawing_type_title_tx,
        mark_description_tx,
        examiner_employee_no,
        organization_cd,
        event_json_doc,
        inventory_create_ts,
        lock_control_no,
        create_ts,
        create_user_id,
        last_mod_ts,
        last_mod_user_id,
        cast('1' AS BOOLEAN) AS is_tm_exam
    FROM   (
           SELECT DISTINCT '100'          AS review_type_cd,
                  app.serial_num_tx     AS serial_num_tx,
                  app.source_system_nm  AS source_system_nm,
                  NULL                  AS search_present_in,
                  first_tm.effective_ts AS source_event_dt,
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
                   from_utc_timestamp(current_timestamp(),'America/New_York')   AS create_ts,
                   'etl'               AS create_user_id,
                   from_utc_timestamp(current_timestamp(),'America/New_York')   AS last_mod_ts,
                   'etl'               AS last_mod_user_id,
                   0                   AS lock_control_no,
                   row_number() over (PARTITION BY first_tm.serial_num_tx ORDER BY first_tm.effective_ts ASC) AS visit_number
             FROM            ${stg_tqr_db}.src_trm_application app
             join            sub_src_trm_application_event first_tm
             ON              app.serial_num_tx=first_tm.serial_num_tx
             join            filingbasis fb
             ON              app.serial_num_tx=fb.serial_num_tx
             left join       ${stg_tqr_db}.employee_organization emp
             ON              app.examiner_employee_no=emp.employee_no
             WHERE           NOT EXISTS
                                       (
                                              SELECT 1
                                              FROM   intervening7daysfirst int24first_tm
                                              WHERE  int24first_tm.serial_num_tx = app.serial_num_tx )
             AND             first_tm.effective_ts <= date_sub(from_utc_timestamp(current_timestamp(),'America/New_York'),7)
             AND             first_tm.effective_ts >= to_date('${dataload_dt}')
       )tab
    WHERE  visit_number = 1)SRC
    WHERE NOT EXISTS(
        SELECT 1 FROM ${stg_tqr_db}.event_inventory_stage TRGT
        WHERE CONCAT_WS(',',TRGT.review_type_cd,TRGT.serial_num_tx,TRGT.source_event_dt) = CONCAT_WS(',',SRC.review_type_cd,SRC.serial_num_tx,SRC.source_event_dt)
        );

    """)
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(stg_tqr_db, job_name, job_start_ts,'failed',0,e)
    raise

# COMMAND ----------

# try:
#     df_count = df_event_inventory_stage_first.count()
#     df_event_inventory_stage_first.write.mode("append").format("delta").insertInto(f'{stg_tqr_db}.{trgt_tbl_name}')
#     end_job_cntl(stg_tqr_db, job_name, job_start_ts,'completed', df_count,"job completed successfully")
# except Exception as e:
#     print("Exception message: {}".format(e))
#     end_job_cntl(stg_tqr_db, job_name, job_start_ts,'failed',0,e)
#     raise

# COMMAND ----------

#ConcurrentAppendException: [DELTA_CONCURRENT_APPEND] ConcurrentAppendException: Files were added to the root of the table by a concurrent update. Please try the operation again.
import time
 
max_retries = 3
retry_delay = 5  # seconds
 
for attempt in range(max_retries):
    try:
        df_count = df_event_inventory_stage_first.count()
        df_event_inventory_stage_first.write.mode("append").format("delta").insertInto(f'{stg_tqr_db}.{trgt_tbl_name}')
        end_job_cntl(stg_tqr_db, job_name, job_start_ts, 'completed', df_count, "job completed successfully")
        break
    except Exception as e:
        if attempt < max_retries - 1:
            print(f"Attempt {attempt + 1} failed with exception: {e}. Retrying after {retry_delay} seconds...")
            time.sleep(retry_delay)
        else:
            print("Exception message: {}".format(e))
            end_job_cntl(stg_tqr_db, job_name, job_start_ts, 'failed', 0, e)
            raise

# COMMAND ----------

dbutils.notebook.exit(f"Completed Loading {stg_tqr_db}.{trgt_tbl_name}. Number of records appended: {df_count} ")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Unit test cells below

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tqr.silver.job_log where job_nm = 'ntb_tqr_silver_event_inventory_stage_first_tm_exam' order by start_ts desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tqr.silver.job_control where job_nm = 'ntb_tqr_silver_event_inventory_stage_first_tm_exam' order by load_ts desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from tqr.silver.event_inventory_stage

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tqr.silver.event_inventory_stage
