# Databricks notebook source
# DBTITLE 1,Get Parameters
# add text parameter default to dev
dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.dropdown("index", "trademark_applications", ["trademark_applications", "tqr"])
idx = dbutils.widgets.get("index")

# COMMAND ----------

# DBTITLE 1,Configuration
import yaml
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
if dbx_env =='qa':
    dbx_env = 'test'
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# DBTITLE 1,Run Common Functions and Parameters Notebook
# MAGIC %run  ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
print(trgt_catalog)

# COMMAND ----------

# DBTITLE 1,Job Time Control
import pytz
from pytz import timezone

job_name = f"ntb_silver_opensearch_load_{idx}"
start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
print(f'{start_ts=}')
control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,start_ts)

# COMMAND ----------

database = 'silver'
#control_table = 'cdc_batch_job_control'
#job_history_table = 'cdc_batch_job_history'

spark.conf.set('conf.trgt_catalog',  trgt_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.dbx_env', dbx_env)
spark.conf.set('conf.idx', idx)


# COMMAND ----------

# DBTITLE 1,Query based on Parameter
if idx == "trademark_applications":
    df_os = spark.sql(f"""
                  --currently in use
                  SELECT DISTINCT
                  --add in composite key, seems unique when distinct is maintained
                  CASE WHEN pre_exam_history_history_order IS NULL THEN concat(serial_number, ':', 'HO',':', '-1')
                  ELSE concat(serial_number, ':', 'HO', ':', pre_exam_history_history_order) END AS tm_app, 
                  serial_number AS ser_num,
                  assignee AS assignee,
                  pre_exam_status AS pre_exam_status,
                  trademark_track_type AS submission_type, 
                  from_utc_timestamp(try_to_timestamp(filing_date), 'US/Eastern') AS filing_ts,
                  from_utc_timestamp(try_to_timestamp(last_updated), 'US/Eastern') AS last_updated_ts, 
                  from_utc_timestamp(try_to_timestamp(date_last_uploaded), 'US/Eastern') AS last_uploaded_ts,
                  from_utc_timestamp(try_to_timestamp(date_pre_exam_received), 'US/Eastern') AS pre_exam_received_ts,
                  pre_exam_history_history_action AS history_action,
                  pre_exam_history_history_by AS history_by,
                  from_utc_timestamp(try_to_timestamp(pre_exam_history_history_date_time), 'US/Eastern') AS history_ts,
                  pre_exam_history_history_from AS history_from,
                  pre_exam_history_history_to AS history_to,
                  pre_exam_history_history_order AS history_order,
                  pre_exam_history_latest_order_no AS latest_order_no,
                  CURRENT_TIMESTAMP() AS create_ts,
                  CURRENT_TIMESTAMP() AS last_updt_ts

                  FROM {trgt_catalog}.bronze.pea_trademark_applications
                   """)
elif idx == "tqr":
    df_os = spark.sql(f"""
                      SELECT DISTINCT
                      CONCAT('tqr', ':', serial_number, ':', hash(*)) AS tqr_app,
                      assignee AS assignee,
                      from_utc_timestamp(try_to_timestamp(date_uploaded), 'US/Eastern') AS date_uploaded_ts,
                      pre_exam_status AS pre_exam_status,
                      review_manager AS review_manager,
                      from_utc_timestamp(try_to_timestamp(review_started), 'US/Eastern') AS review_started,
                      reviewer AS reviewer,
                      serial_number AS serial_number,
                      class_comments_action AS class_comments_action,
                      class_comments_message AS class_comments_message,
                      design_search_code_comments_action AS design_search_code_comments_action,
                      design_search_code_comments_message AS design_search_code_comments_message,
                      mark_drawing_code_comments_action AS mark_drawing_code_comments_actions,
                      mark_drawing_code_comments_message AS mark_drawing_code_comments_message,
                      pseudomarks_comments_action AS pseudomarks_comments_action,
                      pseudomarks_comments_message AS pseudomarks_comments_message,
                      word_mark_comments_action AS word_mark_comments_action,
                      word_mark_comments_message AS word_mark_comments_message,
                      CURRENT_TIMESTAMP() AS create_ts,
                      current_timestamp() AS last_updt_ts

                      FROM {trgt_catalog}.bronze.pea_tqr
                      """)

# COMMAND ----------

recs_count = df_os.count()

df_os.createOrReplaceTempView("os_temp")

# COMMAND ----------

recs_count

# COMMAND ----------

if recs_count > 0:
    if idx == "trademark_applications":
        try:
            spark.sql(f"""MERGE INTO {trgt_catalog}.{database}.pea_trademark_applications AS trgt
            USING os_temp src
            ON trgt.tm_app = src.tm_app
            WHEN MATCHED THEN
                UPDATE SET 
                ser_num = src.ser_num,
                assignee = src.assignee,
                pre_exam_status = src.pre_exam_status,
                submission_type = src.submission_type,
                filing_ts = src.filing_ts,
                last_updated_ts = src.last_updated_ts,
                last_uploaded_ts = src.last_uploaded_ts,
                pre_exam_received_ts = src.pre_exam_received_ts,
                history_action = src.history_action,
                history_by = src.history_by,
                history_ts = src.history_ts,
                history_from = src.history_from,
                history_to = src.history_to,
                history_order = src.history_order,
                latest_order_no = src.latest_order_no,
                create_ts = src.create_ts,
                last_updt_ts = current_timestamp()
            WHEN NOT MATCHED THEN 
                INSERT (tm_app, ser_num, assignee, pre_exam_status, submission_type, filing_ts, last_updated_ts, last_uploaded_ts, pre_exam_received_ts, history_action, history_by, history_ts, history_from, history_to, history_order, latest_order_no, create_ts, last_updt_ts)
                VALUES (src.tm_app, src.ser_num, src.assignee, src.pre_exam_status, src.submission_type, src.filing_ts, src.last_updated_ts, src.last_uploaded_ts, src.pre_exam_received_ts, src.history_action, src.history_by, src.history_ts, src.history_from, src.history_to, src.history_order, src.latest_order_no, current_timestamp(), current_timestamp())
            """)
            
            end_job_cntl(f"{trgt_catalog}.silver", job_name, start_ts,'completed',recs_count,"job completed successfully")

        except Exception as e:
            print("Exception message: {}".format(e))
            end_job_cntl(f"{trgt_catalog}.silver", job_name, start_ts,'failed',0,e)
            raise

    elif idx == "tqr":
        try:
            spark.sql(f"""MERGE INTO {trgt_catalog}.{database}.pea_tqr AS trgt
            USING os_temp src
            ON trgt.tqr_app = src.tqr_app
            WHEN MATCHED THEN
                UPDATE SET
                assignee = src.assignee,
                date_uploaded_ts = src.date_uploaded_ts,
                pre_exam_status = src.pre_exam_status,
                review_manager = src.review_manager,
                review_started = src.review_started,
                reviewer = src.reviewer,
                serial_number = src.serial_number,
                class_comments_action = src.class_comments_action,
                class_comments_message = src.class_comments_message,
                design_search_code_comments_action = src.design_search_code_comments_action,
                design_search_code_comments_message = src.design_search_code_comments_message,
                mark_drawing_code_comments_actions = src.mark_drawing_code_comments_actions,
                mark_drawing_code_comments_message = src.mark_drawing_code_comments_message,
                pseudomarks_comments_action = src.pseudomarks_comments_action,
                pseudomarks_comments_message = src.pseudomarks_comments_message,
                word_mark_comments_action = src.word_mark_comments_action,
                word_mark_comments_message = src.word_mark_comments_message,
                create_ts = src.create_ts,
                last_updt_ts = current_timestamp()
            WHEN NOT MATCHED THEN 
                INSERT (tqr_app, assignee, date_uploaded_ts, pre_exam_status, review_manager, review_started, reviewer, serial_number, class_comments_action, class_comments_message, 
                design_search_code_comments_action, design_search_code_comments_message, mark_drawing_code_comments_actions, mark_drawing_code_comments_message, pseudomarks_comments_action, pseudomarks_comments_message, word_mark_comments_action, word_mark_comments_message, create_ts, last_updt_ts)
                VALUES (src.tqr_app, src.assignee, src.date_uploaded_ts, src.pre_exam_status, src.review_manager, src.review_started, src.reviewer, src.serial_number, src.class_comments_action, src.class_comments_message, src.design_search_code_comments_action, src.design_search_code_comments_message, src.mark_drawing_code_comments_actions, src.mark_drawing_code_comments_message, src.pseudomarks_comments_action, src.pseudomarks_comments_message, src.word_mark_comments_action, src.word_mark_comments_message, current_timestamp(), current_timestamp())
            """)
            end_job_cntl(f"{trgt_catalog}.silver", job_name, start_ts,'completed',recs_count,"job completed successfully")
            
        except Exception as e:
            print("Exception message: {}".format(e))
            end_job_cntl(f"{trgt_catalog}.silver", job_name, start_ts,'failed',0,e)
            raise
else:
    end_job_cntl(f"{trgt_catalog}.silver", job_name, start_ts,'completed',recs_count,"job completed successfully")

