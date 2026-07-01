# Databricks notebook source
# MAGIC %md
# MAGIC # TMOG Metrics Silver Load

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initial Setup

# COMMAND ----------

# DBTITLE 1,Environment Settings
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=},{dbx_env=}")

# COMMAND ----------

# DBTITLE 1,Shared Functions
# MAGIC %run ./../shared/ntb_common_func_and_params 

# COMMAND ----------

# DBTITLE 1,Set Catalogs
common_configs = read_yaml(config_file)
reporting_catalog = common_configs["schema"]["trgt_catalog"]
tmngpdb_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
print(reporting_catalog, tmngpdb_catalog)

# COMMAND ----------

# DBTITLE 1,Begin Job
job_name = "ntb_tmog_metrics_silver_load"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Creation

# COMMAND ----------

# DBTITLE 1,Create View: Fact | Review Query
spark.sql(f"""
select distinct
  review_gid,
  review_query_gid fk_review_query_gid,
  date(publication_date) publication_date,
  serial_number,
  previous_bounce_number,
  review_status_code,
  review_status_title,
  review_status_description,
  initial_review_employee_id,
  initial_review_employee_organization_code,
  initial_review_timestamp,
  latest_review_employee_id,
  latest_review_timestamp,
  og_page_number,
  print_error_indicator,
  review_query_content,
  review_query_note_text,
  review_query_note_type_code,
  review_query_note_sequence_number,
  review_query_note_type,
  review_query_note_description,
  initial_review_query_note_employee_id,
  -- temporary patch to resolve dual-active orgs
  first(
    initial_review_query_note_employee_organization_code
  ) over (
    partition by 
      review_gid, 
      review_query_gid, 
      review_query_note_sequence_number
    order by 
      latest_review_query_note_timestamp
  ) initial_review_query_note_employee_organization_code,
  initial_review_query_note_timestamp,
  latest_review_query_note_employee_id,
  first(
    latest_review_query_note_employee_organization_code
  ) over (
    partition by 
      review_gid, 
      review_query_gid, 
      review_query_note_sequence_number
    order by 
      latest_review_query_note_timestamp
  ) latest_review_query_note_employee_organization_code,
  latest_review_query_note_timestamp
from
  {reporting_catalog}.bronze.tmog_metrics_transactions
where
  review_query_gid is not null
"""
).createOrReplaceTempView("_vw_review_query")
vw_review_query = spark.sql("select * from _vw_review_query").dropDuplicates()
vw_review_query.createOrReplaceTempView("vw_review_query")

# COMMAND ----------

# DBTITLE 1,Create View: Fact | Review Query Appeal
spark.sql(f"""
select distinct
  review_query_appeal_gid,
  review_query_gid fk_review_query_gid,
  review_query_appeal_approval_indicator,
  review_query_appeal_result_date,
  review_query_appeal_proceeding_number,
  review_query_appeal_decision_description,
  review_query_appeal_reason_description,
  review_query_appeal_director_email_sent_indicator,
  review_query_appeal_result_code,
  review_query_appeal_result,
  review_query_appeal_result_description,
  initial_review_query_appeal_employee_id,
  initial_review_query_appeal_employee_organization_code,
  initial_review_query_appeal_timestamp,
  latest_review_query_appeal_employee_id,
  latest_review_query_appeal_employee_organization_code,
  latest_review_query_appeal_timestamp,
  review_query_appeal_status_timestamp,
  review_query_appeal_sequence_number,
  review_query_appeal_note,
  review_query_appeal_status_code,
  review_query_appeal_status,
  review_query_appeal_status_description
from
  {reporting_catalog}.bronze.tmog_metrics_transactions 
where
  review_query_appeal_gid is not null
"""
).createOrReplaceTempView("_vw_review_query_appeal")
vw_review_query_appeal = spark.sql(
    "select * from _vw_review_query_appeal"
).dropDuplicates()
vw_review_query_appeal.createOrReplaceTempView("vw_review_query_appeal")

# COMMAND ----------

# DBTITLE 1,Create View: Fact | Review Query Grounds
spark.sql(f"""
select distinct
  review_query_ground_id,
  review_query_gid fk_review_query_gid,
  review_query_ground_code,
  review_query_ground,
  review_query_ground_description,
  review_query_ground_order_number,
  review_query_ground_grouping_number,
  review_query_ground_type_code,
  review_query_ground_type,
  review_query_ground_type_description,
  review_query_ground_class_id,
  -- employee_review_query_ground_id,
  first(
    initial_review_query_employee_id
  ) over (
    partition by review_query_ground_id 
    order by initial_review_query_timestamp desc
  ) initial_review_query_employee_id,
  first(
    initial_review_query_employee_organization_code
  ) over (
    partition by review_query_ground_id 
    order by initial_review_query_timestamp desc
  ) initial_review_query_employee_organization_code,
  min(
    initial_review_query_timestamp
  ) over (
    partition by review_query_ground_id
  ) initial_review_query_timestamp,
  first(
    latest_review_query_employee_id
  ) over (
    partition by review_query_ground_id 
    order by latest_review_query_timestamp desc
  ) latest_review_query_employee_id,
  first(
    latest_review_query_employee_organization_code
  ) over (
    partition by review_query_ground_id 
    order by latest_review_query_timestamp desc
  ) latest_review_query_employee_organization_code,
  first(
    latest_review_query_timestamp
  ) over (
    partition by review_query_ground_id 
    order by latest_review_query_timestamp desc
  ) latest_review_query_timestamp,
  review_query_assignment_date,
  first(initial_employee_review_query_status_employee_id)  over (
    partition by review_query_ground_id -- replaced employee_review_query_ground_id 
    order by initial_employee_review_query_status_timestamp 
  ) initial_employee_review_query_status_employee_id,
  min(
    initial_employee_review_query_status_timestamp
  ) over (
    partition by review_query_ground_id -- replaced employee_review_query_ground_id 
  ) initial_employee_review_query_status_timestamp,
  first(
    employee_review_query_status_code
  ) over (
    partition by review_query_ground_id -- replaced employee_review_query_ground_id 
    order by latest_employee_review_query_status_timestamp desc
  ) employee_review_query_status_code,
  first(
    employee_review_query_status_code_description
  ) over (
    partition by review_query_ground_id -- replaced employee_review_query_ground_id 
    order by latest_employee_review_query_status_timestamp desc
  ) employee_review_query_status_code_description,
  first(
    employee_review_query_status_reason_description
  ) over (
    partition by review_query_ground_id -- replaced employee_review_query_ground_id 
    order by latest_employee_review_query_status_timestamp desc
  ) employee_review_query_status_reason_description,
  first(
    latest_employee_review_query_status_employee_id
  ) over (
    partition by review_query_ground_id -- replaced employee_review_query_ground_id 
    order by latest_employee_review_query_status_timestamp desc
  ) latest_employee_review_query_status_employee_id,
  max(
    latest_employee_review_query_status_timestamp
  ) over (
    partition by review_query_ground_id -- replaced employee_review_query_ground_id 
  ) latest_employee_review_query_status_timestamp
from
  {reporting_catalog}.bronze.tmog_metrics_transactions 
where
  review_query_ground_id is not null
"""
).createOrReplaceTempView("_vw_review_query_ground")
vw_review_query_ground = spark.sql(
    "select * from _vw_review_query_ground"
).dropDuplicates()
vw_review_query_ground.createOrReplaceTempView("vw_review_query_ground")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert

# COMMAND ----------

# DBTITLE 1,Insert: TMOG Review Query Transactions
display(
    spark.sql(
        f"""
        insert overwrite 
            {reporting_catalog}.silver.tmog_metrics_review_query_transactions (
                review_gid,
                fk_review_query_gid,
                publication_date,
                serial_number,
                previous_bounce_number,
                review_status_code,
                review_status_title,
                review_status_description,
                initial_review_employee_id,
                initial_review_employee_organization_code,
                initial_review_timestamp,
                latest_review_employee_id,
                latest_review_timestamp,
                og_page_number,
                print_error_indicator,
                review_query_content,
                review_query_note_text,
                review_query_note_type_code,
                review_query_note_sequence_number,
                review_query_note_type,
                review_query_note_description,
                initial_review_query_note_employee_id,
                initial_review_query_note_employee_organization_code,
                initial_review_query_note_timestamp,
                latest_review_query_note_employee_id,
                latest_review_query_note_employee_organization_code,
                latest_review_query_note_timestamp,
                create_user,
                create_timestamp
            )
        select
            review_gid,
            fk_review_query_gid,
            publication_date,
            serial_number,
            previous_bounce_number,
            review_status_code,
            review_status_title,
            review_status_description,
            initial_review_employee_id,
            initial_review_employee_organization_code,
            initial_review_timestamp,
            latest_review_employee_id,
            latest_review_timestamp,
            og_page_number,
            print_error_indicator,
            review_query_content,
            review_query_note_text,
            review_query_note_type_code,
            review_query_note_sequence_number,
            review_query_note_type,
            review_query_note_description,
            initial_review_query_note_employee_id,
            initial_review_query_note_employee_organization_code,
            initial_review_query_note_timestamp,
            latest_review_query_note_employee_id,
            latest_review_query_note_employee_organization_code,
            latest_review_query_note_timestamp,
            'TMOG_METRICS_SILVER_LOAD' create_user,
            current_timestamp create_timestamp
        from
            vw_review_query
    """
    )
)

# COMMAND ----------

# DBTITLE 1,Insert: TMOG Review Query Appeal Transactions
display(
    spark.sql(f"""
        insert overwrite 
            {reporting_catalog}.silver.tmog_metrics_review_query_appeal_transactions (
                review_query_appeal_gid,
                fk_review_query_gid,
                review_query_appeal_approval_indicator,
                review_query_appeal_result_date,
                review_query_appeal_proceeding_number,
                review_query_appeal_decision_description,
                review_query_appeal_reason_description,
                review_query_appeal_director_email_sent_indicator,
                review_query_appeal_result_code,
                review_query_appeal_result,
                review_query_appeal_result_description,
                initial_review_query_appeal_employee_id,
                initial_review_query_appeal_employee_organization_code,
                initial_review_query_appeal_timestamp,
                latest_review_query_appeal_employee_id,
                latest_review_query_appeal_employee_organization_code,
                latest_review_query_appeal_timestamp,
                review_query_appeal_status_timestamp,
                review_query_appeal_sequence_number,
                review_query_appeal_note,
                review_query_appeal_status_code,
                review_query_appeal_status,
                review_query_appeal_status_description,
                create_user,
                create_timestamp
            )
        select
            review_query_appeal_gid,
            fk_review_query_gid,
            review_query_appeal_approval_indicator,
            review_query_appeal_result_date,
            review_query_appeal_proceeding_number,
            review_query_appeal_decision_description,
            review_query_appeal_reason_description,
            review_query_appeal_director_email_sent_indicator,
            review_query_appeal_result_code,
            review_query_appeal_result,
            review_query_appeal_result_description,
            initial_review_query_appeal_employee_id,
            initial_review_query_appeal_employee_organization_code,
            initial_review_query_appeal_timestamp,
            latest_review_query_appeal_employee_id,
            latest_review_query_appeal_employee_organization_code,
            latest_review_query_appeal_timestamp,
            review_query_appeal_status_timestamp,
            review_query_appeal_sequence_number,
            review_query_appeal_note,
            review_query_appeal_status_code,
            review_query_appeal_status,
            review_query_appeal_status_description,
            'TMOG_METRICS_SILVER_LOAD' create_user,
            current_timestamp create_timestamp
        from
            vw_review_query_appeal
    """)
)

# COMMAND ----------

# DBTITLE 1,Insert: TMOG Review Query Ground Transactions
display(
    spark.sql(
        f"""
        insert overwrite 
            {reporting_catalog}.silver.tmog_metrics_review_query_ground_transactions (
                review_query_ground_id,
                fk_review_query_gid,
                review_query_ground_code,
                review_query_ground,
                review_query_ground_description,
                review_query_ground_order_number,
                review_query_ground_grouping_number,
                review_query_ground_type_code,
                review_query_ground_type,
                review_query_ground_type_description,
                review_query_ground_class_id,
                initial_review_query_employee_id,
                initial_review_query_employee_organization_code,
                initial_review_query_timestamp,
                latest_review_query_employee_id,
                latest_review_query_employee_organization_code,
                latest_review_query_timestamp,
                review_query_assignment_date,
                employee_review_query_status_code,
                employee_review_query_status_code_description,
                employee_review_query_status_reason_description,
                create_user,
                create_timestamp
            )
        select
            review_query_ground_id,
            fk_review_query_gid,
            review_query_ground_code,
            review_query_ground,
            review_query_ground_description,
            review_query_ground_order_number,
            review_query_ground_grouping_number,
            review_query_ground_type_code,
            review_query_ground_type,
            review_query_ground_type_description,
            review_query_ground_class_id,
            initial_review_query_employee_id,
            initial_review_query_employee_organization_code,
            initial_review_query_timestamp,
            latest_review_query_employee_id,
            latest_review_query_employee_organization_code,
            latest_review_query_timestamp,
            review_query_assignment_date,
            employee_review_query_status_code,
            employee_review_query_status_code_description,
            employee_review_query_status_reason_description,
            'TMOG_METRICS_SILVER_LOAD' create_user,
            current_timestamp create_timestamp
        from
            vw_review_query_ground
    """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teardown

# COMMAND ----------

# DBTITLE 1,End Job
count_tmog_metrics_review_query_transactions = spark.sql(
    f"select count(*) as cnt from {reporting_catalog}.silver.tmog_metrics_review_query_transactions"
).collect()[0]["cnt"]
count_tmog_metrics_review_query_appeal_transactions = spark.sql(
    f"select count(*) as cnt from {reporting_catalog}.silver.tmog_metrics_review_query_appeal_transactions"
).collect()[0]["cnt"]
count_tmog_metrics_review_query_ground_transactions = spark.sql(
    f"select count(*) as cnt from {reporting_catalog}.silver.tmog_metrics_review_query_ground_transactions"
).collect()[0]["cnt"]
table_counts: list[int] = [
    count_tmog_metrics_review_query_transactions,
    count_tmog_metrics_review_query_appeal_transactions,
    count_tmog_metrics_review_query_ground_transactions,
]
num_empty_tables: int = count_empty(table_counts)

if not num_empty_tables:
    end_job_cntl(
        f"{reporting_catalog}.silver",
        job_name,
        job_start_ts,
        "completed",
        count_tmog_metrics_review_query_transactions
        + count_tmog_metrics_review_query_appeal_transactions
        + count_tmog_metrics_review_query_ground_transactions,
        "job completed successfully",
    )
    dbutils.notebook.exit(
        f"""
        Job completed with:
        - [{count_tmog_metrics_review_query_transactions}] records for `tmog_metrics_review_query_transactions`
        - [{count_tmog_metrics_review_query_appeal_transactions}] records for `tmog_metrics_review_query_appeal_transactions`
        - [{count_tmog_metrics_review_query_ground_transactions}] records for `tmog_metrics_review_query_ground_transactions`
        """
    )
else:
    raise ValueError(
        f"{num_empty_tables} tables loaded 0 records. Tables must have at least 1 record to move on to next task."
    )