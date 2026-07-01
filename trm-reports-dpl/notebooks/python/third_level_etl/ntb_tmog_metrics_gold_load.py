# Databricks notebook source
# MAGIC %md
# MAGIC # TMOG Metrics Gold Load

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initial Setup

# COMMAND ----------

# DBTITLE 1,Environment Settings
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=}, {dbx_env=}")

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
job_name = "ntb_tmog_metrics_gold_load"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Creation

# COMMAND ----------

# MAGIC %md
# MAGIC #### Source Tables

# COMMAND ----------

# DBTITLE 1,Base Views
spark.sql(
    f"select * from {reporting_catalog}.silver.tmog_metrics_review_query_transactions"
).createOrReplaceTempView("review_query")
spark.sql(
    f"select * from {reporting_catalog}.silver.tmog_metrics_review_query_appeal_transactions"
).createOrReplaceTempView("review_query_appeal")
spark.sql(
    f"select * from {reporting_catalog}.silver.tmog_metrics_review_query_ground_transactions"
).createOrReplaceTempView("review_query_ground")
spark.sql(f"select * from {tmngpdb_catalog}.bronze.stnd_class").createOrReplaceTempView(
    "class_definition"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Based on Date Attributes

# COMMAND ----------

# DBTITLE 1,Time Views
spark.sql("""
with review_queries_day_deduped as (
  select
    date(initial_review_timestamp) initial_review_query_date,
    count(distinct fk_review_query_gid) review_queries_day_total
  from
    review_query
  group by
    all
),
review_queries_day_basis as (
  select
    initial_review_query_date,
    review_queries_day_total,
    sum(review_queries_day_total) over (partition by null order by initial_review_query_date) review_queries_rolling_total
  from
    review_queries_day_deduped
)
select
  initial_review_query_date,
  review_queries_day_total,
  nvl(lag(review_queries_day_total) over (order by initial_review_query_date), 0) review_queries_day_previous_total,
  review_queries_rolling_total
from
  review_queries_day_basis
""").createOrReplaceTempView("review_queries_day_lagged")

spark.sql(
  """
  with review_query_appeals_day_deduped as (
    select
      date(review_query_appeal_status_timestamp) review_query_appeal_status_date,
      count(distinct review_query_appeal_gid) review_query_appeals_day_total
    from
      review_query_appeal
    group by 
      all
  ),
  review_query_appeals_day_status_basis as (
    select
      review_query_appeal_status_date,
      review_query_appeals_day_total,
      sum(review_query_appeals_day_total) over (partition by null order by review_query_appeal_status_date) review_query_appeals_rolling_total
    from
      review_query_appeals_day_deduped
  )
  select
    review_query_appeal_status_date,
    review_query_appeals_day_total,
    nvl(lag(review_query_appeals_day_total) over (order by review_query_appeal_status_date), 0) review_query_appeals_day_previous_total,
    review_query_appeals_rolling_total
  from
    review_query_appeals_day_status_basis
"""
).createOrReplaceTempView("review_query_appeals_day_status_lagged")

spark.sql(
    """
  with review_query_appeals_day_query_appeal_deduped as (
    select 
      date(initial_review_query_appeal_timestamp) initial_review_query_appeal_date,
      count(distinct review_query_appeal_gid) review_query_appeals_day_total,
      count(1) over (partition by null order by date(initial_review_query_appeal_timestamp)) review_query_appeals_rolling_total
    from
      review_query_appeal
    group by
      all
  ),
  review_query_appeals_day_query_appeal_basis as (
    select 
      initial_review_query_appeal_date,
      review_query_appeals_day_total,
      sum(review_query_appeals_day_total) over (partition by null order by initial_review_query_appeal_date) review_query_appeals_rolling_total
    from
      review_query_appeals_day_query_appeal_deduped
  )
  select
    initial_review_query_appeal_date,
    review_query_appeals_day_total,
    nvl(lag(review_query_appeals_day_total) over (order by initial_review_query_appeal_date), 0) review_query_appeals_day_previous_total,
    review_query_appeals_rolling_total
  from
    review_query_appeals_day_query_appeal_basis
"""
).createOrReplaceTempView("review_query_appeals_day_appeal_lagged")

spark.sql(
    """
  with review_query_grounds_day_deduped as (
    select 
      date(initial_review_query_timestamp) initial_review_query_date,
      count(distinct review_query_ground_id) review_query_grounds_day_total
    from
      review_query_ground
    group by 
      all
  ),
  review_query_grounds_day_basis as (
    select
      initial_review_query_date,
      review_query_grounds_day_total,
      sum(review_query_grounds_day_total) over (partition by null order by initial_review_query_date) review_query_grounds_rolling_total
    from
      review_query_grounds_day_deduped
  )
  select
    initial_review_query_date,
    review_query_grounds_day_total,
    nvl(lag(review_query_grounds_day_total) over (order by initial_review_query_date), 0) review_query_grounds_day_previous_total,
    review_query_grounds_rolling_total
  from
    review_query_grounds_day_basis
"""
).createOrReplaceTempView("review_query_grounds_day_lagged")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review Queries

# COMMAND ----------

# DBTITLE 1,Create View: Employee Review Query Metrics (System Time)
# MAGIC %sql
# MAGIC create or replace temp view vw_tmog_metrics_employee_review_query_metrics as
# MAGIC with review_query_employee_deduped as (
# MAGIC   select  
# MAGIC     initial_review_employee_id initial_review_query_employee_id,
# MAGIC     date(initial_review_timestamp) initial_review_query_date,
# MAGIC     count(distinct fk_review_query_gid) employee_review_queries_day_total
# MAGIC   from
# MAGIC     review_query
# MAGIC   group by 
# MAGIC     all
# MAGIC ),
# MAGIC employee_review_query_basis as (
# MAGIC   select 
# MAGIC     initial_review_query_employee_id,
# MAGIC     initial_review_query_date,
# MAGIC     employee_review_queries_day_total,
# MAGIC     sum(employee_review_queries_day_total) over (partition by initial_review_query_employee_id order by initial_review_query_date) employee_review_queries_rolling_total
# MAGIC   from
# MAGIC     review_query_employee_deduped
# MAGIC ),
# MAGIC employee_review_queries_lagged as (
# MAGIC   select
# MAGIC     initial_review_query_employee_id,
# MAGIC     initial_review_query_date,
# MAGIC     employee_review_queries_day_total,
# MAGIC     nvl(lag(employee_review_queries_day_total) over (partition by initial_review_query_employee_id order by initial_review_query_date), 0) employee_review_queries_day_previous_total,
# MAGIC     employee_review_queries_rolling_total
# MAGIC   from
# MAGIC     employee_review_query_basis
# MAGIC )
# MAGIC select distinct
# MAGIC   a.initial_review_query_date,
# MAGIC   b.initial_review_query_employee_id,
# MAGIC   b.employee_review_queries_day_total,
# MAGIC   nvl(b.employee_review_queries_day_previous_total, 0) employee_review_queries_day_previous_total,
# MAGIC   b.employee_review_queries_rolling_total,
# MAGIC   a.review_queries_day_total,
# MAGIC   a.review_queries_day_previous_total,
# MAGIC   a.review_queries_rolling_total,
# MAGIC   'TMOG_METRICS_GOLD_LOAD' create_user,
# MAGIC   current_timestamp() create_timestamp
# MAGIC from
# MAGIC   review_queries_day_lagged a
# MAGIC     join employee_review_queries_lagged b
# MAGIC       on a.initial_review_query_date = b.initial_review_query_date

# COMMAND ----------

# DBTITLE 1,Create View: Publication Query Review Metrics (System Time)
# MAGIC %sql
# MAGIC create or replace temp view vw_tmog_metrics_publication_review_query_metrics as
# MAGIC with publication_date_review_query_deduped as (
# MAGIC   select 
# MAGIC     date(publication_date) publication_date,
# MAGIC     date(initial_review_timestamp) initial_review_query_date,
# MAGIC     count(distinct fk_review_query_gid) publication_date_review_queries_day_total
# MAGIC   from
# MAGIC     review_query
# MAGIC   group by
# MAGIC     all
# MAGIC ),
# MAGIC publication_date_review_query_basis as (
# MAGIC   select
# MAGIC     publication_date,
# MAGIC     initial_review_query_date,
# MAGIC     publication_date_review_queries_day_total,
# MAGIC     sum(publication_date_review_queries_day_total) over (partition by publication_date order by initial_review_query_date) publication_date_review_queries_rolling_total
# MAGIC   from 
# MAGIC     publication_date_review_query_deduped
# MAGIC ),
# MAGIC publication_date_review_queries_lagged as (
# MAGIC   select
# MAGIC     publication_date,
# MAGIC     initial_review_query_date,
# MAGIC     publication_date_review_queries_day_total,
# MAGIC     nvl(lag(publication_date_review_queries_day_total) over (partition by publication_date order by initial_review_query_date), 0) publication_date_review_queries_day_previous_total,
# MAGIC     publication_date_review_queries_rolling_total
# MAGIC   from
# MAGIC     publication_date_review_query_basis
# MAGIC )
# MAGIC select distinct
# MAGIC   a.initial_review_query_date,
# MAGIC   b.publication_date,
# MAGIC   b.publication_date_review_queries_day_total,
# MAGIC   nvl(b.publication_date_review_queries_day_previous_total, 0) publication_date_review_queries_day_previous_total,
# MAGIC   b.publication_date_review_queries_rolling_total,
# MAGIC   a.review_queries_day_total,
# MAGIC   a.review_queries_day_previous_total,
# MAGIC   a.review_queries_rolling_total,
# MAGIC   'TMOG_METRICS_GOLD_LOAD' create_user,
# MAGIC   current_timestamp() create_timestamp
# MAGIC from
# MAGIC   review_queries_day_lagged a
# MAGIC     join publication_date_review_queries_lagged b
# MAGIC       on a.initial_review_query_date = b.initial_review_query_date

# COMMAND ----------

# DBTITLE 1,Create View: Case Review Query Metrics (System Time)
# MAGIC %sql
# MAGIC create or replace temp view vw_tmog_metrics_case_review_query_metrics as
# MAGIC with case_review_query_basis as (
# MAGIC   select
# MAGIC     date(initial_review_timestamp) initial_review_query_date,
# MAGIC     count(1) case_review_queries_day_total,
# MAGIC     count(distinct serial_number) distinct_case_review_queries_day_total
# MAGIC   from
# MAGIC     review_query
# MAGIC   group by
# MAGIC     all
# MAGIC )
# MAGIC select distinct
# MAGIC   initial_review_query_date,
# MAGIC   case_review_queries_day_total,
# MAGIC   distinct_case_review_queries_day_total,
# MAGIC   nvl(lag(case_review_queries_day_total) over (order by initial_review_query_date), 0) case_review_queries_day_previous_total,
# MAGIC   nvl(lag(distinct_case_review_queries_day_total) over (order by initial_review_query_date), 0) distinct_case_review_queries_day_previous_total,
# MAGIC   sum(case_review_queries_day_total) over (order by initial_review_query_date) case_review_queries_rolling_total,
# MAGIC   sum(distinct_case_review_queries_day_total) over (order by initial_review_query_date) distinct_case_review_queries_rolling_total,
# MAGIC   'TMOG_METRICS_GOLD_LOAD' create_user,
# MAGIC   current_timestamp() create_timestamp
# MAGIC from
# MAGIC   case_review_query_basis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review Query Appeals

# COMMAND ----------

# MAGIC %md
# MAGIC #### System Time

# COMMAND ----------

# DBTITLE 1,Create View: Employee Review Query Appeal Metrics (System Time)
# MAGIC %sql
# MAGIC create or replace temp view vw_tmog_metrics_employee_review_query_appeal_metrics as
# MAGIC with employee_review_query_appeals_deduped as (
# MAGIC   select
# MAGIC     initial_review_query_appeal_employee_id,
# MAGIC     date(initial_review_query_appeal_timestamp) initial_review_query_appeal_date,
# MAGIC     count(distinct fk_review_query_gid) employee_review_query_appeals_day_total
# MAGIC   from
# MAGIC     review_query_appeal
# MAGIC   group by 
# MAGIC     all
# MAGIC ),
# MAGIC employee_review_query_appeals_basis as (
# MAGIC   select
# MAGIC     initial_review_query_appeal_employee_id,
# MAGIC     initial_review_query_appeal_date,
# MAGIC     employee_review_query_appeals_day_total,
# MAGIC     sum(employee_review_query_appeals_day_total) over (partition by initial_review_query_appeal_employee_id order by initial_review_query_appeal_date)employee_review_query_appeals_rolling_total
# MAGIC   from
# MAGIC     employee_review_query_appeals_deduped
# MAGIC ),
# MAGIC employee_review_queries_lagged as (
# MAGIC   select
# MAGIC     initial_review_query_appeal_employee_id,
# MAGIC     initial_review_query_appeal_date,
# MAGIC     employee_review_query_appeals_day_total,
# MAGIC     nvl(lag(employee_review_query_appeals_day_total) over (partition by initial_review_query_appeal_employee_id order by initial_review_query_appeal_date), 0) employee_review_query_appeals_day_previous_total,
# MAGIC     employee_review_query_appeals_rolling_total
# MAGIC   from
# MAGIC     employee_review_query_appeals_basis
# MAGIC )
# MAGIC select distinct
# MAGIC   a.initial_review_query_appeal_date,
# MAGIC   b.initial_review_query_appeal_employee_id,
# MAGIC   b.employee_review_query_appeals_day_total,
# MAGIC   nvl(b.employee_review_query_appeals_day_previous_total, 0) employee_review_query_appeals_day_previous_total,
# MAGIC   b.employee_review_query_appeals_rolling_total,
# MAGIC   a.review_query_appeals_day_total,
# MAGIC   a.review_query_appeals_day_previous_total,
# MAGIC   a.review_query_appeals_rolling_total,
# MAGIC   'TMOG_METRICS_GOLD_LOAD' create_user,
# MAGIC   current_timestamp() create_timestamp
# MAGIC from
# MAGIC   review_query_appeals_day_appeal_lagged a
# MAGIC     join employee_review_queries_lagged b
# MAGIC       on a.initial_review_query_appeal_date = b.initial_review_query_appeal_date

# COMMAND ----------

# DBTITLE 1,Create View: Result Review Query Appeal Metrics (System Time)
# MAGIC %sql
# MAGIC create or replace temp view vw_tmog_metrics_result_review_query_appeal_metrics as
# MAGIC with result_review_query_appeals_deduped as (
# MAGIC   select
# MAGIC     review_query_appeal_result_code,
# MAGIC     review_query_appeal_result,
# MAGIC     review_query_appeal_result_description,
# MAGIC     date(initial_review_query_appeal_timestamp) initial_review_query_appeal_date,
# MAGIC     count(distinct review_query_appeal_gid) result_review_query_appeals_day_total
# MAGIC   from
# MAGIC     review_query_appeal
# MAGIC   group by 
# MAGIC     all
# MAGIC ),
# MAGIC  result_review_query_appeals_basis as (
# MAGIC   select
# MAGIC     review_query_appeal_result_code,
# MAGIC     review_query_appeal_result,
# MAGIC     review_query_appeal_result_description,
# MAGIC     initial_review_query_appeal_date,
# MAGIC     result_review_query_appeals_day_total,
# MAGIC     sum(result_review_query_appeals_day_total) over (partition by review_query_appeal_result_code order by initial_review_query_appeal_date) result_review_query_appeals_rolling_total
# MAGIC   from
# MAGIC     result_review_query_appeals_deduped
# MAGIC ),
# MAGIC result_review_query_appeal_lagged as (
# MAGIC   select
# MAGIC     review_query_appeal_result_code,
# MAGIC     review_query_appeal_result,
# MAGIC     review_query_appeal_result_description,
# MAGIC     initial_review_query_appeal_date,
# MAGIC     result_review_query_appeals_day_total,
# MAGIC     nvl(lag(result_review_query_appeals_day_total) over (partition by review_query_appeal_result_code order by initial_review_query_appeal_date), 0) result_query_appeals_day_previous_total,
# MAGIC     result_review_query_appeals_rolling_total
# MAGIC   from
# MAGIC     result_review_query_appeals_basis
# MAGIC )
# MAGIC select distinct
# MAGIC   a.initial_review_query_appeal_date,
# MAGIC   b.review_query_appeal_result_code,
# MAGIC   b.review_query_appeal_result,
# MAGIC   b.review_query_appeal_result_description,
# MAGIC   b.result_review_query_appeals_day_total,
# MAGIC   nvl(b.result_query_appeals_day_previous_total, 0) result_review_query_appeals_day_previous_total,
# MAGIC   b.result_review_query_appeals_rolling_total,
# MAGIC   a.review_query_appeals_day_total,
# MAGIC   a.review_query_appeals_day_previous_total,
# MAGIC   a.review_query_appeals_rolling_total,
# MAGIC   'TMOG_METRICS_GOLD_LOAD' create_user,
# MAGIC   current_timestamp() create_timestamp
# MAGIC from
# MAGIC   review_query_appeals_day_appeal_lagged a
# MAGIC     join result_review_query_appeal_lagged b
# MAGIC       on a.initial_review_query_appeal_date = b.initial_review_query_appeal_date

# COMMAND ----------

# DBTITLE 1,Create View: Status Review Query Appeal Metrics (System Time)
# MAGIC %sql
# MAGIC create or replace temp view vw_tmog_metrics_status_review_query_appeal_metrics as
# MAGIC with status_review_query_appeals_deduped as (
# MAGIC   select
# MAGIC     review_query_appeal_status,
# MAGIC     review_query_appeal_status_code,
# MAGIC     review_query_appeal_status_description,
# MAGIC     date(initial_review_query_appeal_timestamp) initial_review_query_appeal_date,
# MAGIC     count(distinct review_query_appeal_gid) status_review_query_appeals_day_total
# MAGIC   from
# MAGIC     review_query_appeal
# MAGIC   group by
# MAGIC     all
# MAGIC ),
# MAGIC status_review_query_appeals_basis as (
# MAGIC   select
# MAGIC     review_query_appeal_status,
# MAGIC     review_query_appeal_status_code,
# MAGIC     review_query_appeal_status_description,
# MAGIC     initial_review_query_appeal_date,
# MAGIC     status_review_query_appeals_day_total,
# MAGIC     sum(status_review_query_appeals_day_total) over (partition by review_query_appeal_status order by initial_review_query_appeal_date) status_review_query_appeals_rolling_total
# MAGIC   from
# MAGIC     status_review_query_appeals_deduped
# MAGIC ),
# MAGIC status_review_query_appeals_lagged as (
# MAGIC   select
# MAGIC     review_query_appeal_status,
# MAGIC     review_query_appeal_status_code,
# MAGIC     review_query_appeal_status_description,
# MAGIC     initial_review_query_appeal_date,
# MAGIC     status_review_query_appeals_day_total,
# MAGIC     nvl(lag(status_review_query_appeals_day_total) over (partition by review_query_appeal_status order by initial_review_query_appeal_date), 0) status_review_query_appeals_day_previous_total,
# MAGIC     status_review_query_appeals_rolling_total
# MAGIC   from
# MAGIC     status_review_query_appeals_basis
# MAGIC )
# MAGIC select distinct
# MAGIC   a.initial_review_query_appeal_date,
# MAGIC   b.review_query_appeal_status,
# MAGIC   b.review_query_appeal_status_description,
# MAGIC   b.status_review_query_appeals_day_total,
# MAGIC   nvl(b.status_review_query_appeals_day_previous_total, 0) status_review_query_appeals_day_previous_total,
# MAGIC   b.status_review_query_appeals_rolling_total,
# MAGIC   a.review_query_appeals_day_total,
# MAGIC   a.review_query_appeals_day_previous_total,
# MAGIC   a.review_query_appeals_rolling_total,
# MAGIC   'TMOG_METRICS_GOLD_LOAD' create_user,
# MAGIC   current_timestamp() create_timestamp
# MAGIC from
# MAGIC   review_query_appeals_day_appeal_lagged a
# MAGIC     join status_review_query_appeals_lagged b
# MAGIC       on a.initial_review_query_appeal_date = b.initial_review_query_appeal_date

# COMMAND ----------

# MAGIC %md
# MAGIC #### Status Time

# COMMAND ----------

# DBTITLE 1,Create View: Employee Review Query Appeal Metrics (Status Time)
# MAGIC %sql
# MAGIC create or replace temp view vw_tmog_metrics_employee_review_query_appeal_status_metrics as
# MAGIC with employee_review_query_appeals_deduped as (
# MAGIC   select
# MAGIC     initial_review_query_appeal_employee_id,
# MAGIC     date(review_query_appeal_status_timestamp) review_query_appeal_status_date,
# MAGIC     count(distinct review_query_appeal_gid) employee_review_query_appeals_day_total
# MAGIC   from
# MAGIC     review_query_appeal
# MAGIC   group by 
# MAGIC     all
# MAGIC ),
# MAGIC employee_review_query_appeals_basis as (
# MAGIC   select
# MAGIC     initial_review_query_appeal_employee_id,
# MAGIC     review_query_appeal_status_date,
# MAGIC     employee_review_query_appeals_day_total,
# MAGIC     sum(employee_review_query_appeals_day_total) over (partition by initial_review_query_appeal_employee_id order by review_query_appeal_status_date) employee_review_query_appeals_rolling_total
# MAGIC   from
# MAGIC     employee_review_query_appeals_deduped
# MAGIC ),
# MAGIC employee_review_query_appeals_lagged as (
# MAGIC   select
# MAGIC     initial_review_query_appeal_employee_id,
# MAGIC     review_query_appeal_status_date,
# MAGIC     employee_review_query_appeals_day_total,
# MAGIC     nvl(lag(employee_review_query_appeals_day_total) over (partition by initial_review_query_appeal_employee_id order by review_query_appeal_status_date), 0) employee_query_appeals_day_previous_total,
# MAGIC     employee_review_query_appeals_rolling_total
# MAGIC   from
# MAGIC     employee_review_query_appeals_basis
# MAGIC )
# MAGIC select distinct
# MAGIC   a.review_query_appeal_status_date,
# MAGIC   b.initial_review_query_appeal_employee_id,
# MAGIC   b.employee_review_query_appeals_day_total,
# MAGIC   nvl(b.employee_query_appeals_day_previous_total, 0) employee_query_appeals_day_previous_total,
# MAGIC   b.employee_review_query_appeals_rolling_total,
# MAGIC   a.review_query_appeals_day_total,
# MAGIC   a.review_query_appeals_day_previous_total,
# MAGIC   a.review_query_appeals_rolling_total,
# MAGIC   'TMOG_METRICS_GOLD_LOAD' create_user,
# MAGIC   current_timestamp() create_timestamp
# MAGIC from
# MAGIC   review_query_appeals_day_status_lagged a
# MAGIC     join employee_review_query_appeals_lagged b
# MAGIC       on a.review_query_appeal_status_date = b.review_query_appeal_status_date

# COMMAND ----------

# DBTITLE 1,Create View: Result Review Query Appeal Metrics (Status Time)
# MAGIC %sql
# MAGIC create or replace temp view vw_tmog_metrics_result_review_query_appeal_status_metrics as
# MAGIC with result_query_appeals_deduped as (
# MAGIC   select 
# MAGIC     review_query_appeal_result_code,
# MAGIC     review_query_appeal_result,
# MAGIC     review_query_appeal_result_description,
# MAGIC     date(review_query_appeal_status_timestamp) review_query_appeal_status_date,
# MAGIC     count(distinct review_query_appeal_gid) result_review_query_appeals_day_total
# MAGIC   from
# MAGIC     review_query_appeal
# MAGIC   group by
# MAGIC     all
# MAGIC ),
# MAGIC result_query_appeals_basis as (
# MAGIC   select 
# MAGIC     review_query_appeal_result_code,
# MAGIC     review_query_appeal_result,
# MAGIC     review_query_appeal_result_description,
# MAGIC     review_query_appeal_status_date,
# MAGIC     result_review_query_appeals_day_total,
# MAGIC     sum(result_review_query_appeals_day_total) over (partition by review_query_appeal_result_code order by review_query_appeal_status_date)result_review_query_appeals_rolling_total
# MAGIC   from
# MAGIC     result_query_appeals_deduped
# MAGIC ),
# MAGIC result_review_query_appeals_lagged as (
# MAGIC   select
# MAGIC     review_query_appeal_result_code,
# MAGIC     review_query_appeal_result,
# MAGIC     review_query_appeal_result_description,
# MAGIC     review_query_appeal_status_date,
# MAGIC     result_review_query_appeals_day_total,
# MAGIC     nvl(lag(result_review_query_appeals_day_total) over (partition by review_query_appeal_result_code order by review_query_appeal_status_date),0) result_review_query_appeals_day_previous_total,
# MAGIC     result_review_query_appeals_rolling_total
# MAGIC   from
# MAGIC     result_query_appeals_basis
# MAGIC )
# MAGIC select distinct
# MAGIC   a.review_query_appeal_status_date,
# MAGIC   b.review_query_appeal_result_code,
# MAGIC   b.review_query_appeal_result,
# MAGIC   b.review_query_appeal_result_description,
# MAGIC   b.result_review_query_appeals_day_total,
# MAGIC   nvl(b.result_review_query_appeals_day_previous_total, 0) result_review_query_appeals_day_previous_total,
# MAGIC   b.result_review_query_appeals_rolling_total,
# MAGIC   a.review_query_appeals_day_total,
# MAGIC   a.review_query_appeals_day_previous_total,
# MAGIC   a.review_query_appeals_rolling_total,
# MAGIC   'TMOG_METRICS_GOLD_LOAD' create_user,
# MAGIC   current_timestamp() create_timestamp
# MAGIC from
# MAGIC   review_query_appeals_day_status_lagged a
# MAGIC     join result_review_query_appeals_lagged b
# MAGIC       on a.review_query_appeal_status_date = b.review_query_appeal_status_date

# COMMAND ----------

# DBTITLE 1,Create View: Status Review Query Appeal Metrics (Status Time)
# MAGIC %sql
# MAGIC create or replace temp view vw_tmog_metrics_status_review_query_appeal_status_metrics as
# MAGIC with status_review_query_appeal_status_deduped as (
# MAGIC   select
# MAGIC     review_query_appeal_status,
# MAGIC     review_query_appeal_status_code,
# MAGIC     review_query_appeal_status_description,
# MAGIC     date(review_query_appeal_status_timestamp) review_query_appeal_status_date,
# MAGIC     count(distinct review_query_appeal_gid) status_review_query_appeals_day_total
# MAGIC   from
# MAGIC     review_query_appeal
# MAGIC   group by 
# MAGIC     all
# MAGIC ),
# MAGIC status_review_query_appeal_status_basis as (
# MAGIC   select 
# MAGIC     review_query_appeal_status,
# MAGIC     review_query_appeal_status_code,
# MAGIC     review_query_appeal_status_description,
# MAGIC     review_query_appeal_status_date,
# MAGIC     status_review_query_appeals_day_total,
# MAGIC     sum(status_review_query_appeals_day_total) over (partition by review_query_appeal_status order by review_query_appeal_status_date) status_review_query_appeals_rolling_total
# MAGIC   from
# MAGIC     status_review_query_appeal_status_deduped
# MAGIC ),
# MAGIC status_review_query_appeal_lagged as (
# MAGIC   select
# MAGIC     review_query_appeal_status,
# MAGIC     review_query_appeal_status_code,
# MAGIC     review_query_appeal_status_description,
# MAGIC     review_query_appeal_status_date,
# MAGIC     status_review_query_appeals_day_total,
# MAGIC     nvl(lag(status_review_query_appeals_day_total) over (partition by review_query_appeal_status order by review_query_appeal_status_date), 0) status_review_query_appeals_day_previous_total,
# MAGIC     status_review_query_appeals_rolling_total
# MAGIC   from
# MAGIC     status_review_query_appeal_status_basis
# MAGIC )
# MAGIC select distinct
# MAGIC   a.review_query_appeal_status_date,
# MAGIC   b.review_query_appeal_status,
# MAGIC   b.review_query_appeal_status_description,
# MAGIC   b.status_review_query_appeals_day_total,
# MAGIC   nvl(b.status_review_query_appeals_day_previous_total, 0) status_review_query_appeals_day_previous_total,
# MAGIC   b.status_review_query_appeals_rolling_total,
# MAGIC   a.review_query_appeals_day_total,
# MAGIC   a.review_query_appeals_day_previous_total,
# MAGIC   a.review_query_appeals_rolling_total,
# MAGIC   'TMOG_METRICS_GOLD_LOAD' create_user,
# MAGIC   current_timestamp() create_timestamp
# MAGIC from
# MAGIC   review_query_appeals_day_status_lagged a
# MAGIC     join status_review_query_appeal_lagged b
# MAGIC       on a.review_query_appeal_status_date = b.review_query_appeal_status_date

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review Query Grounds

# COMMAND ----------

# DBTITLE 1,Create View: Employee Review Query Ground Metrics (System Time)
# MAGIC %sql
# MAGIC create or replace temp view vw_tmog_metrics_employee_review_query_ground_metrics as
# MAGIC with employee_ground_deduped as (
# MAGIC   select 
# MAGIC     initial_review_query_employee_id,
# MAGIC     date(initial_review_query_timestamp) initial_review_query_date,
# MAGIC     count(distinct review_query_ground_id) employee_review_query_grounds_day_total
# MAGIC   from
# MAGIC     review_query_ground
# MAGIC   group by
# MAGIC     all
# MAGIC ),
# MAGIC employee_ground_basis as (
# MAGIC   select 
# MAGIC     initial_review_query_employee_id,
# MAGIC     initial_review_query_date,
# MAGIC     employee_review_query_grounds_day_total,
# MAGIC     sum(employee_review_query_grounds_day_total) over (partition by initial_review_query_employee_id order by initial_review_query_date) employee_review_queries_rolling_total
# MAGIC   from
# MAGIC     employee_ground_deduped
# MAGIC ),
# MAGIC employee_review_queries_lagged as (
# MAGIC   select
# MAGIC     initial_review_query_employee_id,
# MAGIC     initial_review_query_date,
# MAGIC     employee_review_query_grounds_day_total,
# MAGIC     nvl(lag(employee_review_query_grounds_day_total) over (partition by initial_review_query_employee_id order by initial_review_query_date), 0) employee_review_queries_day_previous_total,
# MAGIC     employee_review_queries_rolling_total
# MAGIC   from
# MAGIC     employee_ground_basis
# MAGIC )
# MAGIC select distinct
# MAGIC   a.initial_review_query_date,
# MAGIC   b.initial_review_query_employee_id,
# MAGIC   b.employee_review_query_grounds_day_total,
# MAGIC   nvl(b.employee_review_queries_day_previous_total, 0) employee_review_queries_day_previous_total,
# MAGIC   b.employee_review_queries_rolling_total,
# MAGIC   a.review_query_grounds_day_total,
# MAGIC   a.review_query_grounds_day_previous_total,
# MAGIC   a.review_query_grounds_rolling_total,
# MAGIC   'TMOG_METRICS_GOLD_LOAD' create_user,
# MAGIC   current_timestamp() create_timestamp
# MAGIC from
# MAGIC   review_query_grounds_day_lagged a
# MAGIC     join employee_review_queries_lagged b
# MAGIC       on a.initial_review_query_date = b.initial_review_query_date

# COMMAND ----------

# DBTITLE 1,Create View: Class Review Query Ground Metrics (System Time)
# MAGIC %sql
# MAGIC create or replace temp view vw_tmog_metrics_review_query_ground_class_metrics as
# MAGIC with class_definitions as (
# MAGIC   select
# MAGIC     a.class_id,
# MAGIC     a.class_no class_number,
# MAGIC     a.fk_class_schedule_cd class_schedule_code,
# MAGIC     a.goods_services_ct goods_and_services_category
# MAGIC   from
# MAGIC     class_definition a
# MAGIC ),
# MAGIC class_ground_deduped as (
# MAGIC   select
# MAGIC     a.review_query_ground_class_id,
# MAGIC     b.class_number,
# MAGIC     b.class_schedule_code,
# MAGIC     b.goods_and_services_category,
# MAGIC     date(initial_review_query_timestamp) initial_review_query_date,
# MAGIC     count(distinct review_query_ground_class_id) class_review_queries_day_total
# MAGIC   from
# MAGIC     review_query_ground a
# MAGIC       join class_definitions b
# MAGIC         on a.review_query_ground_class_id = b.class_id
# MAGIC   group by
# MAGIC     all
# MAGIC ),
# MAGIC class_ground_basis as (
# MAGIC   select
# MAGIC     review_query_ground_class_id,
# MAGIC     class_number,
# MAGIC     class_schedule_code,
# MAGIC     goods_and_services_category,
# MAGIC     initial_review_query_date,
# MAGIC     class_review_queries_day_total,
# MAGIC     sum(class_review_queries_day_total) over (partition by review_query_ground_class_id order by initial_review_query_date) class_review_queries_rolling_total
# MAGIC   from
# MAGIC     class_ground_deduped
# MAGIC ),
# MAGIC class_review_queries_lagged as (
# MAGIC   select
# MAGIC     review_query_ground_class_id,
# MAGIC     class_number,
# MAGIC     class_schedule_code,
# MAGIC     goods_and_services_category,
# MAGIC     initial_review_query_date,
# MAGIC     class_review_queries_day_total,
# MAGIC     nvl(lag(class_review_queries_day_total) over (partition by review_query_ground_class_id order by initial_review_query_date), 0) class_review_queries_day_previous_total,
# MAGIC     class_review_queries_rolling_total
# MAGIC   from
# MAGIC     class_ground_basis
# MAGIC )
# MAGIC select distinct
# MAGIC   a.initial_review_query_date,
# MAGIC   b.review_query_ground_class_id,
# MAGIC   b.class_number,
# MAGIC   b.class_schedule_code,
# MAGIC   b.goods_and_services_category,
# MAGIC   b.class_review_queries_day_total,
# MAGIC   nvl(b.class_review_queries_day_previous_total, 0) class_review_queries_day_previous_total,
# MAGIC   b.class_review_queries_rolling_total,
# MAGIC   a.review_query_grounds_day_total,
# MAGIC   a.review_query_grounds_day_previous_total,
# MAGIC   a.review_query_grounds_rolling_total,
# MAGIC   'TMOG_METRICS_GOLD_LOAD' create_user,
# MAGIC   current_timestamp() create_timestamp
# MAGIC from
# MAGIC   review_query_grounds_day_lagged a
# MAGIC     join class_review_queries_lagged b
# MAGIC       on a.initial_review_query_date = b.initial_review_query_date

# COMMAND ----------

# DBTITLE 1,Create View: Ground Type Review Query Ground Metrics (System Time)
# MAGIC %sql
# MAGIC create or replace temp view vw_tmog_metrics_review_query_ground_type_metrics as
# MAGIC with ground_type_review_query_deduped as (
# MAGIC   select
# MAGIC     review_query_ground_type,
# MAGIC     date(initial_review_query_timestamp) initial_review_query_date,
# MAGIC     count(distinct review_query_ground_id) ground_type_review_queries_day_total
# MAGIC   from
# MAGIC     review_query_ground
# MAGIC   group by 
# MAGIC     all
# MAGIC ),
# MAGIC ground_type_review_query_basis as (
# MAGIC   select 
# MAGIC     review_query_ground_type,
# MAGIC     initial_review_query_date,
# MAGIC     ground_type_review_queries_day_total,
# MAGIC     sum(ground_type_review_queries_day_total) over (partition by review_query_ground_type order by initial_review_query_date) ground_type_review_queries_rolling_total
# MAGIC   from
# MAGIC     ground_type_review_query_deduped
# MAGIC ),
# MAGIC ground_type_review_queries_lagged as (
# MAGIC   select
# MAGIC     review_query_ground_type,
# MAGIC     initial_review_query_date,
# MAGIC     ground_type_review_queries_day_total,
# MAGIC     nvl(lag(ground_type_review_queries_day_total) over (partition by review_query_ground_type order by initial_review_query_date), 0) ground_type_review_queries_day_previous_total,
# MAGIC     ground_type_review_queries_rolling_total
# MAGIC   from
# MAGIC     ground_type_review_query_basis
# MAGIC )
# MAGIC select distinct
# MAGIC   a.initial_review_query_date,
# MAGIC   b.review_query_ground_type,
# MAGIC   b.ground_type_review_queries_day_total,
# MAGIC   nvl(b.ground_type_review_queries_day_previous_total, 0) ground_type_queries_day_previous_total,
# MAGIC   b.ground_type_review_queries_rolling_total,
# MAGIC   a.review_query_grounds_day_total,
# MAGIC   a.review_query_grounds_day_previous_total,
# MAGIC   a.review_query_grounds_rolling_total,
# MAGIC   'TMOG_METRICS_GOLD_LOAD' create_user,
# MAGIC   current_timestamp() create_timestamp
# MAGIC from
# MAGIC   review_query_grounds_day_lagged a
# MAGIC     join ground_type_review_queries_lagged b
# MAGIC       on a.initial_review_query_date = b.initial_review_query_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert Queries Metrics

# COMMAND ----------

# DBTITLE 1,Insert: tmog_metrics_employee_review_query_metrics
display(
  spark.sql(
    f"""
    insert overwrite {reporting_catalog}.gold.tmog_metrics_employee_review_query_metrics (
      initial_review_query_date,
      initial_review_query_employee_id,
      employee_review_queries_day_total,
      employee_review_queries_day_previous_total,
      employee_review_queries_rolling_total,
      review_queries_day_total,
      review_queries_day_previous_total,
      review_queries_rolling_total,
      create_user,
      create_timestamp
    )
    select 
      initial_review_query_date,
      initial_review_query_employee_id,
      employee_review_queries_day_total,
      employee_review_queries_day_previous_total,
      employee_review_queries_rolling_total,
      review_queries_day_total,
      review_queries_day_previous_total,
      review_queries_rolling_total,
      create_user,
      create_timestamp 
  from 
    vw_tmog_metrics_employee_review_query_metrics
  """)
)

# COMMAND ----------

# DBTITLE 1,Insert: tmog_metrics_publication_review_query_metrics
display(
  spark.sql(
    f"""
    insert overwrite {reporting_catalog}.gold.tmog_metrics_publication_review_query_metrics (
      initial_review_date,
      publication_date,
      publication_date_review_queries_day_total,
      publication_date_review_queries_day_previous_total,
      publication_date_review_queries_rolling_total,
      review_queries_day_total,
      review_queries_day_previous_total,
      review_queries_rolling_total,
      create_user,
      create_timestamp
    )
    select 
      initial_review_query_date,
      publication_date,
      publication_date_review_queries_day_total,
      publication_date_review_queries_day_previous_total,
      publication_date_review_queries_rolling_total,
      review_queries_day_total,
      review_queries_day_previous_total,
      review_queries_rolling_total,
      create_user,
      create_timestamp 
  from 
    vw_tmog_metrics_publication_review_query_metrics
  """)
)

# COMMAND ----------

# DBTITLE 1,Insert: tmog_metrics_case_review_query_metrics
display(
  spark.sql(
    f"""
    insert overwrite {reporting_catalog}.gold.tmog_metrics_case_review_query_metrics (
      initial_review_date,
      case_review_queries_day_total,
      distinct_case_review_queries_day_total,
      case_review_queries_day_previous_total,
      distinct_case_review_queries_day_previous_total,
      case_review_queries_rolling_total,
      distinct_case_review_queries_rolling_total,
      create_user,
      create_timestamp
    )
    select 
      initial_review_query_date,
      case_review_queries_day_total,
      distinct_case_review_queries_day_total,
      case_review_queries_day_previous_total,
      distinct_case_review_queries_day_previous_total,
      case_review_queries_rolling_total,
      distinct_case_review_queries_rolling_total,
      create_user,
      create_timestamp 
    from 
      vw_tmog_metrics_case_review_query_metrics
  """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert Appeal Metrics

# COMMAND ----------

# DBTITLE 1,Insert: tmog_metrics_employee_review_query_appeal_metrics
display(
  spark.sql(
    f"""
    insert overwrite {reporting_catalog}.gold.tmog_metrics_employee_review_query_appeal_metrics (
      initial_review_query_appeal_date,
      initial_review_query_appeal_employee_id,
      employee_review_query_appeals_day_total,
      employee_review_query_appeals_day_previous_total,
      employee_review_query_appeals_rolling_total,
      review_query_appeals_day_total,
      review_query_appeals_day_previous_total,
      review_query_appeals_rolling_total,
      create_user,
      create_timestamp
    )
    select 
      initial_review_query_appeal_date,
      initial_review_query_appeal_employee_id,
      employee_review_query_appeals_day_total,
      employee_review_query_appeals_day_previous_total,
      employee_review_query_appeals_rolling_total,
      review_query_appeals_day_total,
      review_query_appeals_day_previous_total,
      review_query_appeals_rolling_total,
      create_user,
      create_timestamp 
  from 
    vw_tmog_metrics_employee_review_query_appeal_metrics
  """)
)

# COMMAND ----------

# DBTITLE 1,Insert: tmog_metrics_employee_review_query_appeal_status_metrics
display(
  spark.sql(
    f"""
    insert overwrite {reporting_catalog}.gold.tmog_metrics_employee_review_query_appeal_status_metrics (
      review_query_appeal_status_date,
      initial_review_query_appeal_employee_id,
      employee_review_query_appeals_day_total,
      employee_review_query_appeals_day_previous_total,
      employee_review_query_appeals_rolling_total,
      review_query_appeals_day_total,
      review_query_appeals_day_previous_total,
      review_query_appeals_rolling_total,
      create_user,
      create_timestamp
    )
    select 
      review_query_appeal_status_date,
      initial_review_query_appeal_employee_id,
      employee_review_query_appeals_day_total,
      employee_query_appeals_day_previous_total,
      employee_review_query_appeals_rolling_total,
      review_query_appeals_day_total,
      review_query_appeals_day_previous_total,
      review_query_appeals_rolling_total,
      create_user,
      create_timestamp
    from 
      vw_tmog_metrics_employee_review_query_appeal_status_metrics
  """)
)

# COMMAND ----------

# DBTITLE 1,Insert: tmog_metrics_result_review_query_appeal_metrics
display(
  spark.sql(
    f"""
      insert overwrite {reporting_catalog}.gold.tmog_metrics_result_review_query_appeal_metrics (
        initial_review_query_appeal_date,
        review_query_appeal_result_code,
        review_query_appeal_result,
        review_query_appeal_result_description,
        result_review_query_appeals_day_total,
        result_review_query_appeals_day_previous_total,
        result_review_query_appeals_rolling_total,
        review_query_appeals_day_total,
        review_query_appeals_day_previous_total,
        review_query_appeals_rolling_total,
        create_user,
        create_timestamp
      )
      select 
        initial_review_query_appeal_date,
        review_query_appeal_result_code,
        review_query_appeal_result,
        review_query_appeal_result_description,
        result_review_query_appeals_day_total,
        result_review_query_appeals_day_previous_total,
        result_review_query_appeals_rolling_total,
        review_query_appeals_day_total,
        review_query_appeals_day_previous_total,
        review_query_appeals_rolling_total,
        create_user,
        create_timestamp 
      from 
        vw_tmog_metrics_result_review_query_appeal_metrics
  """)
)

# COMMAND ----------

# DBTITLE 1,Insert: tmog_metrics_result_review_query_appeal_status_metrics
display(
  spark.sql(
    f"""
    insert overwrite {reporting_catalog}.gold.tmog_metrics_result_review_query_appeal_status_metrics (
      review_query_appeal_status_date,
      review_query_appeal_result_code,
      review_query_appeal_result,
      review_query_appeal_result_description,
      result_review_query_appeals_day_total,
      result_review_query_appeals_day_previous_total,
      result_review_query_appeals_rolling_total,
      review_query_appeals_day_total,
      review_query_appeals_day_previous_total,
      review_query_appeals_rolling_total,
      create_user,
      create_timestamp
    )
    select 
      review_query_appeal_status_date,
      review_query_appeal_result_code,
      review_query_appeal_result,
      review_query_appeal_result_description,
      result_review_query_appeals_day_total,
      result_review_query_appeals_day_previous_total,
      result_review_query_appeals_rolling_total,
      review_query_appeals_day_total,
      review_query_appeals_day_previous_total,
      review_query_appeals_rolling_total,
      create_user,
      create_timestamp 
    from 
      vw_tmog_metrics_result_review_query_appeal_status_metrics
  """)
)

# COMMAND ----------

# DBTITLE 1,Insert: tmog_metrics_status_review_query_appeal_metrics
display(
  spark.sql(
    f"""
    insert overwrite {reporting_catalog}.gold.tmog_metrics_status_review_query_appeal_metrics (
      initial_review_query_appeal_date,
      review_query_appeal_status,
      review_query_appeal_status_description,
      status_review_query_appeals_day_total,
      status_review_query_appeals_day_previous_total,
      status_review_query_appeals_rolling_total,
      review_query_appeals_day_total,
      review_query_appeals_day_previous_total,
      review_query_appeals_rolling_total,
      create_user,
      create_timestamp
    )
    select 
      initial_review_query_appeal_date,
      review_query_appeal_status,
      review_query_appeal_status_description,
      status_review_query_appeals_day_total,
      status_review_query_appeals_day_previous_total,
      status_review_query_appeals_rolling_total,
      review_query_appeals_day_total,
      review_query_appeals_day_previous_total,
      review_query_appeals_rolling_total,
      create_user,
      create_timestamp 
    from 
      vw_tmog_metrics_status_review_query_appeal_metrics
  """)
)

# COMMAND ----------

# DBTITLE 1,Insert: tmog_metrics_status_review_query_appeal_status_metrics
display(
  spark.sql(
    f"""
    insert overwrite {reporting_catalog}.gold.tmog_metrics_status_review_query_appeal_status_metrics (
      review_query_appeal_status_date,
      review_query_appeal_status,
      review_query_appeal_status_description,
      status_review_query_appeals_day_total,
      status_review_query_appeals_day_previous_total,
      status_review_query_appeals_rolling_total,
      review_query_appeals_day_total,
      review_query_appeals_day_previous_total,
      review_query_appeals_rolling_total,
      create_user,
      create_timestamp
    )
    select 
      review_query_appeal_status_date,
      review_query_appeal_status,
      review_query_appeal_status_description,
      status_review_query_appeals_day_total,
      status_review_query_appeals_day_previous_total,
      status_review_query_appeals_rolling_total,
      review_query_appeals_day_total,
      review_query_appeals_day_previous_total,
      review_query_appeals_rolling_total,
      create_user,
      create_timestamp
    from vw_tmog_metrics_status_review_query_appeal_status_metrics
  """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert Ground Metrics

# COMMAND ----------

# DBTITLE 1,Insert: tmog_metrics_employee_review_query_ground_metrics
display(
  spark.sql(
    f"""
    insert overwrite {reporting_catalog}.gold.tmog_metrics_employee_review_query_ground_metrics (
      initial_review_date,
      initial_review_employee_id,
      employee_review_queries_day_total,
      employee_review_queries_day_previous_total,
      employee_review_queries_rolling_total,
      review_queries_day_total,
      review_queries_day_previous_total,
      review_queries_rolling_total,
      create_user,
      create_timestamp
    )
    select 
      initial_review_query_date,
      initial_review_query_employee_id,
      employee_review_query_grounds_day_total,
      employee_review_queries_day_previous_total,
      employee_review_queries_rolling_total,
      review_query_grounds_day_total,
      review_query_grounds_day_previous_total,
      review_query_grounds_rolling_total,
      create_user,
      create_timestamp
    from 
      vw_tmog_metrics_employee_review_query_ground_metrics
  """)
)

# COMMAND ----------

# DBTITLE 1,Insert: tmog_metrics_review_query_ground_class_metrics
display(
  spark.sql(
    f"""
    insert overwrite {reporting_catalog}.gold.tmog_metrics_review_query_ground_class_metrics (
      initial_review_query_date,
      ground_class_id,
      class_number,
      class_schedule_code,
      goods_and_services_category,
      class_review_queries_day_total,
      class_review_queries_day_previous_total,
      class_review_queries_rolling_total,
      review_queries_day_total,
      review_queries_day_previous_total,
      review_queries_rolling_total,
      create_user,
      create_timestamp
    )
    select 
      initial_review_query_date,
      review_query_ground_class_id,
      class_number,
      class_schedule_code,
      goods_and_services_category,
      class_review_queries_day_total,
      class_review_queries_day_previous_total,
      class_review_queries_rolling_total,
      review_query_grounds_day_total,
      review_query_grounds_day_previous_total,
      review_query_grounds_rolling_total,
      create_user,
      create_timestamp 
    from 
      vw_tmog_metrics_review_query_ground_class_metrics
  """)
)

# COMMAND ----------

# DBTITLE 1,Insert: tmog_metrics_review_query_ground_type_metrics
display(
  spark.sql(
    f"""
    insert overwrite {reporting_catalog}.gold.tmog_metrics_review_query_ground_type_metrics (
      initial_review_query_date,
      ground_type,
      ground_type_review_queries_day_total,
      ground_type_review_queries_day_previous_total,
      ground_type_review_queries_rolling_total,
      review_queries_day_total,
      review_queries_day_previous_total,
      review_queries_rolling_total,
      create_user,
      create_timestamp
  )
  select 
    initial_review_query_date,
    review_query_ground_type,
    ground_type_review_queries_day_total,
    ground_type_queries_day_previous_total,
    ground_type_review_queries_rolling_total,
    review_query_grounds_day_total,
    review_query_grounds_day_previous_total,
    review_query_grounds_rolling_total,
    create_user,
    create_timestamp
  from 
    vw_tmog_metrics_review_query_ground_type_metrics
  """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teardown

# COMMAND ----------

# DBTITLE 1,End Job
count_tmog_metrics_employee_review_query_metrics = spark.sql(
    f"select count(*) as cnt from {reporting_catalog}.gold.tmog_metrics_employee_review_query_metrics"
).collect()[0]["cnt"]
count_tmog_metrics_publication_review_query_metrics = spark.sql(
    f"select count(*) as cnt from {reporting_catalog}.gold.tmog_metrics_publication_review_query_metrics"
).collect()[0]["cnt"]
count_tmog_metrics_case_review_query_metrics = spark.sql(
    f"select count(*) as cnt from {reporting_catalog}.gold.tmog_metrics_case_review_query_metrics"
).collect()[0]["cnt"]
count_tmog_metrics_employee_review_query_appeal_metrics = spark.sql(
    f"select count(*) as cnt from {reporting_catalog}.gold.tmog_metrics_employee_review_query_appeal_metrics"
).collect()[0]["cnt"]
count_tmog_metrics_employee_review_query_appeal_status_metrics = spark.sql(
    f"select count(*) as cnt from {reporting_catalog}.gold.tmog_metrics_employee_review_query_appeal_status_metrics"
).collect()[0]["cnt"]
count_tmog_metrics_result_review_query_appeal_metrics = spark.sql(
    f"select count(*) as cnt from {reporting_catalog}.gold.tmog_metrics_result_review_query_appeal_metrics"
).collect()[0]["cnt"]
count_tmog_metrics_result_review_query_appeal_status_metrics = spark.sql(
    f"select count(*) as cnt from {reporting_catalog}.gold.tmog_metrics_result_review_query_appeal_status_metrics"
).collect()[0]["cnt"]
count_tmog_metrics_status_review_query_appeal_metrics = spark.sql(
    f"select count(*) as cnt from {reporting_catalog}.gold.tmog_metrics_status_review_query_appeal_metrics"
).collect()[0]["cnt"]
count_tmog_metrics_status_review_query_appeal_status_metrics = spark.sql(
    f"select count(*) as cnt from {reporting_catalog}.gold.tmog_metrics_status_review_query_appeal_status_metrics"
).collect()[0]["cnt"]
count_tmog_metrics_employee_review_query_ground_metrics = spark.sql(
    f"select count(*) as cnt from {reporting_catalog}.gold.tmog_metrics_employee_review_query_ground_metrics"
).collect()[0]["cnt"]
count_tmog_metrics_review_query_ground_class_metrics = spark.sql(
    f"select count(*) as cnt from {reporting_catalog}.gold.tmog_metrics_review_query_ground_class_metrics"
).collect()[0]["cnt"]
count_tmog_metrics_review_query_ground_type_metrics = spark.sql(
    f"select count(*) as cnt from {reporting_catalog}.gold.tmog_metrics_review_query_ground_type_metrics"
).collect()[0]["cnt"]

table_counts: list[int] = [
    count_tmog_metrics_employee_review_query_metrics,
    count_tmog_metrics_publication_review_query_metrics,
    count_tmog_metrics_case_review_query_metrics,
    count_tmog_metrics_employee_review_query_appeal_metrics,
    count_tmog_metrics_employee_review_query_appeal_status_metrics,
    count_tmog_metrics_result_review_query_appeal_metrics,
    count_tmog_metrics_result_review_query_appeal_status_metrics,
    count_tmog_metrics_status_review_query_appeal_metrics,
    count_tmog_metrics_status_review_query_appeal_status_metrics,
    count_tmog_metrics_employee_review_query_ground_metrics,
    count_tmog_metrics_review_query_ground_class_metrics,
    count_tmog_metrics_review_query_ground_type_metrics,
]
num_empty_tables: int = count_empty(table_counts)

if not num_empty_tables:
    end_job_cntl(
        f"{reporting_catalog}.silver",
        job_name,
        job_start_ts,
        "completed",
        count_tmog_metrics_employee_review_query_metrics
        + count_tmog_metrics_publication_review_query_metrics
        + count_tmog_metrics_case_review_query_metrics
        + count_tmog_metrics_employee_review_query_appeal_metrics
        + count_tmog_metrics_employee_review_query_appeal_status_metrics
        + count_tmog_metrics_result_review_query_appeal_metrics
        + count_tmog_metrics_result_review_query_appeal_status_metrics
        + count_tmog_metrics_status_review_query_appeal_metrics
        + count_tmog_metrics_status_review_query_appeal_status_metrics
        + count_tmog_metrics_employee_review_query_ground_metrics
        + count_tmog_metrics_review_query_ground_class_metrics
        + count_tmog_metrics_review_query_ground_type_metrics,
        "job completed successfully",
    )
    dbutils.notebook.exit(
        f"""
        Job completed with:
        - [{count_tmog_metrics_employee_review_query_metrics}] records for tmog_metrics_employee_review_query_metrics
        - [{count_tmog_metrics_publication_review_query_metrics}] records for tmog_metrics_publication_review_query_metrics
        - [{count_tmog_metrics_case_review_query_metrics}] records for tmog_metrics_case_review_query_metrics
        - [{count_tmog_metrics_employee_review_query_appeal_metrics}] records for tmog_metrics_employee_review_query_appeal_metrics
        - [{count_tmog_metrics_employee_review_query_appeal_status_metrics}] records for tmog_metrics_employee_review_query_appeal_status_metrics
        - [{count_tmog_metrics_result_review_query_appeal_metrics}] records for tmog_metrics_result_review_query_appeal_metrics
        - [{count_tmog_metrics_result_review_query_appeal_status_metrics}] records for tmog_metrics_result_review_query_appeal_status_metrics
        - [{count_tmog_metrics_status_review_query_appeal_metrics}] records for tmog_metrics_status_review_query_appeal_metrics
        - [{count_tmog_metrics_status_review_query_appeal_status_metrics}] records for tmog_metrics_status_review_query_appeal_status_metrics
        - [{count_tmog_metrics_employee_review_query_ground_metrics}] records for tmog_metrics_employee_review_query_ground_metrics
        - [{count_tmog_metrics_review_query_ground_class_metrics}] records for tmog_metrics_review_query_ground_class_metrics
        - [{count_tmog_metrics_review_query_ground_type_metrics}] records for tmog_metrics_review_query_ground_type_metrics
        """
    )
else:
    raise ValueError(
        f"{num_empty_tables} tables loaded 0 records. Tables must have at least 1 record."
    )