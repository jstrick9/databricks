# Databricks notebook source
# MAGIC %md
# MAGIC # TMOG Metrics Bronze Load

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
tmworker_catalog = common_configs["schema"]["tmworker_catalog"]
edw_scope = common_configs["secrets"]["edw_scope"]
print(reporting_catalog, tmngpdb_catalog, tmworker_catalog, edw_scope)

# COMMAND ----------

# DBTITLE 1,Begin Job
job_name = "ntb_tmog_metrics_bronze_load"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Creation

# COMMAND ----------

# DBTITLE 1,Create View: TMOG
# Note: ignoring role codes for now due to lack of completion
spark.sql(f"""
select distinct
    review_gid,
    initial_review_employee_id,
    initial_review_employee_organization_code,
    initial_review_timestamp,
    latest_review_employee_id,
    latest_review_timestamp,
    publication_date,
    serial_number,
    previous_bounce_number,
    review_status_code,
    review_status_title,
    review_status_description,
    d.review_query_gid,
    d.og_page_no og_page_number,
    d.print_error_in print_error_indicator,
    d.query_tx review_query_content,
    review_query_note_type_code,
    review_query_note_type,
    review_query_note_description,
    review_query_ground_id,
    review_query_ground_code,
    review_query_ground,
    review_query_ground_description,
    review_query_ground_order_number,
    review_query_ground_grouping_number,
    review_query_ground_type_code,
    review_query_ground_type,
    review_query_ground_type_description,
    t.fk_class_id review_query_ground_class_id,
    j.employee_review_query_id,
    j.cfk_employee_no initial_review_query_employee_id,
    j.cfk_organization_cd initial_review_query_employee_organization_code,
    j.create_ts initial_review_query_timestamp,
    j.last_mod_user_id latest_review_query_employee_id,
    j.last_mod_ts latest_review_query_timestamp,
    j.review_assignment_dt review_query_assignment_date,
    initial_employee_review_query_status_employee_id,
    initial_employee_review_query_status_timestamp,
    employee_review_query_status_code,
    employee_review_query_status_code_description,
    employee_review_query_status_reason_description,
    latest_employee_review_query_status_employee_id,
    latest_employee_review_query_status_timestamp,
    review_query_note_sequence_number,
    initial_review_query_note_employee_id,
    initial_review_query_note_employee_organization_code,
    initial_review_query_note_timestamp,
    latest_review_query_note_employee_id,
    latest_review_query_note_timestamp,
    review_query_note_text,
    m.review_query_appeal_id,
    m.approval_in review_query_appeal_approval_indicator,
    review_query_appeal_gid,
    review_query_appeal_result_date,
    review_query_appeal_proceeding_number,
    review_query_appeal_decision_description,
    review_query_appeal_reason_description,
    review_query_appeal_director_email_sent_indicator,
    review_query_appeal_result_code,
    review_query_appeal_result,
    review_query_appeal_result_description,
    p.employee_query_appeal_id,
    p.cfk_employee_no initial_review_query_appeal_employee_id,
    p.cfk_employee_role_cd initial_review_query_appeal_employee_role_code,
    p.cfk_organization_cd initial_review_query_appeal_employee_organization_code,
    p.create_ts initial_review_query_appeal_timestamp,
    p.last_mod_user_id latest_review_query_appeal_employee_id,
    p.last_mod_ts latest_review_query_appeal_timestamp,
    review_query_appeal_status_timestamp,
    r.note_sequence_no review_query_appeal_sequence_number,
    r.appeal_note_tx review_query_appeal_note,
    review_query_appeal_status_code,
    review_query_appeal_status,
    review_query_appeal_status_description
from 
    (
        select 
            b.og_tm_review_gid review_gid,
            b.cfk_reviewer_employee_no initial_review_employee_id,
            b.cfk_organization_cd initial_review_employee_organization_code,
            b.create_ts initial_review_timestamp,
            b.last_mod_user_id latest_review_employee_id,
            b.last_mod_ts latest_review_timestamp,
            b.publication_dt publication_date,
            b.dn_tm_serial_num_tx serial_number,
            b.previous_og_bounce_no previous_bounce_number,
            c.tm_review_status_cd review_status_code,
            c.title_tx review_status_title,
            c.description_tx review_status_description
        from 
            {tmngpdb_catalog}.bronze.og_tm_review b
            inner join {tmngpdb_catalog}.bronze.stnd_tm_review_status c
                on b.fk_tm_review_status_cd = c.tm_review_status_cd
    ) b
    inner join {tmngpdb_catalog}.bronze.review_query d
        on b.review_gid = d.fk_og_tm_review_gid
    left join (
        select
            e.fk_review_query_gid,
            e.note_sequence_no review_query_note_sequence_number,
            e.cfk_employee_no initial_review_query_note_employee_id,
            e.cfk_organization_cd initial_review_query_note_employee_organization_code,
            e.create_ts initial_review_query_note_timestamp,
            e.last_mod_user_id latest_review_query_note_employee_id,
            e.last_mod_ts latest_review_query_note_timestamp,
            e.note_tx review_query_note_text,
            f.note_type_cd review_query_note_type_code,
            f.title_tx review_query_note_type,
            f.description_tx review_query_note_description
        from 
            {tmngpdb_catalog}.bronze.review_query_note e
            inner join {tmngpdb_catalog}.bronze.stnd_note_type f
                on e.fk_note_type_cd = f.note_type_cd
    ) e
        on d.review_query_gid = e.fk_review_query_gid
    left join (
        select 
            g.fk_review_query_gid,
            g.query_ground_id review_query_ground_id,
            h.ground_cd review_query_ground_code,
            h.title_tx review_query_ground,
            h.description_tx review_query_ground_description,
            h.sort_order_no review_query_ground_order_number,
            h.grouping_no review_query_ground_grouping_number,
            i.ground_type_cd review_query_ground_type_code,
            i.title_tx review_query_ground_type,
            i.description_tx review_query_ground_type_description
        from 
            {tmngpdb_catalog}.bronze.query_ground g
        inner join {tmngpdb_catalog}.bronze.stnd_ground h
            on g.fk_ground_cd = h.ground_cd
            and g.fk_ground_type_cd = h.fk_ground_type_cd
        inner join {tmngpdb_catalog}.bronze.stnd_ground_type i
            on h.fk_ground_type_cd = i.ground_type_cd
    ) g
        on d.review_query_gid = g.fk_review_query_gid
    left join {tmngpdb_catalog}.bronze.employee_review_query j
        on g.review_query_ground_id = j.fk_query_ground_id
    left join (
        select distinct
            k.fk_employee_review_query_id,
            k.create_user_id initial_employee_review_query_status_employee_id,
            k.create_ts initial_employee_review_query_status_timestamp,
            k.last_mod_ts latest_employee_review_query_status_timestamp,
            k.last_mod_user_id latest_employee_review_query_status_employee_id,
            -- Folding on the query id to prevent nulls; this may not make sense in practice
            k.status_reason_tx employee_review_query_status_reason_description,
            l.query_review_status_cd employee_review_query_status_code,
            l.title_tx employee_review_query_status_code_description
        from 
            {tmngpdb_catalog}.bronze.employee_review_query_stat k
            inner join {tmngpdb_catalog}.bronze.stnd_query_review_status l
                on k.fk_query_review_status_cd = l.query_review_status_cd
    ) k
        on j.employee_review_query_id = k.fk_employee_review_query_id
    left join {tmngpdb_catalog}.bronze.review_query_appeal m
      on g.review_query_ground_id = m.review_query_appeal_id
    left join (
        select 
            n.query_appeal_gid review_query_appeal_gid,
            n.appeal_result_dt review_query_appeal_result_date,
            n.appeal_proceeding_no review_query_appeal_proceeding_number,
            n.appeal_decision_tx review_query_appeal_decision_description,
            n.appeal_reason_tx review_query_appeal_reason_description,
            n.director_email_sent_in review_query_appeal_director_email_sent_indicator,
            o.appeal_result_cd review_query_appeal_result_code,
            o.title_tx review_query_appeal_result,
            o.description_tx review_query_appeal_result_description
        from 
            {tmngpdb_catalog}.bronze.query_appeal n
            inner join {tmngpdb_catalog}.bronze.stnd_appeal_result o
                on n.fk_appeal_result_cd = o.appeal_result_cd
    ) n
        on m.fk_query_appeal_gid = n.review_query_appeal_gid
    left join {tmngpdb_catalog}.bronze.employee_query_appeal p
        on n.review_query_appeal_gid = p.fk_query_appeal_gid
    left join (
        select
            q.fk_employee_query_appeal_id,
            q.appeal_status_ts review_query_appeal_status_timestamp,
            s.appeal_status_cd review_query_appeal_status_code,
            s.title_tx review_query_appeal_status,
            s.description_tx review_query_appeal_status_description
        from
            {tmngpdb_catalog}.bronze.query_appeal_status q
            inner join {tmngpdb_catalog}.bronze.stnd_appeal_status s
                on q.fk_appeal_status_cd = s.appeal_status_cd
    ) q
        on p.employee_query_appeal_id = q.fk_employee_query_appeal_id
    left join {tmngpdb_catalog}.bronze.query_appeal_note r
        on p.employee_query_appeal_id = r.fk_employee_query_appeal_id
    left join {tmngpdb_catalog}.bronze.review_query_class t
        on g.review_query_ground_id = t.fk_query_ground_id
where
    1 = 1
    and b.review_status_code = 'Q'
    -- and b.initial_review_timestamp >= '2015-10-01'
"""
).createOrReplaceTempView("vw_tmog_metrics_base")

# COMMAND ----------

# DBTITLE 1,Create View: Employee
# we can probably reduce this using a predicate for the grade end date >= min(review_timestamp); I'll keep it here for now
employee = read_data_from_oracle_conn_dsu_cmn(sql_query="select * from emp_grade", scope_name=edw_scope)
employee.createOrReplaceTempView("employee")
print("Employee sample:")
display(spark.sql("select * from employee").limit(5))

# COMMAND ----------

# DBTITLE 1,Create View: Worker Base
spark.sql(f"""
select distinct
  a.emp_no employee_id,
  a.employee_name_full employee_name,
  a.org_nm employee_organization,
  a.org_cd employee_organization_code,
  timestamp(a.grade_start_dt) role_start_bound_timestamp,
  timestamp(date(a.grade_end_dt) + interval 1 day) role_end_bound_timestamp,
  case 
    when date(a.grade_end_dt) = max(date(a.grade_end_dt)) over (partition by a.emp_no) then true 
    else false 
  end is_latest,
  count(1) over (
    partition by 
      a.emp_no, 
      timestamp(a.grade_start_dt), 
      timestamp(date(a.grade_end_dt) + interval 1 day)
  ) > 1 is_role_overlapping,
  row_number() over (
    partition by 
      a.emp_no, 
      timestamp(a.grade_start_dt), 
      timestamp(date(a.grade_end_dt) + interval 1 day) 
    order by a.grade
  ) role_number
from
  employee a
""").createOrReplaceTempView("vw_worker_base")

# COMMAND ----------

# DBTITLE 1,Audit: Duplicate Grade Base
print("Workers with potential law office / department conflicts:")
display(
    spark.sql("""
        select 
            distinct 
                employee_id, 
                employee_organization, 
                role_start_bound_timestamp,
                role_end_bound_timestamp,
                role_number 
        from 
            vw_worker_base 
        where 
            is_role_overlapping = true
        order by 
            employee_id,
            role_number
    """)
)

# COMMAND ----------

# DBTITLE 1,Create View: TMOG Worker Base
# MAGIC %sql
# MAGIC create or replace temp view vw_tmog_metrics_worker_base as
# MAGIC with cte_dim_review_employees as (
# MAGIC   select
# MAGIC     a.review_gid transaction_id,
# MAGIC     b.role_start_bound_timestamp,
# MAGIC     a.initial_review_timestamp transaction_timestamp,
# MAGIC     b.role_end_bound_timestamp,
# MAGIC     a.initial_review_employee_id employee_id,
# MAGIC     b.employee_name,
# MAGIC     max_by(
# MAGIC       nvl(a.initial_review_employee_organization_code, b.employee_organization_code),
# MAGIC       b.role_start_bound_timestamp
# MAGIC     ) employee_organization_code,
# MAGIC     'REVIEW' transaction_type
# MAGIC   from
# MAGIC     vw_tmog_metrics_base a
# MAGIC       left join vw_worker_base b
# MAGIC         on a.initial_review_employee_id = b.employee_id
# MAGIC         and (
# MAGIC           (
# MAGIC             a.initial_review_timestamp >= b.role_start_bound_timestamp
# MAGIC             and b.role_end_bound_timestamp is null
# MAGIC           )
# MAGIC           or (
# MAGIC             a.initial_review_timestamp >= b.role_start_bound_timestamp
# MAGIC             and b.is_latest = true
# MAGIC           )
# MAGIC           or (
# MAGIC             a.initial_review_timestamp between
# MAGIC               b.role_start_bound_timestamp
# MAGIC             and
# MAGIC               b.role_end_bound_timestamp
# MAGIC           )
# MAGIC         )
# MAGIC   where
# MAGIC     a.initial_review_employee_id is not null
# MAGIC   group by
# MAGIC     all
# MAGIC   union
# MAGIC   select distinct
# MAGIC     a.review_gid transaction_id,
# MAGIC     b.role_start_bound_timestamp,
# MAGIC     a.latest_review_timestamp transaction_timestamp,
# MAGIC     b.role_end_bound_timestamp,
# MAGIC     a.latest_review_employee_id employee_id,
# MAGIC     b.employee_name,
# MAGIC     max_by(b.employee_organization_code, b.role_start_bound_timestamp) employee_organization_code,
# MAGIC     'REVIEW' transaction_type
# MAGIC   from
# MAGIC     vw_tmog_metrics_base a
# MAGIC       left join vw_worker_base b
# MAGIC         on a.latest_review_employee_id = b.employee_id
# MAGIC         and (
# MAGIC           (
# MAGIC             a.latest_review_timestamp >= b.role_start_bound_timestamp
# MAGIC             and b.role_end_bound_timestamp is null
# MAGIC           )
# MAGIC           or (
# MAGIC             a.latest_review_timestamp >= b.role_start_bound_timestamp
# MAGIC             and b.is_latest = true
# MAGIC           )
# MAGIC           or (
# MAGIC             a.latest_review_timestamp between
# MAGIC               b.role_start_bound_timestamp
# MAGIC             and
# MAGIC               b.role_end_bound_timestamp
# MAGIC           )
# MAGIC         )
# MAGIC   where
# MAGIC     a.latest_review_employee_id is not null
# MAGIC   group by
# MAGIC     all
# MAGIC ),
# MAGIC cte_dim_review_query_employees as (
# MAGIC   select distinct
# MAGIC     a.review_query_gid transaction_id,
# MAGIC     b.role_start_bound_timestamp,
# MAGIC     a.initial_review_query_timestamp transaction_timestamp,
# MAGIC     b.role_end_bound_timestamp,
# MAGIC     a.initial_review_query_employee_id employee_id,
# MAGIC     b.employee_name,
# MAGIC     max_by(
# MAGIC       nvl(a.initial_review_query_employee_organization_code, b.employee_organization_code),
# MAGIC       b.role_start_bound_timestamp
# MAGIC     ) employee_organization_code,
# MAGIC     'REVIEW_QUERY' transaction_type
# MAGIC   from
# MAGIC     vw_tmog_metrics_base a
# MAGIC       left join vw_worker_base b
# MAGIC         on a.initial_review_query_employee_id = b.employee_id
# MAGIC         and (
# MAGIC           (
# MAGIC             a.initial_review_query_timestamp >= b.role_start_bound_timestamp
# MAGIC             and b.role_end_bound_timestamp is null
# MAGIC           )
# MAGIC           or (
# MAGIC             a.initial_review_query_timestamp >= b.role_start_bound_timestamp
# MAGIC             and b.is_latest = true
# MAGIC           )
# MAGIC           or (
# MAGIC             a.initial_review_query_timestamp between
# MAGIC               b.role_start_bound_timestamp
# MAGIC             and
# MAGIC               b.role_end_bound_timestamp
# MAGIC           )
# MAGIC         )
# MAGIC   where
# MAGIC     a.initial_review_query_employee_id is not null
# MAGIC   group by
# MAGIC     all
# MAGIC   union
# MAGIC   select
# MAGIC     a.review_query_gid transaction_id,
# MAGIC     b.role_start_bound_timestamp,
# MAGIC     a.latest_review_query_timestamp transaction_timestamp,
# MAGIC     b.role_end_bound_timestamp,
# MAGIC     a.latest_review_query_employee_id employee_id,
# MAGIC     b.employee_name,
# MAGIC     max_by(b.employee_organization_code, b.role_start_bound_timestamp) employee_organization_code,
# MAGIC     'REVIEW_QUERY' transaction_type
# MAGIC   from
# MAGIC     vw_tmog_metrics_base a
# MAGIC       left join vw_worker_base b
# MAGIC         on a.latest_review_query_employee_id = b.employee_id
# MAGIC         and (
# MAGIC           (
# MAGIC             a.latest_review_query_timestamp >= b.role_start_bound_timestamp
# MAGIC             and b.role_end_bound_timestamp is null
# MAGIC           )
# MAGIC           or (
# MAGIC             a.latest_review_query_timestamp >= b.role_start_bound_timestamp
# MAGIC             and b.is_latest = true
# MAGIC           )
# MAGIC           or (
# MAGIC             a.latest_review_query_timestamp between
# MAGIC               b.role_start_bound_timestamp
# MAGIC             and
# MAGIC               b.role_end_bound_timestamp
# MAGIC           )
# MAGIC         )
# MAGIC   where
# MAGIC     a.latest_review_query_employee_id is not null
# MAGIC   group by
# MAGIC     all
# MAGIC ),
# MAGIC cte_dim_review_query_note_employees as (
# MAGIC   select
# MAGIC     a.review_query_gid transaction_id,
# MAGIC     b.role_start_bound_timestamp,
# MAGIC     a.latest_review_query_timestamp transaction_timestamp,
# MAGIC     b.role_end_bound_timestamp,
# MAGIC     a.initial_review_query_note_employee_id employee_id,
# MAGIC     b.employee_name,
# MAGIC     max_by(
# MAGIC       nvl(a.initial_review_query_employee_organization_code, b.employee_organization_code),
# MAGIC       b.role_start_bound_timestamp
# MAGIC     ) employee_organization_code,
# MAGIC     'REVIEW_QUERY_NOTE' transaction_type
# MAGIC   from
# MAGIC     vw_tmog_metrics_base a
# MAGIC       left join vw_worker_base b
# MAGIC         on a.initial_review_query_note_employee_id = b.employee_id
# MAGIC         and (
# MAGIC           (
# MAGIC             a.latest_review_query_timestamp >= b.role_start_bound_timestamp
# MAGIC             and b.role_end_bound_timestamp is null
# MAGIC           )
# MAGIC           or (
# MAGIC             a.latest_review_query_timestamp >= b.role_start_bound_timestamp
# MAGIC             and b.is_latest = true
# MAGIC           )
# MAGIC           or (
# MAGIC             a.latest_review_query_timestamp between
# MAGIC               b.role_start_bound_timestamp
# MAGIC             and
# MAGIC               b.role_end_bound_timestamp
# MAGIC           )
# MAGIC         )
# MAGIC   where
# MAGIC     a.initial_review_query_note_employee_id is not null
# MAGIC   group by
# MAGIC     all
# MAGIC   union
# MAGIC   select
# MAGIC     a.review_query_gid transaction_id,
# MAGIC     b.role_start_bound_timestamp,
# MAGIC     a.latest_review_query_timestamp transaction_timestamp,
# MAGIC     b.role_end_bound_timestamp,
# MAGIC     a.latest_review_query_note_employee_id employee_id,
# MAGIC     b.employee_name,
# MAGIC     max_by(b.employee_organization_code, b.role_start_bound_timestamp) employee_organization_code,
# MAGIC     'REVIEW_QUERY_NOTE' transaction_type
# MAGIC   from
# MAGIC     vw_tmog_metrics_base a
# MAGIC       left join vw_worker_base b
# MAGIC         on a.latest_review_query_note_employee_id = b.employee_id
# MAGIC         and (
# MAGIC           (
# MAGIC             a.latest_review_query_timestamp >= b.role_start_bound_timestamp
# MAGIC             and b.role_end_bound_timestamp is null
# MAGIC           )
# MAGIC           or (
# MAGIC             a.latest_review_query_timestamp >= b.role_start_bound_timestamp
# MAGIC             and b.is_latest = true
# MAGIC           )
# MAGIC           or (
# MAGIC             a.latest_review_query_timestamp between
# MAGIC               b.role_start_bound_timestamp
# MAGIC             and
# MAGIC               b.role_end_bound_timestamp
# MAGIC           )
# MAGIC         )
# MAGIC   where
# MAGIC     a.latest_review_query_note_employee_id is not null
# MAGIC   group by
# MAGIC     all
# MAGIC ),
# MAGIC cte_dim_review_query_appeal_employees as (
# MAGIC   select
# MAGIC     a.review_query_appeal_id transaction_id,
# MAGIC     b.role_start_bound_timestamp,
# MAGIC     a.latest_review_query_timestamp transaction_timestamp,
# MAGIC     b.role_end_bound_timestamp,
# MAGIC     a.initial_review_query_appeal_employee_id employee_id,
# MAGIC     b.employee_name,
# MAGIC     max_by(
# MAGIC       nvl(a.initial_review_query_appeal_employee_organization_code, b.employee_organization_code),
# MAGIC       b.role_start_bound_timestamp
# MAGIC     ) employee_organization_code,
# MAGIC     'REVIEW_QUERY_APPEAL' transaction_type
# MAGIC   from
# MAGIC     vw_tmog_metrics_base a
# MAGIC       left join vw_worker_base b
# MAGIC         on a.initial_review_query_appeal_employee_id = b.employee_id
# MAGIC         and (
# MAGIC           (
# MAGIC             a.latest_review_query_timestamp >= b.role_start_bound_timestamp
# MAGIC             and b.role_end_bound_timestamp is null
# MAGIC           )
# MAGIC           or (
# MAGIC             a.latest_review_query_timestamp >= b.role_start_bound_timestamp
# MAGIC             and b.is_latest = true
# MAGIC           )
# MAGIC           or (
# MAGIC             a.latest_review_query_timestamp between
# MAGIC               b.role_start_bound_timestamp
# MAGIC             and
# MAGIC               b.role_end_bound_timestamp
# MAGIC           )
# MAGIC         )
# MAGIC   where
# MAGIC     a.initial_review_query_appeal_employee_id is not null
# MAGIC   group by
# MAGIC     all
# MAGIC   union
# MAGIC   select
# MAGIC     a.review_query_appeal_id transaction_id,
# MAGIC     b.role_start_bound_timestamp,
# MAGIC     a.latest_review_query_timestamp transaction_timestamp,
# MAGIC     b.role_end_bound_timestamp,
# MAGIC     a.latest_review_query_appeal_employee_id employee_id,
# MAGIC     b.employee_name,
# MAGIC     max_by(b.employee_organization_code, b.role_start_bound_timestamp) employee_organization_code,
# MAGIC     'REVIEW_QUERY_APPEAL' transaction_type
# MAGIC   from
# MAGIC     vw_tmog_metrics_base a
# MAGIC       left join vw_worker_base b
# MAGIC         on a.latest_review_query_appeal_employee_id = b.employee_id
# MAGIC         and (
# MAGIC           (
# MAGIC             a.latest_review_query_timestamp >= b.role_start_bound_timestamp
# MAGIC             and b.role_end_bound_timestamp is null
# MAGIC           )
# MAGIC           or (
# MAGIC             a.latest_review_query_timestamp >= b.role_start_bound_timestamp
# MAGIC             and b.is_latest = true
# MAGIC           )
# MAGIC           or (
# MAGIC             a.latest_review_query_timestamp between
# MAGIC               b.role_start_bound_timestamp
# MAGIC             and
# MAGIC               b.role_end_bound_timestamp
# MAGIC           )
# MAGIC         )
# MAGIC   where
# MAGIC     a.latest_review_query_appeal_employee_id is not null
# MAGIC   group by
# MAGIC     all
# MAGIC )
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   cte_dim_review_employees
# MAGIC union
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   cte_dim_review_query_employees
# MAGIC union
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   cte_dim_review_query_note_employees
# MAGIC union
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   cte_dim_review_query_appeal_employees;

# COMMAND ----------

# DBTITLE 1,Create View: TMOG Worker Imputed (Best Estimate)
# MAGIC %sql
# MAGIC create or replace temp view vw_tmog_metrics_worker_imputed as
# MAGIC select
# MAGIC   transaction_id,
# MAGIC   transaction_type,
# MAGIC   transaction_timestamp,
# MAGIC   employee_id,
# MAGIC   case
# MAGIC     when employee_id = 'tmog' then 'AUTO'
# MAGIC     when employee_id in ('0', '-1') then 'Special ID'
# MAGIC     else
# MAGIC       nvl(
# MAGIC         employee_name,
# MAGIC         lag(employee_name)
# MAGIC           ignore nulls over (
# MAGIC             partition by employee_id, transaction_type
# MAGIC             order by transaction_timestamp
# MAGIC           )
# MAGIC       )
# MAGIC   end employee_name,
# MAGIC   case
# MAGIC     when employee_id = 'tmog' then 'Auto'
# MAGIC     when employee_id in ('0', '-1') then 'Special ID'
# MAGIC     else
# MAGIC       nvl(
# MAGIC         employee_organization_code,
# MAGIC         lag(employee_organization_code)
# MAGIC           ignore nulls over (
# MAGIC             partition by employee_id, transaction_type
# MAGIC             order by transaction_timestamp
# MAGIC           )
# MAGIC       )
# MAGIC   end employee_organization_code,
# MAGIC   case
# MAGIC     when
# MAGIC       employee_id in ('tmog', '0', '-1')
# MAGIC       or employee_name is null
# MAGIC     then
# MAGIC       true
# MAGIC     else false
# MAGIC   end is_employee_name_hardcoded,
# MAGIC   case
# MAGIC     when
# MAGIC       employee_id in ('tmog', '0', '-1')
# MAGIC       or employee_organization_code is null
# MAGIC     then
# MAGIC       true
# MAGIC     else false
# MAGIC   end is_employee_organization_hardcoded,
# MAGIC   case
# MAGIC     when role_start_bound_timestamp is null then true
# MAGIC     else false
# MAGIC   end is_employee_information_imputed_from_history
# MAGIC from
# MAGIC   vw_tmog_metrics_worker_base;

# COMMAND ----------

# DBTITLE 1,Create View: TMOG Worker
spark.sql("""
select distinct
  transaction_id,
  transaction_type,
  transaction_timestamp,
  employee_id,
  nvl(employee_name, 'Data Unavailable') employee_name,
  nvl(employee_organization_code, 'Data Unavailable') employee_organization_code,
  is_employee_name_hardcoded,
  is_employee_organization_hardcoded,
  is_employee_information_imputed_from_history,
  'TMOG_METRICS_BRONZE_LOAD' create_user,
  current_timestamp create_timestamp
from
  vw_tmog_metrics_worker_imputed
""").createOrReplaceTempView("_vw_tmog_metrics_worker_transactions")
vw_tmog_metrics_worker_transactions = spark.sql("select * from _vw_tmog_metrics_worker_transactions").dropDuplicates()
vw_tmog_metrics_worker_transactions.createOrReplaceTempView("vw_tmog_metrics_worker_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert

# COMMAND ----------

# DBTITLE 1,Insert: TMOG Metrics Worker
display(
    spark.sql(f"""
    insert overwrite 
        {reporting_catalog}.bronze.tmog_metrics_worker_transactions (
            transaction_id,
            transaction_type,
            transaction_timestamp,
            employee_id,
            employee_name,
            employee_organization_code,
            is_employee_name_hardcoded,
            is_employee_organization_hardcoded,
            is_employee_information_imputed_from_history,
            create_timestamp,
            create_user
        )
    select 
        transaction_id,
        transaction_type,
        transaction_timestamp,
        employee_id,
        employee_name,
        employee_organization_code,
        is_employee_name_hardcoded,
        is_employee_organization_hardcoded,
        is_employee_information_imputed_from_history,
        create_timestamp,
        create_user
    from 
        vw_tmog_metrics_worker_transactions
""")
)

# COMMAND ----------

# DBTITLE 1,Optimize: TMOG Metrics Worker Transactions
display(
    spark.sql(
        f"""
        optimize 
            {reporting_catalog}.bronze.tmog_metrics_worker_transactions 
        zorder by 
            (transaction_id, employee_id)
    """)
)

# COMMAND ----------

# DBTITLE 1,Create View: TMOG Metrics Base
# Note: Should rewrite the joins and pushdown to above, but for now, I think it's okay.
spark.sql(f"""
select
  distinct
  a.review_gid,
  a.initial_review_employee_id,
  nvl(
    a.initial_review_employee_organization_code,
    b.employee_organization_code
  ) initial_review_employee_organization_code,
  a.initial_review_timestamp,
  a.latest_review_employee_id,
  c.employee_organization_code latest_review_employee_organization_code,
  a.latest_review_timestamp,
  a.publication_date,
  a.serial_number,
  a.previous_bounce_number,
  a.review_status_code,
  a.review_status_title,
  a.review_status_description,
  a.review_query_gid,
  a.og_page_number,
  a.print_error_indicator,
  a.review_query_content,
  a.review_query_note_type_code,
  a.review_query_note_type,
  a.review_query_note_description,
  a.review_query_ground_id,
  a.review_query_ground_code,
  a.review_query_ground,
  a.review_query_ground_description,
  a.review_query_ground_order_number,
  a.review_query_ground_grouping_number,
  a.review_query_ground_type_code,
  a.review_query_ground_type,
  a.review_query_ground_type_description,
  a.review_query_ground_class_id,
  a.employee_review_query_id,
  a.initial_review_query_employee_id,
  nvl(
    a.initial_review_query_employee_organization_code,
    d.employee_organization_code
  ) initial_review_query_employee_organization_code,
  a.initial_review_query_timestamp,
  a.latest_review_query_employee_id,
  e.employee_organization_code latest_review_query_employee_organization_code,
  a.latest_review_query_timestamp,
  a.review_query_assignment_date,
  a.initial_employee_review_query_status_employee_id,
  a.initial_employee_review_query_status_timestamp,
  a.employee_review_query_status_code,
  a.employee_review_query_status_code_description,
  a.employee_review_query_status_reason_description,
  a.latest_employee_review_query_status_employee_id,
  a.latest_employee_review_query_status_timestamp,
  a.employee_review_query_status_reason_description,
  a.review_query_note_sequence_number,
  a.initial_review_query_note_employee_id,
  nvl(a.initial_review_query_note_employee_organization_code, f.employee_organization_code) initial_review_query_note_employee_organization_code,
  a.initial_review_query_note_timestamp,
  a.latest_review_query_note_employee_id,
  g.employee_organization_code latest_review_query_note_employee_organization_code,
  a.latest_review_query_note_timestamp,
  a.review_query_note_text,
  a.review_query_appeal_id,
  a.review_query_appeal_approval_indicator,
  a.review_query_appeal_gid,
  a.review_query_appeal_result_date,
  a.review_query_appeal_proceeding_number,
  a.review_query_appeal_decision_description,
  a.review_query_appeal_reason_description,
  a.review_query_appeal_director_email_sent_indicator,
  a.review_query_appeal_result_code,
  a.review_query_appeal_result,
  a.review_query_appeal_result_description,
  a.initial_review_query_appeal_employee_id,
  nvl(
    a.initial_review_query_appeal_employee_organization_code,
    h.employee_organization_code
  ) initial_review_query_appeal_employee_organization_code,
  a.initial_review_query_appeal_timestamp,
  a.latest_review_query_appeal_employee_id,
  i.employee_organization_code latest_review_query_appeal_employee_organization_code,
  a.latest_review_query_appeal_timestamp,
  a.review_query_appeal_status_timestamp,
  a.review_query_appeal_sequence_number,
  a.review_query_appeal_note,
  a.review_query_appeal_status_code,
  a.review_query_appeal_status,
  a.review_query_appeal_status_description,
  case
    when
      (
        a.initial_review_employee_organization_code is not null
        and a.initial_review_query_employee_organization_code is not null
        and a.initial_review_query_appeal_employee_organization_code is not null
        and a.initial_review_query_note_employee_organization_code is not null
      )
    then
      true
    else false
  end is_employee_attributes_derived_by_foreign_key,
  'TMOG_METRICS_BRONZE_LOAD' create_user,
  current_timestamp create_timestamp
from
  vw_tmog_metrics_base a
    left join {reporting_catalog}.bronze.tmog_metrics_worker_transactions b
      on a.review_gid = b.transaction_id
      and a.initial_review_employee_id = b.employee_id
      and b.transaction_type = 'REVIEW'
    left join {reporting_catalog}.bronze.tmog_metrics_worker_transactions c
      on a.review_gid = c.transaction_id
      and a.latest_review_employee_id = c.employee_id
      and c.transaction_type = 'REVIEW'
    left join {reporting_catalog}.bronze.tmog_metrics_worker_transactions d
      on a.review_query_gid = d.transaction_id
      and a.initial_review_query_employee_id = d.employee_id
      and d.transaction_type = 'REVIEW_QUERY'
    left join {reporting_catalog}.bronze.tmog_metrics_worker_transactions e
      on a.review_query_gid = e.transaction_id
      and a.latest_review_query_employee_id = e.employee_id
      and e.transaction_type = 'REVIEW_QUERY'
    left join {reporting_catalog}.bronze.tmog_metrics_worker_transactions f
      on a.review_query_gid = f.transaction_id
      and a.initial_review_query_note_employee_id = f.employee_id
      and f.transaction_type = 'REVIEW_QUERY_NOTE'
    left join {reporting_catalog}.bronze.tmog_metrics_worker_transactions g
      on a.review_query_gid = g.transaction_id
      and a.latest_review_query_note_employee_id = g.employee_id
      and g.transaction_type = 'REVIEW_QUERY_NOTE'
    left join {reporting_catalog}.bronze.tmog_metrics_worker_transactions h
      on a.review_query_appeal_gid = h.transaction_id
      and a.initial_review_query_appeal_employee_id = h.employee_id
      and h.transaction_type = 'REVIEW_QUERY_APPEAL'
    left join {reporting_catalog}.bronze.tmog_metrics_worker_transactions i
      on a.review_query_appeal_gid = i.transaction_id
      and a.latest_review_query_appeal_employee_id = i.employee_id
      and i.transaction_type = 'REVIEW_QUERY_APPEAL'
""").createOrReplaceTempView("_vw_tmog_metrics_transactions")
vw_tmog_metrics_transactions = spark.sql("select * from _vw_tmog_metrics_transactions").dropDuplicates()
vw_tmog_metrics_transactions.createOrReplaceTempView("vw_tmog_metrics_transactions")

# COMMAND ----------

# DBTITLE 1,Insert: TMOG Metrics Transactions
display(
    spark.sql(f"""
        insert overwrite 
            {reporting_catalog}.bronze.tmog_metrics_transactions (
                review_gid,
                initial_review_employee_id,
                initial_review_employee_organization_code,
                initial_review_timestamp,
                latest_review_employee_id,
                latest_review_employee_organization_code,
                latest_review_timestamp,
                publication_date,
                serial_number,
                previous_bounce_number,
                review_status_code,
                review_status_title,
                review_status_description,
                review_query_gid,
                og_page_number,
                print_error_indicator,
                review_query_content,
                review_query_note_type_code,
                review_query_note_type,
                review_query_note_description,
                review_query_ground_id,
                review_query_ground_code,
                review_query_ground,
                review_query_ground_description,
                review_query_ground_order_number,
                review_query_ground_grouping_number,
                review_query_ground_type_code,
                review_query_ground_type,
                review_query_ground_type_description,
                review_query_ground_class_id,
                employee_review_query_id,
                initial_review_query_employee_id,
                initial_review_query_employee_organization_code,
                initial_review_query_timestamp,
                latest_review_query_employee_id,
                latest_review_query_employee_organization_code,
                latest_review_query_timestamp,
                review_query_assignment_date,
                initial_employee_review_query_status_employee_id,
                initial_employee_review_query_status_timestamp,
                employee_review_query_status_code,
                employee_review_query_status_code_description,
                employee_review_query_status_reason_description,
                latest_employee_review_query_status_timestamp,
                latest_employee_review_query_status_employee_id,
                review_query_note_sequence_number,
                initial_review_query_note_employee_id,
                initial_review_query_note_employee_organization_code,
                initial_review_query_note_timestamp,
                latest_review_query_note_employee_id,
                latest_review_query_note_employee_organization_code,
                latest_review_query_note_timestamp,
                review_query_note_text,
                review_query_appeal_id,
                review_query_appeal_approval_indicator,
                review_query_appeal_gid,
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
                is_employee_attributes_derived_by_foreign_key,
                create_user,
                create_timestamp
            )
        select 
            review_gid,
            initial_review_employee_id,
            initial_review_employee_organization_code,
            initial_review_timestamp,
            latest_review_employee_id,
            latest_review_employee_organization_code,
            latest_review_timestamp,
            publication_date,
            serial_number,
            previous_bounce_number,
            review_status_code,
            review_status_title,
            review_status_description,
            review_query_gid,
            og_page_number,
            print_error_indicator,
            review_query_content,
            review_query_note_type_code,
            review_query_note_type,
            review_query_note_description,
            review_query_ground_id,
            review_query_ground_code,
            review_query_ground,
            review_query_ground_description,
            review_query_ground_order_number,
            review_query_ground_grouping_number,
            review_query_ground_type_code,
            review_query_ground_type,
            review_query_ground_type_description,
            review_query_ground_class_id,
            employee_review_query_id,
            initial_review_query_employee_id,
            initial_review_query_employee_organization_code,
            initial_review_query_timestamp,
            latest_review_query_employee_id,
            latest_review_query_employee_organization_code,
            latest_review_query_timestamp,
            review_query_assignment_date,
            initial_employee_review_query_status_employee_id,
            initial_employee_review_query_status_timestamp,
            employee_review_query_status_code,
            employee_review_query_status_code_description,
            employee_review_query_status_reason_description,
            latest_employee_review_query_status_timestamp,
            latest_employee_review_query_status_employee_id,
            review_query_note_sequence_number,
            initial_review_query_note_employee_id,
            initial_review_query_note_employee_organization_code,
            initial_review_query_note_timestamp,
            latest_review_query_note_employee_id,
            latest_review_query_note_employee_organization_code,
            latest_review_query_note_timestamp,
            review_query_note_text,
            review_query_appeal_id,
            review_query_appeal_approval_indicator,
            review_query_appeal_gid,
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
            is_employee_attributes_derived_by_foreign_key,
            create_user,
            create_timestamp
        from 
            vw_tmog_metrics_transactions
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teardown

# COMMAND ----------

# DBTITLE 1,End Job
count_tmog_metrics_worker_transactions: int = spark.sql(
    f"select count(*) as cnt from {reporting_catalog}.bronze.tmog_metrics_worker_transactions"
).collect()[0]["cnt"]
count_tmog_metrics_transactions: int = spark.sql(
    f"select count(*) as cnt from {reporting_catalog}.bronze.tmog_metrics_transactions"
).collect()[0]["cnt"]
table_counts: list[int] = [
    count_tmog_metrics_transactions,
    count_tmog_metrics_worker_transactions,
]
num_empty_tables: int = count_empty(table_counts)

if not num_empty_tables:
    end_job_cntl(
        f"{reporting_catalog}.silver",
        job_name,
        job_start_ts,
        "completed",
        count_tmog_metrics_worker_transactions + count_tmog_metrics_transactions,
        "Job completed successfully",
    )
    dbutils.notebook.exit(
        f"""
        Job completed with:
        - [{count_tmog_metrics_worker_transactions}] records for tmog_metrics_worker_transactions
        - [{count_tmog_metrics_transactions}] records for tmog_metrics_transactions
        """
    )
else:
    raise ValueError(
        f"{num_empty_tables} tables loaded 0 records. Tables must have at least 1 record to move on to next task."
    )