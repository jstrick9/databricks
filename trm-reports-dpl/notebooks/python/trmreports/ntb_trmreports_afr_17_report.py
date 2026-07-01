# Databricks notebook source
# MAGIC %md
# MAGIC # AFR17 Report
# MAGIC ## Overview
# MAGIC This ETL generates the report for AFR17, which lists aggregate status counts for cases with pending status. The primary dependency for this result set is OS34, which must be run as a prerequisite to this report.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# DBTITLE 1,Environment
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=},{dbx_env=}")

# COMMAND ----------

# DBTITLE 1,Import Shared Functions
# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,Configs
common_configs = read_yaml(config_file)
reporting_catalog = common_configs["schema"]["trgt_catalog"]
print(reporting_catalog)

# COMMAND ----------

# DBTITLE 1,Begin Job
job_name = "ntb_trm_reports_afr_17_report"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Views

# COMMAND ----------

# DBTITLE 1,View: vw_applications (Staging)
STATUS_GROUPS = {
    13: ('Amended, Awaiting Action by Examiner (Initial)',  [616,640,641,643,644,645,646,647,648,649,661,663,665,666]),
    14: ('Awaiting First Action by Examiner',               [638]),
     8: ('In Preexamination Processing',                    [630,631]),
    21: ('Amended, Awaiting Action by Examiner (Second)',   [753,756,757,806,807,808,809,810,811,812,813,814,815,816,817]),
    19: ('Administrative Processing of Statements of Use', [744,745,746,747]),
    20: ('Undergoing Second Examination',                   [748]),
    25: ('In Postexamination Processing',                   [680,681,686,818,819]),
    23: ('Other Pending Applications',                      [760,763,771,774,794,801,802,650,651,652,653,654]),
}

SUBSET_GROUPS = {
    12: ('Applications Under Initial Examination',  [13, 14]),
    18: ('Applications Under Second Examination',   [19, 20, 21]),
}

def _codes(stage_num):
    return STATUS_GROUPS[stage_num][1]

def _in_list(codes):
    return '(' + ','.join(str(c) for c in codes) + ')'

sub_label_cases = '\n      '.join(
    f"when status_code in {_in_list(label_codes)} then '{label}'"
    for stage_num, (label, label_codes) in STATUS_GROUPS.items()
)
sub_stage_cases = '\n      '.join(
    f"when status_code in {_in_list(label_codes)} then {stage_num}"
    for stage_num, (label, label_codes) in STATUS_GROUPS.items()
)
all_leaf_codes = sorted({c for _, codes in STATUS_GROUPS.values() for c in codes})

subset_label_cases = '\n      '.join(
    f"when status_code in {_in_list([c for s in leaf_stages for c in _codes(s)])} then '{label}'"
    for stage_num, (label, leaf_stages) in SUBSET_GROUPS.items()
)
subset_stage_cases = '\n      '.join(
    f"when status_code in {_in_list([c for s in leaf_stages for c in _codes(s)])} then {stage_num}"
    for stage_num, (label, leaf_stages) in SUBSET_GROUPS.items()
)
all_subset_codes = sorted({c for _, leaf_stages in SUBSET_GROUPS.values() for s in leaf_stages for c in _codes(s)})

spark.sql(f"""
  select
    serial_num,
    status_code,
    is_counted_noa,
    is_counted_application,
    latest,
    max(num_active_classes) as num_active_classes
  from
    {reporting_catalog}.silver.os34_report_status_detail
  where
    is_counted_application = true
    and latest = true
  group by
    serial_num,
    status_code,
    is_counted_noa,
    is_counted_application,
    latest
""").createOrReplaceTempView("deduped")

spark.sql(f"""
  with under_initial_sub_subset as (
  select
    case
      {sub_label_cases}
    end stage_of_processing,
    case
      {sub_stage_cases}
    end stage,
    count(distinct serial_num) application_files,
    sum(num_active_classes) classes
  from
    deduped
  where
    status_code in {_in_list(all_leaf_codes)}
  group by
    all
),
under_initial_subset as (
  select
    case
      {subset_label_cases}
    end stage_of_processing,
    case
      {subset_stage_cases}
    end stage,
    count(distinct serial_num) application_files,
    sum(num_active_classes) classes
  from
    deduped
  where
    status_code in {_in_list(all_subset_codes)}
  group by
    all
),
itu as (
  select
    'Intent-to-Use Applications Pending Use' stage_of_processing,
    16 stage,
    count(distinct serial_num) application_files,
    sum(num_active_classes) classes
  from
    deduped
  where
    is_counted_noa = true
),
-- Stage 10 = sum of children: stage 12 + 16 + 18 + 23
under_initial as (
  select
    'Under Examination, Total' stage_of_processing,
    10 stage,
    sum(application_files) application_files,
    sum(classes) classes
  from (
    select * from itu
    union all
    select * from under_initial_subset
    union all
    select * from under_initial_sub_subset where stage = 23
  )
),
-- Stage 6 = sum of children: stage 8 + 10 + 25
pending_total as (
  select
    'Pending Applications, Total' stage_of_processing,
    6 stage,
    sum(application_files) application_files,
    sum(classes) classes
  from (
    select * from under_initial                                    -- stage 10
    union all
    select * from under_initial_sub_subset where stage = 8        -- stage 8 (In Preexamination Processing)
    union all
    select * from under_initial_sub_subset where stage = 25       -- stage 25 (In Postexamination Processing)
  )
),
output as (
  select * from under_initial
  union all
  select * from under_initial_subset
  union all
  select * from under_initial_sub_subset
  union all
  select * from pending_total
  union all
  select * from itu
),
second_exam_values as (
  select
    application_files as second_exam_app_files,
    classes           as second_exam_classes
  from output
  where stage_of_processing = 'Applications Under Second Examination'
),
final_output as (
  select
    o.*,
    case
      when o.stage_of_processing in (
        'Pending Applications, Total',
        'Under Examination, Total',
        'Intent-to-Use Applications Pending Use'
      ) then o.application_files - s.second_exam_app_files
      else o.application_files
    end as adjusted_application_files,
    case
      when o.stage_of_processing in (
        'Pending Applications, Total',
        'Under Examination, Total',
        'Intent-to-Use Applications Pending Use'
      ) then o.classes - s.second_exam_classes
      else o.classes
    end as adjusted_classes
  from output o
  cross join second_exam_values s
)
select * from final_output
""").createOrReplaceTempView("vw_applications")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge

# COMMAND ----------

# DBTITLE 1,Merge: Staging (Upsert)
spark.sql(f"""
    merge into
    {reporting_catalog}.gold.afr_pending_applications_quarterly as target
    using (
        select
            coalesce(src.load_date,  tgt.load_date)  as load_date,
            coalesce(src.stage,      tgt.stage)       as stage,
            src.stage_of_processing,
            src.application_files,
            src.classes,
            src.adjusted_application_files,
            src.adjusted_classes,
            src.update_ts,
            src.update_user_id,
            case when src.stage is null then 'SOFT_DELETE' else 'UPSERT' end as merge_action
        from (
            select
                current_date()            as load_date,
                stage,
                stage_of_processing,
                application_files,
                classes,
                adjusted_application_files,
                adjusted_classes,
                current_timestamp()                as update_ts,
                'AFR17_QUARTERLY_REPORT_ETL'       as update_user_id
            from vw_applications
        ) as src
        full outer join (
            select stage, load_date
            from {reporting_catalog}.gold.afr_pending_applications_quarterly
            where load_date = current_date()
        ) as tgt
            on src.stage = tgt.stage
           and src.load_date = tgt.load_date
    ) as source
    on
        target.stage     = source.stage
        and target.load_date = source.load_date
    when matched and source.merge_action = 'SOFT_DELETE'
    then update set
        target.is_active     = false,
        target.update_ts     = current_timestamp(),
        target.update_user_id = 'AFR17_QUARTERLY_REPORT_ETL'
    when matched and source.merge_action = 'UPSERT'
    and (
        target.stage_of_processing       is distinct from source.stage_of_processing
        or target.application_files      is distinct from source.application_files
        or target.classes                is distinct from source.classes
        or target.adjusted_application_files is distinct from source.adjusted_application_files
        or target.adjusted_classes       is distinct from source.adjusted_classes
        or target.is_active = false
    )
    then update set
        target.stage_of_processing       = source.stage_of_processing,
        target.application_files         = source.application_files,
        target.classes                   = source.classes,
        target.adjusted_application_files = source.adjusted_application_files,
        target.adjusted_classes          = source.adjusted_classes,
        target.is_active                 = true,
        target.update_ts                 = source.update_ts,
        target.update_user_id            = source.update_user_id
    when not matched and source.merge_action = 'UPSERT'
    then insert (
        load_date,
        stage,
        stage_of_processing,
        application_files,
        classes,
        adjusted_application_files,
        adjusted_classes,
        is_active,
        create_ts,
        create_user_id,
        update_ts,
        update_user_id
    )
    values (
        source.load_date,
        source.stage,
        source.stage_of_processing,
        source.application_files,
        source.classes,
        source.adjusted_application_files,
        source.adjusted_classes,
        true,
        current_timestamp(),
        'AFR17_QUARTERLY_REPORT_ETL',
        source.update_ts,
        source.update_user_id
    )
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teardown

# COMMAND ----------

# DBTITLE 1,End Job
end_job_cntl(
    f"{reporting_catalog}.silver",
    job_name,
    job_start_ts,
    "completed",
    0,
    "job completed successfully",
)
dbutils.notebook.exit(f"Job completed successfully.")