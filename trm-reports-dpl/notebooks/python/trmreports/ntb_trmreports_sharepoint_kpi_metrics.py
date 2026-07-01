# Databricks notebook source
# DBTITLE 1,Environment Settings
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=},{dbx_env=}")

# COMMAND ----------

# DBTITLE 1,Import Shared Functions
# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,Set Configuration
common_configs = read_yaml(config_file)
reporting_catalog = common_configs["schema"]["trgt_catalog"]

# COMMAND ----------

# DBTITLE 1,Begin Job
job_name = "ntb_trmreports_sharepoint_kpi_metrics"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# DBTITLE 1,Main View
spark.sql(
    f"""
create or replace temp view records as
with ap as (
  select
    sum(active_classes_firstaction * first_action_pendency_ph)
    / sum(active_classes_firstaction) `1AP`
  from
    {reporting_catalog}.gold.pendency_dashboard
  where
    on_hold = 0
    and fa_pendency_fy = (
      select
        max(fa_pendency_fy)
      from
        {reporting_catalog}.gold.pendency_dashboard
    )
),
dp as (
  select
    sum(active_classes_disposal * disposal_pendency) / sum(active_classes_disposal) `DP`
  from
    {reporting_catalog}.gold.pendency_dashboard
  where
    on_hold = 0
    and total_pendency_fy = (
      select
        max(total_pendency_fy)
      from
        {reporting_catalog}.gold.pendency_dashboard
    )
    and pendency_category = 'No Suspension or Opposition'
),
pendency as (
  select
    *
  from
    ap join dp
),
base_quality as (
  select
    review_type,
    law_office,
    iff(qualitymetricdeficientindicator = true, 1, 0) qualitymetricdeficientindicator,
    iff(overallexcellentindicator = true, 1, 0) overallexcellentindicator
  from
    {reporting_catalog}.gold.quality_dashboard
  where
    year(lastreviewdatetime + interval 3 months) = (
      select
        max(year(lastreviewdatetime + interval 3 months))
      from
        {reporting_catalog}.gold.quality_dashboard
    )
),
first_action as (
  select
    (count(law_office) - sum(qualitymetricdeficientindicator)) / count(law_office) `1AC`,
    (sum(overallexcellentindicator)) / count(law_office) `EOA`
  from
    base_quality
  where
    review_type = 'First Action'
),
final_action as (
  select
    ((count(law_office) - sum(qualitymetricdeficientindicator)) / count(law_office)) * 0.109 output
  from
    base_quality
  where
    review_type = 'Final Action'
),
pub as (
  select
    ((count(law_office) - sum(qualitymetricdeficientindicator)) / count(law_office)) * 0.891 output
  from
    base_quality
  where
    review_type = 'PUB'
),
fc as (
  select
    p.output + fa.output `FC`
  from
    pub p join final_action fa
),
quality as (
  select
    *
  from
    fc join first_action
),
current_and_previous_fy as (
  select
    max(filing_fy) current_filing_fy,
    max(filing_fy) - 1 previous_filing_fy
  from
    {reporting_catalog}.gold.filings_dashboard b
),
base_filings as (
  select
    a.ser_num,
    a.pendency_cal_start_dt,
    right(a.pendency_cal_start_dt, 5) date_join,
    a.filing_fy,
    a.fixed_count
  from
    {reporting_catalog}.gold.filings_dashboard a
  where
    a.top_2_years = true
),
current_fy_aggregates as (
  select
    a.filing_fy,
    sum(fixed_count) sum_fixed_count_cfy,
    min(pendency_cal_start_dt) min_pendency_cal_start_dt_cfy,
    max(pendency_cal_start_dt) max_pendency_cal_start_dt_cfy
  from
    base_filings a
      inner join current_and_previous_fy b
        on a.filing_fy = b.current_filing_fy
  group by
    all
),
previous_fy_aggregates as (
  select
    a.filing_fy,
    sum(fixed_count) sum_fixed_count_pfy,
    min(pendency_cal_start_dt) min_pendency_cal_start_dt_pfy,
    max(pendency_cal_start_dt) max_pendency_cal_start_dt_pfy
  from
    base_filings a
      inner join current_and_previous_fy b
        on a.filing_fy = b.previous_filing_fy
  group by
    all
),
filings as (
  select
    round(((sum_fixed_count_cfy - sum_fixed_count_pfy) / sum_fixed_count_pfy), 3) `Filings Gr`,
    sum_fixed_count_cfy `Filings`
  from
    (
      select
        *
      from
        current_fy_aggregates
    )
      join (
        select
          *
        from
          previous_fy_aggregates
      )
),
unexamined as (
  select
    unexamined_classes `Unex`
  from
    {reporting_catalog}.gold.inventory_unexamined_hstry
  where
    unexamined_date = (
      select
        max(unexamined_date)
      from
        {reporting_catalog}.gold.inventory_unexamined_hstry
    )
)
select distinct
  round(`1AP`, 3) `1ap`,
  round(`DP`, 3) `dp`,
  round(`1AC`, 3) `1ac`,
  round(`EOA`, 3) `eoa`,
  round(`FC`, 3) `fc`,
  `Filings` `filings`,
  `Filings Gr` `filings_gr`,
  `Unex` `unex`,
  current_date `as_of`,
  current_user `modified_by`,
  current_date `modified`
from
  quality q join pendency p join filings f join unexamined u
"""
)

# COMMAND ----------

# DBTITLE 1,Show Sample
# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   records

# COMMAND ----------

# DBTITLE 1,Set Previous Latest to False
display(
    spark.sql(
        f"""
        update {reporting_catalog}.gold.sharepoint_kpi_metrics
        set
            latest = false
        where
            latest = true
    """
    )
)

# COMMAND ----------

# DBTITLE 1,Insert Records
display(
    spark.sql(
        f"""
    insert into {reporting_catalog}.gold.sharepoint_kpi_metrics (
        `1ap`,
        `dp`,
        `1ac`,
        `eoa`,
        `fc`,
        `filings`,
        `filings_gr`,
        `unex`,
        `as_of`,
        `modified_by`,
        `modified`,
        `latest`
    )
    select
        `1ap`,
        `dp`,
        `1ac`,
        `eoa`,
        `fc`,
        `filings`,
        `filings_gr`,
        `unex`,
        `as_of`,
        `modified_by`,
        `modified`,
        true as latest
    from
        records
    """
    )
)

# COMMAND ----------

# DBTITLE 1,Show Inserted
display(
    spark.sql(
        f"select * from {reporting_catalog}.gold.sharepoint_kpi_metrics order by `id` desc"
    )
)

# COMMAND ----------

# DBTITLE 1,End Job
end_job_cntl(
    f"{reporting_catalog}.silver",
    job_name,
    job_start_ts,
    "completed",
    1,
    "job completed successfully",
)
dbutils.notebook.exit(f"Job completed with 1 record.")