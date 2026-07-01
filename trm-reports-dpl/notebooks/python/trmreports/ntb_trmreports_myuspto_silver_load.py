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
tmngpdb_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
jbteasps_catalog = common_configs["schema"]["trm_jbteasps_src_catalog"]
tm_practitioner_catalog = common_configs["schema"]["tm_practitioner_catalog"]

spark.conf.set("config.tmngpdb_catalog", tmngpdb_catalog)
spark.conf.set("config.reporting_catalog", reporting_catalog)
spark.conf.set("config.jbteasps_catalog", jbteasps_catalog)
spark.conf.set("config.tm_practitioner_catalog", tm_practitioner_catalog)

# primary_email, cc_email = common_configs["alerting"]["myuspto_monitor"]["email"], common_configs["alerting"]["myuspto_monitor"]["cc"]
# print(reporting_catalog, tmngpdb_catalog, jbteasps_catalog, trm_scope, primary_email, cc_email)

# COMMAND ----------

# DBTITLE 1,Begin Job
job_name = "ntb_trmreports_myuspto_monitor"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# DBTITLE 1,Patrons
# MAGIC %sql
# MAGIC select
# MAGIC   pi.patron_id as patron_id,
# MAGIC   pi.user_acct_nm as patron_account_name,
# MAGIC   pi.given_nm || ' ' || pi.family_nm as patron_name,
# MAGIC   pi.electronic_addr_locator_tx as patron_email,
# MAGIC   pi.src_create_ts as account_creation_date,
# MAGIC   -1 as create_user,
# MAGIC   current_date as create_dt
# MAGIC from
# MAGIC   ${tm_practitioner_catalog}.silver.patron_id pi
# MAGIC where
# MAGIC   pi.acct_type_cd = 'X'
# MAGIC qualify
# MAGIC   row_number() over (partition by pi.patron_id order by pi.dim_patron_id desc) = 1

# COMMAND ----------

# DBTITLE 1,Filings
# MAGIC %sql
# MAGIC select
# MAGIC   al.serial_no as serial_number,
# MAGIC   upper(al.cfk_patron_id) as patron_id,
# MAGIC   al.create_ts as submission_time_ts,
# MAGIC   al.fk_form_cd as form_type,
# MAGIC   -1 as create_user,
# MAGIC   current_date as create_dt
# MAGIC from
# MAGIC   ${jbteasps_catalog}.bronze.audit_log al
# MAGIC where
# MAGIC   al.cfk_patron_id ilike '%-%-%-%-%'
# MAGIC   and al.fk_transaction_type_cd = 'Submission'
# MAGIC   and al.serial_no is not null
# MAGIC   and al.create_ts >= (current_date - interval 90 days)

# COMMAND ----------

# MAGIC %sql
# MAGIC with sponsors as (
# MAGIC   select
# MAGIC     cfk_sponsorer_id patron_id,
# MAGIC     nvl(count(distinct cfk_sponsoree_id), 0) as number_distinct_sponsored
# MAGIC   from
# MAGIC     ${tm_practitioner_catalog}.silver.patron_sponsorship
# MAGIC   group by
# MAGIC     patron_id
# MAGIC ),
# MAGIC sponsored as (
# MAGIC   select distinct
# MAGIC     cfk_sponsoree_id patron_id,
# MAGIC     nvl(count(distinct cfk_sponsorer_id), 0) as number_distinct_sponsors
# MAGIC   from
# MAGIC     ${tm_practitioner_catalog}.silver.patron_sponsorship
# MAGIC   group by
# MAGIC     patron_id
# MAGIC )
# MAGIC select distinct
# MAGIC   pi.patron_id,
# MAGIC   case
# MAGIC     when
# MAGIC       psr.patron_id is not null
# MAGIC       and psrd.patron_id is not null
# MAGIC     then
# MAGIC       'Has Both Sponsored and Been Sponsored'
# MAGIC     when
# MAGIC       psr.patron_id is not null
# MAGIC       and psrd.patron_id is null
# MAGIC     then
# MAGIC       'Has Sponsored'
# MAGIC     when
# MAGIC       psr.patron_id is null
# MAGIC       and psrd.patron_id is not null
# MAGIC     then
# MAGIC       'Has Been Sponsored'
# MAGIC     else 'Has Never Sponsored Nor Been Sponsored'
# MAGIC   end as sponsorship_status,
# MAGIC   nvl(psr.number_distinct_sponsored, 0) number_distinct_sponsored,
# MAGIC   nvl(psrd.number_distinct_sponsors, 0) number_distinct_sponsors
# MAGIC from
# MAGIC   ${tm_practitioner_catalog}.silver.patron_id pi
# MAGIC     left join sponsors psr
# MAGIC       on pi.patron_id = psr.patron_id
# MAGIC     left join sponsored psrd
# MAGIC       on pi.patron_id = psrd.patron_id

# COMMAND ----------

# DBTITLE 1,Logic
with patron_information as (
  select
    lower(pi.patron_id) as patron_id,
    pi.user_acct_nm as patron_account_name,
    pi.given_nm || ' ' || pi.family_nm as patron_name,
    pi.electronic_addr_locator_tx as patron_email,
    pi.src_create_ts as account_creation_date,
    iff(rand() +.3 <.4, 'Y', 'N') as alert
  from
    tm_practitioner.silver.patron_id pi
  where
    pi.acct_type_cd = 'X' qualify row_number() over (
      partition by pi.patron_id
      order by
        pi.bgn_dt desc
    ) = 1
),
base as (
  select
    al.serial_no as serial_number,
    1 as number_of_forms,
    upper(al.cfk_patron_id) as patron_id,
    pi.patron_account_name,
    pi.patron_name,
    pi.patron_email,
    pi.account_creation_date,
    pi.alert,
    al.create_ts as submission_time_ts,
    hour(al.create_ts) as hour_submission_time_ts,
    al.fk_form_cd as form_type,
    size(collect_set(al.signatory_nm) over (partition by pi.patron_id)) as num_distinct_signatory_names,
    case
      when al.create_ts = max(al.create_ts) over (partition by al.cfk_patron_id) then 1
      else 0
    end as distinct_patron_flg
  from
    trm_jbteasps_dev.bronze.audit_log al
    inner join patron_information pi on al.cfk_patron_id = pi.patron_id
  where
    al.cfk_patron_id ilike '%-%-%-%-%'
    and al.fk_transaction_type_cd = 'Submission'
    and al.serial_no is not null
    and al.fk_form_cd != 'WOA'
),
aggregates as (
  select
    distinct patron_id,
    patron_name,
    patron_account_name,
    patron_email,
    account_creation_date,
    alert,
    sum(
      case
        when submission_time_ts >= (current_date - interval 90 days) then 1
        else 0
      end
    ) over (partition by patron_id) as submissions_in_ninety_days,
    distinct_patron_flg,
    submission_time_ts,
    hour_submission_time_ts,
    num_distinct_signatory_names,
    form_type,
    serial_number,
    sum(1) over (partition by patron_id, date(submission_time_ts)) as total_forms_in_day,
    1 < sum(1) over (partition by patron_id, date(submission_time_ts)) as over_threshold,
    date_diff(
      HOUR,
      min(submission_time_ts) over (partition by patron_id, date(submission_time_ts)),
      max(submission_time_ts) over (partition by patron_id, date(submission_time_ts))
    ) as hours_between_first_and_last_submission,
    min(submission_time_ts) over (
      partition by patron_id,
      date(submission_time_ts)
    ) as time_of_first_transaction_in_day,
    max(submission_time_ts) over (
      partition by patron_id,
      date(submission_time_ts)
    ) as time_of_last_transaction_in_day,
    date_add(date(submission_time_ts), 90) as age_off_date,
    case
      when date_add(date(submission_time_ts), 90) >= current_date then true
      else false
    end as watch,
    date(submission_time_ts) = max(date(submission_time_ts)) over (partition by patron_id) as latest_session,
    date(submission_time_ts) as day,
    date(submission_time_ts) = current_date as flagged
  from
    base
),
sponsors as (
  select
    cfk_sponsorer_id patron_id,
    nvl(count(distinct cfk_sponsoree_id), 0) as number_distinct_sponsored
  from
    tm_practitioner_dev.silver.patron_sponsorship
  group by
    patron_id
),
sponsored as (
  select
    distinct cfk_sponsoree_id patron_id,
    nvl(count(distinct cfk_sponsorer_id), 0) as number_distinct_sponsors
  from
    tm_practitioner_dev.silver.patron_sponsorship
  group by
    patron_id
)
select
  a.*,
  nvl(number_distinct_sponsored, 0) number_distinct_sponsored,
  nvl(number_distinct_sponsors, 0) number_distinct_sponsors,
  nvl(prt.selected_role_nm, 'Undefined') as role_type,
  case
    when psr.patron_id is not null
    and psrd.patron_id is not null then 'Has Both Sponsored and Been Sponsored'
    when psr.patron_id is not null
    and psrd.patron_id is null then 'Has Sponsored'
    when psr.patron_id is null
    and psrd.patron_id is not null then 'Has Been Sponsored'
    else 'Has Never Sponsored Nor Been Sponsored'
  end as sponsorship_status
from
  aggregates a
  left join tm_practitioner_dev.silver.patron_role_type prt on a.patron_id = prt.cfk_patron_id
  left join sponsors psr on a.patron_id = psr.patron_id
  left join sponsored psrd on a.patron_id = psrd.patron_id
where
  total_forms_in_day > 1
  and over_threshold = true -- and watch = true
order by
  patron_id,
  submission_time_ts;

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
dbutils.notebook.exit(f"Job completed with {email_output.count()} records.")
