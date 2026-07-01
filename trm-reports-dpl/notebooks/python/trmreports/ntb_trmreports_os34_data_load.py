# Databricks notebook source
# MAGIC %md
# MAGIC #OS34 Report
# MAGIC ##Overview
# MAGIC This ETL generates the load tables for OS34, which is responsible for generating two sets of counts:
# MAGIC - Statuses belonging to two groups: Abandonments and Non-Abandonments
# MAGIC - Pending case and class categories: ITU, NOA, Post-NOA, USE, and Totals
# MAGIC ##Notes
# MAGIC The tables this workflow generates use two imporant flags: `is_static` and `latest`. 
# MAGIC
# MAGIC `latest` identifies the freshest successful load in the workflow. `is_static` identifies end-of-the-month views which end users use to identify counts for cases and classes that should be recorded. These serve as "hold" numbers for downstream reporting. `is_static` is true when the data load executes on the first day of the month, and is false in any other case. This means that if the downstream report is viewed, for example, October 5th, the numbers generated for the report will be as of October 1st. 
# MAGIC
# MAGIC This ETL also generates the `RV24` Dashboard data, containing a case level view of pending cases recognized as "deferred revenue" (being a subset that are currently in 630, 631, and 638 status).  Importantly, this is insert-overwritten the first day of the month, as opposed to the sibling `totals` and `status codes` tables for OS34, which are loaded daily. 
# MAGIC
# MAGIC Finally, a separate FYTD abandonments table is generated. This table is very similar to the `status codes` table, but only contains a subset of the total dataset, per fiscal year logic. Taken from the lookback function logic:
# MAGIC
# MAGIC > For every month except October, a range will be used from the beginning of the
# MAGIC > fiscal year to the current date of the load; however, for October, the generated range
# MAGIC > will hold for th previous fiscal year.
# MAGIC > 
# MAGIC > A few examples:
# MAGIC > - For October 2025, the load will include status changes from October 1, 2024 up
# MAGIC > to (not including) October 1, 2025.
# MAGIC > - For November 2025, the load will include status changes from include October 1, 2025
# MAGIC > up to (not including) November 1, 2025.
# MAGIC > - For December 2025, the load will include status changes from include October 1, 2025
# MAGIC > up to (not including) December 1, 2025.
# MAGIC > - For April 2025, the load will include status changes from include October 1, 2025
# MAGIC > up to (not including) April 1, 2025.

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
tmngpdb_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
print(reporting_catalog, tmngpdb_catalog)

# COMMAND ----------

# DBTITLE 1,Begin Job
job_name = "ntb_os34_data_load"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# DBTITLE 1,Functions
def parse_codes(filter_codes: list[str]) -> str:
    """
    Helper to parse an iter of strings and wrap
    it in parentheses.

    parse_codes(["1", "2", "3"]) -> (1, 2, 3)
    """
    pre_string: str = ", ".join(filter_codes)
    return f"({pre_string})"

def generate_lookback_predicate(
    date_column: str = "`action_date`", custom_date: datetime.datetime = None
) -> str:
    """
    Helper function to generate a date range for fiscal-year-to-date.

    For every month except October, a range will be used from the beginning of the
    fiscal year to the current date of the load; however, for October, the generated range
    will hold for th previous fiscal year.

    A few examples:
    - For October 2025, the load will include status changes from October 1, 2024 up
    to (not including) October 1, 2025.
    - For November 2025, the load will include status changes from include October 1, 2025
    up to (not including) November 1, 2025.
    - For December 2025, the load will include status changes from include October 1, 2025
    up to (not including) December 1, 2025.
    - For April 2025, the load will include status changes from include October 1, 2025
    up to (not including) April 1, 2025.
    """
    if not custom_date:
        current_date: datetime.datetime = datetime.datetime.today()
    else:
        current_date: datetime.datetime = custom_date
    beginning_of_month = current_date.replace(day=1)
    beginning_of_month_bound: str = beginning_of_month.strftime("%Y-%m-%d")
    month: int = current_date.month
    year: int = current_date.year
    if month >= 10:
        if month == 10:
            print(
                f"The range generated will be from {year-1}-10-01 to {year}-09-30, inclusive:"
            )
            range_predicate: str = (
                f"{date_column} >= '{year-1}-10-01' and {date_column} < '{year}-10-01'"
            )
        else:
            print(
                f"The range generated will be from {year}-10-01 to {beginning_of_month_bound}, exclusive:"
            )
            range_predicate: str = (
                f"{date_column} >= '{year}-10-01' and {date_column} < '{beginning_of_month_bound}'"
            ) 
    else:
        print(
            f"The range generated will be from {year-1}-10-01 to {year}-{month}-01, exclusive:"
        )
        range_predicate: str = f"{date_column} >= '{year-1}-10-01' and {date_column} < '{beginning_of_month_bound}'"
    print(f"range_predicate = {range_predicate}")
    return range_predicate

# COMMAND ----------

# DBTITLE 1,Globals
retention_period_years: int = 2

abandonments_status_codes: list[str] = [
    "600",
    "601",
    "602",
    "603",
    "604",
    "605",
    "606",
    "607",
    "608",
    "609",
    "612",
    "614",
    "618",
]

non_abandonments_status_codes: list[str] = [
    "616",
    "630",
    "631",
    "638",
    "640",
    "641",
    "642",
    "643",
    "644",
    "645",
    "646",
    "647",
    "648",
    "649",
    "650",
    "651",
    "652",
    "653",
    "654",
    "661",
    "663",
    "664",
    "665",
    "666",
    "667",
    "668",
    "672",
    "680",
    "681",
    "686",
    "688",
    "689",
    "690",
    "692",
    "693",
    "694",
    "715",
    "718",
    "719",
    "720",
    "721",
    "722",
    "724",
    "725",
    "730",
    "731",
    "732",
    "733",
    "734",
    "744",
    "745",
    "746",
    "747",
    "748",
    "752",
    "753",
    "756",
    "757",
    "760",
    "762",
    "763",
    "764",
    "765",
    "766",
    "772",
    "773",
    "774",
    "775",
    "777",
    "779",
    "782",
    "783",
    "784",
    "785",
    "794",
    "801",
    "802",
    "803",
    "806",
    "807",
    "808",
    "809",
    "810",
    "811",
    "812",
    "813",
    "814",
    "815",
    "816",
    "817",
    "818",
    "819",
]

am_status_codes: list[str] = [
    "688",
    "718",
    "719",
    "721",
    "720",
    "722",
    "723",
    "724",
    "725",
    "730",
    "731",
    "732",
    "733",
    "734",
    "744",
    "745",
    "746",
    "747",
    "748",
    "752",
    "753",
    "782",
    "783",
    "784",
    "785",
    "806",
    "807",
    "808",
    "809",
    "810",
    "813",
    "814",
    "815",
    "816",
    "817",
    "818",
    "819",
    "812",
    "811",
]

itu_status_codes: list[str] = [
    "616",
    "630",
    "631",
    "632",
    "638",
    "640",
    "641",
    "642",
    "643",
    "644",
    "645",
    "646",
    "647",
    "648",
    "649",
    "650",
    "651",
    "652",
    "653",
    "654",
    "661",
    "663",
    "664",
    "665",
    "666",
    "667",
    "668",
    "672",
    "680",
    "681",
    "686",
    "689",
    "690",
    "692",
    "693",
    "694",
    "715",
    "756",
    "757",
    "760",
    "762",
    "763",
    "764",
    "765",
    "766",
    "771",
    "772",
    "773",
    "774",
    "775",
    "777",
    "779",
    "794",
    "801",
    "802",
    "803",
]

eligible_totals_status_codes: list[str] = [
    "616",
    "620",
    "622",
    "630",
    "631",
    "632",
    "638",
    "640",
    "641",
    "642",
    "643",
    "644",
    "645",
    "646",
    "647",
    "648",
    "649",
    "650",
    "651",
    "652",
    "653",
    "654",
    "661",
    "663",
    "664",
    "665",
    "666",
    "667",
    "668",
    "672",
    "680",
    "681",
    "686",
    "688",
    "689",
    "690",
    "692",
    "693",
    "694",
    "715",
    "718",
    "719",
    "720",
    "721",
    "722",
    "724",
    "725",
    "730",
    "731",
    "732",
    "733",
    "734",
    "744",
    "745",
    "746",
    "747",
    "748",
    "752",
    "753",
    "756",
    "757",
    "760",
    "762",
    "763",
    "764",
    "765",
    "766",
    "772",
    "773",
    "774",
    "775",
    "777",
    "779",
    "782",
    "783",
    "784",
    "785",
    "794",
    "801",
    "802",
    "803",
    "806",
    "807",
    "808",
    "809",
    "810",
    "811",
    "812",
    "813",
    "814",
    "815",
    "816",
    "817",
    "818",
    "819",
]

deferred_revenue_codes: list[str] = ["630", "631", "638"]

am_exclusion_status_codes: list[str] = ["620", "622", "632"]

class_codes: list[str] = ["'6'", "'W'", "'P'"] # these are considered "active" class status codes, even though "6" is the genuine active class status

abandonments_status_codes_filter: str = parse_codes(abandonments_status_codes)
non_abandonments_status_codes_filter: str = parse_codes(non_abandonments_status_codes)
am_status_codes_filter: str = parse_codes(am_status_codes)
eligible_totals_status_codes_filter: str = parse_codes(eligible_totals_status_codes)
am_exclusion_status_codes_filter: str = parse_codes(am_exclusion_status_codes)
itu_status_codes_filter: str = parse_codes(itu_status_codes)
class_codes_filter: str = parse_codes(class_codes)
deferred_revenue_codes_filter: str = parse_codes(deferred_revenue_codes)
is_concurrent_use_with_no_active_registration_filter: str = "(status_code = 771 and registration_num is null)"

print(f"abandonments_status_codes_filter: {abandonments_status_codes_filter}\n")
print(f"non_abandonments_status_codes_filter: {non_abandonments_status_codes_filter}\n")
print(f"am_status_codes_filter: {am_status_codes_filter}\n")
print(f"eligible_totals_status_codes_filter: {eligible_totals_status_codes_filter}\n")
print(f"am_exclusion_status_codes_filter: {am_exclusion_status_codes_filter}\n")
print(f"itu_status_codes_filter: {itu_status_codes_filter}\n")
print(f"class_codes_filter: {class_codes_filter}\n")
print(f"deferred_revenue_codes_filter: {deferred_revenue_codes_filter}\n")
print(f"is_concurrent_use_with_no_active_registration_filter: {is_concurrent_use_with_no_active_registration_filter}\n")
if dbx_env in ("dev", "test"):
    print("Lower environment detected. Using custom lookback date...")
    from dateutil.relativedelta import relativedelta
    delta_two_years = datetime.datetime.now() - relativedelta(years=2)
    lookback_predicate: str = generate_lookback_predicate(date_column="`action_date`", custom_date=delta_two_years)
else:
    print("Job running in production. Lookback date is static.")
    lookback_predicate: str = generate_lookback_predicate(date_column="`action_date`")

# COMMAND ----------

# DBTITLE 1,Sanity Check: Codes
display(spark.sql(f"""
select
  *
from
  (
    select
      'class codes' `table`,
      try_cast(tm_class_status_cd as string) `code`,
      description_tx description
    from
      {tmngpdb_catalog}.bronze.stnd_tm_class_status
    union all
    select
      'status codes' `table`,
      try_cast(status_no as string) `code`,
      description_tx description
    from
      {tmngpdb_catalog}.bronze.stnd_legacy_status
  )
order by
  `table`,
  `code`
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Views

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ignored Records
# MAGIC Records are ignored if they:
# MAGIC 1. have a status change the day of the run
# MAGIC 2. have a status change pre-dated for the future

# COMMAND ----------

# DBTITLE 1,View: Records Excluded From This Run
spark.sql(
    f"""
    select
        trademark_gid,
        serial_num_tx serial_num,
        status_dt status_date,
        legacy_status_cd status_code
    from
        {tmngpdb_catalog}.bronze.trademark
    where
        date(status_dt) >= current_date
"""
).createOrReplaceTempView("excluded_records")

display(spark.sql("select * from excluded_records"))

# COMMAND ----------

# DBTITLE 1,View: Status and Registration
spark.sql(f"""
  select
    a.trademark_gid,
    a.serial_num_tx serial_num,
    a.legacy_status_cd status_code,
    a.registration_num registration_num,
    date(a.status_dt) status_date,
    date(a.filing_dt) filing_date
  from
    {tmngpdb_catalog}.bronze.trademark a
  where not exists(
      select 
        1
      from 
        excluded_records b
      where 
        b.trademark_gid = a.trademark_gid
    )
""").createOrReplaceTempView("status_and_registration")

# COMMAND ----------

# DBTITLE 1,View: Classes
# TODO: double-check active class filter
spark.sql(f"""
  select
  distinct
    fk_trademark_gid,
    right(fk_trademark_gid, 8) serial_num,
    sum(case when fk_tm_class_status_cd in {class_codes_filter} then 1 else 0 end) over (partition by fk_trademark_gid) num_active_classes,
    sum(case when fk_tm_class_status_cd = '6' then 1 else 0 end) over (partition by fk_trademark_gid) num_active_status_active_classes,
    sum(case when fk_tm_class_status_cd = 'W' then 1 else 0 end) over (partition by fk_trademark_gid) num_waived_status_active_classes,
    sum(case when fk_tm_class_status_cd = 'P' then 1 else 0 end) over (partition by fk_trademark_gid) num_partial_status_active_classes,
    sum(case when fk_tm_class_status_cd not in {class_codes_filter} then 1 else 0 end) over (partition by fk_trademark_gid) num_inactive_classes,
    sum(1) over (partition by fk_trademark_gid) num_any_classes
  from
    {tmngpdb_catalog}.bronze.tm_class
"""
).createOrReplaceTempView("classes")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Abandonments / Non-Abandonments

# COMMAND ----------

# DBTITLE 1,View: Abandonments
spark.sql(f"""
  select
    distinct
    b.status_code,
    a.description_tx status_description,
    b.serial_num,
    nvl(c.num_any_classes, 0) active_class_cnt,
    date(d.milestone_dt) action_date,
    true abandoned
  from
    {tmngpdb_catalog}.bronze.stnd_legacy_status a
    left join status_and_registration b
      on a.status_no = b.status_code
    left join classes c
      on b.trademark_gid = c.fk_trademark_gid
    left join {tmngpdb_catalog}.bronze.tm_milestone d
      on b.trademark_gid = d.fk_trademark_gid
  where
    b.status_code in {abandonments_status_codes_filter}
    and (d.fk_tm_milestone_cd = 'ABAND')
""").createOrReplaceTempView("abandonments")

# COMMAND ----------

# DBTITLE 1,View: Non-Abandonments
spark.sql(f"""
  select
    distinct
    b.status_code,
    a.description_tx status_description,
    b.serial_num,
    nvl(c.num_active_classes, 0) active_class_cnt,
    b.status_date action_date,
    false as abandoned
  from
    {tmngpdb_catalog}.bronze.stnd_legacy_status a
    left join status_and_registration b
      on a.status_no = b.status_code
    left join classes c
      on b.trademark_gid = c.fk_trademark_gid
    left join {tmngpdb_catalog}.bronze.tm_milestone d
      on b.trademark_gid = d.fk_trademark_gid
  where
    (
      b.status_code in {non_abandonments_status_codes_filter}
      or {is_concurrent_use_with_no_active_registration_filter}
    )
    and c.num_active_classes > 0
""").createOrReplaceTempView("non_abandonments")

# COMMAND ----------

# DBTITLE 1,View: Unioned Abandonments and Non-Abandonments
spark.sql("""
  select * from abandonments
    union all
  select * from non_abandonments
""").createOrReplaceTempView("monthly")

# COMMAND ----------

# DBTITLE 1,View: Audit Detail Unioned Abandonments and Non-Abandonments
spark.sql(f"""
  select 
    *,
    current_date load_date,
    day(current_date) = 1 is_static,
    true latest,
    'OS34_ETL_LOAD' create_user,
    current_timestamp as create_timestamp
  from 
    monthly
  where
    {lookback_predicate}
"""
).createOrReplaceTempView("incoming_abandonments_detail_fytd")

# COMMAND ----------

# DBTITLE 1,View: Abandonments FYTD
spark.sql(f"""
  select
    status_code,
    status_description,
    abandoned,
    sum(1) case_count,
    sum(active_class_cnt) class_count
  from
    monthly
  where 
    abandoned = true
    and {lookback_predicate}
  group by
    all
"""
).createOrReplaceTempView("monthly_records_abandonments_aggregates_fytd")

# COMMAND ----------

# DBTITLE 1,View: Incoming Abandonments FYTD
spark.sql("""
  select
    *,
    current_date load_date,
    day(current_date) = 1 is_static,
    true latest,
    'OS34_ETL_LOAD' create_user,
    current_timestamp as create_timestamp
  from
    monthly_records_abandonments_aggregates_fytd
"""
).createOrReplaceTempView("incoming_abandonments_fytd")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deferred Revenue

# COMMAND ----------

# DBTITLE 1,View: Deferred Revenue
spark.sql(f"""
    select 
        serial_num,
        status_code,
        active_class_cnt active_classes,
        `action_date` status_date,
        current_date load_date,
        'OS34_ETL_LOAD' create_user,
        current_timestamp as create_timestamp
    from 
        monthly
    where 
        status_code in {deferred_revenue_codes_filter}
""").createOrReplaceTempView("deferred_revenue")

# COMMAND ----------

# DBTITLE 1,View: Monthly Aggregates
spark.sql("""
  select
    status_code,
    status_description,
    abandoned,
    sum(1) case_count,
    sum(active_class_cnt) class_count
  from
    monthly
  group by
    all
""").createOrReplaceTempView("monthly_records_aggregates")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Statuses

# COMMAND ----------

# DBTITLE 1,View: Incoming Status Records
spark.sql("""
  select
    *,
    current_date load_date,
    day(current_date) = 1 is_static,
    true latest,
    'OS34_ETL_LOAD' create_user,
    current_timestamp as create_timestamp
  from
    monthly_records_aggregates
"""
).createOrReplaceTempView("incoming_status_records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Totals

# COMMAND ----------

# DBTITLE 1,View: ITU
spark.sql(f"""
  select
    fk_trademark_gid,
    right(fk_trademark_gid, 8) serial_num,
    case
      when current_in = 'Y' then true
      else false
    end as is_currently_filed_itu,
    case
      when filed_in = 'Y' then true
      else false
    end as is_filed_itu
  from
    {tmngpdb_catalog}.bronze.tm_filing_basis
  where
    fk_filing_basis_cd = '1(b)'
""").createOrReplaceTempView("itu")

# COMMAND ----------

# DBTITLE 1,View: NOA
spark.sql(f"""
  select
    right(itu.fk_trademark_gid, 8) serial_num,
    date(tmm.milestone_dt) as noa_date
  from
    {tmngpdb_catalog}.bronze.tm_itu itu
    inner join {tmngpdb_catalog}.bronze.tm_milestone tmm on itu.fk_trademark_gid = tmm.fk_trademark_gid
    inner join {tmngpdb_catalog}.bronze.stnd_tm_milestone stmm on tmm.fk_tm_milestone_cd = stmm.tm_milestone_cd
  where
    stmm.tm_milestone_cd = 'NOA'
""").createOrReplaceTempView("noa")

# COMMAND ----------

# DBTITLE 1,View: Base Detail
spark.sql(f"""
  select
    a.serial_num,
    a.status_code,
    nvl(d.is_currently_filed_itu, false) is_currently_filed_itu,
    nvl(d.is_filed_itu, false) is_filed_itu,
    nvl(b.num_active_classes, 0) num_active_classes,
    a.registration_num,
    a.filing_date,
    a.status_date,
    case
      when 
        c.serial_num is null
        and (
          d.is_currently_filed_itu = true
          or d.is_filed_itu = true
        ) 
      then true
      when 
        c.serial_num is null
        and (
          d.is_currently_filed_itu = false
          and d.is_filed_itu = false
        ) 
      then false
      when 
        c.serial_num is not null
        and a.status_code in {am_status_codes_filter} 
       then false
      else true
    end is_itu,
    case
      when c.serial_num is not null
      and a.status_code in {am_status_codes_filter}
      and (
        d.is_currently_filed_itu = true
        or d.is_filed_itu = true
      ) 
      then true
      else false
    end is_noa,
    case
      when 
        a.status_code in {am_status_codes_filter} 
      then true
      else false
    end has_eligible_non_registered_and_active_status,
    case
      when 
        b.serial_num is null 
      then true
      else false
    end has_no_classes
  from status_and_registration a
    left join 
      classes b on a.serial_num = b.serial_num
    left join 
      noa c on a.serial_num = c.serial_num
    left join 
      itu d on a.serial_num = d.serial_num
  where
    a.status_code in {eligible_totals_status_codes_filter}
    or {is_concurrent_use_with_no_active_registration_filter}
""").createOrReplaceTempView("all_detail")

# COMMAND ----------

# DBTITLE 1,View: Base Detail Continued

spark.sql(f"""
select 
    *,
    case 
        when 
            num_active_classes > 0
            and status_code not in {am_exclusion_status_codes_filter} 
        then true
        else false
    end is_counted_application,
    case 
        when 
            num_active_classes > 0
            and is_itu = false
            and is_noa = true 
        then true
        else false
    end is_counted_noa,
    case  
        when 
            num_active_classes > 0
            and status_code not in {am_exclusion_status_codes_filter}
            and is_noa = false
            and is_itu = false 
        then true
        else false
    end is_counted_use,
    case 
        when 
            num_active_classes > 0
            and is_itu = true
            and is_noa = false
            and status_code not in {am_exclusion_status_codes_filter}
            and status_code in {itu_status_codes_filter} 
        then true
        else false
    end is_counted_itu
    from 
        all_detail   
""").createOrReplaceTempView("all_detail")

# COMMAND ----------

# DBTITLE 1,View: Incoming Status Detail
# MAGIC %sql
# MAGIC create or replace temp view incoming_status_detail as
# MAGIC select
# MAGIC   *,
# MAGIC   current_date load_date,
# MAGIC   day(current_date) = 1 is_static,
# MAGIC   true latest,
# MAGIC   'OS34_ETL_LOAD' create_user,
# MAGIC   current_timestamp as create_timestamp
# MAGIC from
# MAGIC   all_detail;

# COMMAND ----------

# DBTITLE 1,View: Incoming Status Total Detail
spark.sql(f"""
    select
        a.status_date,
        count(
            distinct 
            case
                when 
                    a.is_counted_application = true
                then a.serial_num
                else null
            end
        ) total_application_cases,
        sum(
            case
                when 
                    a.is_counted_application = true
                then a.num_active_classes
                else 0
            end
        ) total_application_classes,
        count(
            distinct 
            case
                when 
                    a.is_counted_noa = true
                then a.serial_num
                else null
            end
        ) total_noa_cases,
        sum(
            case
                when 
                    a.is_counted_noa = true
                then num_active_classes
                else 0
            end
        ) total_noa_classes,
        count(
            distinct case
                when 
                    a.is_counted_use = true
                then a.serial_num
                else null
            end
        ) total_use_cases,
        sum(
            case
                when
                    a.is_counted_use = true
                then a.num_active_classes
                else 0
            end
        ) total_use_classes,
        count(
            distinct case
                when 
                    a.is_counted_itu = true
                then a.serial_num
                else null
            end
        ) total_itu_cases,
        sum(
            case
                when 
                    a.is_counted_itu = true
                then a.num_active_classes
                else 0
            end
        ) total_itu_classes,
        current_date load_date,
        day(current_date) = 1 is_static,
        true latest,
        'OS34_ETL_LOAD' create_user,
        current_timestamp as create_timestamp
    from
        incoming_status_detail a
    group by 
        all
""").createOrReplaceTempView("incoming_status_total_detail")

# COMMAND ----------

# DBTITLE 1,View: Incoming Total Detail
spark.sql("""
    select
        sum(total_application_cases) total_application_cases,
        sum(total_application_classes) total_application_classes,
        sum(total_noa_cases) total_noa_cases,
        sum(total_noa_classes) total_noa_classes,
        sum(total_use_cases) total_use_cases,
        sum(total_use_classes) total_use_classes,
        sum(total_itu_cases) total_itu_cases,
        sum(total_itu_classes) total_itu_classes,
        current_date load_date,
        day(current_date) = 1 is_static,
        true latest,
        'OS34_ETL_LOAD' create_user,
        current_timestamp as create_timestamp
    from
        incoming_status_total_detail
    group by all
""").createOrReplaceTempView("incoming_status_total")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inserts

# COMMAND ----------

# MAGIC %md
# MAGIC ### Details

# COMMAND ----------

# DBTITLE 1,Insert: Abandonments Detail
display(
    spark.sql(
        f"""
        merge into
            {reporting_catalog}.silver.os34_report_abandonments_detail_fytd as target
        using
            incoming_abandonments_detail_fytd source
        on
            target.serial_num = source.serial_num
            and target.load_date = source.load_date
        when matched then update set
            target.status_code = source.status_code,
            target.status_description = source.status_description,
            target.serial_num = source.serial_num,
            target.active_class_cnt = source.active_class_cnt,
            target.action_date = source.action_date,
            target.abandoned = source.abandoned,
            target.load_date = source.load_date,
            target.is_static = source.is_static,
            target.latest = source.latest,
            target.create_user = source.create_user,
            target.create_timestamp = source.create_timestamp
        when not matched then insert (
            status_code,
            status_description,
            serial_num,
            active_class_cnt,
            action_date,
            abandoned,
            load_date,
            is_static,
            latest,
            create_user,
            create_timestamp
        )
        values (
            source.status_code,
            source.status_description,
            source.serial_num,
            source.active_class_cnt,
            source.action_date,
            source.abandoned,
            source.load_date,
            source.is_static,
            source.latest,
            source.create_user,
            source.create_timestamp
        )
        when not matched by source and target.load_date = current_date then delete
        when not matched by source and
        target.latest = true
        and target.load_date < current_date
        then update set target.latest = false
  """
    )
)

# COMMAND ----------

# DBTITLE 1,Insert: Deferred Revenue
if datetime.datetime.today().day == 1: # user only wants updated on 1st of every month
    # should not be CDC since change in records for load_date adds no value
    display(
        spark.sql(f"""
            insert overwrite 
                {reporting_catalog}.gold.os34_report_deferred_revenue_cases (
                    serial_num,
                    status_code,
                    active_classes,
                    status_date,
                    load_date,
                    create_user,
                    create_timestamp
                )
            select
                serial_num,
                status_code,
                active_classes,
                status_date,
                load_date,
                create_user,
                create_timestamp
            from
                deferred_revenue
        """)
    )

# COMMAND ----------

# DBTITLE 1,Insert: Status Detail
display(
    # should not be CDC since change in records for load_date adds no value
    spark.sql(
        f"""
        insert overwrite 
            {reporting_catalog}.silver.os34_report_status_detail (
                serial_num, 
                status_code, 
                registration_num, 
                status_date, 
                is_currently_filed_itu, 
                is_filed_itu, 
                filing_date, 
                num_active_classes, 
                has_eligible_non_registered_and_active_status, 
                has_no_classes, 
                is_counted_application,
                is_counted_noa,
                is_counted_use,
                is_counted_itu,
                load_date, 
                is_static, 
                latest, 
                create_user, 
                create_timestamp
            )
        select
            serial_num, 
            status_code, 
            registration_num, 
            status_date, 
            is_currently_filed_itu, 
            is_filed_itu, 
            filing_date, 
            num_active_classes, 
            has_eligible_non_registered_and_active_status, 
            has_no_classes, 
            is_counted_application,
            is_counted_noa,
            is_counted_use,
            is_counted_itu,
            load_date, 
            is_static, 
            latest, 
            create_user, 
            create_timestamp
        from
            incoming_status_detail
    """
    )
)

# COMMAND ----------

# DBTITLE 1,Insert: Status Total Detail
display(
    # should not be CDC since change in records for load_date adds no value
    spark.sql(
        f"""
        insert overwrite 
            {reporting_catalog}.silver.os34_report_status_total_detail (
            status_date,
            total_application_cases,
            total_application_classes,
            total_noa_cases,
            total_noa_classes,
            total_use_cases,
            total_use_classes,
            total_itu_cases,
            total_itu_classes,
            load_date,
            is_static,
            latest,
            create_user,
            create_timestamp
        )
        select
            status_date,
            total_application_cases,
            total_application_classes,
            total_noa_cases,
            total_noa_classes,
            total_use_cases,
            total_use_classes,
            total_itu_cases,
            total_itu_classes,
            load_date,
            is_static,
            latest,
            create_user,
            create_timestamp
        from
            incoming_status_total_detail
    """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregates

# COMMAND ----------

# DBTITLE 1,Insert: Insert Abandonments
display(
    spark.sql(
        f"""
        merge into
            {reporting_catalog}.gold.os34_report_abandonments_fytd as target
        using 
            incoming_abandonments_fytd source
        on
            target.status_code = source.status_code
            and target.load_date = source.load_date
        when matched then update set
            target.status_code = source.status_code,
            target.status_description = source.status_description,
            target.abandoned = source.abandoned,
            target.case_count = source.case_count,
            target.class_count = source.class_count,
            target.load_date = source.load_date,
            target.is_static = source.is_static,
            target.latest = source.latest,
            target.create_user = source.create_user,
            target.create_timestamp = source.create_timestamp
        when not matched then insert (
            status_code,
            status_description,
            abandoned,
            case_count,
            class_count,
            load_date,
            is_static,
            latest,
            create_user,
            create_timestamp
        )
        values (
            source.status_code,
            source.status_description,
            source.abandoned,
            source.case_count,
            source.class_count,
            source.load_date,
            source.is_static,
            source.latest,
            source.create_user,
            source.create_timestamp
        )
        when not matched by source and target.load_date = current_date then delete
        when not matched by source and
            target.latest = true
            and target.load_date < current_date
        then update set target.latest = false
  """)
)

# COMMAND ----------

# DBTITLE 1,Insert: Incoming Statuses
display(
    spark.sql(
        f"""
        merge into
            {reporting_catalog}.gold.os34_report_statuses as target
        using incoming_status_records source
        on
            target.status_code = source.status_code
            and target.load_date = source.load_date
        when matched then update set
            target.status_code = source.status_code,
            target.status_description = source.status_description,
            target.abandoned = source.abandoned,
            target.case_count = source.case_count,
            target.class_count = source.class_count,
            target.load_date = source.load_date,
            target.is_static = source.is_static,
            target.latest = source.latest,
            target.create_user = source.create_user,
            target.create_timestamp = source.create_timestamp
        when not matched then insert (
            status_code,
            status_description,
            abandoned,
            case_count,
            class_count,
            load_date,
            is_static,
            latest,
            create_user,
            create_timestamp
        )
        values (
            source.status_code,
            source.status_description,
            source.abandoned,
            source.case_count,
            source.class_count,
            source.load_date,
            source.is_static,
            source.latest,
            source.create_user,
            source.create_timestamp
        )
        when not matched by source and target.load_date = current_date then delete
        when not matched by source 
            and target.latest = true
            and target.load_date < current_date
        then update set target.latest = false
""")
)

# COMMAND ----------

# DBTITLE 1,Insert: Incoming Totals
display(
  spark.sql(f"""
  merge into
    {reporting_catalog}.gold.os34_report_totals as target
  using incoming_status_total source
  on
    target.load_date = source.load_date
  when matched then 
  update set 
    target.tot_app_cases = source.total_application_cases,
    target.tot_app_class = source.total_application_classes,
    target.totnoacases = source.total_noa_cases,
    target.totnoaclass = source.total_noa_classes,
    target.totusecase = source.total_use_cases,
    target.totuseclass = source.total_use_classes,
    target.itu_cases = source.total_itu_cases,
    target.itu_class = source.total_itu_classes,
    target.load_date = source.load_date,
    target.is_static = source.is_static,
    target.latest = source.latest,
    target.create_user = source.create_user,
    target.create_timestamp = source.create_timestamp
  when not matched then insert (
      tot_app_cases,
      tot_app_class,
      totnoacases,
      totnoaclass,
      totusecase,
      totuseclass,
      itu_cases,
      itu_class,
      load_date,
      is_static,
      latest,
      create_user,
      create_timestamp
    )
    values (
      source.total_application_cases,
      source.total_application_classes,
      source.total_noa_cases,
      source.total_noa_classes,
      source.total_use_cases,
      source.total_use_classes,
      source.total_itu_cases,
      source.total_itu_classes,
      source.load_date,
      source.is_static,
      source.latest,
      source.create_user,
      source.create_timestamp
    )
    when not matched by source and target.load_date = current_date then delete
    when not matched by source 
      and target.latest = true
      and target.load_date < current_date
    then update set target.latest = false
  """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity Check Results

# COMMAND ----------

# DBTITLE 1,Show Loads in Last Three Days: Statuses
display(
  spark.sql(f"""
    select
      *
    from
      {reporting_catalog}.gold.os34_report_statuses
    where
      load_date >= current_date - interval 3 days
    order by
      create_timestamp desc,
      abandoned,
      status_code
  """)
)

# COMMAND ----------

# DBTITLE 1,Show Latest Load: Statuses
display(
  spark.sql(f"""
    select
      *
    from
      {reporting_catalog}.gold.os34_report_statuses
    where
      latest = true
    order by
      abandoned,
      status_code
  """)
)

# COMMAND ----------

# DBTITLE 1,Show Latest Load: Totals
display(
  spark.sql(f"""
    select
      *
    from
      {reporting_catalog}.gold.os34_report_totals
    where
      latest = true
  """)
)

# COMMAND ----------

# DBTITLE 1,Show Latest Load: Last 3 Runs
display(
  spark.sql(f"""
    select
      *
    from
      {reporting_catalog}.gold.os34_report_totals
    order by
      create_timestamp desc
    limit 3
  """)
)

# COMMAND ----------

# DBTITLE 1,Sanity Check: No Duplicates on Load Date
duplicate_totals: int = spark.sql(f"""
    select 
        load_date, 
        count(1) 
    from 
        {reporting_catalog}.gold.os34_report_totals 
    where 
        load_date = current_date 
    group by 
        all 
    having count(1) > 1
""").count()

duplicate_statuses: int = spark.sql(f"""
    select
        status_code,
        load_date,
        count(1)
    from
        {reporting_catalog}.gold.os34_report_statuses
    where
        load_date = current_date
    group by
        all
    having
        count(1) > 1
""").count()

duplicate_abandonments_fytd: int = spark.sql(f"""
    select
        status_code,
        load_date,
        count(1)
    from
        {reporting_catalog}.gold.os34_report_abandonments_fytd
    where
        load_date = current_date
    group by
        all
    having
        count(1) > 1
""").count()
quality_result: bool = duplicate_abandonments_fytd + duplicate_statuses + duplicate_totals > 0
print(f"Duplicates found: {quality_result}")

# COMMAND ----------

# DBTITLE 1,Audit: Counts
display(
    spark.sql(
        f"""
        select
            *
        from
            (
                select
                    tot_app_cases,
                    tot_app_class
                from
                    {reporting_catalog}.gold.os34_report_totals
                where
                    latest = true
            ) a
        join (
            select
                sum(case_count) tot_app_cases_from_status,
                sum(class_count) tot_app_classes_from_status
            from
                {reporting_catalog}.gold.os34_report_statuses
            where
                latest = true
                and abandoned = false
        ) b
    """)
)

# COMMAND ----------

# DBTITLE 1,Audit: Counts For End Job
status_count_dq = spark.sql(f"""
    select
        1
    from
        {reporting_catalog}.gold.os34_report_statuses
    where
        latest = true
""").count()

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
    1,
    "job completed successfully",
)
dbutils.notebook.exit(f"Job completed with [{status_count_dq}] records for statuses, and [1] record for totals.")