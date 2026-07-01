# Databricks notebook source
# MAGIC %md
# MAGIC # Anomaly ETL: Gold Layer
# MAGIC
# MAGIC Reads from the four silver tables and builds one gold Delta table that
# MAGIC power the anomaly-report dashboard.  Runtime dashboard parameters
# MAGIC (`:order_type`, `:unit_of_time`, etc.) are **not** applied here: they are
# MAGIC applied at query time by the companion SQL file.
# MAGIC
# MAGIC ### Gold Tables Written
# MAGIC | Table | Cluster columns | Dashboard query |
# MAGIC |---|---|---|
# MAGIC | `gold.patron_overview` | `cfk_patron_id`, `applicant_bin` | `ntb_anomaly_gold_query.sql` |
# MAGIC
# MAGIC ### Design Notes
# MAGIC - Table is **full-refresh overwrite**: gold is rebuilt from silver on every run.
# MAGIC - `OPTIMIZE` is called after the table write to compact files and improve query performance.

# COMMAND ----------

# DBTITLE 1,Environment & Config
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")
dbutils.widgets.text("exam_link", "https://review.tm-examcenter.aws.uspto.gov/review/")
exam_link = dbutils.widgets.get("exam_link")


config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=}, {dbx_env=}")

# COMMAND ----------

# DBTITLE 1,Common Functions & Parameters
# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,Imports
import logging
from pyspark.sql.functions import (
    current_timestamp,
    lit,
)
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    force=True
)
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
logging.getLogger("py4j.clientserver").setLevel(logging.ERROR)
log = logging.getLogger("anomaly_gold_etl")
log.setLevel(logging.INFO)

# COMMAND ----------

# DBTITLE 1,Catalog & Schema Variables
common_configs = read_yaml(config_file)
reporting_catalog = target_catalog = common_configs["schema"]["trm_reporting_catalog"]
jbteasps_catalog = common_configs["schema"]["trm_jbteasps_src_catalog"]
tm_practitioner_catalog = common_configs["schema"]["tm_practitioner_catalog"]
run_env = dbx_env

bronze = "bronze"
silver = "silver"
gold = "gold"

print(f"{target_catalog=}, {jbteasps_catalog=}, {tm_practitioner_catalog=}, {run_env=}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared Temp Views: Silver Sources
# MAGIC
# MAGIC Register silver tables as temp views so gold transformations can use
# MAGIC Spark SQL without repeating catalog/schema path strings.

# COMMAND ----------

# DBTITLE 1,View: vw_fact_submission
log.info("Registering vw_fact_submission")
spark.sql(
    f"""
    create or replace temp view vw_fact_submission as
    select 
        myuspto_patron_id,
        transaction_id,
        serial_number,
        registration_number,
        submission_time,
        submission_date,
        submission_hour,
        filing_time,
        filing_date,
        filing_hour,
        form_code,
        proof_code,
        signature_type,
        signatory_name,
        signatory_position,
        source_system,
        transaction_type
    from {reporting_catalog}.silver.fact_submission
"""
)

# COMMAND ----------

# DBTITLE 1,View: vw_dim_patron_account (current versions only)
log.info("Registering vw_dim_patron_account")
spark.sql(
    f"""
    create or replace temp view vw_dim_patron_account as
    select 
        myuspto_patron_id,
        account_patron_nickname,
        account_patron_name,
        account_email,
        selected_role,
        account_status
    from {reporting_catalog}.silver.dim_patron_account
    where is_current = true
"""
)

# COMMAND ----------

# DBTITLE 1,View: vw_dim_patron_signature
log.info("Registering vw_dim_patron_signature")
spark.sql(
    f"""
    create or replace temp view vw_dim_patron_signature as
    select 
        myuspto_patron_id,
        usually_signed_name_as,
        usually_signed_position_as,
        usually_signed_type_as,
        first_submission_time,
        last_submission_time
    from 
        {reporting_catalog}.silver.dim_patron_signature
"""
)

# COMMAND ----------

# DBTITLE 1,View: vw_fact_sponsorship (current versions only)
log.info("Registering vw_fact_sponsorship")
spark.sql(
    f"""
    create or replace temp view vw_fact_sponsorship as
    select 
        myuspto_patron_id,
        has_sponsored,
        has_been_sponsored_by
    from {reporting_catalog}.silver.fact_sponsorship
    where is_current = true
"""
)

# COMMAND ----------

# DBTITLE 1,View: vw_applicant_bin (patrons' latest applicant bin)
log.info("Registering vw_applicant_bin")
spark.sql(
    f"""
    create or replace temp view vw_applicant_bin as
    select cfk_patron_id myuspto_patron_id, max_by(applicant_bin, load_date) applicant_bin
    from {reporting_catalog}.gold.unsupervised_anomalies_cumulative
    group by all
    """
)

# COMMAND ----------

# DBTITLE 1,View: vw_submissions_considered (submissions that were used in the model to determine anomaly status)
log.info("Registering vw_submissions_considered")
spark.sql(
    f"""
    create or replace temp view vw_submissions_considered_by_model as
    select distinct transaction_id
    from vw_fact_submission a
    join {reporting_catalog}.gold.unsupervised_anomalies_cumulative b
        on a.myuspto_patron_id = b.cfk_patron_id
            and submission_date between (load_date - interval 365 days) and load_date
            and form_code in ('FTK', 'BAS', 'APPB')
    """
)

# COMMAND ----------

# DBTITLE 1,View: vw_anomaly_list  (patrons on the current anomaly list with latest score)
log.info("Registering vw_anomaly_list")
spark.sql(
    f"""
    create or replace temp view vw_anomaly_list as
    select
        a.cfk_patron_id myuspto_patron_id,
        a.latest_anomaly_score,
        a.times_appeared,
        a.first_appeared,
        a.last_appeared,
        b.applicant_bin
    from {reporting_catalog}.gold.unsupervised_anomalies a
    inner join vw_applicant_bin b on a.cfk_patron_id = b.myuspto_patron_id
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Build: patron_overview
# MAGIC
# MAGIC Aggregates submission statistics and
# MAGIC joins account, sponsorship, and anomaly-score data (where applicable).  The dashboard query
# MAGIC (`ntb_anomaly_gold_query.sql`) applies runtime ranking on top.

# COMMAND ----------

# DBTITLE 1,Build: anomaly_patron_overview
patron_overview_df = (
    spark.sql(f"""
        with submissions as (
            select
                a.myuspto_patron_id,
                a.transaction_id,
                a.serial_number,
                a.registration_number,
                a.submission_time,
                a.submission_date,
                a.submission_hour,
                a.filing_time,
                a.filing_date,
                a.filing_hour,
                a.form_code,
                a.proof_code,
                a.signature_type,
                a.signatory_name,
                a.signatory_position,
                a.source_system,
                (b.transaction_id is not null) = true is_submission_considered_by_model
            from 
                vw_fact_submission a
                left join vw_submissions_considered_by_model b 
                    on a.transaction_id = b.transaction_id 
        ),
        daily_counts as (
            select
                myuspto_patron_id,
                submission_date,
                count(*) as submissions_on_day,
                sum(case when is_submission_considered_by_model = true then 1 else 0 end) as submissions_on_day_considered_by_model
            from 
                submissions
            group by
                myuspto_patron_id,
                submission_date
        ),
        daily_submissions as (
            select 
                myuspto_patron_id,
                avg(submissions_on_day) avg_submissions_per_day,
                min(submissions_on_day) min_submissions_one_day,
                max(submissions_on_day) max_submissions_one_day,
                avg(submissions_on_day_considered_by_model) avg_submissions_per_day_considered_by_model,
                min(submissions_on_day_considered_by_model) min_submissions_one_day_considered_by_model,
                max(submissions_on_day_considered_by_model) max_submissions_one_day_considered_by_model
            from 
                daily_counts
            group by 
                myuspto_patron_id
        ),
        submissions_aggregated as (
            select
                myuspto_patron_id,
                count(*) total_submissions,
                sum(case when is_submission_considered_by_model = true then 1 else 0 end) total_submissions_considered_by_model,
                count(distinct serial_number) total_distinct_cases,
                count(distinct case when is_submission_considered_by_model = true then serial_number end) total_distinct_cases_considered_by_model
            from 
                submissions
            group by 
                myuspto_patron_id
        ),
        signatures as (
            select
                myuspto_patron_id,
                usually_signed_name_as,
                usually_signed_position_as,
                usually_signed_type_as,
                first_submission_time,
                last_submission_time
            from 
                vw_dim_patron_signature
        ),
        aggregations as (
            select
                a.myuspto_patron_id,
                a.serial_number,
                a.registration_number,
                a.transaction_id,
                a.submission_time,
                a.submission_date,
                a.submission_hour,
                a.filing_time,
                a.filing_date,
                a.filing_hour,
                a.form_code,
                a.proof_code,
                a.signature_type,
                a.signatory_name,
                a.signatory_position,
                a.source_system,
                b.usually_signed_name_as,
                b.usually_signed_position_as,
                b.usually_signed_type_as,
                b.first_submission_time,
                b.last_submission_time,
                c.total_distinct_cases,
                c.total_distinct_cases_considered_by_model,
                c.total_submissions,
                c.total_submissions_considered_by_model,
                d.avg_submissions_per_day,
                d.min_submissions_one_day,
                d.max_submissions_one_day,
                d.avg_submissions_per_day_considered_by_model,
                d.min_submissions_one_day_considered_by_model,
                d.max_submissions_one_day_considered_by_model
            from
                submissions a
                left join signatures b
                    on a.myuspto_patron_id = b.myuspto_patron_id
                left join submissions_aggregated c
                    on a.myuspto_patron_id = c.myuspto_patron_id
                left join daily_submissions d
                    on a.myuspto_patron_id = d.myuspto_patron_id
        )
        select
            a.myuspto_patron_id,
            a.account_patron_nickname,
            a.account_patron_name,
            a.account_email,
            a.selected_role,
            b.applicant_bin,
            a.account_status,
            (b.myuspto_patron_id is not null) = true is_on_anomaly_list,
            b.latest_anomaly_score,
            b.times_appeared,
            b.first_appeared,
            b.last_appeared,
            c.usually_signed_name_as,
            c.usually_signed_position_as,
            c.usually_signed_type_as,
            c.first_submission_time,
            c.last_submission_time,
            d.has_sponsored,
            d.has_been_sponsored_by,
            nvl(c.total_distinct_cases, 0) total_distinct_cases,
            nvl(c.total_distinct_cases_considered_by_model, 0) total_distinct_cases_considered_by_model,
            nvl(c.total_submissions, 0) total_submissions,
            nvl(c.total_submissions_considered_by_model, 0) total_submissions_considered_by_model,
            nvl(c.min_submissions_one_day, 0) min_submissions_one_day,
            nvl(c.avg_submissions_per_day, 0) avg_submissions_per_day,
            nvl(c.max_submissions_one_day, 0) max_submissions_one_day,
            nvl(c.min_submissions_one_day_considered_by_model, 0) min_submissions_one_day_considered_by_model,
            nvl(c.avg_submissions_per_day_considered_by_model, 0) avg_submissions_per_day_considered_by_model,
            nvl(c.max_submissions_one_day_considered_by_model, 0) max_submissions_one_day_considered_by_model,
            c.transaction_id,
            c.serial_number,
            c.registration_number,
            '{exam_link}' || c.serial_number tm_exam_link,
            c.submission_time,
            c.submission_date,
            c.submission_hour,
            c.filing_time,
            c.filing_date,
            c.filing_hour,
            c.form_code,
            c.proof_code,
            c.signature_type,
            c.signatory_name,
            c.signatory_position,
            c.source_system
        from
            vw_dim_patron_account a
        left join vw_anomaly_list b
            on a.myuspto_patron_id = b.myuspto_patron_id
        left join aggregations c
            on a.myuspto_patron_id = c.myuspto_patron_id
        left join vw_fact_sponsorship d
            on a.myuspto_patron_id = d.myuspto_patron_id
    """
    )
    .withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit("ANOMALY_REPORT_DASHBOARD_ETL"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit("ANOMALY_REPORT_DASHBOARD_ETL"))
)

# COMMAND ----------

# DBTITLE 1,Write: patron_overview (Overwrite)
log.info("Writing patron_overview (overwrite)")

patron_overview_df.write.mode("overwrite").option("overwriteSchema", "true").format(
    "delta"
).saveAsTable(f"{reporting_catalog}.gold.patron_overview")

log.info("patron_overview write complete")

# COMMAND ----------

# DBTITLE 1,OPTIMIZE: patron_overview
log.info("Optimizing patron_overview")
display(spark.sql(f"optimize {reporting_catalog}.gold.patron_overview"))

# COMMAND ----------

# DBTITLE 1,Analyze: Compute Statistics for patron_overview
display(spark.sql(f"analyze table {reporting_catalog}.gold.patron_overview compute statistics for all columns"))
