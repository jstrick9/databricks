# Databricks notebook source
# MAGIC %md
# MAGIC # Anomaly ETL: Silver Layer
# MAGIC
# MAGIC Loads bronze, silver, and gold source data into shared temp views, builds input DataFrames,
# MAGIC and writes three silver Delta tables using SCD2 merge logic for slowly-changing
# MAGIC dimensions and an overwrite strategy for the append fact table.
# MAGIC
# MAGIC ### Sources
# MAGIC | Schema | Table |
# MAGIC |---|---|
# MAGIC | `trm_jbteasps.bronze` | `audit_log`, `sponsorship`, `stnd_source_system`, `interested_party` |
# MAGIC | `tm_practitioner.bronze` | `dim_patron` |
# MAGIC | `tm_practitioner.silver` | `dim_account` |
# MAGIC | `trm_reporting.gold` | `unsupervised_anomalies`, `unsupervised_anomalies_cumulative` |
# MAGIC | `myuspto.silver` | `interested_party`, `login_user` |
# MAGIC
# MAGIC ### Silver Tables Written
# MAGIC | Table | Strategy | SCD |
# MAGIC |---|---|---|
# MAGIC | `silver.fact_submission` | Overwrite (full refresh) | No |
# MAGIC | `silver.dim_patron_signature` | Overwrite (full refresh) | No |
# MAGIC | `silver.dim_patron_account` | SCD2 MERGE: expire changed rows, insert new version | **Yes** |
# MAGIC | `silver.fact_sponsorship` | SCD2 MERGE: expire changed rows, insert new version | **Yes** |
# MAGIC
# MAGIC ### SCD2 Pattern
# MAGIC 1. Match on natural key (`myuspto_patron_id`) where `is_current = true`.
# MAGIC 2. When a matched row has changed business attributes, set `effective_end_ts = now()`, `is_current = false`.
# MAGIC 3. Insert the new version with `effective_start_ts = now()`, `effective_end_ts = null`, `is_current = true`.
# MAGIC 4. When no match, insert as new current row.

# COMMAND ----------

# DBTITLE 1,Environment & Config
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")

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
    col,
    current_timestamp,
    lit,
    date_format,
    concat_ws,
    collect_set,
    array_join,
    coalesce,
)
from delta.tables import DeltaTable

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
log = logging.getLogger("anomaly_silver_etl")
log.setLevel(logging.INFO)

# COMMAND ----------

# DBTITLE 1,Catalog & Schema Variables
common_configs = read_yaml(config_file)
reporting_catalog = target_catalog = common_configs["schema"]["trm_reporting_catalog"]
tmngpdb_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
tmintltm_catalog = common_configs["schema"]["tmintltm_src_catalog"]
jbteasps_catalog = common_configs["schema"]["trm_jbteasps_src_catalog"]
tm_practitioner_catalog = common_configs["schema"]["tm_practitioner_catalog"]
myuspto_catalog = common_configs["schema"]["myuspto_catalog"]
run_env = dbx_env

print(f"{target_catalog=}, {tmngpdb_catalog=}, {tmintltm_catalog=}, {jbteasps_catalog=}, {tm_practitioner_catalog=}, {myuspto_catalog=}, {run_env=}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Views

# COMMAND ----------

# DBTITLE 1,View: vw_cases_and_registrations
log.info("Creating vw_cases_and_registrations")
spark.sql(f"""
create or replace temp view cases_and_registrations as
with international_reference_numbers as (
  select
    dn_serial_num serial_number,
    international_us_ref_no
  from
    {tmintltm_catalog}.bronze.international_application a
      join {tmintltm_catalog}.bronze.base_application b
        on a.international_application_gid = b.fk_international_appl_gid
)
select distinct
  a.serial_num_tx serial_number,
  registration_num registration_number,
  international_us_ref_no reference_number
from
  {tmngpdb_catalog}.bronze.trademark a
    left join international_reference_numbers b
      on a.serial_num_tx = b.serial_number
""")

# COMMAND ----------

# DBTITLE 1,View: vw_audit_log_submissions
log.info("Creating vw_audit_log_submissions")
spark.sql(f"""
    create or replace temp view vw_audit_log_submissions as
    select
        a.cfk_patron_id myuspto_patron_id,
        a.dn_patron_first_nm, 
        a.dn_patron_middle_nm, 
        a.dn_patron_last_nm,
        a.dn_patron_email_address_tx,
        a.dn_patron_tm_role selected_role,
        max(coalesce(a.serial_no, b.serial_number)) serial_number,
        try_cast(max(coalesce(a.registration_no, b.registration_number)) as bigint) registration_number,
        a.audit_log_id transaction_id,
        a.create_ts submission_time,
        date(a.create_ts) submission_date,
        hour(a.create_ts) submission_hour,
        a.filing_dt filing_time,
        date(a.filing_dt) filing_date,
        hour(a.filing_dt) filing_hour,
        a.fk_form_cd form_code,
        a.fk_proof_cd proof_code,
        a.fk_signature_type_cd signature_type,
        a.signatory_nm signatory_name,
        a.signatory_position_nm signatory_position,
        a.fk_source_system_id,
        a.fk_transaction_type_cd transaction_type
    from {jbteasps_catalog}.bronze.audit_log a
    left join cases_and_registrations b
        on (
            a.serial_no = b.serial_number 
            or a.registration_no = b.registration_number 
            or a.reference_no = b.reference_number
        )
    where 
      cfk_patron_id like '%-%-%-%-%'
      and fk_form_cd != 'WOA'
      and fk_transaction_type_cd = 'Submission'
    group by 
        all
""")

# COMMAND ----------

# DBTITLE 1,View: vw_account_status
log.info("Creating vw_account_status")
spark.sql(f"""
    create or replace temp view vw_account_status as
    select
        b.cfk_patron_id myuspto_patron_id,
        'deactivated' account_status
    from
        {myuspto_catalog}.bronze.login_user a 
    join {myuspto_catalog}.bronze.interested_party b 
        on a.fk_interested_party_id = b.interested_party_id 
    where
        a.account_activated_in in ('D', 'F')
        and b.cfk_patron_id is not null
""")

# COMMAND ----------

# DBTITLE 1,View: vw_patron_account
log.info("Creating vw_patron_account")
spark.sql(f"""
    create or replace temp view vw_patron_account as
    with known as ( 
    select
        lower(patron_id) myuspto_patron_id,
        max_by(nickname_nm, dim_patron_id) account_patron_nickname,
        max_by(initcap(concat_ws(' ', given_nm, middle_nm, family_nm)), dim_patron_id) account_patron_name,
        max_by(electronic_addr_locator_tx, dim_patron_id) account_email,
        null selected_role
    from {tm_practitioner_catalog}.bronze.dim_patron
    group by all
    ),
    assumed as (
    select
        myuspto_patron_id,
        max_by(initcap(concat_ws(' ', dn_patron_first_nm, dn_patron_middle_nm, dn_patron_last_nm)), transaction_id) account_patron_nickname,
        max_by(initcap(concat_ws(' ', dn_patron_first_nm, dn_patron_middle_nm, dn_patron_last_nm)), transaction_id) account_patron_name,
        max_by(dn_patron_email_address_tx, transaction_id) account_email,
        max_by(selected_role, transaction_id) selected_role
    from vw_audit_log_submissions
    group by all
    )
    select 
        coalesce(a.myuspto_patron_id, b.myuspto_patron_id) myuspto_patron_id,
        coalesce(a.account_patron_nickname, b.account_patron_nickname) account_patron_nickname,
        coalesce(a.account_patron_name, b.account_patron_name) account_patron_name, 
        coalesce(a.account_email, b.account_email) account_email,
        b.selected_role selected_role
    from known a
    full outer join assumed b
        on a.myuspto_patron_id = b.myuspto_patron_id 
""")

# COMMAND ----------

# DBTITLE 1,View: vw_interested_party
log.info("Creating vw_interested_party")
spark.sql(f"""
    create or replace temp view vw_interested_party as
    select distinct
        cfk_patron_id myuspto_patron_id,
        selected_role_nm
    from {jbteasps_catalog}.bronze.interested_party
""")

# COMMAND ----------

# DBTITLE 1,View: vw_anomalies_latest
log.info("Creating vw_anomalies_latest")
spark.sql(f"""
    create or replace temp view vw_anomalies_latest as
    select cfk_patron_id myuspto_patron_id, applicant_bin
    from {reporting_catalog}.gold.unsupervised_anomalies_cumulative
    where load_date = (select max(load_date) load_date from {reporting_catalog}.gold.unsupervised_anomalies_cumulative)
""")

# COMMAND ----------

# DBTITLE 1,View: vw_sponsorship_outbound
log.info("Creating vw_sponsorship_outbound")
spark.sql(f"""
    create or replace temp view vw_sponsorship_outbound as
    select distinct
        cfk_sponsorer_id as myuspto_patron_id,
        sort_array(collect_set(cfk_sponsoree_id) over (partition by cfk_sponsorer_id)) as has_sponsored
    from {jbteasps_catalog}.bronze.sponsorship
""")

# COMMAND ----------

# DBTITLE 1,View: vw_sponsorship_inbound
log.info("Creating vw_sponsorship_inbound")
spark.sql(f"""
    create or replace temp view vw_sponsorship_inbound as
    select distinct
        cfk_sponsoree_id as myuspto_patron_id,
        sort_array(collect_set(cfk_sponsorer_id) over (partition by cfk_sponsoree_id)) as has_been_sponsored_by
    from {jbteasps_catalog}.bronze.sponsorship
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input DataFrames

# COMMAND ----------

# DBTITLE 1,submissions_df: base submission fact
log.info("Loading submissions_df")
submissions_df = spark.sql("select * from vw_audit_log_submissions")

# COMMAND ----------

# DBTITLE 1,anomalies_df: current anomaly list joined to latest cumulative score
log.info("Loading anomalies_df")
anomalies_df = spark.sql(f"""
    select
        a.cfk_patron_id myuspto_patron_id,
        a.latest_anomaly_score,
        a.times_appeared,
        a.first_appeared,
        a.last_appeared,
        b.applicant_bin
    from {reporting_catalog}.gold.unsupervised_anomalies a
    inner join vw_anomalies_latest b
        on a.cfk_patron_id = b.myuspto_patron_id
""")

# COMMAND ----------

# DBTITLE 1,patron_dim_df: account info + status + role
log.info("Loading patron_dim_df")
patron_dim_df = spark.sql(f"""
    select
        a.myuspto_patron_id,
        a.account_patron_nickname,
        a.account_patron_name,
        a.account_email,
        coalesce(a.selected_role, b.selected_role_nm, 'Role Information Not Available') as selected_role,
        nvl(c.account_status, 'active') as account_status
    from vw_patron_account a
    left join vw_interested_party b
        on a.myuspto_patron_id = b.myuspto_patron_id
    left join vw_account_status c
        on a.myuspto_patron_id = c.myuspto_patron_id
""")

# COMMAND ----------

# DBTITLE 1,sponsorship_df: combined sponsor / sponsored-by per patron
log.info("Loading sponsorship_df")
sponsorship_df = spark.sql("""
    select
        coalesce(b.myuspto_patron_id, a.myuspto_patron_id) as myuspto_patron_id,
        a.has_sponsored,
        b.has_been_sponsored_by
    from vw_sponsorship_outbound a
    full outer join vw_sponsorship_inbound b
        on b.myuspto_patron_id = a.myuspto_patron_id
""")

# COMMAND ----------

# DBTITLE 1,patron_signature: aggregated signature information by patron
log.info("Loading patron_signature")

patron_signature_df = (
    spark.sql(f"""
    with names as (
        select
            myuspto_patron_id,
            signatory_name,
            count(*) num_names,
            max(submission_time) name_last_seen
        from vw_audit_log_submissions
        where signatory_name is not null
        group by myuspto_patron_id, signatory_name
    ),
    positions as (
        select
            myuspto_patron_id,
            signatory_position,
            count(*) num_positions,
            max(submission_time) position_last_seen
        from vw_audit_log_submissions
        where signatory_position is not null
        group by myuspto_patron_id, signatory_position
    ),
    types as (
        select
            myuspto_patron_id,
            signature_type,
            count(*) num_types,
            max(submission_time) type_last_seen
        from vw_audit_log_submissions
        where signature_type is not null
        group by myuspto_patron_id, signature_type
    ),
    modal_name as (
        select myuspto_patron_id, signatory_name as usually_signed_name_as
        from names
        qualify row_number() over (partition by myuspto_patron_id order by num_names desc, name_last_seen desc) = 1
    ),
    modal_position as (
        select myuspto_patron_id, signatory_position as usually_signed_position_as
        from positions
        qualify row_number() over (partition by myuspto_patron_id order by num_positions desc, position_last_seen desc) = 1
    ),
    modal_type as (
        select myuspto_patron_id, signature_type as usually_signed_type_as
        from types
        qualify row_number() over (partition by myuspto_patron_id order by num_types desc, type_last_seen desc) = 1
    ),
    aggregated as (
        select
            myuspto_patron_id,
            collect_set(signatory_name) signed_name_as,
            collect_set(signatory_position) signed_position_as,
            collect_set(signature_type) signed_type_as,
            min(submission_time) first_submission_time,
            max(submission_time) last_submission_time
        from vw_audit_log_submissions
        group by myuspto_patron_id
    ),
    name_map as (
        select
            myuspto_patron_id,
            map_from_entries(collect_list(struct(signatory_name as key, num_names as value))) signed_name_counts
        from names
        group by myuspto_patron_id
    ),
    position_map as (
        select
            myuspto_patron_id,
            map_from_entries(collect_list(struct(signatory_position as key, num_positions as value))) signed_position_counts
        from positions
        group by myuspto_patron_id
    ),
    type_map as (
        select
            myuspto_patron_id,
            map_from_entries(collect_list(struct(signature_type as key, num_types as value))) signed_type_counts
        from types
        group by myuspto_patron_id
    )
    select
        a.myuspto_patron_id,
        b.signed_name_counts,
        e.usually_signed_name_as,
        c.signed_position_counts,
        f.usually_signed_position_as,
        d.signed_type_counts,
        g.usually_signed_type_as,
        a.first_submission_time,
        a.last_submission_time
    from aggregated a
    left join name_map b 
        on a.myuspto_patron_id = b.myuspto_patron_id
    left join position_map c 
        on a.myuspto_patron_id = c.myuspto_patron_id
    left join type_map d
        on a.myuspto_patron_id = d.myuspto_patron_id
    left join modal_name e 
        on a.myuspto_patron_id = e.myuspto_patron_id
    left join modal_position f 
        on a.myuspto_patron_id = f.myuspto_patron_id
    left join modal_type g
        on a.myuspto_patron_id = g.myuspto_patron_id
    """
    )
    .withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit("ANOMALY_REPORT_DASHBOARD_ETL"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit("ANOMALY_REPORT_DASHBOARD_ETL"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Transformation

# COMMAND ----------

# DBTITLE 1,Add and Enrich: fact_submission
log.info("Building fact_submission")

fact_submission_df = (
    submissions_df.join(
        spark.table(f"{jbteasps_catalog}.bronze.stnd_source_system").select(
            "source_system_id", col("full_nm").alias("source_system")
        ),
        on=[col("fk_source_system_id") == col("source_system_id")],
        how="left",
    )
    .select(
        "myuspto_patron_id",
        "selected_role",
        "transaction_id",
        "serial_number",
        "registration_number",
        "submission_time",
        "submission_date",
        "submission_hour",
        "filing_time",
        "filing_date",
        "filing_hour",
        "form_code",
        "proof_code",
        "signature_type",
        "signatory_name",
        "signatory_position",
        "source_system",
        "transaction_type",
    )
    .withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit("ANOMALY_REPORT_DASHBOARD_ETL"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit("ANOMALY_REPORT_DASHBOARD_ETL"))
)

# COMMAND ----------

# DBTITLE 1,Add and Enrich: dim_patron_account
log.info("Building dim_patron_account (incoming)")

dim_patron_account_incoming_df = (
    patron_dim_df
    .withColumn("effective_start_ts", current_timestamp())
    .withColumn("effective_end_ts", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))
    .withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit("ANOMALY_REPORT_DASHBOARD_ETL"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit("ANOMALY_REPORT_DASHBOARD_ETL"))
)

# COMMAND ----------

# DBTITLE 1,Add and Enrich: fact_sponsorship
log.info("Building fact_sponsorship (incoming)")

fact_sponsorship_incoming_df = (
    sponsorship_df
    .withColumn("effective_start_ts", current_timestamp())
    .withColumn("effective_end_ts", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))
    .withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit("ANOMALY_REPORT_DASHBOARD_ETL"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit("ANOMALY_REPORT_DASHBOARD_ETL"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver: fact_submission (Overwrite)

# COMMAND ----------

# DBTITLE 1,Write: fact_submission (Overwrite)
log.info("Writing fact_submission (overwrite)")

fact_submission_df.write.mode("overwrite").option("overwriteSchema", "true").format(
    "delta"
).saveAsTable(f"{reporting_catalog}.silver.fact_submission")

log.info("fact_submission write complete")

# COMMAND ----------

# DBTITLE 1,OPTIMIZE: fact_submission
log.info("Optimizing fact_submission")
display(spark.sql(f"optimize {reporting_catalog}.silver.fact_submission"))

# COMMAND ----------

# DBTITLE 1,Analyze: Compute Statistics for fact_submission
display(spark.sql(f"analyze table {reporting_catalog}.silver.fact_submission compute statistics for all columns"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver: dim_signature (Overwrite)

# COMMAND ----------

# DBTITLE 1,Write: dim_patron_signature (Overwrite)
log.info("Writing dim_patron_signature (overwrite)")

patron_signature_df.write.mode("overwrite").option("overwriteSchema", "true").format(
    "delta"
).saveAsTable(f"{reporting_catalog}.silver.dim_patron_signature")

log.info("dim_patron_signature write complete")

# COMMAND ----------

# DBTITLE 1,OPTIMIZE: dim_patron_signature
log.info("Optimizing dim_patron_signature")
display(spark.sql(f"optimize {reporting_catalog}.silver.dim_patron_signature"))

# COMMAND ----------

# DBTITLE 1,Analyze: Compute Statistics for dim_patron_signature
display(spark.sql(f"analyze table {reporting_catalog}.silver.dim_patron_signature compute statistics for all columns"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver: dim_patron_account (SCD2 Merge)

# COMMAND ----------

# DBTITLE 1,SCD2 Merge: dim_patron_account
log.info("SCD2 merge: dim_patron_account")

_dim_patron_target = DeltaTable.forName(
    spark, f"{reporting_catalog}.silver.dim_patron_account"
)

_dim_patron_business_cols = [
    "account_patron_nickname",
    "account_patron_name",
    "account_email",
    "selected_role",
    "account_status",
]

_dim_patron_changed_condition = " or ".join(
    [f"target.{c} is distinct from source.{c}" for c in _dim_patron_business_cols]
)

(
    _dim_patron_target.alias("target")
    .merge(
        dim_patron_account_incoming_df.alias("source"),
        "target.myuspto_patron_id = source.myuspto_patron_id and target.is_current = true",
    )
    .whenMatchedUpdate(
        condition=_dim_patron_changed_condition,
        set={
            "effective_end_ts": "source.effective_start_ts",
            "is_current": "false",
            "update_ts": "source.update_ts",
            "update_user_id": "source.update_user_id",
        },
    )
    .whenNotMatchedInsertAll()
    .execute()
)

_dim_patron_post_merge = spark.read.format("delta").table(
    f"{reporting_catalog}.silver.dim_patron_account"
)

(
    dim_patron_account_incoming_df.alias("source")
    .join(
        _dim_patron_post_merge.alias("target"),
        (col("source.myuspto_patron_id") == col("target.myuspto_patron_id"))
        & (col("target.is_current") == False)
        & (col("target.effective_end_ts") == col("source.effective_start_ts")),
        how="inner",
    )
    .select("source.*")
    .write.format("delta")
    .mode("append")
    .saveAsTable(f"{reporting_catalog}.silver.dim_patron_account")
)

log.info("dim_patron_account SCD2 merge complete")

# COMMAND ----------

# DBTITLE 1,OPTIMIZE: dim_patron_account
log.info("Optimizing dim_patron_account")
display(spark.sql(f"optimize {reporting_catalog}.silver.dim_patron_account"))

# COMMAND ----------

# DBTITLE 1,Analyze: Compute Statistics for dim_patron_account
display(spark.sql(f"analyze table {reporting_catalog}.silver.dim_patron_account compute statistics for all columns"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver: fact_sponsorship (SCD2 Merge)

# COMMAND ----------

# DBTITLE 1,SCD2 Merge: fact_sponsorship
log.info("SCD2 merge: fact_sponsorship")

_sponsorship_target = DeltaTable.forName(
    spark, f"{reporting_catalog}.silver.fact_sponsorship"
)

_sponsorship_business_cols = ["has_sponsored", "has_been_sponsored_by"]

_sponsorship_changed_condition = " or ".join(
    [f"target.{c} is distinct from source.{c}" for c in _sponsorship_business_cols]
)

(
    _sponsorship_target.alias("target")
    .merge(
        fact_sponsorship_incoming_df.alias("source"),
        "target.myuspto_patron_id = source.myuspto_patron_id and target.is_current = true",
    )
    .whenMatchedUpdate(
        condition=_sponsorship_changed_condition,
        set={
            "effective_end_ts": "source.effective_start_ts",
            "is_current": "false",
            "update_ts": "source.update_ts",
            "update_user_id": "source.update_user_id",
        },
    )
    .whenNotMatchedInsertAll()
    .execute()
)

_fact_sponsorship_post_merge = spark.read.format("delta").table(
    f"{reporting_catalog}.silver.fact_sponsorship"
)

(
    fact_sponsorship_incoming_df.alias("source")
    .join(
        _fact_sponsorship_post_merge.alias("target"),
        (col("source.myuspto_patron_id") == col("target.myuspto_patron_id"))
        & (col("target.is_current") == False)
        & (col("target.effective_end_ts") == col("source.effective_start_ts")),
        how="inner",
    )
    .select("source.*")
    .write.format("delta")
    .mode("append")
    .saveAsTable(f"{reporting_catalog}.silver.fact_sponsorship")
)

log.info("fact_sponsorship SCD2 merge complete")

# COMMAND ----------

# DBTITLE 1,OPTIMIZE: fact_sponsorship
log.info("Optimizing fact_sponsorship")
display(spark.sql(f"optimize {reporting_catalog}.silver.fact_sponsorship"))

# COMMAND ----------

# DBTITLE 1,Analyze: Compute Statistics for fact_sponsorship
display(spark.sql(f"analyze table {reporting_catalog}.silver.fact_sponsorship compute statistics for all columns"))
