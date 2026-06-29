# Databricks notebook source
# DBTITLE 1,Imports
import pytz
from pytz import timezone
import datetime

# COMMAND ----------

# DBTITLE 1,Set Runtime Environment, Configuration File, Job Control
dbutils.widgets.text("dbx_env", "dev")
dbutils.widgets.text("procedure_category_code", "ZERO_COUNT_CHECK")

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
procedure_category_code = dbutils.widgets.get("procedure_category_code").rstrip()
env = dbx_env.upper()

job_name = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
    .split("/")[-1]
)

job_start_ts = datetime.datetime.now().astimezone(pytz.timezone("US/Eastern"))
src_sys_name = "TDET_SEARCH"

config_file_name = "tdet-conf.yaml"
config_file = f"../config/{dbx_env}/{config_file_name}"
print(f"{config_file=} {job_name=} {job_start_ts=}")

# COMMAND ----------

# DBTITLE 1,Get Common Functions and Parameters
# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,Read Configuration File
common_configs = read_yaml(config_file)
data_quality_catalog = common_configs["schema"]["data_quality_catalog"]
tdet_catalog = common_configs["schema"]["trgt_catalog"]
email_id = common_configs["alerting"]["email"]
tmngpdb_catalog = common_configs["schema"]["source_tmngpdb_catalog"]

print(f"{data_quality_catalog=} {src_sys_name=} {tmngpdb_catalog=} {procedure_category_code=} {email_id=}")

# COMMAND ----------

# DBTITLE 1,Set Environment
spark.conf.set("config.data_quality_catalog", data_quality_catalog)
spark.conf.set("config.tmngpdb_catalog", tmngpdb_catalog)
spark.conf.set("config.tdet_catalog", tdet_catalog)
spark.conf.set("config.src_sys_name", src_sys_name)
spark.conf.set("config.dbx_env", dbx_env)

# COMMAND ----------

# DBTITLE 1,Insert Job Control
begin_job_cntl(
    ctlg_db_name=f"{tdet_catalog}.silver", job_name=job_name, job_start_ts=job_start_ts
)

# COMMAND ----------

# DBTITLE 1,Collect TDET DQ Procedures
try:
    dq_procs = spark.sql(
        f"""
    select
        cpdr.PROC_ID,
        cpdr.PROC_CTGRY_CD, 
        cdvqr.SRC_SYS_NAME,
        cpvqa.PROC_NAME,
        cpvqa.QUERY_SET_ID,
        cpvqa.QUERY_DQ_CD,
        cdvqr.QUERY_TEXT,
        cdvqr.QUERY_NAME,
        cpvqa.ERR_THRSHLD_PCT
    from
        {data_quality_catalog}.silver.cmn_dq_vrfctn_query_rfrnc cdvqr
        join {data_quality_catalog}.silver.cmn_proc_vrfctn_query_asctn cpvqa on cdvqr.QUERY_NAME = cpvqa.TRGT_QUERY_NAME
        join {data_quality_catalog}.silver.cmn_proc_defn_rfrnc cpdr on cpdr.PROC_NAME = cpvqa.PROC_NAME
    where
        cpvqa.SRC_SYS_NAME = '{src_sys_name}' 
        and cpdr.PROC_CTGRY_CD = '{procedure_category_code}'
    order by 
        cpvqa.QUERY_SET_ID
    """
    ).collect()
    num_dq_procs = len(dq_procs)
    spark.conf.set("config.num_dq_procs", num_dq_procs)
    print(f"{num_dq_procs} DQ verification procedures will be executed:\n")
    for proc in dq_procs:
        print(f"{proc.PROC_ID}: {proc.PROC_NAME}")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{tdet_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Iterate Through Each DQ Procedure, Insert Each Result
try:
    for proc in dq_procs:
        proc_id = proc.PROC_ID
        proc_name = proc.PROC_NAME
        proc_query_set_id = proc.QUERY_SET_ID
        proc_query_dq_code = proc.QUERY_DQ_CD
        proc_query_name = proc.QUERY_NAME
        proc_category_code = proc.PROC_CTGRY_CD
        proc_error_threshold_pct = proc.ERR_THRSHLD_PCT
        proc_result = spark.sql(proc.QUERY_TEXT).collect()[0].QRY_CNT

        print(f"{proc_id=}")
        print(f"-- {proc_name=}")
        print(f"-- {proc_query_set_id=}")
        print(f"-- {proc_query_dq_code=}")
        print(f"-- {proc_category_code=}")
        print(f"-- {proc_error_threshold_pct=}")
        print(f"-- {proc_query_name=}")
        print(f"-- {proc_result=}")
        
        display(
            spark.sql(
                f"""
                insert into
                    {data_quality_catalog}.silver.cmn_proc_vrfctn_rslt (
                    PROC_ID,
                    PROC_NAME,
                    PROC_CTGRY_CD,
                    QUERY_SET_ID,
                    QUERY_DQ_CD,
                    SRC_QUERY_NAME,
                    TRGT_QUERY_NAME,
                    JOB_LOG_ID,
                    JOB_START_TS,
                    RPTD_SRC_RSLT_CNT,
                    RPTD_TRGT_RSLT_CNT,
                    ERR_THRSHLD_PCT,
                    RPTD_VRNC_PCT,
                    DQ_RSLT_MSG,
                    AUDT_INSRT_ID,
                    AUDT_INSRT_TS,
                    SRC_SYS_NAME
                )
                select
                    PROC_ID,
                    PROC_NAME,
                    PROC_CTGRY_CD,
                    QUERY_SET_ID,
                    QUERY_DQ_CD,
                    SRC_QUERY_NAME,
                    TRGT_QUERY_NAME,
                    JOB_LOG_ID,
                    JOB_START_TS,
                    RPTD_SRC_RSLT_CNT,
                    RPTD_TRGT_RSLT_CNT,
                    ERR_THRSHLD_PCT,
                    RPTD_VRNC_PCT,
                    DQ_RSLT_MSG,
                    AUDT_INSRT_ID,
                    AUDT_INSRT_TS,
                    SRC_SYS_NAME
                from
                (
                    select
                        procedure_results.*,
                        case
                            when RPTD_VRNC_PCT > ERR_THRSHLD_PCT then 'Y'
                            else 'N'
                        end as VRNC_IND
                    from
                    (
                        select
                            '{proc_id}' as PROC_ID,
                            '{proc_name}' as PROC_NAME,
                            '{proc_category_code}' as PROC_CTGRY_CD,
                            '{proc_query_set_id}' as QUERY_SET_ID,
                            '{proc_query_dq_code}' as QUERY_DQ_CD,
                            null as SRC_QUERY_NAME,
                            '{proc_query_name}' as TRGT_QUERY_NAME,
                            null as JOB_LOG_ID,
                            from_utc_timestamp(current_timestamp(), 'America/New_York') as JOB_START_TS,
                            0 as RPTD_SRC_RSLT_CNT,
                            '{proc_result}' as RPTD_TRGT_RSLT_CNT,
                            cast('{proc_error_threshold_pct}' as double) as ERR_THRSHLD_PCT,
                            case
                                when RPTD_TRGT_RSLT_CNT > 0 then 100
                                else 0
                            end as RPTD_VRNC_PCT,
                            case
                                when RPTD_TRGT_RSLT_CNT < 1 then 'No duplicates found in data during distinct count verification'
                                else 'Duplicates found in data during distinct count verification'
                            end AS DQ_RSLT_MSG,
                            'ETL' AS AUDT_INSRT_ID,
                            from_utc_timestamp(current_timestamp(), 'America/New_York') as AUDT_INSRT_TS,
                            '{src_sys_name}' as SRC_SYS_NAME
                    ) as procedure_results
                )
                """
            )
        )
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{tdet_catalog}.silver", job_name, job_start_ts, 'failed', 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Send Email for DQ Verification that Exceeded Threshold, Update Job Control
try:
    df_dq_query_counts_variance = spark.sql(
    """
        select
            *
            except
            (ERR_THRSHLD_PCT)
        from
        (
            select
                SRC_SYS_NAME,
                PROC_CTGRY_CD,
                SRC_QUERY_NAME as TRGT_TABLE_NAME,
                RPTD_SRC_RSLT_CNT,
                RPTD_TRGT_RSLT_CNT,
                RPTD_VRNC_PCT,
                ERR_THRSHLD_PCT,
                JOB_START_TS
            from
                ${config.data_quality_catalog}.silver.cmn_proc_vrfctn_rslt
            where
                SRC_SYS_NAME = '{src_sys_name}' 
                and PROC_CTGRY_CD = '{procedure_category_code}'
            order by
                job_start_ts desc
            limit
                ${config.num_dq_procs}
        )
        where
            RPTD_VRNC_PCT > ERR_THRSHLD_PCT
    """
    )
    num_exceeded_dq_checks = df_dq_query_counts_variance.count()

except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{tdet_catalog}.silver", job_name, job_start_ts,'failed',0,e)
    raise

# COMMAND ----------

try:
    if num_exceeded_dq_checks > 0:
        Appdf = df_dq_query_counts_variance
        params = {}
        pd.set_option("display.max_colwidth", 0)
        params["INDEXED"] = Appdf.toPandas().to_html()
        notify = Notify()
        templ_str = f"[TEST] {src_sys_name}: {procedure_category_code} Data Quality Report"
        msg = notify.compose_email(
            templ_str,
            f"{src_sys_name}: {procedure_category_code} Data Quality Report - {env}",
            email_id,
            params,
        )
        notify.send_mail(msg)
        raise ValueError(f"Expected 0 DQ checks to fail. Instead, found {num_exceeded_dq_checks}")
    else:
        print(
            f"No email notification sent for data variance since value counts for {tdet_catalog} {procedure_category_code} do not exceed threshold."
        )
        end_job_cntl(
            f"{tdet_catalog}.silver",
            job_name,
            job_start_ts,
            "completed",
            num_exceeded_dq_checks,
            "Job success: all DQ checks have passed."
        )
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{tdet_catalog}.silver", job_name, job_start_ts,'failed',0,e)
    raise

# COMMAND ----------

# DBTITLE 1,Exit Notebook
dbutils.notebook.exit(f"Completed data verification for {procedure_category_code} in {tdet_catalog}.")
