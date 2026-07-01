# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import when

# COMMAND ----------

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
run_env = common_configs["schema"]["tmngpdb_src_catalog"]
edw_scope = common_configs["secrets"]["edw_scope"]
email_id = common_configs["alerting"]["fee_issue_alert"]["email"]
print(reporting_catalog, run_env, edw_scope, email_id)

# COMMAND ----------

# DBTITLE 1,Set Job Control
job_name = "ntb_trmreports_fee_issue_alert_trigger"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# DBTITLE 1,Milestone Input
try:
    input_58 = spark.sql(
        f"""
        select
            *
        from
            {reporting_catalog}.silver.milestone
        """
    )

    select_59 = input_58.select(
        [
            "ser_num",
            "first_action_dt_ph",
            "am_1_actn_ct_dt",
            "first_action_type",
            "filing_dt",
            "ib_notification_dt",
            "published_dt",
            "noa_dt",
            "abandonment_dt",
            "aban_dt_ph",
            "registration_dt",
            "disposal_type",
            "ext1_dt",
            "ext2_dt",
            "ext3_dt",
            "ext4_dt",
            "ext5_dt",
            "cancellation_dt",
            "renewal_dt",
            "revival_dt",
            "susp_check_dt",
            "am_cls_ct_actv",
            "pendency_cal_start_dt",
            "pendency_cal_end_dt",
            "noa_registration_check",
            "wgtd_1st_actn_pendency",
            "first_action_cd",
            "disposal_pendency",
            "suspension",
            "ttab",
            "disposal_dt",
            "dock_dt",
            "am_flg_66a_cur",
            "am_flg_66a_fil",
            "noa_dt_ph",
            "filing_fy",
            "non_pro_se",
            "first_action_pendency_ph",
            "last_modified_date",
            "processing_pend",
            "processing_pend_days",
        ]
    )
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Max Filing Year Milestone
try:
    summarize_39 = input_58.selectExpr("max(filing_fy) as max_filing_fy")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Join Max Filing Year Milestone
try:
    join_40 = (
        select_59.alias("right")
        .join(
            other=summarize_39.alias("left"),
            on=[col("right.filing_fy") == col("left.max_filing_fy")],
            how="inner",
        )
        .selectExpr("* except(right.filing_fy)")
    )
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Bibliography Input
try:
    input_54 = spark.sql(
        f"""select 
                * 
            from 
                {reporting_catalog}.silver.bibliography 
            where 
                filing_method_filed not in ('6 TER', 'MADRID') 
                and AM_STAT <> 622
        """
    )
    select_56 = input_54.select(
        [
            "ser_num",
            # "test_pctram_link",
            "law_office",
            "filing_basis_cur",
            "filing_method_filed",
            "filing_method_cur",
            "filing_basis_fil",
            "filing_basis_amed",
            "registration_number",
            "am_flg_66a_fil",
            "am_flg_44d_fil",
            "am_flg_44e_fil",
            "flg_paper_fil",
            "am_stat",
            "am_flg_no_bas_fil",
            "am_flg_teasrf_fil",
            "am_flg_use_fil",
            "am_flg_itu_fil",
            "am_flg_teaspl_fil",
            "last_modified_date",
            "filing_basis_grp",
            "mark_dwg_cd",
            "mark_dwg_desc",
            "mark_nm_short",
            "mark_nm",
            "tmng_image_link",
            "tm_analytics_ts",
            "exmr_eid",
        ]
    )
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Inner Join Bibliography to Milestone
try:
    join_9 = (
        join_40.alias("left")
        .join(
            other=select_56.alias("right"),
            on=[col("left.ser_num") == col("right.ser_num")],
            how="inner",
        )
        .selectExpr(
            """
            * except(
                left.ser_num, 
                left.last_modified_date, 
                left.ib_notification_dt
            )
            """
        )
    )
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Divisionals Input
try:
    input_50 = spark.sql(
        f"""
        select
            ser_num,
            filing_dt,
            ib_notification_dt,
            dv_type,
            ref_ser_num,
            dv_dt_rqst,
            dv_dt_complete,
            last_modified_date,
            trans_dt
        from
            {reporting_catalog}.silver.divisionals
        where
            DV_TYPE = 'CHILD'
        """
    )
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

try:
    select_51 = input_50.select(
        [
            "ser_num",
            "filing_dt",
            "ib_notification_dt",
            "dv_type",
            "ref_ser_num",
            "dv_dt_rqst",
            "dv_dt_complete",
            "last_modified_date",
            "trans_dt",
        ]
    )
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Left Anti-Join Bibliography and Milestone to Divisionals
try:
    join_16 = (
        join_9.alias("left")
        .join(
            other=select_51.alias("right"),
            on=[col("left.ser_num") == col("right.ser_num")],
            how="left_anti",
        )
    )
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Filings Dashboard Input
try:
    input_48 = spark.sql(
        f"""
        select
            *
        from
            {reporting_catalog}.gold.filings_dashboard
        """
    )

    select_49 = input_48.select(
        [
            "ser_num",
            "pendency_cal_start_dt",
            "filing_fy",
            "non_pro_se",
            "filing_method_filed",
            "filing_basis_grp",
            "class",
            "name",
            "city",
            "ste_ctry_cd",
            "postal_cd",
            "ctry_nm",
            "country_or_area_name",
            "count",
            "max_pendency_cal_start_dt",
            "coordinated_class",
            "filing_fy2",
            "filing_fy_month_int",
            "filing_fy_quarter",
            "filing_fy_month",
            "top_2_years",
            "fee_paid_class",
            "max_filing_fy",
            # "pctram_link",
            "fixed_count",
            "realtime_count",
            "tram_count",
            "goods_or_services",
            "concat_goods_or_services",
            "entity_type",
            "applicant_bin",
        ]
    )

    summarize_45 = select_49.selectExpr("max(Filing_FY) as max_filing_fy")

    join_46 = select_49.alias("right").join(
        other=summarize_45.alias("left"),
        on=[col("left.max_filing_fy") == col("right.filing_fy")],
        how="inner",
    )
    
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Sum Fixed and Realtime Counts from Filings Dashboard
try:
    summarize_43 = join_46.selectExpr(
        "sum(fixed_count) as sum_fixed_count", "sum(realtime_count) as sum_realtime_count"
    )
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Calculate Test Column of Filings Dashboard
try:
    formula_44 = summarize_43.selectExpr(
        """
            case
                when (sum_realtime_count - sum_fixed_count) / sum_realtime_count >= 0.025 
                    then 1 
                    else 0 
            end as test
        """
    )
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Forecast View Input
try:
    input_2 = read_data_from_oracle_conn_dsu(
        sql_query="""
            SELECT 
                -- FORECAST.VW_TM_SALE_TRAN.* 
                FORECAST.VW_TM_SALE_TRAN.PSTNG_REF_TX,
                FORECAST.VW_TM_SALE_TRAN.PRJCT_CD,
                FORECAST.VW_TM_SALE_TRAN.ACCTG_DT,
                REV_SRC_CD
            FROM 
                FORECAST.VW_TM_SALE_TRAN
        """,
        schema_name="",
        secrets_name=edw_scope,
    )
    filter_1 = input_2.where(
        """
        REV_SRC_CD IN (
            '6001',
            '7001',
            '7007',
            '7009',
            '7931',
            '7933'
        )
        """
    )
    formula_3 = filter_1.selectExpr(
        """
        case
            when regexp_count(PRJCT_CD, '[a-zA-Z]') > 0 
            then PSTNG_REF_TX
            else PRJCT_CD
        end as PRJCT_CD
        """
    )
    formula_3.cache()
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Right Anti-Join Forecast to Divionals/Bibliography/Milestone
try:
    join_5 = (
        formula_3.alias("left")
        .join(
            other=join_16.alias("right"),
            on=[col("left.prjct_cd") == col("right.ser_num")],
            how="right",
        )
        .where("left.prjct_cd is null")
    )
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Count Serial Number of Join
try:
    summarize_17 = join_5.selectExpr("count(right.ser_num) as count")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Append Test Result to Serial Number Count
try:
    append_fields_35 = formula_44.join(other=summarize_17, how="cross")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Determine Continue Process Flag
try:
    formula_19 = append_fields_35.withColumn(
        "continue_process", when((col("count") > 100) | (col("test") == 1), 1).otherwise(0)
    )
    formula_19.cache()
    formula_19_result = formula_19.collect()[0]
    continue_process = formula_19_result.continue_process
    display(formula_19)
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Send Email Condition
try:
    send_email = True if continue_process > 0 else False
    if send_email:
        email_body_counts = f'Test result: {formula_19_result["test"]}, Total record count: {formula_19_result["count"]}'

        email_subj = "Fee Issue Alert Triggered"
        email_body = "<strong>Fee Issue Alert Summary<br><br>" + email_body_counts + '</strong>'
        from_addr = 'Trademark_Analytics@uspto.gov'

        send_email_report(
            job_nm = job_name,
            subject = email_subj,
            send_from = from_addr,
            send_to = email_id,
            html_body= email_body
        )
    else:
        print(
            f"""
            No condition has been met to send an alert based on the following results:
            - Test result `{formula_19_result["test"]}` is not equal to Test Result 1.
            - Record count `{formula_19_result["count"]}` does not exceed the 100 record threshold.
        """
        )
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

end_job_cntl(
    f"{reporting_catalog}.silver",
    job_name,
    job_start_ts,
    "completed",
    formula_19_result["count"],
    "job completed successfully",
)
