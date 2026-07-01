# Databricks notebook source
pip install xlsxwriter openpyxl

# COMMAND ----------

# DBTITLE 1,Imports
from io import BytesIO
import pandas as pd
from pyspark.sql.functions import col, countDistinct, datediff, expr, when, sum
import smtplib
from os.path import basename
from email import encoders
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate

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
spark.conf.set("config.reporting_catalog", reporting_catalog)
tmngpdb_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
dq_catalog = common_configs['schema']['data_quality_catalog']
altrx_schema = common_configs['schema']['altrx_schema']
edw_scope = common_configs["secrets"]["edw_scope"]
primary_email, cc_email = common_configs["alerting"]["preexam_fee_checker"]["email"], common_configs["alerting"]["preexam_fee_checker"]["cc"]
print(reporting_catalog, tmngpdb_catalog, edw_scope, primary_email, cc_email)

# COMMAND ----------

# DBTITLE 1,Begin Job
job_name = "ntb_trmreports_preexam_fee_checker"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# DBTITLE 1,Begin ETL
input_121 = read_data_from_oracle_conn_dsu(
    sql_query="""
        SELECT 
            -- FORECAST.VW_TM_SALE_TRAN.* 
            FORECAST.VW_TM_SALE_TRAN.PSTNG_REF_TX,
            FORECAST.VW_TM_SALE_TRAN.PRJCT_CD,
            FORECAST.VW_TM_SALE_TRAN.ACCTG_DT,
            FORECAST.VW_TM_SALE_TRAN.TRAN_AM,
            TRAN_STATUS_CD,
            REV_SRC_CD,
            UNIT_QT
        FROM 
            FORECAST.VW_TM_SALE_TRAN
    """,
    schema_name="",
    secrets_name=edw_scope,
)
filter_95 = input_121.where(
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
formula_143 = filter_95.selectExpr(
    [
        """
        case
            when regexp_count(PRJCT_CD, '[a-zA-Z]') > 0 
            then PSTNG_REF_TX
            else PRJCT_CD
        end as PRJCT_CD
        """,
        "ACCTG_DT",
        "TRAN_AM",
        "TRAN_STATUS_CD",
        "UNIT_QT"
    ]
)
formula_143.cache()

# COMMAND ----------

# DBTITLE 1,Bibliography Input
input_185 = spark.sql(f"select * from {reporting_catalog}.silver.bibliography")
select_186 = input_185.select(
    [
        "SER_NUM",
        "LAW_OFFICE",
        "FILING_BASIS_CUR",
        "FILING_METHOD_FILED",
        "FILING_METHOD_CUR",
        "FILING_BASIS_FIL",
        "FILING_BASIS_AMED",
        "REGISTRATION_NUMBER",
        "AM_FLG_66A_FIL",
        "AM_FLG_44D_FIL",
        "AM_FLG_44E_FIL",
        "FLG_PAPER_FIL",
        "AM_STAT",
        "AM_FLG_NO_BAS_FIL",
        "AM_FLG_TEASRF_FIL",
        "AM_FLG_USE_FIL",
        "AM_FLG_ITU_FIL",
        "AM_FLG_TEASPL_FIL",
        "LAST_MODIFIED_DATE",
        "FILING_BASIS_GRP",
        "MARK_DWG_CD",
        "MARK_DWG_DESC",
        "MARK_NM_SHORT",
        "MARK_NM",
        "TMNG_IMAGE_LINK",
        "TM_ANALYTICS_TS",
        "EXMR_EID",
    ]
)

# COMMAND ----------

# DBTITLE 1,Milestone Input
input_182 = spark.sql(f"select * from {reporting_catalog}.silver.milestone")
select_183 = input_182.select(
    [
        "SER_NUM",
        "first_action_dt_ph",
        "AM_1_ACTN_CT_DT",
        "first_action_type",
        "FILING_DT",
        "IB_NOTIFICATION_DT",
        "PUBLISHED_DT",
        "NOA_DT",
        "ABANDONMENT_DT",
        "ABAN_DT_PH",
        "REGISTRATION_DT",
        "Disposal_Type",
        "EXT1_DT",
        "EXT2_DT",
        "EXT3_DT",
        "EXT4_DT",
        "EXT5_DT",
        "CANCELLATION_DT",
        "RENEWAL_DT",
        "REVIVAL_DT",
        "SUSP_CHECK_DT",
        "AM_CLS_CT_ACTV",
        "Pendency_Cal_Start_DT",
        "Pendency_Cal_End_DT",
        "noa_registration_check",
        "WGTD_1ST_ACTN_PENDENCY",
        "first_action_cd",
        "DISPOSAL_PENDENCY",
        "Suspension",
        "TTAB",
        "Disposal_DT",
        "DOCK_DT",
        "AM_FLG_66A_CUR",
        "AM_FLG_66A_FIL",
        "NOA_DT_PH",
        "Filing_FY",
        "non_pro_se",
        "first_action_pendency_ph",
        "LAST_MODIFIED_DATE",
        "PROCESSING_PEND",
        "PROCESSING_PEND_Days",
    ]
)

# COMMAND ----------

join_174 = (
    select_186.alias("left")
    .join(
        other=select_183.alias("right"),
        on=[col("left.SER_NUM") == col("right.SER_NUM")],
        how="inner",
    )
    .select(
        [
            "left.SER_NUM",
            "left.LAW_OFFICE",
            "left.FILING_BASIS_CUR",
            "left.FILING_METHOD_FILED",
            "left.FILING_METHOD_CUR",
            "left.FILING_BASIS_FIL",
            "left.FILING_BASIS_AMED",
            "left.REGISTRATION_NUMBER",
            "left.AM_FLG_66A_FIL",
            "left.AM_FLG_44D_FIL",
            "left.AM_FLG_44E_FIL",
            "left.FLG_PAPER_FIL",
            "left.AM_STAT",
            "left.AM_FLG_NO_BAS_FIL",
            "left.AM_FLG_TEASRF_FIL",
            "left.AM_FLG_USE_FIL",
            "left.AM_FLG_ITU_FIL",
            "left.AM_FLG_TEASPL_FIL",
            "left.LAST_MODIFIED_DATE",
            "left.FILING_BASIS_GRP",
            "left.MARK_DWG_CD",
            "left.MARK_DWG_DESC",
            "left.MARK_NM_SHORT",
            "left.MARK_NM",
            "left.TMNG_IMAGE_LINK",
            "left.TM_ANALYTICS_TS",
            "left.EXMR_EID",
            col("right.SER_NUM").alias("Right_SER_NUM"),
            "right.first_Action_DT_PH",
            "right.AM_1_ACTN_CT_DT",
            "right.first_Action_Type",
            "right.FILING_DT",
            "right.IB_NOTIFICATION_DT",
            "right.PUBLISHED_DT",
            "right.NOA_DT",
            "right.ABANDONMENT_DT",
            "right.ABAN_DT_PH",
            "right.REGISTRATION_DT",
            "right.Disposal_Type",
            "right.EXT1_DT",
            "right.EXT2_DT",
            "right.EXT3_DT",
            "right.EXT4_DT",
            "right.EXT5_DT",
            "right.CANCELLATION_DT",
            "right.RENEWAL_DT",
            "right.REVIVAL_DT",
            "right.SUSP_CHECK_DT",
            "right.AM_CLS_CT_ACTV",
            "right.Pendency_Cal_Start_DT",
            "right.Pendency_Cal_End_DT",
            "right.NOA_REGISTRATION_CHECK",
            "right.WGTD_1ST_ACTN_PENDENCY",
            "right.first_Action_CD",
            "right.DISPOSAL_PENDENCY",
            "right.Suspension",
            "right.TTAB",
            "right.Disposal_DT",
            "right.DOCK_DT",
            "right.AM_FLG_66A_CUR",
            col("right.AM_FLG_66A_FIL").alias("Right_AM_FLG_66A_FIL"),
            "right.NOA_DT_PH",
            "right.Filing_FY",
            "right.NON_PRO_SE",
            "right.first_Action_Pendency_PH",
            col("right.LAST_MODIFIED_DATE").alias("Right_LAST_MODIFIED_DATE"),
            "right.PROCESSING_PEND",
        ]
    )
)

# COMMAND ----------

input_179 = spark.sql(f"select * from {reporting_catalog}.silver.divisionals")
select_180 = input_179.select(
    [
        "SER_NUM",
        "FILING_DT",
        "IB_NOTIFICATION_DT",
        "DV_TYPE",
        "REF_SER_NUM",
        "DV_DT_RQST",
        "DV_DT_COMPLETE",
        "LAST_MODIFIED_DATE",
        "TRANS_DT",
    ]
)

# COMMAND ----------

join_175 = join_174.alias("left").join(
    other=select_180.alias("right"),
    on=[col("left.SER_NUM") == col("right.SER_NUM")],
    how="left_anti",
)

# COMMAND ----------

join_90 = formula_143.alias("left").join(
    other=join_175.alias("right"),
    on=[col("PRJCT_CD") == col("SER_NUM")],
    how="inner",
)

# COMMAND ----------

formula_91 = join_90.withColumn(
    "DAYS_BTW_POSTED_AND_PEND_START_DT",
    datediff(col("ACCTG_DT"), col("Pendency_Cal_Start_DT")),
).select(
    [
        expr(
            """
            CASE 
                WHEN 
                    FILING_BASIS_FIL = 'MADRID' AND DAYS_BTW_POSTED_AND_PEND_START_DT <= 50 
                    OR 
                    FILING_BASIS_FIL != 'MADRID' AND DAYS_BTW_POSTED_AND_PEND_START_DT <= 15
                THEN 1 
                ELSE 0
            END AS FEE_FLAG
            """
        ),
        expr(
            """
            CASE 
                WHEN 
                    REGISTRATION_DT IS NULL 
                THEN 1 
                WHEN ACCTG_DT < REGISTRATION_DT THEN 1
                ELSE 0
            END AS `Registration Flag`
            """
        ),
        "TRAN_AM",
        "PRJCT_CD",
        "TRAN_STATUS_CD",
        "UNIT_QT",
        "Filing_FY",
        "SER_NUM"
    ]
)

# COMMAND ----------

filter_93 = formula_91.where("`Registration Flag` = 1 and Filing_FY >= 2010")

# COMMAND ----------

find_replace_99 = filter_93.select([
    "FEE_FLAG",
    "Registration Flag",
    "TRAN_AM",
    "PRJCT_CD",
    "TRAN_STATUS_CD",
    "UNIT_QT",
    "Filing_FY",
    "SER_NUM",
    expr("CASE WHEN FEE_FLAG = 1 THEN 1 ELSE 0 END AS Credit_Flag")
]).where("FEE_FLAG = 1")

# COMMAND ----------

formula_100 = find_replace_99.select(
    [
        "FEE_FLAG",
        "Registration Flag",
        "TRAN_AM",
        "PRJCT_CD",
        "TRAN_STATUS_CD",
        "UNIT_QT",
        "Filing_FY",
        "SER_NUM",
        "Credit_Flag",
        expr("""
            CASE 
                WHEN 
                    Credit_Flag = 1 
                THEN 'A' 
                ELSE TRAN_STATUS_CD 
            END AS TRAN_STATUS_CD
        """),
    ]
)

# COMMAND ----------

# DBTITLE 1,YR Summary for Comparison
summarize_101 = formula_100.groupBy("Filing_FY").agg(sum("UNIT_QT"))

# COMMAND ----------

summarize_104 = formula_100.groupBy("SER_NUM").agg(sum(("UNIT_QT")).alias("Fixed_Count"))

# COMMAND ----------

formula_108 = filter_93.select(
    [
        expr("* except(UNIT_QT)"),
        expr("""
        CASE 
            WHEN 
                TRAN_STATUS_CD = 'R' 
                OR TRAN_AM < 0
            THEN 0
            ELSE UNIT_QT
        END AS UNIT_QT
        """),
    ]
)

# COMMAND ----------

summarize_106 = formula_108.groupBy("SER_NUM").agg(sum(("UNIT_QT")).alias("Realtime_Count"))

# COMMAND ----------

join_107 = summarize_104.alias("left").join(
    other=summarize_106.alias("right"),
    on=[col("left.SER_NUM") == col("right.SER_NUM")],
    how="right",
)

# COMMAND ----------

cleansing_113 = join_107.select(
    [
        col("right.SER_NUM").alias("SER_NUM"),
        expr("""
            CASE 
                WHEN Realtime_Count IS NULL 
                    THEN 0
                    ELSE Realtime_Count
            END AS Realtime_Count
        """),
        expr("""
            CASE 
                WHEN Fixed_Count IS NULL 
                    THEN 0
                    ELSE Fixed_Count
            END AS Fixed_Count
        """)
    ]
)

# COMMAND ----------

input_195 = spark.sql(
    f"""
    select 
        class_status as Class_Status,
        class as CLASS,
        ser_num as SER_NUM,
        cl_cls_us_ct as CL_CLS_US_CT,
        cl_cls_us as CL_CLS_US,
        cl_dt_stat as CL_DT_STAT,
        cl_flg_anoth_form as CL_FLG_ANOTH_FORM,
        vt_ser_num as VT_SER_NUM,
        vt_class as VT_Class,
        goods_and_services_desc as `Goods &Services Desc`
    from 
        {reporting_catalog}.silver.class
"""
)

# COMMAND ----------

select_196 = input_195.select(
    [
        "Class_Status",
        "CLASS",
        "SER_NUM",
        "CL_CLS_US_CT",
        "CL_CLS_US",
        "CL_DT_STAT",
        "CL_FLG_ANOTH_FORM",
        "VT_SER_NUM",
        "VT_Class",
        "Goods &Services Desc",
    ]
)

# COMMAND ----------

filter_89 = select_196.where("Class_Status != 'INACTIVE-Insufficient Fee Received'")

# COMMAND ----------

input_156 = spark.sql(
   f"""
    select 
        tm.serial_num_tx as AM_SER_NUM,
        tm.legacy_status_cd as AM_STAT
    from 
        {tmngpdb_catalog}.bronze.trademark tm
"""
)
select_157 = input_156.select(["AM_SER_NUM", "AM_STAT"])

# COMMAND ----------

join_114 = filter_89.alias("left").join(
    other=select_157.alias("right"),
    on=[col("SER_NUM") == col("right.AM_SER_NUM")],
    how="inner",
)

# COMMAND ----------

filter_115 = join_114.where("AM_STAT in (630, 638)")

# COMMAND ----------

summarize_116 = filter_115.groupBy("SER_NUM", col("AM_STAT").alias("TRAM Status")).agg(countDistinct("Class").alias("CountDistinct_Class"))

# COMMAND ----------

join_117 = (
    cleansing_113.alias("left")
    .join(
        other=summarize_116.alias("right"),
        on=[col("left.SER_NUM") == col("right.SER_NUM")],
        how="inner",
    )
    .select(
        [
            col("left.SER_NUM").alias("SER_NUM"),
            expr("CAST(Realtime_Count AS INTEGER) Realtime_Count"),
            expr("CAST(Fixed_Count AS INTEGER) Fixed_Count"),
            "TRAM Status",
            "CountDistinct_Class",
        ]
    )
)

# COMMAND ----------

input_159 = spark.sql(
    f"""
    select
        right(be.cfk_object_gid, 8) as CM_SER_NUM,
        ber.BUSINESS_EVENT_REASON_CD as CM_ENT_CD
    from
        {tmngpdb_catalog}.bronze.business_event be
    left join {tmngpdb_catalog}.bronze.stnd_business_event_reason ber on be.fk_business_event_reason_id = ber.business_event_reason_id
    where
        (ber.BUSINESS_EVENT_REASON_CD like 'DRRR%')
        or (ber.BUSINESS_EVENT_REASON_CD like 'PARI%')
        or (ber.BUSINESS_EVENT_REASON_CD like 'PWFG%')                      
"""
)

select_160 = input_159.select(
    [col("CM_SER_NUM").alias("SERIAL_NUMBER"), col("CM_ENT_CD")]
)

# COMMAND ----------

summarize_133 = select_160.select("SERIAL_NUMBER").distinct()

# COMMAND ----------

join_134 = (
    summarize_133.alias("left")
    .join(
        other=join_117.alias("right"),
        on=[col("left.SERIAL_NUMBER") == col("right.SER_NUM")],
        how="right",
    )
    .where(col("left.SERIAL_NUMBER").isNull())
).select(
    [
        "right.SER_NUM",
        col("right.Realtime_Count").alias("Fees Paid"),
        col("right.CountDistinct_Class").alias("TRAM Classes"),
        "TRAM Status",
    ]
)

# COMMAND ----------

cleansing_119 = join_134.select(
    [
        expr("IFF(SER_NUM IS NULL, 0, SER_NUM) as SER_NUM"),
        expr("IFF(`Fees Paid` IS NULL, 0, `Fees Paid`) as `Fees Paid`"),
        expr("IFF(`TRAM Classes` IS NULL, 0, `TRAM Classes`) as `TRAM Classes`"),
        expr("IFF(`TRAM Status` IS NULL, 0, `TRAM Status`) as `TRAM Status`"),
    ]
)

# COMMAND ----------

filter_151 = cleansing_119.where("SER_NUM != 97116179")

# COMMAND ----------

formula_118 = filter_151.select(
    [
        "SER_NUM",
        "Fees Paid",
        "TRAM Classes",
        "TRAM Status",
        expr("`Fees Paid` - `TRAM Classes` AS Delta"),
        expr(
            """CASE 
                WHEN `Fees Paid` - `TRAM Classes` < 0 
                    THEN  'Underpayment' 
                    ELSE 'Overpayment' 
                END AS `Discrepancy Type`
            """
        ),
    ]
)

# COMMAND ----------

filter_120 = formula_118.where("Delta != 0")

# COMMAND ----------

filter_141 = filter_120.where("`Discrepancy Type` = 'Underpayment'")

# COMMAND ----------

select_126 = filter_141.select(
    [
        "SER_NUM",
        "Fees Paid",
        "TRAM Classes",
        "Delta",
        "TRAM Status",
        "Discrepancy Type",
    ]
)

# COMMAND ----------

# DBTITLE 1,End ETL
sort_131 = select_126.orderBy("Delta")

# COMMAND ----------

# DBTITLE 1,Get Count for Job - Terminate if No Output
output_count = sort_131.count()
if output_count == 0:
    end_job_cntl(
        f"{reporting_catalog}.silver",
        job_name,
        job_start_ts,
        "completed",
        output_count,
        "job completed successfully",
    )
    dbutils.notebook.exit("Job completed with no records.")

# COMMAND ----------

if dbx_env != "prod":
    sort_131 = spark.sql(
        """
        select * except (
            days_on_report,
            first_report_date,
            first_time_on_report,
            effective_ts,
            begin_effective_ts,
            end_effective_ts
        )
        from trm_reporting_dev.silver.preexam_fee_checker_historical
        where end_effective_ts is null
        limit 1
    """
    )
    display(sort_131)

# COMMAND ----------

# DBTITLE 1,Make Current Report `source` for Merge
sort_131.createOrReplaceTempView("incoming")
display(spark.sql("select * from incoming"))

# COMMAND ----------

# DBTITLE 1,SCD 2 (Non-DLT Workaround)
# MAGIC %sql
# MAGIC create or replace temp view upsert as
# MAGIC -- existing, no match, do not change
# MAGIC select
# MAGIC   a.*
# MAGIC from
# MAGIC   ${config.reporting_catalog}.silver.preexam_fee_checker_historical a
# MAGIC where
# MAGIC   not exists (
# MAGIC     select
# MAGIC       1
# MAGIC     from
# MAGIC       incoming b
# MAGIC     where
# MAGIC       a.ser_num = b.ser_num
# MAGIC   )
# MAGIC union
# MAGIC -- existing, matched but expired, do not change
# MAGIC select
# MAGIC   a.*
# MAGIC from
# MAGIC   ${config.reporting_catalog}.silver.preexam_fee_checker_historical a
# MAGIC where
# MAGIC   exists (
# MAGIC     select
# MAGIC       1
# MAGIC     from
# MAGIC       incoming b
# MAGIC     where
# MAGIC       a.ser_num = b.ser_num
# MAGIC   )
# MAGIC   and a.end_effective_ts is not null
# MAGIC union
# MAGIC -- updates, insert new records, take previous record details
# MAGIC select
# MAGIC   a.*,
# MAGIC   date_diff(current_date, b.first_report_date) days_on_report,
# MAGIC   b.first_report_date first_report_date,
# MAGIC   false first_time_on_report,
# MAGIC   current_timestamp effective_ts,
# MAGIC   current_timestamp begin_effective_ts,
# MAGIC   null end_effective_ts
# MAGIC from
# MAGIC   incoming a
# MAGIC     join ${config.reporting_catalog}.silver.preexam_fee_checker_historical b
# MAGIC       on a.ser_num = b.ser_num
# MAGIC where
# MAGIC   b.end_effective_ts is null
# MAGIC union
# MAGIC -- updates, update existing records, change deactivate effective ts
# MAGIC select
# MAGIC   a.* except (end_effective_ts),
# MAGIC   current_timestamp end_effective_ts
# MAGIC from
# MAGIC   ${config.reporting_catalog}.silver.preexam_fee_checker_historical a
# MAGIC where
# MAGIC   exists (
# MAGIC     select
# MAGIC       1
# MAGIC     from
# MAGIC       incoming b
# MAGIC     where
# MAGIC       a.ser_num = b.ser_num
# MAGIC   )
# MAGIC   and a.end_effective_ts is null
# MAGIC union
# MAGIC -- new, insert new, initialize values
# MAGIC select
# MAGIC   a.*,
# MAGIC   0 days_on_report,
# MAGIC   current_date first_report_date,
# MAGIC   true first_time_on_report,
# MAGIC   current_timestamp effective_ts,
# MAGIC   current_timestamp begin_effective_ts,
# MAGIC   null end_effective_ts
# MAGIC from
# MAGIC   incoming a
# MAGIC where
# MAGIC   not exists (
# MAGIC     select
# MAGIC       1
# MAGIC     from
# MAGIC       ${config.reporting_catalog}.silver.preexam_fee_checker_historical b
# MAGIC     where
# MAGIC       a.ser_num = b.ser_num
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert overwrite ${config.reporting_catalog}.silver.preexam_fee_checker_historical
# MAGIC   select
# MAGIC     *
# MAGIC   from
# MAGIC     upsert

# COMMAND ----------

# DBTITLE 1,Show Sample
# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   ${config.reporting_catalog}.silver.preexam_fee_checker_historical
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view email_output as
# MAGIC select
# MAGIC   ser_num,
# MAGIC   fees_paid,
# MAGIC   tram_classes,
# MAGIC   `delta`,
# MAGIC   tram_status,
# MAGIC   discrepancy_type,
# MAGIC   first_report_date,
# MAGIC   days_on_report,
# MAGIC   first_time_on_report,
# MAGIC   "https://feeprocessingportal.uspto.gov/fpng/fees/historyservice?postingReferenceText=" || ser_num
# MAGIC   || "&feeReferenceGroupCode=TRADEMARK%7CADMIN%7CGENERAL" as fpng_link,
# MAGIC   "https://review.tm-examcenter.aws.uspto.gov/review/" || ser_num as tm_exam_link
# MAGIC from
# MAGIC   ${config.reporting_catalog}.silver.preexam_fee_checker_historical
# MAGIC where
# MAGIC   end_effective_ts is null
# MAGIC   and current_date = date(effective_ts)

# COMMAND ----------

# DBTITLE 1,Convert to DF
email_output = spark.sql("select * from email_output")

# COMMAND ----------

subject="Auto-Generated: Fee Discrepancies New Unexamined Applications Status 630 & 638"
from_addr = "trademark_analytics@uspto.gov"

email_body = f"""Trademark Analytics<br><br>Fee Discrepancies Report"""

attachment_name=f'{datetime.datetime.today().strftime("%Y-%m-%d")} Fee Discrepancies New Unexamined Applications.xlsx'
attachments = [(email_output, attachment_name, 'excel')]

# Send the email with the attachment
send_email_report(
    job_nm = job_name,
    subject = subject,
    send_from = from_addr,
    send_to = primary_email,
    send_to_cc=cc_email,
    html_body= email_body,
    attachments = attachments
)

# COMMAND ----------

#############################################################################################
# 5/2/25 - Commented out data quality check code since it has been succeeding consistently. #
# Allows disabling Alteryx workflow schedule fully, saving resources.                       #
#############################################################################################


# # data quality entry
# tbl1 = f"hive_metastore.{altrx_schema}.preexam_fee_checker" 
# tbl2 = f"{reporting_catalog}.gold.preexam_fee_checker"
# key_cols = ['ser_num']

# dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
# print(dq_result)

# COMMAND ----------

# DBTITLE 1,End Job
end_job_cntl(
    f"{reporting_catalog}.silver",
    job_name,
    job_start_ts,
    "completed",
    output_count,
    "job completed successfully",
)
dbutils.notebook.exit("Job completed with no records.")
