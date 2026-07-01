# Databricks notebook source
# %pip install fpdf2

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs["schema"]["trgt_catalog"]
src_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
tmintltm_src_catalog = common_configs["schema"]["tmintltm_src_catalog"]
spark.conf.set('conf.dbx_env', dbx_env)
primary_email = common_configs["secrets"]["edw_scope"]
email_id, cc_email = common_configs["alerting"]["tmintl_auto_project"]["email"], common_configs["alerting"]["tmintl_auto_project"]["cc"]
altrx_schema = common_configs['schema']['altrx_schema']
dq_catalog = common_configs['schema']['data_quality_catalog']
# print(isinstance(primary_email, str))
#print(isinstance(cc_email, str))
print(trgt_catalog, src_catalog, primary_email, cc_email, altrx_schema,tmintltm_src_catalog)

# COMMAND ----------

import datetime
import pytz
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trmreports_tmintl_auto_protect'

control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,starttime)

# COMMAND ----------

source_df = spark.sql(f"""
    WITH initial_df AS (
    SELECT 
        CAST(SPLIT(AM.AM_SER_NUM, ':')[2] AS INTEGER) AS AM_SER_NUM,
        CM_ENT_CD,
        CM_ENT_DT
    FROM (
        SELECT 
            TM.TRADEMARK_GID AS AM_SER_NUM, 
            TM.FILING_DT AS AM_DT_FIL,
            IRT.NOTIFICATION_DT AS RI_NOTIF_DT
        FROM 
            {src_catalog}.bronze.TRADEMARK TM
        LEFT JOIN 
            {tmintltm_src_catalog}.bronze.INTERNATIONAL_REG_TM IRT
        ON 
            TM.TRADEMARK_GID = IRT.CFK_TRADEMARK_GID
    ) AM
    LEFT JOIN (
        SELECT 
            TO_DATE(BE.EFFECTIVE_TS) AS CM_ENT_DT,
            BER.LEGACY_CM_ENT_CD AS CM_ENT_CD,
            BE.CFK_OBJECT_GID AS CM_SER_NUM,
            BE.ORDER_NO AS CM_ENT_NUM
        FROM 
            {src_catalog}.bronze.BUSINESS_EVENT BE
        LEFT JOIN 
            {src_catalog}.bronze.STND_BUSINESS_EVENT_REASON BER
        ON 
            BE.FK_BUSINESS_EVENT_REASON_ID = BER.BUSINESS_EVENT_REASON_ID
        WHERE 
            BE.FK_OBJECT_TYPE_CD = 'Trademark' 
            AND BER.LEGACY_CM_ENT_CD NOT IN ('MIG0','WEBE','XSCE','XSSE') 
            AND BER.LEGACY_CM_ENT_CD IS NOT NULL
    ) CM
    ON 
        AM.AM_SER_NUM = CM.CM_SER_NUM
),
final_df AS (
    SELECT 
        AM_SER_NUM,
        CASE 
            WHEN CM_ENT_CD = 'DOCK' THEN CAST(CM_ENT_DT AS DATE)
            ELSE NULL
        END AS DOCK_DT
    FROM initial_df
),
trademark_df AS (
    SELECT 
        serial_num_tx, 
        status_dt 
    FROM 
        {src_catalog}.bronze.trademark
)
SELECT 
    CAST(REGEXP_SUBSTR(TM.TRADEMARK_GID, '[^:]+$') AS INTEGER) AS AM_SER_NUM,
    TM.FILING_DT AS AM_DT_FIL,
    TM.LEGACY_STATUS_CD AS AM_STAT,
    fb_66a.AM_FLG_66A_CUR,
    fb_66a.AM_FLG_66A_FIL,
    TM2.status_dt AS AM_STAT_DT,
    FD.DOCK_DT AS AM_DT_DOCK
FROM 
    {src_catalog}.bronze.TRADEMARK TM
LEFT JOIN (
    SELECT
        FK_TRADEMARK_GID,
        CASE WHEN current_in = 'Y' THEN 1 ELSE 0 END AS AM_FLG_66A_CUR,
        CASE WHEN filed_in = 'Y' THEN 1 ELSE 0 END AS AM_FLG_66A_FIL
    FROM 
        {src_catalog}.bronze.TM_FILING_BASIS
    WHERE 
        FK_FILING_BASIS_CD = '66(a)'
) fb_66a
ON 
    TM.TRADEMARK_GID = fb_66a.FK_TRADEMARK_GID
LEFT JOIN final_df FD
ON 
    CAST(REGEXP_SUBSTR(TM.TRADEMARK_GID, '[^:]+$') AS INTEGER) = FD.AM_SER_NUM
LEFT JOIN trademark_df TM2
ON CAST(REGEXP_SUBSTR(TM.TRADEMARK_GID, '[^:]+$') AS STRING) = TM2.serial_num_tx
WHERE 
    fb_66a.AM_FLG_66A_FIL = 1 
    AND TM.LEGACY_STATUS_CD IN ('630', '631', '638', '646')
""")
#display(source_df)

# COMMAND ----------

from pyspark.sql.functions import to_date
source_df = source_df.withColumn("AM_DT_FIL", to_date("AM_DT_FIL"))
#display(source_df)

# COMMAND ----------

from pyspark.sql.functions import when

source_df = source_df.withColumn(
    "AM_FLG_66A_FIL1", when(source_df["AM_FLG_66A_FIL"] == "1", "Yes").otherwise("No")
).withColumn(
    "AM_FLG_66A_CUR1", when(source_df["AM_FLG_66A_CUR"] == "1", "Yes").otherwise("No")
)
#display(source_df)

# COMMAND ----------

from pyspark.sql.functions import col
tbl_pbh = spark.sql(f"select * from {trgt_catalog}.silver.prosecution_history").select(
    col("serial_number").alias("SERIAL_NUMBER"),
    col("ph_action_number").alias("PH_ACTION_NUMBER"),
    col("ph_action_code").alias("PH_ACTION_CODE"),
    col("cm_sys_dt").alias("CM_SYS_DT"),
    col("ph_action_date").alias("PH_ACTION_DATE"),
    col("last_modified_date").alias("LAST_MODIFIED_DATE"),
    col("oracle_apply_time").alias("ORACLE_APPLY_TIME"),
    col("cm_prcd_num").alias("CM_PRCD_NUM"),
    col("ri_notif_dt").alias("RI_NOTIF_DT"),
    col("cm_desc").alias("CM_DESC"),
    col("fifth_char_cm_type").alias("FIFTH_CHAR_CM_TYPE"),
    col("cm_flg_paper").alias("CM_FLG_PAPER"),
    col("ttab_tracking_num").alias("TTAB_TRACKING_NUM"),
    col("tm_worker_eid").alias("TM_WORKER_EID"),
    col("create_ts").alias("CREATE_TS"),
    col("create_user_id").alias("CREATE_USER_ID"),
    col("update_ts").alias("UPDATE_TS"),
    col("update_user_id").alias("UPDATE_USER_ID")
)
#display(tbl_pbh)

# COMMAND ----------

joined_pbh_df = tbl_pbh.join(source_df, tbl_pbh["serial_number"] == source_df["AM_SER_NUM"], "inner")
joined_pbh_df = joined_pbh_df.filter((joined_pbh_df["PH_ACTION_CODE"] != "RFCS") | (joined_pbh_df["PH_ACTION_CODE"] != "NPUB"))
#display(joined_pbh_df)

# COMMAND ----------

from pyspark.sql import functions as F

max_ph_action_df = joined_pbh_df.groupBy("SERIAL_NUMBER").agg(F.max("PH_ACTION_NUMBER").alias("PH_ACTION_NUMBER"))
#display(max_ph_action_df)

# COMMAND ----------

joined_pbh_df = joined_pbh_df.filter(col("AM_DT_DOCK").isNotNull())
result_df = max_ph_action_df.join(joined_pbh_df, ["SERIAL_NUMBER", "PH_ACTION_NUMBER"], "inner")
#result_df = max_ph_action_df.join(joined_pbh_df, (max_ph_action_df["SERIAL_NUMBER"] == joined_pbh_df["SERIAL_NUMBER"]) & (max_ph_action_df["MAX_PH_ACTION_NUMBER"] == joined_pbh_df["PH_ACTION_NUMBER"]), "inner")
#display(result_df)

# COMMAND ----------

# DBTITLE 1,Changing name to avoid ambiguous
result_df = result_df.withColumnRenamed("AM_FLG_66A_FIL", "RIGHT_AM_FLG_66A_FIL") \
    .withColumnRenamed("PH_ACTION_NUMBER", "MAX_PH_ACTION_NUMBER") \
    .withColumnRenamed("AM_DT_DOCK", "RIGHT_AM_DT_DOCK") \
    .withColumnRenamed("AM_FLG_66A_CUR", "RIGHT_AM_FLG_66A_CUR") \
    .withColumnRenamed("AM_STAT", "RIGHT_AM_STAT") \
    .withColumnRenamed("AM_SER_NUM", "RIGHT_AM_SER_NUM") \
    .withColumnRenamed("AM_STAT_DT", "RIGHT_AM_STAT_DT") \
    .withColumnRenamed("AM_DT_FIL", "RIGHT_AM_DT_FIL") \
    .withColumnRenamed("AM_FLG_66A_FIL1", "RIGHT_AM_FLG_66A_FIL1") \
    .withColumnRenamed("AM_FLG_66A_CUR1", "RIGHT_AM_FLG_66A_CUR1")

#display(result_df)

# COMMAND ----------

source_df_alias = source_df.alias("source")
result_df_alias = result_df.alias("result")

joined_df = source_df_alias.join(
    result_df_alias,
    source_df_alias["AM_SER_NUM"] == result_df_alias["SERIAL_NUMBER"],
    "left"
).select(
    source_df_alias["AM_SER_NUM"],
    source_df_alias["AM_STAT"],
    source_df_alias["AM_STAT_DT"],
    source_df_alias["AM_DT_DOCK"],
    source_df_alias["AM_DT_FIL"],
    source_df_alias["AM_FLG_66A_FIL"],
    source_df_alias["AM_FLG_66A_CUR"],
    source_df_alias["AM_FLG_66A_FIL1"],
    source_df_alias["AM_FLG_66A_CUR1"],
    result_df_alias["RIGHT_AM_SER_NUM"].alias("RIGHT_AM_SER_NUM"),
    result_df_alias["RIGHT_AM_STAT"].alias("RIGHT_AM_STAT"),
    result_df_alias["RIGHT_AM_STAT_DT"].alias("RIGHT_AM_STAT_DT"),
    result_df_alias["RIGHT_AM_DT_DOCK"].alias("RIGHT_AM_DT_DOCK"),
    result_df_alias["RIGHT_AM_DT_FIL"].alias("RIGHT_AM_DT_FIL"),
    result_df_alias["RIGHT_AM_FLG_66A_CUR"].alias("RIGHT_AM_FLG_66A_CUR"),
    result_df_alias["RIGHT_AM_FLG_66A_FIL"].alias("RIGHT_AM_FLG_66A_FIL"),
    result_df_alias["RIGHT_AM_FLG_66A_FIL1"].alias("RIGHT_AM_FLG_66A_FIL1"),
    result_df_alias["RIGHT_AM_FLG_66A_CUR1"].alias("RIGHT_AM_FLG_66A_CUR1"),
    result_df_alias["PH_ACTION_CODE"],
    result_df_alias["PH_ACTION_DATE"],
    result_df_alias["CM_DESC"],
    result_df_alias["FIFTH_CHAR_CM_TYPE"],
    result_df_alias["TTAB_TRACKING_NUM"],
    result_df_alias["CREATE_TS"],
    result_df_alias["CREATE_USER_ID"],
    result_df_alias["UPDATE_TS"],
    result_df_alias["UPDATE_USER_ID"]
).filter(
    col("RIGHT_AM_DT_DOCK").isNotNull()
)

#display(joined_df)

# COMMAND ----------

from pyspark.sql.functions import current_date, datediff
reg_tm_df = spark.sql(f"""select 
INTERNATIONAL_REG_TM.DN_SERIAL_NUM,
INTERNATIONAL_REG_TM.AUTO_PROTECT_DT 
from {tmintltm_src_catalog}.bronze.INTERNATIONAL_REG_TM""")
reg_tm_df = reg_tm_df = reg_tm_df.withColumn("Days to Auto Protect", datediff("AUTO_PROTECT_DT", current_date()))
reg_tm_df = reg_tm_df.filter(col("Days to Auto Protect") <= 90)
#display(reg_tm_df)

# COMMAND ----------

from pyspark.sql.functions import col

joined_df1 = joined_df.join(reg_tm_df, joined_df["AM_SER_NUM"] == reg_tm_df["DN_SERIAL_NUM"], "inner").select(
    joined_df["AM_SER_NUM"],
    joined_df["AM_DT_FIL"],
    joined_df["RIGHT_AM_DT_DOCK"],
    joined_df["AM_STAT"],
    joined_df["AM_STAT_DT"],
    joined_df["PH_ACTION_CODE"],
    joined_df["PH_ACTION_DATE"],
    joined_df["AM_FLG_66A_CUR"],
    joined_df["AM_FLG_66A_FIL"],
    joined_df["CM_DESC"],
    reg_tm_df["AUTO_PROTECT_DT"],
    reg_tm_df["Days to Auto Protect"]
)
#display(joined_df1)

# COMMAND ----------

tbl_mil = spark.sql(f"select * from {trgt_catalog}.silver.milestone").select(
    col("ser_num").alias("SER_NUM"),
    col("first_action_dt_ph").alias("FIRST_ACTION_DT_PH"),
    col("am_1_actn_ct_dt").alias("AM_1_ACTN_CT_DT"),
    col("first_action_type").alias("FIRST_ACTION_TYPE"),
    col("filing_dt").alias("FILING_DT"),
    col("ib_notification_dt").alias("IB_NOTIFICATION_DT"),
    col("published_dt").alias("PUBLISHED_DT"),
    col("noa_dt").alias("NOA_DT"),
    col("abandonment_dt").alias("ABANDONMENT_DT"),
    col("aban_dt_ph").alias("ABAN_DT_PH"),
    col("registration_dt").alias("REGISTRATION_DT"),
    col("disposal_type").alias("DISPOAL_TYPE"),
    col("ext1_dt").alias("EXT1_DT"),
    col("ext2_dt").alias("EXT2_DT"),
    col("ext3_dt").alias("EXT3_DT"),
    col("ext4_dt").alias("EXT4_DT"),
    col("ext5_dt").alias("EXT5_DT"),
    col("cancellation_dt").alias("CANCELLATION_DT"),
    col("renewal_dt").alias("RENEWAL_DT"),
    col("revival_dt").alias("REVIVAL_DT"),
    col("susp_check_dt").alias("SUSP_CHECK_DT"),
    col("am_cls_ct_actv").alias("AM_CLS_CT_ACTV"),
    col("pendency_cal_start_dt").alias("PENDENCY_CAL_START_DT"),
    col("pendency_cal_end_dt").alias("PENDENCY_CAL_END_DT"),
    col("noa_registration_check").alias("NOA_REGISTRATION_CHECK"),
    col("wgtd_1st_actn_pendency").alias("WGTD_1ST_ACTN_PENDENCY"),
    col("first_action_cd").alias("FIRST_ACTION_CD"),
    col("disposal_pendency").alias("DISPOSAL_PENDENCY"),
    col("suspension").alias("SUSPENSION"),
    col("ttab").alias("TTAB"),
    col("disposal_dt").alias("DISPOSAL_DT"),
    col("dock_dt").alias("DOCK_DT"),
    col("am_flg_66a_cur").alias("AM_FLG_66A_CUR"),
    col("am_flg_66a_fil").alias("AM_FLG_66A_FIL"),
    col("noa_dt_ph").alias("NOA_DT_PH"),
    col("filing_fy").alias("FILING_FY"),
    col("non_pro_se").alias("NON_PRO_SE"),
    col("first_action_pendency_ph"),
    col("last_modified_date").alias("LAST_MODIFIED_DATE"),
    col("processing_pend").alias("PROCESSING_PEND"),
    col("processing_pend_days").alias("PROCESSING_PEND_DAYS"),
    col("days_in_dock").alias("DAYS_IN_DOCK"),
    col("create_ts").alias("CREATE_TS"),
    col("create_user_id").alias("CREATE_USER_ID"),
    col("update_ts").alias("UPDATE_TS"),
    col("update_user_id").alias("UPDATE_USER_ID")
)

# COMMAND ----------

joined_df2 = joined_df1.join(tbl_mil, joined_df1["AM_SER_NUM"] == tbl_mil["SER_NUM"], "inner").select(
    tbl_mil["PENDENCY_CAL_START_DT"],
    tbl_mil["SER_NUM"]
)
#display(joined_df2)

# COMMAND ----------

joined_df1_alias = joined_df1.alias("df1")
joined_df2_alias = joined_df2.alias("df2")

final_df = joined_df1_alias.join(
    joined_df2_alias,
    joined_df1_alias["AM_SER_NUM"] == joined_df2_alias["SER_NUM"],
    "inner"
)

#display(final_df)

# COMMAND ----------

from pyspark.sql.functions import col, months_between, current_date, current_timestamp, datediff, expr, date_format, to_date, round, add_months

final_df = final_df.withColumn(
    "Auto_Protect_DT_Check",
    date_format(add_months(col("PENDENCY_CAL_START_DT"), months_between(col("AUTO_PROTECT_DT"), col("PENDENCY_CAL_START_DT")).cast("int")), "MM/dd/yyyy")
).withColumn(
    "IB Notification / CRCV Calculation (Months)",
    round(months_between(col("PH_ACTION_DATE"), col("AM_DT_FIL")))
).withColumn(
    "Pendency (Months)",
    round(months_between(current_date(), col("PENDENCY_CAL_START_DT")))
).withColumn(
    "Days to Auto Protect",
    datediff(col("AUTO_PROTECT_DT"), current_date())
).withColumn(
    "Run time",
    current_timestamp()
)
final_df = final_df.drop("AM_FLG_66A_CUR", "AM_FLG_66A_FIL","SER_NUM")
final_df = final_df.filter(
    expr("months_between(current_date(), to_date(PENDENCY_CAL_START_DT, 'yy/MM')) >= 15")
)
final_df = final_df.withColumn(
    "RUN_TIME",
    date_format(col("Run time"), "yyyy-MM-dd HHMMss")
)

# Assuming Pendency_Cal_Start_DT is in the format 'yy/MM'
final_df_filtered = final_df.withColumn(
    "Pendency_Cal_Start_DT_Parsed", 
    to_date(col("Pendency_Cal_Start_DT"), "yy/MM")
)

# Filter the DataFrame
final_df_filtered = final_df_filtered.filter(
    months_between(current_date(), col("Pendency_Cal_Start_DT_Parsed")) >= 15
)
final_df_filtered = final_df_filtered.withColumn(
    "REPORT_PATH",
    expr(r"concat('\\\\s-mde-isl1-9a2-smb.uspto.gov\\AFF_TM_P\\Project SAMPSON\\EmailOutput\\TM_International_Auto_Protect_', RUN_TIME, '.PDF')")
)
#display(final_df_filtered)

# COMMAND ----------

from pyspark.sql.functions import col, count, avg, min, max

final_df_grouped = final_df_filtered.groupBy(
    "AM_SER_NUM", "AM_DT_FIL", "RIGHT_AM_DT_DOCK", "AM_STAT", "AM_STAT_DT", 
    "Pendency (Months)", "Days to Auto Protect", "Auto_Protect_DT_Check", 
    "IB Notification / CRCV Calculation (Months)", "CM_DESC", "RUN_TIME"
).agg(
    count("*").alias("count"), 
)

final_df_grouped = final_df_grouped.orderBy("Days to Auto Protect", ascending=True)
#display(final_df_grouped)

# COMMAND ----------

from pyspark.sql.functions import date_format

final_df_grouped = final_df_grouped.withColumn("AM_DT_FIL", date_format("AM_DT_FIL", "MM/dd/yyyy"))

#display(joined_df)

# COMMAND ----------

from pyspark.sql.types import IntegerType

dbx_table = final_df_grouped.select(
    col("AM_SER_NUM").alias("am_ser_num"),
    col("AM_DT_FIL").alias("am_dt_fil"),
    #col("AM_DT_DOCK").alias("am_dt_dock"),
    col("AM_STAT").alias("am_stat"),
    col("AM_STAT_DT").alias("am_stat_dt"),
    col("Days to Auto Protect").alias("days_to_auto_protect"),
    col("Auto_Protect_DT_Check").alias("auto_protect_dt"),
    col("IB Notification / CRCV Calculation (Months)").alias("ib_notification_crcv_calculation_months"),
    col("CM_DESC").alias("cm_desc")
)

dbx_table = dbx_table.withColumn(
    'AM_DT_FIL', to_date(col('AM_DT_FIL'), 'MM/dd/yyyy')
).withColumn(
    'AM_STAT_DT', to_date(col('AM_STAT_DT'))
).withColumn(
    'auto_protect_dt', to_date(col('auto_protect_dt'), 'MM/dd/yyyy')
).withColumn(
    'ib_notification_crcv_calculation_months', col('ib_notification_crcv_calculation_months').cast(IntegerType())
)

dbx_table = dbx_table.select(
    'am_ser_num',
    'am_dt_fil',
    #'am_dt_dock',
    'am_stat',
    'am_stat_dt',
    'days_to_auto_protect',
    'auto_protect_dt',
    'ib_notification_crcv_calculation_months',
    'cm_desc'
)
#display(dbx_table)

# COMMAND ----------

result_df = final_df_grouped.select(
    col("AM_SER_NUM").alias("Serial Number"),
    col("AM_STAT").alias("Status"),
    col("AM_STAT_DT").alias("Status Date"),
    col("Days to Auto Protect").alias("Days to Auto Protect"),
    col("RIGHT_AM_DT_DOCK").alias("Auto Protect Date"),
    col("CM_DESC").alias("CM Description")
)
#display(result_df)

# COMMAND ----------

target_table = f"{trgt_catalog}.gold.tmintl_auto_protect"
#dbx_table.write.mode("overwrite").format("delta").insertInto(target_table)

# COMMAND ----------

rdate = datetime.datetime.now().strftime('%Y-%m-%d')
mailed_pdf = f"TM_International_Auto_Protect_{rdate}.PDF"
title_tx_1 = """TM International Auto Protection Report"""
title_tx_2 = f"""Run time: {rdate}"""
#title_tx_3 = f"""Category Counts For Issue Date {issue_date}"""
#title_tx_4 = """Category Counts Year-To-Date"""
data_columns = ['column1', 'column2', 'column3', 'column4', 'column5', 'column6']
data_col_1 = data_columns[0]
data_col_2 = data_columns[1]
data_col_3 = data_columns[2]
data_col_4 = data_columns[3]
data_col_5 = data_columns[4]
data_col_6 = data_columns[5]
tm_analytics_image_loc = '../shared/tm_analytics.jpg'
uspto_image_loc = '../shared/uspto_logo.png'
from_addr= 'trademark_analytics@uspto.gov'

# COMMAND ----------

result_df = result_df.toPandas() 

# COMMAND ----------

# DBTITLE 1,PDF Preparation Function (Updated for fpdf2)
from fpdf import FPDF
from fpdf.enums import XPos, YPos

def pdf_prep(
    mailed_pdf,
    uspto_image_loc,
    title_tx_1,
    title_tx_2,
    tm_analytics_image_loc,
    result_df
):
    """This function instantiates class for pdf_prep, creates and saves temp space"""
    class PDF(FPDF):
        def header(self):
            # Logo
            self.image(uspto_image_loc, 10, 8, 33)
            # Arial bold 12
            self.set_font('Arial', 'B', 9)  # Reduced font size
            # Move to the right
            self.cell(80)
            # Title
            self.cell(30, 10, title_tx_1, 0, 0, 'C')
            self.ln(5)  # enter
            self.cell(80)
            self.cell(30, 10, title_tx_2, 0, 0, 'C')
            self.ln(5)
            self.cell(80)
            # Line break
            self.ln(20)
            self.image(tm_analytics_image_loc, 160, 6, 23)

    # Instantiation of inherited class
    pdf = PDF()
    # start creating ....
    pdf.add_page()

    # Calculate column widths based on content
    col_widths = []
    for col in result_df.columns:
        max_width = pdf.get_string_width(col) + 4  # Add some padding
        for value in result_df[col]:
            current_width = pdf.get_string_width(str(value)) + 4
            if current_width > max_width:
                max_width = current_width
        col_widths.append(max_width)

    # Set font for table header
    pdf.set_font('Arial', 'B', 7)  # Reduced font size
    pdf.set_fill_color(0, 75, 126)
    pdf.set_text_color(255, 255, 255)

    # Write the column headers
    for i, col in enumerate(result_df.columns):
        pdf.cell(col_widths[i], 4, col, 1, 0, 'C', fill=True)
    pdf.ln(10)

    # Set font for table rows
    pdf.set_font('Arial', '', 7)  # Reduced font size
    pdf.set_text_color(0, 0, 0)

    # Write the table rows
    for i in range(len(result_df)):
        if i % 2 == 0:
            pdf.set_fill_color(224, 224, 224)
        else:
            pdf.set_fill_color(249, 249, 249)

        for j, col in enumerate(result_df.columns):
            value = str(result_df.iloc[i][col])
            if col == "Days to Auto Protect" and float(value) < 0:
                pdf.set_text_color(255, 0, 0)  # Red color for negative values
            else:
                pdf.set_text_color(0, 0, 0)  # Default color for other values
            pdf.cell(col_widths[j], 5, value, 1, 0, 'C', fill=True)
        pdf.ln(5)

    pdf.output(mailed_pdf, 'F')
    print("done with pdf")

# COMMAND ----------

# DBTITLE 1,old fpdf library
# def pdf_prep(
#     mailed_pdf,
#     uspto_image_loc,
#     title_tx_1,
#     title_tx_2,
#     tm_analytics_image_loc,
#     result_df
# ):
#     """This function instantiates class for pdf_prep, creates and saves temp space"""
#     class PDF(FPDF):
#         def header(self):
#             # Logo
#             self.image(uspto_image_loc, 10, 8, 33)
#             # Arial bold 12
#             self.set_font('Arial', 'B', 9)  # Reduced font size
#             # Move to the right
#             self.cell(80)
#             # Title
#             self.cell(30, 10, title_tx_1, 0, 0, 'C')
#             self.ln(5)  # enter
#             self.cell(80)
#             self.cell(30, 10, title_tx_2, 0, 0, 'C')
#             self.ln(5)
#             self.cell(80)
#             # Line break
#             self.ln(20)
#             self.image(tm_analytics_image_loc, 160, 6, 23)

#     # Instantiation of inherited class
#     pdf = PDF()
#     # start creating ....
#     pdf.add_page()

#     # Calculate column widths based on content
#     col_widths = []
#     for col in result_df.columns:
#         max_width = pdf.get_string_width(col) + 4  # Add some padding
#         for value in result_df[col]:
#             current_width = pdf.get_string_width(str(value)) + 4
#             if current_width > max_width:
#                 max_width = current_width
#         col_widths.append(max_width)

#     # Set font for table header
#     pdf.set_font('Arial', 'B', 7)  # Reduced font size
#     pdf.set_fill_color(0, 75, 126)
#     pdf.set_text_color(255, 255, 255)

#     # Write the column headers
#     for i, col in enumerate(result_df.columns):
#         pdf.cell(col_widths[i], 4, col, 1, 0, 'C', fill=True)
#     pdf.ln(10)

#     # Set font for table rows
#     pdf.set_font('Arial', '', 7)  # Reduced font size
#     pdf.set_text_color(0, 0, 0)

#     # Write the table rows
#     for i in range(len(result_df)):
#         if i % 2 == 0:
#             pdf.set_fill_color(224, 224, 224)
#         else:
#             pdf.set_fill_color(249, 249, 249)

#         for j, col in enumerate(result_df.columns):
#             value = str(result_df.iloc[i][col])
#             if col == "Days to Auto Protect" and float(value) < 0:
#                 pdf.set_text_color(255, 0, 0)  # Red color for negative values
#             else:
#                 pdf.set_text_color(0, 0, 0)  # Default color for other values
#             pdf.cell(col_widths[j], 5, value, 1, 0, 'C', fill=True)
#         pdf.ln(5)

#     pdf.output(mailed_pdf, 'F')
#     print("done with pdf")

# COMMAND ----------

from fpdf import FPDF
import tempfile
import os

if not result_df.empty:    
    parms = {}

    # Save the DataFrame to a temporary directory as a PDF file
    with tempfile.TemporaryDirectory() as tmpdir:
        os.makedirs(tmpdir, exist_ok=True)
        
        filepath1 = f"{tmpdir}/{mailed_pdf}"
        pdf_prep(
            filepath1,
            uspto_image_loc,
            title_tx_1,
            title_tx_2,
            tm_analytics_image_loc,
            result_df
        )

        attachments = [filepath1]

        email_subj = f"""TM International Auto Protection Report"""
        email_body = """
                Attached is the TM International Auto Protection Report. 
                <br><br>
                Thank you. 
            """

        # Send the email with the attachment
        send_email_report(
            job_nm = job_name,
            subject = email_subj,
            send_from = from_addr,
            send_to = email_id,
            send_to_cc = cc_email,
            html_body = email_body,
            attachments = attachments
        )
       
else:
    print("No email notification sent - result_df is empty")

# COMMAND ----------

# data quality entry
#tbl1 = f"{trgt_catalog}.gold.tmintl_auto_protect"
#tbl2 = f"hive_metastore.{altrx_schema}.tmintl_auto_protect"
#key_cols = ['am_ser_num']
#dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
#print(dq_result)

# COMMAND ----------

recs_count = dbx_table.count()
end_job_cntl(f"{trgt_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")
