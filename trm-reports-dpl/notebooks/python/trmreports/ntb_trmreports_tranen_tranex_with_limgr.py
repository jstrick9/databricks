# Databricks notebook source
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
spark.conf.set('conf.dbx_env', dbx_env)
edw_scope = common_configs["secrets"]["edw_scope"]
madrid_scope = common_configs['secrets']['madrid_scope']
primary_email, cc_email = common_configs["alerting"]["tranen_tanex"]["email"], common_configs["alerting"]["tranen_tanex"]["cc"]
altrx_schema = common_configs['schema']['altrx_schema']
dq_catalog = common_configs['schema']['data_quality_catalog']

# COMMAND ----------

# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trmreports_tranen_tranex_with_limgr'

control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,starttime)

# COMMAND ----------

# Calculate the date 7 days ago from today
seven_days_ago = (datetime.datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

tm_transaction_df = read_data_from_oracle_conn_dsu(
    sql_query=f"""
select TISUSRP.TM_TRANSACTION_STATUS.FK_TTO_ID,
       TISUSRP.TM_TRANSACTION_STATUS.OCCURRENCE_TS,
       TISUSRP.TM_TRANSACTION_STATUS.FK_STTS_CD,
       TISUSRP.TM_TRANSACTION_STATUS.FK_STSA_CD 
from TISUSRP.TM_TRANSACTION_STATUS 
where TISUSRP.TM_TRANSACTION_STATUS.OCCURRENCE_TS >= To_Date('{seven_days_ago}', 'YYYY-MM-DD') 
  and TISUSRP.TM_TRANSACTION_STATUS.FK_STTS_CD = 'COMPLETED' 
  and TISUSRP.TM_TRANSACTION_STATUS.FK_STSA_CD = 'IBOUT'
""", 
    schema_name="",
    secrets_name=madrid_scope
)

# COMMAND ----------

tm_transaction_df_out  = read_data_from_oracle_conn_dsu(
    sql_query=f"""
select TISUSRP.TM_TRANSACTION_OUT.ID,
    TISUSRP.TM_TRANSACTION_OUT.SERIAL_NUM,
    TISUSRP.TM_TRANSACTION_OUT.XML_DOC,
    TISUSRP.TM_TRANSACTION_OUT.CONTROL_NO,
    TISUSRP.TM_TRANSACTION_OUT.FK_IRN_IB_REF_NUM,
    TISUSRP.TM_TRANSACTION_OUT.PMT_DETAILS,
    TISUSRP.TM_TRANSACTION_OUT.FK_TO_ID 
from TISUSRP.TM_TRANSACTION_OUT 
where ((TISUSRP.TM_TRANSACTION_OUT.FK_ST_CD = 'TRANEN') or (TISUSRP.TM_TRANSACTION_OUT.FK_ST_CD = 'TRANEX'))""", 
    schema_name="",
    secrets_name=madrid_scope
)

# COMMAND ----------

tm_transaction_df = tm_transaction_df.withColumnRenamed("OCCURRENCE_TS", "OCCURRENCE")\
                                     .withColumnRenamed("FK_STTS_CD", "FK_STTS")\
                                     .withColumnRenamed("FK_STSA_CD", "FK_STSA")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,expr
from pyspark.sql.types import StringType
import cx_Oracle
# Define a UDF to convert BLOB to string
def blob_to_string(blob):
    return blob.decode('utf-8')


blob_to_string_udf = udf(blob_to_string, StringType())

# COMMAND ----------

tm_transaction_df_out  = tm_transaction_df_out.withColumn("XML_TEXT", blob_to_string_udf(tm_transaction_df_out["XML_DOC"]))

joined_df = tm_transaction_df.join(tm_transaction_df_out, tm_transaction_df.FK_TTO_ID == tm_transaction_df_out.ID).drop(tm_transaction_df.FK_TTO_ID, tm_transaction_df.OCCURRENCE, tm_transaction_df.FK_STTS, tm_transaction_df.FK_STSA)
#display(joined_df)
#display(tm_transaction_df_out )

# COMMAND ----------

import xml.etree.ElementTree as ET
from pyspark.sql.functions import udf, col, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType

def extract_fields(xml_text):
    try:
        if xml_text is None:
            return None
        root = ET.fromstring(xml_text)
        
        def get_values(tag):
            return [elem.text for elem in root.findall(f".//{tag}") if elem.text is not None]
        
        namadty_values = get_values("NAMADTY")
        offref_values = get_values("OFFREF")
        email_values = get_values("EMAIL")
        basappn_values = get_values("BASAPPN")
        limgr_values = get_values("LIMGR")
        dcpcd_values = get_values("DCPCD")
        dcpcd_2_values = get_values("DCPCD_2")
        dcpcd_3_values = get_values("DCPCD_3")
        dcpcd_4_values = get_values("DCPCD_4")
        gsgr_values = get_values("GSGR")
        gsfoot_values = get_values("GSFOOT")
        
        return {
            "NAMADTY": ",".join(namadty_values) if namadty_values else None,
            "OFFREF": ",".join(offref_values) if offref_values else None,
            "EMAIL": ",".join(email_values) if email_values else None,
            "BASAPPN": ",".join(basappn_values) if basappn_values else None,
            "LIMGR": ",".join(limgr_values) if limgr_values else None,
            "DCPCD": ",".join(dcpcd_values) if dcpcd_values else None,
            "DCPCD_2": ",".join(dcpcd_2_values) if dcpcd_2_values else None,
            "DCPCD_3": ",".join(dcpcd_3_values) if dcpcd_3_values else None,
            "DCPCD_4": ",".join(dcpcd_4_values) if dcpcd_4_values else None,
            "GSGR": ",".join(gsgr_values) if gsgr_values else None,
            "GSFOOT": ",".join(gsfoot_values) if gsfoot_values else None
        }
    except Exception as e:
        return None

schema = StructType([
    StructField("NAMADTY", StringType(), True),
    StructField("OFFREF", StringType(), True),
    StructField("EMAIL", StringType(), True),
    StructField("BASAPPN", StringType(), True),
    StructField("LIMGR", StringType(), True),
    StructField("DCPCD", StringType(), True),
    StructField("DCPCD_2", StringType(), True),
    StructField("DCPCD_3", StringType(), True),
    StructField("DCPCD_4", StringType(), True),
    StructField("GSGR", StringType(), True),
    StructField("GSFOOT", StringType(), True)
])
extract_fields_udf = udf(extract_fields, schema)
joined_df = joined_df.withColumn("extracted_fields", extract_fields_udf(col("XML_DOC")))
joined_df = joined_df.select(
    col("*"),
    col("extracted_fields.NAMADTY").alias("NAMADTY"),
    col("extracted_fields.OFFREF").alias("OFFREF"),
    col("extracted_fields.EMAIL").alias("EMAIL"),
    col("extracted_fields.BASAPPN").alias("BASAPPN"),
    col("extracted_fields.LIMGR").alias("LIMGR"),
    col("extracted_fields.DCPCD").alias("DCPCD"),
    col("extracted_fields.DCPCD_2").alias("DCPCD_2"),
    col("extracted_fields.DCPCD_3").alias("DCPCD_3"),
    col("extracted_fields.DCPCD_4").alias("DCPCD_4"),
    col("extracted_fields.GSGR").alias("GSGR"),
    col("extracted_fields.GSFOOT").alias("GSFOOT")
).drop("extracted_fields")

namadty_df = joined_df.select("ID","SERIAL_NUM", "CONTROL_NO", "FK_IRN_IB_REF_NUM","PMT_DETAILS", "NAMADTY")
namadty_df = namadty_df.withColumn("NAMADTY", regexp_replace(col("NAMADTY"), ",", ", "))
offref_df = joined_df.select("ID", "CONTROL_NO", "SERIAL_NUM", "FK_IRN_IB_REF_NUM","PMT_DETAILS","FK_TO_ID","OFFREF")
email_df = joined_df.select("ID", "CONTROL_NO", "SERIAL_NUM", "FK_IRN_IB_REF_NUM","PMT_DETAILS","FK_TO_ID","EMAIL")
email_df = email_df.withColumn("EMAIL", regexp_replace(col("EMAIL"), ",", ", "))
basappn_df = joined_df.select("ID", "CONTROL_NO", "SERIAL_NUM", "FK_IRN_IB_REF_NUM","PMT_DETAILS","FK_TO_ID", "BASAPPN")
limgr_df = joined_df.select("ID", "CONTROL_NO", "SERIAL_NUM", "FK_IRN_IB_REF_NUM","PMT_DETAILS", "LIMGR","DCPCD","DCPCD_2", "DCPCD_3","DCPCD_4","GSGR","GSFOOT" )

#display(namadty_df)
#display(offref_df)
#display(email_df)
#display(basappn_df)
#display(limgr_df)

# COMMAND ----------

from pyspark.sql.functions import trim, split
limgr_df = limgr_df.withColumn("LIMGR", trim(col("LIMGR")))
limgr_df = limgr_df.dropna(subset=["LIMGR"])

limgr_df = limgr_df.withColumn("LIMGR_SPLIT", split(col("LIMGR"), "\n"))
limgr_df = limgr_df.withColumn("LIMGR_1", col("LIMGR_SPLIT").getItem(0)) \
                   .withColumn("LIMGR_2", col("LIMGR_SPLIT").getItem(1)) \
                   .withColumn("LIMGR_3", col("LIMGR_SPLIT").getItem(2)) \
                   .drop("LIMGR_SPLIT")
limgr_df = limgr_df.select("ID", "CONTROL_NO", "SERIAL_NUM", "FK_IRN_IB_REF_NUM")
#display(limgr_df)

# COMMAND ----------

offref_df = offref_df.join(
    basappn_df,
    (basappn_df.ID == offref_df.ID) & (basappn_df.CONTROL_NO == offref_df.CONTROL_NO)
).select(
    offref_df.ID,
    offref_df.SERIAL_NUM,
    offref_df.CONTROL_NO,
    offref_df.FK_IRN_IB_REF_NUM,
    offref_df.PMT_DETAILS,
    offref_df.FK_TO_ID,
    offref_df.OFFREF,
    basappn_df.BASAPPN
)

#display(offref_df)

# COMMAND ----------

namadty_df = offref_df.join(
    namadty_df,
    (namadty_df.ID == offref_df.ID) & (namadty_df.CONTROL_NO == offref_df.CONTROL_NO)
).select(
    offref_df.ID,
    offref_df.SERIAL_NUM,
    offref_df.CONTROL_NO,
    offref_df.FK_IRN_IB_REF_NUM,
    offref_df.PMT_DETAILS,
    offref_df.FK_TO_ID,
    offref_df.OFFREF,
    offref_df.BASAPPN,
    namadty_df.NAMADTY.alias("NAME_TYPE")
)

# COMMAND ----------

email_final_df = namadty_df.join(
    email_df,
    (email_df.ID == namadty_df.ID) & (email_df.CONTROL_NO == namadty_df.CONTROL_NO)
).select(
    namadty_df.ID,
    namadty_df.SERIAL_NUM,
    namadty_df.CONTROL_NO,
    namadty_df.FK_IRN_IB_REF_NUM,
    namadty_df.PMT_DETAILS,
    namadty_df.FK_TO_ID,
    namadty_df.OFFREF,
    offref_df.BASAPPN,
    namadty_df.NAME_TYPE,
    email_df.EMAIL
)

#display(email_final_df)

# COMMAND ----------

final_df = email_final_df.join(
    limgr_df,
    (limgr_df.ID == email_final_df.ID) & (limgr_df.CONTROL_NO == email_final_df.CONTROL_NO)
).select(
    email_final_df.ID,
    email_final_df.SERIAL_NUM,
    email_final_df.CONTROL_NO,
    email_final_df.FK_IRN_IB_REF_NUM,
    email_final_df.PMT_DETAILS,
    email_final_df.FK_TO_ID,
    email_final_df.OFFREF,
    email_final_df.BASAPPN,
    email_final_df.NAME_TYPE,
    email_final_df.EMAIL
)
#display(final_df)

# COMMAND ----------

from pyspark.sql.functions import date_format, concat, date_add, current_date
result_df = final_df.join(tm_transaction_df, final_df.ID == tm_transaction_df.FK_TTO_ID).select(
    date_format(tm_transaction_df["OCCURRENCE"], "yyyy-MM-dd").alias("IBOUT_DATE"),
    tm_transaction_df["FK_STTS"].alias("IBOUT_STATUS"),
    tm_transaction_df["FK_STSA"].alias("FK_STSA_CD"),
    final_df["CONTROL_NO"],
    final_df["OFFREF"],
    final_df["BASAPPN"],
    final_df["NAME_TYPE"],
    final_df["EMAIL"],
    final_df["FK_IRN_IB_REF_NUM"],
    final_df["PMT_DETAILS"]
)
tab_name = result_df.withColumn(
    "TAB_NAME",
    concat(
        date_format(date_add(current_date(), -7), "yyyyMMdd"),
        lit(" - "),
        date_format(current_date(), "yyyyMMdd")
    )
)

#display(result_df)

# COMMAND ----------

dbx_table = result_df.select(
    result_df.IBOUT_DATE.alias("ibout_date"),
    result_df.IBOUT_STATUS.alias("ibout_status"),
    result_df.FK_STSA_CD.alias("fk_stsa_cd"),
    result_df.CONTROL_NO.alias("control_no"),
    result_df.OFFREF.alias("offref"),
    result_df.BASAPPN.alias("basappn"),
    result_df.NAME_TYPE.alias("name_type"),
    result_df.EMAIL.alias("email"),
    result_df.FK_IRN_IB_REF_NUM.alias("fk_irn_ib_ref_num"),
    result_df.PMT_DETAILS.alias("pmt_details")
)
#display(dbx_table)

# COMMAND ----------

dbx_table.write.mode("overwrite").format("delta").insertInto(f"{trgt_catalog}.gold.tranen_tranex_with_limgr")

# COMMAND ----------

email_output = result_df

# COMMAND ----------

tab_name = tab_name.select("TAB_NAME").first()["TAB_NAME"]
from_addr = "trademark_analytics@uspto.gov"
email_subj = f'TRANEN and TRANEX with LIMGR'
email_body = """Good morning,

Attached is the TRANEN and TRANEX with LIMGR Weekly Report.
"""
attachments = [(email_output, f"TRANEN and TRANEX with LIMGR {tab_name}.xlsx", "excel")]

# Send the email with the attachment
send_email_report(
    job_nm = job_name,
    subject = email_subj,
    send_from = from_addr,
    send_to = primary_email,
    send_to_cc=cc_email,
    html_body= email_body,
    attachments = attachments
)

# COMMAND ----------

# data quality entry
# tbl1 = f"{trgt_catalog}.gold.tranen_tranex_with_limgr"
# tbl2 = f"hive_metastore.{altrx_schema}.tranen_tranex_with_limgr"
# key_cols = ['control_no']
# dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
# print(dq_result)

# COMMAND ----------

recs_count = result_df.count()
end_job_cntl(f"{trgt_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")