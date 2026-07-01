# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
tmngpdb_catalog = common_configs['schema']['tmngpdb_src_catalog']
tmworker_catalog = common_configs['schema']['tmworker_catalog']
edw_scope = common_configs['secrets']['edw_scope']
to_addr = common_configs['alerting']['new_fee_cd']['email']
altrx_schema = common_configs['schema']['altrx_schema']
dq_catalog = common_configs['schema']['data_quality_catalog']
primary_email, cc_email = common_configs["alerting"]["tm_fee_code_daily"]["email"], common_configs["alerting"]["tm_fee_code_daily"]["cc"]
# FEE table doesn't exist on dev server, instead FEE_2
if dbx_env == 'prod':
    ip2_tbl = 'REVENUE_SUMMARY'
else:
    ip2_tbl = 'REVENUE_SUMMARY_2'

# COMMAND ----------

from datetime import datetime
import pytz
# set current time for both while loop and job control
curntdt = datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trmreports_tm_fee_code_daily_income'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

revenue_df = read_data_from_oracle_conn_dsu(
    sql_query=f"select * from FORECAST.{ip2_tbl} where fee_cd_act_sum_acctg_da >= to_date('2024-10-01', 'YYYY-MM-DD')", 
    schema_name="",
    secrets_name=edw_scope,
)

# COMMAND ----------

fee_cd = spark.sql(f"select cat_2_desc as CAT_2_DESC,fee_cd as FEE_CD, fee_cd_grp_id as FEE_CD_GRP_ID,patent_tm as CAT_1_DESC, tm_paper_electronic as TM_PAPER_ELECTRONIC  from {reporting_catalog}.bronze.fee_cd")
#display(fee_cd)

# COMMAND ----------

from pyspark.sql.functions import to_date, current_date

filtered_revenue_df = revenue_df.filter(
    (to_date(revenue_df.FEE_CD_ACT_SUM_ACCTG_DA) >= "2024-10-01") &
    (to_date(revenue_df.FEE_CD_ACT_SUM_ACCTG_DA) < current_date())
)
filtered_fee_cd = fee_cd.filter(~fee_cd['FEE_CD'].isin(['8901', '8902', '8904', '9101', '9201', '9202', '9209']))

# COMMAND ----------

from pyspark.sql import Row

# Create the initial DataFrame
data = [Row(FEE_CD='6951'), Row(FEE_CD='6209'), Row(FEE_CD='7954')]
text_df = spark.createDataFrame(data)

# Perform the union
union_df = filtered_fee_cd.unionByName(text_df, allowMissingColumns=True)

#display(union_df)

# COMMAND ----------

joined_df = union_df.join(filtered_revenue_df, union_df['FEE_CD'] == filtered_revenue_df['FEE_CD'], 'inner')

# Select only one FEE_CD and FEE_NM column
joined_df = joined_df.select(
    filtered_revenue_df['FEE_CD'],
    filtered_revenue_df['FEE_NM'],
    union_df["CAT_2_DESC"],
    union_df["CAT_1_DESC"],
    union_df["TM_PAPER_ELECTRONIC"],
    filtered_revenue_df['FEE_CD_ACT_SUM_ID_NO'],
    filtered_revenue_df['FEE_CD_ACT_SUM_ACCTG_DA'],
    filtered_revenue_df['REV_FEE_CD_ACT_SUM_CREAT_TS'],
    filtered_revenue_df['CASH_DAILY_CL'],
    filtered_revenue_df['CASH_MTD_CL'],
    filtered_revenue_df['CASH_YTD_CL'],
    filtered_revenue_df['CASH_DLY_CU_NO'],
    filtered_revenue_df['CASH_MTD_CU_NO'],
    filtered_revenue_df['CASH_YTD_CU_NO'],
    filtered_revenue_df['CASH_DAILY_INCOME'],
    filtered_revenue_df['CASH_MONTH_TO_DATE_INCOME'],
    filtered_revenue_df['CASH_YEAR_TO_DATE_INCOME'],
    filtered_revenue_df['DEPOSIT_ACCT_DAILY_INCOME'],
    filtered_revenue_df['DEPOSIT_ACCT_MTD_INCOME'],
    filtered_revenue_df['DEPOSIT_ACCT_YTD_INCOME']
)

#display(joined_df)

# COMMAND ----------

from pyspark.sql.functions import col, when, date_format

joined_df = joined_df.withColumn(
    "DAILY_INCOME_EDW",
    col("CASH_DAILY_INCOME") + col("DEPOSIT_ACCT_DAILY_INCOME")
)

joined_df = joined_df.withColumn(
    "DAILY_INCOME_EDW",
    when(
        (col("FEE_CD") == "7951") & (col("CASH_DLY_CU_NO") > 0),
        col("DAILY_INCOME_EDW") - col("CASH_DLY_CU_NO")
    ).otherwise(col("DAILY_INCOME_EDW"))
)

joined_df = joined_df.filter(joined_df.DAILY_INCOME_EDW != 0)
#display(joined_df)

# COMMAND ----------

from pyspark.sql.functions import date_format, col
from pyspark.sql.types import IntegerType, DateType

selected_df = joined_df.select(
    "FEE_CD_ACT_SUM_ACCTG_DA", "FEE_CD", "FEE_NM", "DAILY_INCOME_EDW",
    "TM_PAPER_ELECTRONIC", "CAT_1_DESC", "CAT_2_DESC"
)
selected_df = selected_df.withColumn("DAILY_INCOME_EDW", col("DAILY_INCOME_EDW").cast(IntegerType()))
selected_df = selected_df.orderBy(col("FEE_CD_ACT_SUM_ACCTG_DA").desc(), col("FEE_CD").asc())
selected_df = selected_df.withColumn("FEE_CD_ACT_SUM_ACCTG_DA", date_format("FEE_CD_ACT_SUM_ACCTG_DA", "yyyy-MM-dd"))
selected_df = selected_df.withColumn("FEE_CD_ACT_SUM_ACCTG_DA", col("FEE_CD_ACT_SUM_ACCTG_DA").cast(DateType()))
#display(selected_df)

# COMMAND ----------

# DBTITLE 1,write to DBX
target_table_name = f"{reporting_catalog}.gold.tm_fee_code_daily_income"
selected_df.write.mode("overwrite").format("delta").insertInto(target_table_name)

# COMMAND ----------

email_output = selected_df

# COMMAND ----------

#############################################################################################
# 5/2/25 - Commented out data quality check code since it has been succeeding consistently. #
# Allows disabling Alteryx workflow schedule fully, saving resources.                       #
#############################################################################################


# # data quality entry
# tbl1 = f"{reporting_catalog}.gold.tm_fee_code_daily_income"
# tbl2 = f"hive_metastore.{altrx_schema}.tm_fee_code_daily_income"
# key_cols = ['fee_cd']
# dq_result = alteryx_data_match(tbl2, tbl1, key_cols, job_name, dq_catalog)
# print(dq_result)

# COMMAND ----------

recs_count = selected_df.count()
end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")

# COMMAND ----------

from datetime import datetime

# Get today's date in the desired format
today_date = datetime.now().strftime("%Y-%m-%d")

# Update the attachment name with today's date
attachment = f"TM_Fee_Code_Daily_Income_{today_date}.xlsx"

print(f"Sending email to: {primary_email} [primary], {cc_email} [cc]")
send_mail(
    send_from="trademark_analytics@uspto.gov",
    send_to=primary_email,  
    send_to_cc=cc_email,
    subject="Fee Code Daily Income",
    text="""
    See Attached. 
    Fee Code Daily Income.""",
    data_to_attach=email_output,
    attachment_name=attachment
)