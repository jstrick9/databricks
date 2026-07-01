# Databricks notebook source
# MAGIC %md
# MAGIC ### **ntb_attorney_outlier_growth_foreign_applicants**

# COMMAND ----------

from bs4 import BeautifulSoup
from pyspark.sql.functions import array_sort

# COMMAND ----------

# DBTITLE 1,setting up env
dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env")
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name

print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,Get Current Date
from datetime import datetime
current_date = datetime.today().strftime("%B %d, %Y")

# COMMAND ----------

common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
tmngpdb_src_catalog = common_configs['schema']['tmngpdb_src_catalog']
run_env = common_configs['schema']['tmngpdb_src_catalog']
primary_email, cc_email = common_configs["alerting"]["Attorney_Outlier_Growth"]["email"], common_configs["alerting"]["Attorney_Outlier_Growth"]["cc"]
altrx_schema = common_configs['schema']['altrx_schema']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(reporting_catalog,tmngpdb_src_catalog,primary_email, cc_email,altrx_schema,data_quality_catalog)
data_layer = "bronze"

# COMMAND ----------

# DBTITLE 1,Start Job Control
job_name = 'ntb_trmreports_attorney_outlier_growth_foreign_applicants'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,job_start_ts)

# COMMAND ----------

# DBTITLE 1,Inputs
input_102=spark.sql(f"""SELECT * EXCEPT(create_ts,create_user_id,update_ts,update_user_id) FROM {reporting_catalog}.silver.correspondence """)
input_105=spark.sql(f"""select * EXCEPT(create_ts,create_user_id,update_ts,update_user_id) from {reporting_catalog}.gold.filings_dashboard""")
## Need to load the data from production for the first time.
input_110= spark.sql(f"""SELECT * FROM {reporting_catalog}.gold.attorney_history""")

# COMMAND ----------

fltr_59 = input_102.filter(col("atty_nm").isNotNull() & (col("atty_nm") != ""))

# COMMAND ----------

#input_105
fltr_3 = input_105.filter("top_2_years == 'True'") 

# COMMAND ----------

# Define the expressions for the conditions
pendency_cal_start_expr = expr("""
    CASE 
        WHEN DATEDIFF(Pendency_Cal_Start_DT, TO_DATE(CONCAT(YEAR(Pendency_Cal_Start_DT), '-01-01'))) + 1 < 274 
        THEN DATEDIFF(Pendency_Cal_Start_DT, TO_DATE(CONCAT(YEAR(Pendency_Cal_Start_DT), '-01-01'))) + 1 + 92 
        WHEN DATEDIFF(Pendency_Cal_Start_DT, TO_DATE(CONCAT(YEAR(Pendency_Cal_Start_DT), '-01-01'))) + 1 >= 274 
        THEN DATEDIFF(Pendency_Cal_Start_DT, TO_DATE(CONCAT(YEAR(Pendency_Cal_Start_DT), '-01-01'))) + 1 - 273 
        ELSE 0 
    END
""")

max_pendency_cal_start_expr = expr("""
    CASE 
        WHEN DATEDIFF(Max_Pendency_Cal_Start_DT, TO_DATE(CONCAT(YEAR(Max_Pendency_Cal_Start_DT), '-01-01'))) + 1 < 274 
        THEN DATEDIFF(Max_Pendency_Cal_Start_DT, TO_DATE(CONCAT(YEAR(Max_Pendency_Cal_Start_DT), '-01-01'))) + 1 + 92 
        WHEN DATEDIFF(Max_Pendency_Cal_Start_DT, TO_DATE(CONCAT(YEAR(Max_Pendency_Cal_Start_DT), '-01-01'))) + 1 >= 274 
        THEN DATEDIFF(Max_Pendency_Cal_Start_DT, TO_DATE(CONCAT(YEAR(Max_Pendency_Cal_Start_DT), '-01-01'))) + 1 - 273 
        ELSE 0 
    END
""")

# Apply the expressions and create the FYTD column
frml_4 = fltr_3.withColumn(
    "FYTD",
    abs_col(pendency_cal_start_expr) <= abs_col(max_pendency_cal_start_expr)
)

fltr_5 = frml_4.filter("FYTD == True")

# COMMAND ----------

# R: fltr_5
# L: input_102
join_6 = input_102.alias("LJ") \
  .join(fltr_5.alias("RJ"),
        on=input_102["ser_num"] == fltr_5["ser_num"]) \
.select(
col('LJ.ser_num').alias('input_102_ser_num'),
'cor_nm',
'firm_nm',
'add_line1',
'add_line2',
'city_nm',
'zipcode',
'state_cd',
'state_nm',
'ctry_cd',
col('LJ.ctry_nm').alias('input_102_ctry_nm'),
'ctry_name_caps',
col('LJ.country_or_area_name').alias('input_102_country_or_area_name'),
'iso_alpha3_code',
'ip_att_docket_ref',
'atty_nm',
'domestic_rep',
'at_email_auth',
'at_email',
'cr_email1',
'cr_email2',
'cr_email3',
'cr_email4',
'cr_email_auth',
col('RJ.ser_num').alias('fltr_5_ser_num'),
'pendency_cal_start_dt',
'filing_fy',
'non_pro_se',
'filing_method_filed',
'filing_basis_grp',
'class',
'name',
'city',
'ste_ctry_cd',
'postal_cd',
# col('RJ.ctry_nm').alias('Right_ctry_nm'),
# col('RJ.country_or_area_name').alias('Right_country_or_area_name'),
'count',
'max_pendency_cal_start_dt',
'coordinated_class',
'filing_fy2',
'filing_fy_month_int',
'filing_fy_quarter',
'filing_fy_month',
'top_2_years',
'fee_paid_class',
'max_filing_fy',
'pctram_link',
'fixed_count',
'realtime_count',
'tram_count',
'goods_or_services',
'concat_goods_or_services',
'entity_type',
'applicant_bin',
'FYTD')

column_count = len(join_6.columns)
print(f"The number of columns in the DataFrame is: {column_count}")

# COMMAND ----------

# Filter 10 and filter 07 -- New code
fltr_10 = join_6.filter(col("atty_nm").isNotNull() & (col("atty_nm") != ""))

fltr_7 = fltr_10.filter((fltr_10["input_102_country_or_area_name"] != 'United States of America') | (fltr_10["input_102_country_or_area_name"].isNull()))

# COMMAND ----------

# new code 
sumrz_61 = fltr_7.select(col("atty_nm").alias("ATTY_NM"),col("input_102_country_or_area_name").alias("Atty_Countries")).distinct()

sumrz_62 = sumrz_61.groupBy(col("ATTY_NM")).agg(concat_ws(", ", array_sort(collect_list("Atty_Countries"))).alias("OWNR_CNTRY"))



# COMMAND ----------

sumrz_11 = fltr_7.groupBy(
    col("atty_nm").alias("ATTY_NM"), 
    col("filing_fy").alias("Filing_FY")
).agg(
    _sum("realtime_count").alias("Sum_Realtime_Count")
)

# COMMAND ----------

# Filter 69 and filter 28
fltr_69_28= join_6.filter(
    ((col("input_102_country_or_area_name") != "United States of America") | 
    col("input_102_country_or_area_name").isNull()) 
).filter(join_6["Filing_FY"] == join_6["Max_Filing_FY"])


sumrz_25 = fltr_69_28 \
    .agg(_sum("realtime_count").alias("Sum_Realtime_Count"),
         max("Pendency_Cal_Start_DT").alias("Max_Pendency_Cal_Start_DT"))


# COMMAND ----------

# input_102
# fltr_59 fltr_3
join_58 =  fltr_59.alias("LJ") \
  .join(fltr_3.alias("RJ"),
        on=fltr_59["ser_num"] == fltr_3["ser_num"]
        ) \
.select(
col('LJ.ser_num').alias('fltr_59_ser_num'),
"cor_nm",
"firm_nm",
"add_line1",
"add_line2",
"city_nm",
"zipcode",
"state_cd",
"state_nm",
"ctry_cd",
col("LJ.ctry_nm").alias("fltr_59_ctry_nm"),
"ctry_name_caps",
col('LJ.country_or_area_name').alias('fltr_59_country_or_area_name'),
"iso_alpha3_code",
"ip_att_docket_ref",
col("LJ.atty_nm").alias("fltr_59_atty_nm"),
"domestic_rep",
"at_email_auth",
"at_email",
"cr_email1",
"cr_email2",
"cr_email3",
"cr_email4",
"cr_email_auth",
col('RJ.ser_num').alias('fltr_3_ser_num'),
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
# "ctry_nm",
# "country_or_area_name",
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
"pctram_link",
"fixed_count",
"realtime_count",
"tram_count",
"goods_or_services",
"concat_goods_or_services",
"entity_type",
"applicant_bin",
"output_record_count")
 

# COMMAND ----------

# Filter 67 and filter 68
# New Code
fltr_67 = join_58.filter(col("fltr_59_atty_nm").isNotNull() & (col("fltr_59_atty_nm") != ""))
    
fltr_68=  fltr_67.filter( 
    (join_58["fltr_59_country_or_area_name"] != 'United States of America') | (join_58["fltr_59_country_or_area_name"].isNull()))

# COMMAND ----------

# New Code  
window_spec = Window.partitionBy("ATTY_NM").orderBy(col("Filing_FY").asc())  
multi_row_12 = sumrz_11.withColumn("ID", row_number().over(window_spec) )

# COMMAND ----------

sumrz_13 = multi_row_12.groupBy("ATTY_NM").agg(max("ID").alias("Max_ID"))

join_14 = multi_row_12.alias("LJ")\
  .join(sumrz_13.alias("RJ"),
        col("LJ.ATTY_NM") == col("RJ.ATTY_NM"), "inner") \
          .select(
            col("LJ.ATTY_NM").alias("multi_row_12_ATTY_NM"),
            "Filing_FY",
            "Sum_Realtime_count",
            "ID",
            col("RJ.ATTY_NM").alias("sumrz_13_ATTY_NM"),
            "Max_id"
          )

# COMMAND ----------

sumrz_15= fltr_7.agg(max("Filing_FY").alias("Max_Filing_FY"))

# COMMAND ----------

# R: fltr_68 L:sumrz_15
join_44 = sumrz_15.alias("LJ") \
  .join(fltr_68.alias("RJ"),
        col("LJ.Max_Filing_FY") == col("RJ.filing_fy"), "inner")
        

# COMMAND ----------


# Create the J anchor DataFrame (records that joined from L to R)
join_44_j_anchor = join_44.select("LJ.*", "RJ.*")

# Create the R anchor DataFrame (records from R that didn't join to L)
join_44_r_anchor = fltr_68.alias("RJ").join(
    sumrz_15.alias("LJ"),
    col("RJ.filing_fy") == col("LJ.Max_Filing_FY"),
    "left_anti"
)

# COMMAND ----------

sumrz_46 = join_44_r_anchor.groupBy(col("fltr_59_atty_nm").alias("ATTY_NM")) \
  .agg(
    _sum("realtime_count").alias("PFY_Total")
  )

sumrz_47 = join_44_j_anchor.groupBy(col("fltr_59_atty_nm").alias("ATTY_NM")) \
  .agg(
    _sum("realtime_count").alias("FYTD_Total")
  )  

# COMMAND ----------

# sumrz_15  & join_14
max_filing_fy_value = sumrz_15.collect()[0][0]
appnd_16 = join_14.withColumn("Max_Filing_FY",lit(max_filing_fy_value))

# COMMAND ----------

frml_17 = appnd_16.withColumn(
    "Flag",
    when((col("Max_ID") != 2) & (col("Filing_FY") == col("Max_Filing_FY")), col("Max_Filing_FY") - 1)
    .when((col("Max_ID") != 2) & (col("Filing_FY") != col("Max_Filing_FY")), col("Max_Filing_FY"))
    .otherwise(lit(None))
)

# COMMAND ----------

fltr_18_T = frml_17.filter(col("Flag").isNotNull())
fltr_18_F = frml_17.filter(col("Flag").isNull())

frml_19 = fltr_18_T.withColumn("Sum_Realtime_count", lit(1)) \
                   .withColumn("PFY_Total", lit(0))

sel_22 = frml_19.select(
    col("multi_row_12_ATTY_NM"),
    col("Sum_Realtime_count"),
    col("ID"),
    col("sumrz_13_ATTY_NM"),
    col("Max_id"),
    col("Max_Filing_FY"),
    col("Flag").alias("Filing_FY"),
    col("PFY_Total")
)


# COMMAND ----------

union_20 = frml_17.unionByName(sel_22,allowMissingColumns=True)

# COMMAND ----------

Total_Fillings_MAXFY = sumrz_25.collect()[0][0]
Max_Pendency_Cal_Start_DT = sumrz_25.collect()[0][1]
appnd_26 = union_20.withColumn(
    "Total_Fillings_MAXFY", lit(Total_Fillings_MAXFY)
).withColumn("Max_Pendency_Cal_Start_DT", lit(Max_Pendency_Cal_Start_DT))
sort_21 = appnd_26.select(
    "multi_row_12_ATTY_NM",
    "Sum_Realtime_count",
    "ID",
    "sumrz_13_ATTY_NM",
    "Max_id",
    "Max_Filing_FY",
    "Filing_FY",
    "PFY_Total",
    "Flag",
    "Total_Fillings_MAXFY",
    "Max_Pendency_Cal_Start_DT",
).sort(col("multi_row_12_ATTY_NM").asc(), col("Filing_FY").asc())
# Asc() added as per feedback

# COMMAND ----------

# Define the window specification
window_spec = Window.orderBy("multi_row_12_ATTY_NM","Filing_FY")
# Use lag to get the previous row's Sum_Realtime_count
lag_count_DF = sort_21.withColumn(
    "prev_Sum_Realtime_count", lag("Sum_Realtime_count").over(window_spec)
)

# Calculate Percent_growth
multi_row_23 = lag_count_DF.withColumn(
    "Percent_Growth",
    when(
        col("Max_Filing_FY") == col("Filing_FY"),
        (
            (col("Sum_Realtime_count") - col("prev_Sum_Realtime_count"))
            / col("prev_Sum_Realtime_count")
        )
        * 100,
    ).otherwise(None),
)

# Drop the temporary column used for lag
multi_row_23 = multi_row_23.drop("prev_Sum_Realtime_count")
fltr_24 = multi_row_23.filter(col("Percent_Growth").isNotNull())\
    .withColumn(
    "Percent_Growth", col("Percent_Growth").cast(DecimalType(19, 4)))


# COMMAND ----------

frml_27 = fltr_24.withColumn(
    "Percent_Total_Filings",
    format_number(
        (col("Sum_Realtime_count") / col("Total_Fillings_MAXFY")) * 100, 4
    ).cast(DecimalType(19, 4)),
).withColumn(
    "GAC",
    (col("Percent_Growth") * col("Percent_Total_Filings")).cast(DecimalType(19, 2)),
)

sumrz_29 = (
    frml_27.select(
        "multi_row_12_ATTY_NM", "Percent_Total_Filings", "Percent_Growth", "GAC"
    )
    .distinct()
    .orderBy(col("GAC").desc())
)


frml_31 = sumrz_29.withColumn(
    "Alert", when(col("GAC") >= 50, True).otherwise(False)
).withColumn("Alert_Type", lit("Attorney Representing Foreign Owner"))
fltr_32 = frml_31.filter("Alert == 'True'")

# COMMAND ----------

fltr_55 = fltr_18_F.filter("ID == 1")  
sumr_41 = fltr_55.groupBy("multi_row_12_ATTY_NM").agg(
    _sum("Sum_Realtime_Count").alias("PFYTD_Total")
)
sumr_42 = frml_19.groupBy("multi_row_12_ATTY_NM").agg(
    _sum("PFY_Total").alias("PFYTD_Total")
)

union_43 = sumr_41.union(sumr_42)


# COMMAND ----------

join_48_f_anchor = (
    union_43.alias("LJ")
    .join(
        sumrz_46.alias("RJ"),
        (col("LJ.multi_row_12_ATTY_NM") == col("RJ.ATTY_NM")),
        "fullouter",
    )
    .select(
        coalesce(col("RJ.ATTY_NM"), col("LJ.multi_row_12_ATTY_NM")).alias("ATTY_NM"),
        col("LJ.PFYTD_Total"),
        col("LJ.multi_row_12_ATTY_NM"),
        col("RJ.PFY_Total"),
    )
)

# COMMAND ----------

# sumrz_47
# join_48_f_anchor Full outer join
join_49_f_anchor = (
    join_48_f_anchor.alias("LJ")
    .join(sumrz_47.alias("RJ"), col("LJ.ATTY_NM") == col("RJ.ATTY_NM"), "fullouter")
    .select(
        col("LJ.ATTY_NM").alias("join_48_f_ATTY_NM"),
        col("LJ.PFYTD_Total").alias("PFYTD_Total"),
        col("LJ.multi_row_12_ATTY_NM").alias("multi_row_12_ATTY_NM"),
        col("LJ.PFY_Total").alias("PFY_Total"),
        col("RJ.ATTY_NM").alias("sumrz_47_ATTY_NM"),
        col("RJ.FYTD_Total").alias("FYTD_Total"),
    )
)

# COMMAND ----------

## Group by clause 
sumrz_52 = join_49_f_anchor.select(
  "join_48_f_ATTY_NM",
  "PFYTD_Total",
  "PFY_Total",
  "FYTD_Total").distinct()
  

# COMMAND ----------

## Replace all nulls value with 0 - as per feedback
clens_53 = sumrz_52.fillna(
    {
        "PFYTD_Total": 0,
        "PFY_Total": 0,
        "FYTD_Total": 0
    }
)

frml_56 = clens_53.withColumn("FYTD_Delta", (col("FYTD_Total") - col("PFYTD_Total")))

# COMMAND ----------

join_54 = fltr_32.alias("LJ").join(
    frml_56.alias("RJ"),
    col("LJ.multi_row_12_ATTY_NM") == col("RJ.join_48_f_ATTY_NM"),
    "inner",
)

join_64 = (
    sumrz_62.alias("LJ")
    .join(join_54.alias("RJ"), col("LJ.ATTY_NM") == col("RJ.multi_row_12_ATTY_NM"))
    .select(
        col("LJ.ATTY_NM"),
        col("LJ.OWNR_CNTRY"),
        col("PFYTD_Total"),
        col("PFY_Total"),
        col("FYTD_Total"),
        col("FYTD_Delta"),
    )
)

# COMMAND ----------

#Defination "uppercase_columns" available in notebook "ntb_comm_imports_altx"
sel_111 = uppercase_columns(input_110)

# COMMAND ----------

## Left Outer Join
join_94_lo = (
    join_64.alias("LJ")
    .join(sel_111.alias("RJ"), col("LJ.ATTY_NM") == col("RJ.ATTY_NM"), "left_outer")
    .select(
        "LJ.ATTY_NM",
        "LJ.OWNR_CNTRY",
        "LJ.PFYTD_Total",
        "LJ.PFY_Total",
        "LJ.FYTD_Total",
        "LJ.FYTD_Delta",
    )
)
## Left anti join
join_94_l_anchor = (
    join_64.alias("LJ")
    .join(sel_111.alias("RJ"), col("LJ.ATTY_NM") == col("RJ.ATTY_NM"), "left_anti")
    .select(
        "LJ.ATTY_NM",
        "LJ.OWNR_CNTRY",
        "LJ.PFYTD_Total",
        "LJ.PFY_Total",
        "LJ.FYTD_Total",
        "LJ.FYTD_Delta",
    )
)
##inner Join
join_94_ln_anchor = (
    join_64.alias("LJ")
    .join(sel_111.alias("RJ"), col("LJ.ATTY_NM") == col("RJ.ATTY_NM"), "inner")
    .select(
        "LJ.ATTY_NM",
        "LJ.OWNR_CNTRY",
        "LJ.PFYTD_Total",
        "LJ.PFY_Total",
        "LJ.FYTD_Total",
        "LJ.FYTD_Delta",
    )
)

frml_96 = join_94_l_anchor.withColumn("ATTY_NM", concat(col("ATTY_NM"), lit(" (New!)")))

union_95 = join_94_ln_anchor.unionByName(frml_96)
sort_93 = union_95.sort(col("FYTD_Total").desc())

# COMMAND ----------

# DBTITLE 1,final Dataframes
attny_histry_df = join_94_lo

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write dataframe in HTML table

# COMMAND ----------

# DBTITLE 1,Styling Email Template
EMAIL_CSS = """
    body {
        font-family: Loto, sans-serif;
        margin: 20px;
        font-size: 12;
    }

    .table-container {
        width: 600px;
        display: flex;
        flex-wrap: wrap;
        float: left;
    }

    .footer {
        clear: both;
        text-align: left;
        padding-top: 20px;
    }

    #container {
        width: 800px;
    }

    #legend {
        width: 190px;
        float: right;
        font-size: 0.7em;
        margin-left: 10px;
    }

    table {
        border-collapse: collapse;
        background-color: #fff;
        box-sizing: border-box;
    }

    th, td {
        text-align: left;
        padding: 12px, 15px;
    }

    thead th {
        background-color: #c8e6fa;
        color: #000000;
        font-weight: bold;
    }

    tbody tr {
        border-bottom: 1px solid #ddd;
    }

    .odd-row {
        background-color: #f0f0f0
    }

    .new-row {
        background-color: #bfffbf
    }
"""

# COMMAND ----------

# DBTITLE 1,creating Email body based on Stylesheet
styling = f"<style>{EMAIL_CSS}</style>"

body = f"""
{styling}

<html><body>
<h3>Attorney Outlier Growth (Foreign Applicants)</h3>

<div id='container'>
<div class='table-container'>
{sort_93.toPandas().sort_values(by="ATTY_NM", axis=0, ascending=True).to_html(index=False)}
</div>
<div id='legend'>
<H5>Acronyms used:<br></H5>
<H5>PFY: Previous Fiscal Year<br> </H5>
<H5>PFYTD: Previous Fiscal Year To Date<br></H5>
<H5>FYTD: Current Fiscal Year To Date<br> </H5>
<H5>FYTD Delta: FYTD - PFYTD </H5>
</div>
</div>
<div class='footer'>Generated on {current_date}</div>
</body></html>
"""

# COMMAND ----------

# DBTITLE 1,Extracting Data with BeautifulSoup
soup = BeautifulSoup(body)

# COMMAND ----------

### Set banded formatting for odd rows
row = 0
for c in soup.find("tbody").descendants:
    if c.name == 'tr':
        if row % 2 > 0:
            c['class'] = 'odd-row'
        row+=1

# COMMAND ----------

### Search for any cells with '(New!)' in their text and apply formatting to entire row if found
for r in soup.find("tbody").descendants:
    if r.name == 'tr':
        new = False
        for c in r:
            if '(New!)' in c.string:
                new = True
                print(c.string)
        if new == True:
            r['class'] = 'new-row'

# COMMAND ----------

email_body = str(soup)

# COMMAND ----------

# Email
to = primary_email
cc = cc_email
from_addr = "Trademark_Analytics@uspto.gov"
subj = f"Attorney Outlier Growth (Foreign Applicants)"

notify = Notify()

# Attach the PDF file
attachments = ""

# Compose the email with the attachment
msg = notify.compose_email_attachment_with_html_body(
    html=email_body,
    subj=subj,
    to=to,
    Cc=cc,
    from_addr=from_addr,
    filepaths=attachments,  # Attach the file using the file path
)

# Send the email
notify.send_mail(msg)
print("Email Sent")

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Write Data into Tables. 

# COMMAND ----------

# DBTITLE 1,Writing the data in tables
attny_histry_df.write.mode("overwrite").format("delta").insertInto(
    f"{reporting_catalog}.gold.attorney_history"
)
recs_count = attny_histry_df.count()
try:
    # data quality entry altrx_schema
    # tbl1 = f"{reporting_catalog}.gold.attorney_history"
    # if dbx_env == "dev":
    #     tbl2 = f"hive_metastore.{altrx_schema}.attorney_history"
    # else:
    #     tbl2 = f"hive_metastore.{altrx_schema}.attorney_history"
    # key_cols = ["ATTY_NM"]
    # dq_catalog = data_quality_catalog
    # # job_name = job_name
    # dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
    # # print(dq_result)

    end_job_cntl(
        f"{reporting_catalog}.silver",
        job_name,
        job_start_ts,
        "completed",
        recs_count,
        "job completed successfully",
    )
    dbutils.notebook.exit(
        f"Completed Loading attroney_history with data quality check"
    )
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise
dbutils.notebook.exit(f"Failed Loading attroney_history ")
