# Databricks notebook source
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Overview of Correspondence ETL
# MAGIC
# MAGIC This notebook will gives us the overview for Correspondence ETL. Which contain the workflow diagram and Input and output dataframs.
# MAGIC Subsequent Notebook will provide the psudo code to flow
# MAGIC

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name

print(f'{config_file=},{dbx_env=}')
#reporting_catalog = "trm_reporting_dev"
schema_silver = "silver"
table_silver = "correspondence"

# COMMAND ----------

# MAGIC %run ./ntb_correspondence_etl_input $config_file=config_file

# COMMAND ----------

# MAGIC %md
# MAGIC ## Domestic Rep code block 

# COMMAND ----------

# DBTITLE 1,INPUT 1
#step 1- Selecting columns and renaming

ip1_df = ip1_df.select(
                    col("VT_TEXT_TYPE"),
                    col("VT_TEXT").alias("DOMESTIC_REP"),
                    col("VT_SER_NUM"),
                    col("VT_ENT_NUM")
)

# COMMAND ----------

# step 2 - sorting by ascending
ip1_df = ip1_df.orderBy("VT_SER_NUM", "VT_ENT_NUM")

# COMMAND ----------

# # step 3 - summarization
#########Review_Comment: Removed group by logic as text is not broken into multiple lines in TRM db
# ip1_df_group_by = ip1_df.groupBy(col("VT_SER_NUM")
#                                  ).agg(concat_ws(", ", collect_list(col("DOMESTIC_REP"))).alias("DOMESTIC_REP")
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cor_2ETL code blcok

# COMMAND ----------

#ip2_df.printSchema()

# COMMAND ----------

# DBTITLE 1,INPUT 2 
# input 2 - step 1 - filter out all Isnull data from CAD_SER_NUM

ip2_df = ip2_df.filter(col("CAD_SER_NUM").isNotNull())

# COMMAND ----------

#step 2 -  selecting columns and renaming them
ip2_df = ip2_df.select(
                    col("CAD_SER_NUM").alias("SER_NUM"),
                    col("CAD_INDVL_FULL_NM").alias("COR_NM"),
                    col("CAD_FIRM_NM").alias("FIRM_NM"),
                    col("CAD_ADDR_LINE1_TX").alias("ADD_LINE1"),
                    col("CAD_ADDR_LINE2_TX").alias("ADD_LINE2"),
                    col("CAD_CITY_NM").alias("CITY_NM"),
                    col("CAD_PSTL_CD").alias("ZIPCODE"),
                    col("CAD_CTRY_CD").alias("CTRY_CD"),
                    col("CAD_CTRY_NM").alias("CTRY_NM"),
                    col("CAD_GEO_RGN_CD").alias("STATE_CD"),
                    col("CAD_GEO_RGN_NM").alias("STATE_NM"),
                    col("AM_ATTY_DKT_NUM").alias("IP_ATT_DOCKET_REF")
    
)

# COMMAND ----------

# DBTITLE 1,INPUT 3 
# input3 step1 &  -> selecting specific coloumns 
ip3_df=ip3_df.select(col("*"))

# COMMAND ----------

# input3 step1 & Step2  -> rename VT_TEXT to ATTY_NM & Remove Group by logic 
#ip3_df_group_by = ip3_df.groupBy(col("VT_SER_NUM")).agg(
#    concat_ws("", collect_list(col("VT_TEXT"))).alias("ATTY_NM")
#)
ip3_df_select = ip3_df.select(col("VT_TEXT_TYPE"),
              col("VT_TEXT").alias("ATTY_NM"),
              col("VT_SER_NUM"),
              col("VT_ENT_NUM"),
              col("LAST_MODIFIED_DATE"),
              col("VT_RSN"))

# COMMAND ----------

# DBTITLE 1,Joining IP2 & IP3 dataframes
#ip2_df
#ip3_df_group_by
joined_df_ip2_ip3_left = \
(
    ip2_df
        .join(ip3_df_select,
             on = [(col("SER_NUM") == col("VT_SER_NUM"))],
             how = "left"
             )
        .select(col("SER_NUM"),
                col("COR_NM"),
                col("FIRM_NM"),
                col("ADD_LINE1"),
                col("ADD_LINE2"),
                col("CITY_NM"),
                col("ZIPCODE"),
                col("CTRY_CD"),
                col("CTRY_NM"),
                col("STATE_CD"),
                col("STATE_NM"),
                col("IP_ATT_DOCKET_REF"),
                col("ATTY_NM")
                )
)   


# COMMAND ----------

# ip2_ip3 step 1 - sorting by ascending
sorted_ip2_ip3_df = joined_df_ip2_ip3_left.sort("SER_NUM")

# COMMAND ----------

# ip2_ip3 step 2 - remove whitespaces from state_nm
ip2_ip3_step2 = sorted_ip2_ip3_df.withColumn("STATE_NM", regexp_replace(col("STATE_NM"),"(\s)",""))

# COMMAND ----------

#Joined_ip2_3 & state_info_df Joined on state_cd and selecting the columns 
joined_ip2_3_stinfo = \
(
    ip2_ip3_step2
        .join(state_info_df,
             on = [(col("STATE_CD") == col("STATE_CODE"))],
             how = "left"
             )
        .select(col("SER_NUM"),
                col("COR_NM"),
                col("FIRM_NM"),
                col("ADD_LINE1"),
                col("ADD_LINE2"),
                col("CITY_NM"),
                col("ZIPCODE"),
                col("CTRY_CD"),
                col("CTRY_NM"),
                col("STATE_CD"),
                col("STATE_NM"),
                col("STATE_NAME"),
                col("IP_ATT_DOCKET_REF"),
                col("ATTY_NM")
                )
)   


# COMMAND ----------

#Pull state name from joined_ip2_3_stinfo
joined_ip2_3_stinfo1 = joined_ip2_3_stinfo.select(col("SER_NUM"),
                col("COR_NM"),
                col("FIRM_NM"),
                col("ADD_LINE1"),
                col("ADD_LINE2"),
                col("CITY_NM"),
                col("ZIPCODE"),
                col("CTRY_CD"),
                col("CTRY_NM"),
                col("STATE_CD"),
                expr("case when STATE_NAME is NULL THEN STATE_NM else STATE_NAME   end as STATE_NM"),
                col("IP_ATT_DOCKET_REF"),
                col("ATTY_NM"))

# COMMAND ----------

#selecting all the columns from joined_ip2_3_stinfo and applying formula.
#discuss - CTRY_CD+ has been changed to CTRY_CD_NEW

ip2_3_stinfo_formula = joined_ip2_3_stinfo1.select(col("*"),
                                                  when(length(joined_ip2_3_stinfo.CTRY_CD) == 2,expr("concat(CTRY_CD,'X')"))
                                                  .when(length(joined_ip2_3_stinfo.CTRY_CD) == 3,joined_ip2_3_stinfo.CTRY_CD)
                                                  .otherwise(None).alias("CTRY_CD_NEW"))



# COMMAND ----------

# DBTITLE 1,Joining ip2_ip3 dataframe with Additional Input "Country_Info"
#ip2_3_stinfo_formula
#cntry_info_df
joined_df_ip2_3_cntry_left = \
(
    ip2_3_stinfo_formula
        .join(cntry_info_df,
             on = [(col("CTRY_CD_NEW") == col("STE_CTRY_CD"))],
             how = "left"
             )
        .select(col("SER_NUM"),
                col("COR_NM"),
                col("ZIPCODE"),
                col("CTRY_CD"),
                col("CTRY_NM"),
                col("STATE_CD"),
                col("STATE_NM"),
                col("FIRM_NM"),
                col("ADD_LINE1"),
                col("ADD_LINE2"),
                col("CITY_NM"),
                col("IP_ATT_DOCKET_REF"),
                col("ATTY_NM"),
                col("CTRY_CD_NEW"),
                col("STE_CTRY_CD"),
                col("CTRY_NAME_CAPS"),
                col("Country or Area Name"),
                col("ISO ALPHA-2 Code"),
                col("ISO ALPHA-3 CODE"),
                col("ISO NUMERIC CODE UN M49")
                )
)   


# COMMAND ----------

# DBTITLE 1,Join two Dataframes
#ip1_df
#joined_df_ip2_3_cntry_left

joined_df_ip2_3_ip1_left = \
(
    joined_df_ip2_3_cntry_left
        .join(ip1_df,
             on = [(col("SER_NUM") == col("VT_SER_NUM"))],
             how = "left"
             )
        .select(col("VT_SER_NUM"),
                col("DOMESTIC_REP"),
                col("SER_NUM"),
                col("COR_NM"),
                col("ZIPCODE"),
                col("CTRY_CD"),
                col("CTRY_NM"),
                col("STATE_CD"),
                col("STATE_NM"),
                col("FIRM_NM"),
                col("ADD_LINE1"),
                col("ADD_LINE2"),
                col("CITY_NM"),
                col("IP_ATT_DOCKET_REF"),
                col("ATTY_NM"),
                col("CTRY_CD_NEW"),
                col("STE_CTRY_CD"),
                col("CTRY_NAME_CAPS"),
                col("Country or Area Name"),
                col("ISO ALPHA-2 Code"),
                col("ISO ALPHA-3 CODE")
                )
)   


# COMMAND ----------

#step 2 -  selecting columns and renaming them
ip123_select_df = joined_df_ip2_3_ip1_left.select(
                col("SER_NUM"),
                col("COR_NM"),
                col("ZIPCODE"),
                col("CTRY_CD"),
                col("CTRY_NM"),
                col("STATE_CD"),
                col("STATE_NM"),
                col("FIRM_NM"),
                col("ADD_LINE1"),
                col("ADD_LINE2"),
                col("CITY_NM"),
                col("IP_ATT_DOCKET_REF"),
                col("ATTY_NM"),
                col("CTRY_NAME_CAPS"),
                col("Country or Area Name"),
                col("ISO ALPHA-2 Code"),
                col("ISO ALPHA-3 CODE"),
                col("DOMESTIC_REP")
)

# COMMAND ----------

# DBTITLE 1,INPUT4 dataframe code starts
# Input 4 step 1 --> Select
ip4_df = ip4_df.select(col("*"))

# COMMAND ----------

# DBTITLE 1,converting IP4 into two dataframe
# Input4 step 2 --> filters with VT_ENT_NUM = 1
# Input 4 from altreyx comming vt_ent_num value with 1,2,3 why we are hardcoding it to 1 only?
# ip4_left_df = ip4_df.where("VT_ENT_NUM == 1")
# # Input 4 step 2 --> filters with VT_ENT_NUM !=1 
# ip4_right_df = ip4_df.withColumnRenamed("VT_TEXT_TYPE","Right_VT_TEXT_TYPE")\
#     .withColumnRenamed("VT_TEXT","VT_TEXT_2")\
#         .withColumnRenamed("VT_SER_NUM","Right_VT_SER_NUM")\
#             .withColumnRenamed("VT_ENT_NUM","Right_VT_ENT_NUM")\
#             .where("VT_ENT_NUM != 1")


# COMMAND ----------

# DBTITLE 1,Join above Dataframes
#input4 step3

# joined_ip4_left_df = \
# (
#     ip4_left_df
#         .join(ip4_right_df,
#              on = [(col("VT_SER_NUM") == col("Right_VT_SER_NUM")),
#                    (col("VT_TEXT_TYPE") == col("Right_VT_TEXT_TYPE"))],
#              how = "left"
#              )
#         .select(col("VT_TEXT_TYPE"),
#                 col("VT_TEXT"),
#                 col("VT_SER_NUM"),
#                 col("VT_ENT_NUM"),
#                 col("Right_VT_TEXT_TYPE"),
#                 col("VT_TEXT_2"),
#                 col("Right_VT_SER_NUM"),
#                 col("Right_VT_ENT_NUM")
#                 )
# )   


# COMMAND ----------

#input4 step5 -->
#selecting all the columns from joined_ip4_left_df and applying formula.

# ip4_df_formula = joined_ip4_left_df.select(col("VT_TEXT_TYPE"),
#                 expr("case when VT_TEXT_2 is NULL THEN VT_TEXT else VT_TEXT + VT_TEXT_2   end as VT_TEXT"),
#                 col("VT_SER_NUM"),
#                 col("VT_ENT_NUM"),
#                 col("Right_VT_TEXT_TYPE"),
#                 col("VT_TEXT_2"),
#                 col("Right_VT_SER_NUM"),
#                 col("Right_VT_ENT_NUM"))
                

ip4_df_formula = ip4_df.select(col("VT_TEXT_TYPE"),
                col("VT_TEXT"),
                col("VT_SER_NUM"),
                col("VT_ENT_NUM")
                )
                


# COMMAND ----------

# DBTITLE 1,creating two dataframes
#Input4 Step6 -->
#Regular expression to add 2 new columns from VT_TEXT
# ip4_df_regex1 = ip4_df_formula.withColumn("AT_EMAIL_AUTH",regexp_extract("VT_TEXT",r'^([Y|N])',1))
# ip4_df_regex2 = ip4_df_regex1.withColumn("AT_EMAIL",regexp_extract("VT_TEXT",r'^([Y|N])(.*)',2))
### Above code has been commented & below new code has been added 

# #Regular expression to add 2 new columns from VT_TEXT
# regex_pattern= r"([Y|N])([a-z0-9!#$%&'*+/=?^_`{|}~-]+[@][a-z0-9!#$%&'*+/=?^_`{|}~-]+.[\w]+|[\w]+[@][\w]+[.][\w]+|[a-z0-9!#$%&'*+/=?^_`{|}~-]+[@][a-z0-9!#$%&'*+/=?^_`{|}~-]+[\.][a-z0-9!#$%&'*+/=?^_`{|}~-]+[.][\w]+|[a-z0-9!#$%&'*+/=?^_`{|}~-]+[\.][a-z0-9!#$%&'*+/=?^_`{|}~-]+[@][a-z0-9!#$%&'*+/=?^_`{|}~-]+.[\w]+|[a-z0-9!#$%&'*+/=?^_`{|}~-]+[\.][a-z0-9!#$%&'*+/=?^_`{|}~-]+[@][a-z0-9!#$%&'*+/=?^_`{|}~-]+[\.][a-z0-9!#$%&'*+/=?^_`{|}~-]+[.][\w]+)*"
#Regular expression to add 2 new columns from VT_TEXT with case insensitivity checkbox ticked
regex_pattern= r"(?i)([Y|N])([a-z0-9!#$%&'*+/=?^_`{|}~-]+[@][a-z0-9!#$%&'*+/=?^_`{|}~-]+.[\w]+|[\w]+[@][\w]+[.][\w]+|[a-z0-9!#$%&'*+/=?^_`{|}~-]+[@][a-z0-9!#$%&'*+/=?^_`{|}~-]+[\.][a-z0-9!#$%&'*+/=?^_`{|}~-]+[.][\w]+|[a-z0-9!#$%&'*+/=?^_`{|}~-]+[\.][a-z0-9!#$%&'*+/=?^_`{|}~-]+[@][a-z0-9!#$%&'*+/=?^_`{|}~-]+.[\w]+|[a-z0-9!#$%&'*+/=?^_`{|}~-]+[\.][a-z0-9!#$%&'*+/=?^_`{|}~-]+[@][a-z0-9!#$%&'*+/=?^_`{|}~-]+[\.][a-z0-9!#$%&'*+/=?^_`{|}~-]+[.][\w]+)*"

ip4_df_regex1 = ip4_df_formula.withColumn(
    "AT_EMAIL_AUTH", regexp_extract("VT_TEXT", regex_pattern, 1)
).withColumn("AT_EMAIL", regexp_extract("VT_TEXT", regex_pattern, 2))


# COMMAND ----------

# DBTITLE 1,Join two Dataframes
#ip123_select_df
#ip4_df_regex2

joined_ip1234_left_df = \
(
    ip123_select_df
        # .join(ip4_df_regex2, -- Code Commented by Naval Hatode 
        .join(ip4_df_regex1, # Code added by Naval Hatode
             on = [(col("SER_NUM") == col("VT_SER_NUM"))],
             how = "left"
             )
        .select(col("SER_NUM"),
                col("COR_NM"),
                col("ZIPCODE"),
                col("CTRY_CD"),
                col("CTRY_NM"),
                col("STATE_CD"),
                col("STATE_NM"),
                col("FIRM_NM"),
                col("ADD_LINE1"),
                col("ADD_LINE2"),
                col("CITY_NM"),
                col("IP_ATT_DOCKET_REF"),
                col("ATTY_NM"),
                col("CTRY_NAME_CAPS"),
                col("Country or Area Name"),
                col("ISO ALPHA-2 Code"),
                col("ISO ALPHA-3 CODE"),
                col("DOMESTIC_REP"),
                col("VT_TEXT_TYPE"),
                col("VT_TEXT"),
                col("VT_SER_NUM"),
                col("VT_ENT_NUM"),
                #col("Right_VT_TEXT_TYPE"),
                #col("VT_TEXT_2"),
                #col("Right_VT_SER_NUM"),
                #col("Right_VT_ENT_NUM"),
                col("AT_EMAIL_AUTH"),
                col("AT_EMAIL")
                )
)   


# COMMAND ----------

#Input1234 step2 selecting required fields
ip1234_select_df = joined_ip1234_left_df.select(col("SER_NUM"),
                col("COR_NM"),
                col("FIRM_NM"),
                col("ADD_LINE1"),
                col("ADD_LINE2"),
                col("CITY_NM"),
                col("ZIPCODE"),
                col("STATE_CD"),
                col("STATE_NM"),
                col("CTRY_CD"),
                col("CTRY_NM"),
                col("CTRY_NAME_CAPS"),
                col("Country or Area Name"),
                col("IP_ATT_DOCKET_REF"),
                col("ATTY_NM"),
                col("DOMESTIC_REP"),
                col("AT_EMAIL_AUTH"),
                col("AT_EMAIL"),
                col("ISO ALPHA-2 Code"),
                #col("Right_VT_TEXT_TYPE"),
                col("VT_TEXT"),
                #col("Right_VT_SER_NUM"),
                #col("Right_VT_ENT_NUM"),
                col("ISO ALPHA-3 CODE")
                )

# COMMAND ----------

# DBTITLE 1,INPUT 5 Code
#Input5 Step1 Selecting fields
ip5_df = ip5_df.select(col("VT_TEXT_TYPE"),
                       col("VT_TEXT").alias("CR_EMAIL"),
                       col("VT_SER_NUM"),
                       col("VT_ENT_NUM"))

# COMMAND ----------

em_win = Window().partitionBy("VT_SER_NUM").orderBy("VT_ENT_NUM")
rank_win = Window().partitionBy("VT_SER_NUM").orderBy(col("VT_ENT_NUM").desc())

# concatente emails in order
ip5_df_group_by = ip5_df.withColumn(
    "CR_EMAIL", concat_ws("", collect_list(col("CR_EMAIL")).over(em_win))
).withColumn(
    "rank", row_number().over(rank_win)
)

# COMMAND ----------

ip5_df_group_by = ip5_df_group_by.filter(col("rank") == 1).select(
    "VT_SER_NUM", "CR_EMAIL"
)

# COMMAND ----------

# DBTITLE 1,Additional code added for input 5 
regex_pattern = r"(?i)(.+?)(?:;|\n|$)"

ip5_df_group_by = ip5_df_group_by.withColumn(
    "CR_EMAIL_DROP", expr("regexp_extract_all(CR_EMAIL, '{0}', 1)".format(regex_pattern))
)

# COMMAND ----------

ip5_df_group_by = ip5_df_group_by.withColumn(
    "cr_email1", col("cr_email_drop")[0]
).withColumn(
    "cr_email2", col("cr_email_drop")[1]
).withColumn(
    "cr_email3", col("cr_email_drop")[2]
).withColumn(
    "cr_email4", col("cr_email_drop")[3]
)

# COMMAND ----------

# input5 step5  -> triming the leading and Trailing whitespaces from additional column
cleaned_ip5_df = ip5_df_group_by.withColumn(
    "CR_EMAIL", regexp_replace(col("CR_EMAIL"), '^\s+|\s+$', '')
).withColumn(
    "CR_EMAIL1", regexp_replace(col("CR_EMAIL1"), '^\s+|\s+$', '')
).withColumn(
    "CR_EMAIL2", regexp_replace(col("CR_EMAIL2"), '^\s+|\s+$', '')
).withColumn(
    "CR_EMAIL3", regexp_replace(col("CR_EMAIL3"), '^\s+|\s+$', '')
).withColumn(
    "CR_EMAIL4", regexp_replace(col("CR_EMAIL4"), '^\s+|\s+$', '')
)

# COMMAND ----------

#input5 step6 -- using formula to replace Yes & No with addtional column CR_Email_Auth

ip5_df_formulated = cleaned_ip5_df.withColumn("CR_Email_Auth",
                                              when(expr("substr(CR_EMAIL1,1,1)=='Y'"),"Yes")
                                              .when(expr("substr(CR_EMAIL1,1,1)=='N'"),"No")
                                              .otherwise('Error'))\
                                                  .withColumn("Email",expr("substr(CR_EMAIL1,2,999)"))



# COMMAND ----------

#input5 step7 -- selecting columns 
ip5_df_final= ip5_df_formulated.select(col("VT_SER_NUM"),
                                       col("Email").alias("CR_EMAIL1"),
                                       col("CR_EMAIL2"),
                                       col("CR_EMAIL3"),
                                       col("CR_EMAIL4"),
                                       col("CR_Email_Auth")
                                       ) 

# COMMAND ----------

# DBTITLE 1,Joining two Dataframes
#ip1234_select_df
#ip5_df_final

joined_all_ip_left_df = \
(
    ip1234_select_df
        .join(ip5_df_final,
             on = [(col("SER_NUM") == col("VT_SER_NUM"))],
             how = "left"
             )
        .select(col("SER_NUM"),
                col("COR_NM"),
                col("FIRM_NM"),
                col("ADD_LINE1"),
                col("ADD_LINE2"),
                col("CITY_NM"),
                col("ZIPCODE"),
                col("STATE_CD"),
                col("STATE_NM"),
                col("CTRY_CD"),
                col("CTRY_NM"),
                col("CTRY_NAME_CAPS"),
                col("Country or Area Name"),
                col("IP_ATT_DOCKET_REF"),
                col("ATTY_NM"),
                col("DOMESTIC_REP"),
                col("AT_EMAIL_AUTH"),
                col("AT_EMAIL"),
                col("ISO ALPHA-2 Code"),
                #col("Right_VT_TEXT_TYPE"),
                col("VT_TEXT"),
                #col("Right_VT_SER_NUM"),
                #col("Right_VT_ENT_NUM"),
                col("ISO ALPHA-3 CODE"),
                col("CR_EMAIL1"),
                col("CR_EMAIL2"),
                col("CR_EMAIL3"),
                col("CR_EMAIL4"),
                col("CR_Email_Auth")
                )
)   


# COMMAND ----------

# All inputs step1  -> triming the leading and Trailing whitespaces from 2 columns
cleaned_all_ip_df = joined_all_ip_left_df.withColumn("ATTY_NM",trim(joined_all_ip_left_df.ATTY_NM))\
                                .withColumn("DOMESTIC_REP",trim(joined_all_ip_left_df.DOMESTIC_REP))



# COMMAND ----------

# All inputs - step 2 - filter out all Isnull data from CAD_SER_NUM

all_ip_df_filter = cleaned_all_ip_df.filter(col("SER_NUM").isNotNull())

# COMMAND ----------

# DBTITLE 1,Final Step -> Summarize data
#All input - step 3 - Final summarize all fields
# final_df_grouping = (
#     all_ip_df_filter
#     .groupBy(
#                 col("SER_NUM"),
#                 col("COR_NM"),
#                 col("FIRM_NM"),
#                 col("ADD_LINE1"),
#                 col("ADD_LINE2"),
#                 col("CITY_NM"),
#                 col("ZIPCODE"),
#                 col("STATE_CD"),
#                 col("STATE_NM"),
#                 col("CTRY_CD"),
#                 col("CTRY_NM"),
#                 col("CTRY_NAME_CAPS"),
#                 col("Country or Area Name"),
#                 col("IP_ATT_DOCKET_REF"),
#                 col("ATTY_NM"),
#                 col("DOMESTIC_REP"),
#                 col("AT_EMAIL_AUTH"),
#                 col("AT_EMAIL"),
#                 col("ISO ALPHA-3 CODE"),
#                 col("CR_EMAIL1"),
#                 col("CR_EMAIL2"),
#                 col("CR_EMAIL3"),
#                 col("CR_EMAIL4"),
#                 col("CR_Email_Auth")
#     ).count().alias("cnt")
# )
## New Code added 
final_df_grouping = (
    all_ip_df_filter.select(    
                col("SER_NUM"),
                col("COR_NM"),
                col("FIRM_NM"),
                col("ADD_LINE1"),
                col("ADD_LINE2"),
                col("CITY_NM"),
                col("ZIPCODE"),
                col("STATE_CD"),
                col("STATE_NM"),
                col("CTRY_CD"),
                col("CTRY_NM"),
                col("CTRY_NAME_CAPS"),
                col("Country or Area Name"),
                col("ISO ALPHA-3 CODE"),
                col("IP_ATT_DOCKET_REF"),
                col("ATTY_NM"),
                col("DOMESTIC_REP"),
                col("AT_EMAIL_AUTH"),
                col("AT_EMAIL"),
                col("CR_EMAIL1"),
                col("CR_EMAIL2"),
                col("CR_EMAIL3"),
                col("CR_EMAIL4"),
                col("CR_Email_Auth")
    ).distinct()
)

# COMMAND ----------

#Final output dataframe - removin count column 
Final_df = final_df_grouping.select(col("SER_NUM"),
                col("COR_NM"),
                col("FIRM_NM"),
                col("ADD_LINE1"),
                col("ADD_LINE2"),
                col("CITY_NM"),
                col("ZIPCODE"),
                col("STATE_CD"),
                col("STATE_NM"),
                col("CTRY_CD"),
                col("CTRY_NM"),
                col("CTRY_NAME_CAPS"),
                col("Country or Area Name").alias("Country_or_Area_Name"),
                col("ISO ALPHA-3 CODE").alias("iso_alpha3_code"),
                col("IP_ATT_DOCKET_REF"),
                col("ATTY_NM"),
                col("DOMESTIC_REP"),
                col("AT_EMAIL_AUTH"),
                col("AT_EMAIL"),
                col("CR_EMAIL1"),
                col("CR_EMAIL2"),
                col("CR_EMAIL3"),
                col("CR_EMAIL4"),
                col("CR_Email_Auth")
                )\
                .withColumn("create_ts", current_timestamp())\
                .withColumn("create_user_id", lit("-1"))\
                .withColumn("update_ts", current_timestamp())\
                .withColumn("update_user_id", lit("-1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Writing dataframe into Silver layer

# COMMAND ----------

print(reporting_catalog)
print(schema_silver )
print(table_silver)

# COMMAND ----------

#Final_df.write.saveAsTable(
#    f"{reporting_catalog}.{schema_silver}.{table_silver}", mode="overwrite"
#)

Final_df.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.{schema_silver}.{table_silver}')
