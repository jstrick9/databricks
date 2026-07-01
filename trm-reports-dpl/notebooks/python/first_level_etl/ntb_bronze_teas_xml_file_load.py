# Databricks notebook source
# MAGIC %md
# MAGIC <pre>
# MAGIC ## Dashboard Requirements
# MAGIC As a Trademark Register Protection Team Member
# MAGIC I want to investigate metrics needed for a report build on goods/services field and classes deleted over time
# MAGIC So that I can see if there is increase post-implementation of the permanent audit program and deletion fee
# MAGIC
# MAGIC Notes:
# MAGIC •	What time periods (anything set or allow users to enter time periods)
# MAGIC •	What visualizations would be most helpful (
# MAGIC •	takes the table and makes it useful for the user
# MAGIC •	types of metrics - good/services deleted in certain classes
# MAGIC Acceptance Criteria:
# MAGIC Track use of deleted goods/services field and classes deleted in TEAS Section 8/71/8&15/71&15/8&9 form over time-- fee implemented in 2021
# MAGIC data from section 7: no fee form (they do deletions as part of this form too... implemented in Jan 2021)
# MAGIC there are dedicated fiels for deletion
# MAGIC Over time see comparison of count of deletions submitted as part of each form
# MAGIC want to see # of sect7 deletions going up... while other's go down as they have a fee associated
# MAGIC as long as they delete.. it helps to ensure the integrity.. section 7 is being more proactive
# MAGIC Notes from meeting 8/5
# MAGIC
# MAGIC data should go as far as 2017
# MAGIC Audit 2017: The launch date of the permanent post-registration audit program is November 1, 2017.
# MAGIC Deletion fee became effective in 2021...
# MAGIC prior to implementation of fee ruke and compare with period after
# MAGIC Pull in international class code
# MAGIC are these deleted prior to filling of section 71?
# MAGIC
# MAGIC ---section 7 submision date
# MAGIC section 7
# MAGIC
# MAGIC --keep number of classes column/number of classes paid
# MAGIC --count of goods and services deleted/per class??
# MAGIC --if entire class is deleted how do we count
# MAGIC --random seperator..
# MAGIC
# MAGIC 10 fields:
# MAGIC
# MAGIC caps;scarfs;belts
# MAGIC caps:1
# MAGIC
# MAGIC belts;scarfs:2
# MAGIC
# MAGIC
# MAGIC Show important dates like:
# MAGIC •	(1) the permanent audit program (November 1, 2017) and 
# MAGIC •	(2) the deletion fee (January 2, 2021)
# MAGIC
# MAGIC
# MAGIC 2. Total deletions for date range by sect
# MAGIC 3. Total amount paid for date range by sect
# MAGIC 4. 
# MAGIC </pre>

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("post_reg_file_type","")#S08N09, S08N15, S71N15, SECT08, SECT71, SECT07
#S08N09,S08N15

# COMMAND ----------

# DBTITLE 1,Define env variables
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
post_reg_file_type = dbutils.widgets.get("post_reg_file_type").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
#config_file = "/Workspace/Users/Pawanpreet.Sangari@USPTO.GOV/bdr-trm-reports-dpl_wfs/notebooks/config/dev/trmreports-conf.yaml"
print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Run common function and parameters ntbk
# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
print(f"{trgt_catalog=}")
spark.conf.set('conf.catalog', trgt_catalog)
spark.conf.set('conf.dbx_env', dbx_env)
spark.conf.set('conf.post_reg_file_type', post_reg_file_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ###below files are corrupted
# MAGIC <pre>
# MAGIC S08N09
# MAGIC sect08 2023 02
# MAGIC </pre>

# COMMAND ----------

from pyspark.sql.functions import input_file_name, col, explode_outer, expr, from_unixtime, to_date, to_timestamp, current_timestamp, lit, udf, trim, regexp_replace, concat_ws, when, length, explode
from pyspark.sql.types import StringType, IntegerType, TimestampType,DateType
import re
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

if post_reg_file_type == 'S08N09':
    bronze_tbl_name = 's08n09'
elif post_reg_file_type == 'S08N15':
    bronze_tbl_name = 's08n15'
elif post_reg_file_type == 'S71N15':
    bronze_tbl_name = 's71n15'
elif post_reg_file_type == 'SECT08':
    bronze_tbl_name = 's08'
elif post_reg_file_type == 'SECT71':
    bronze_tbl_name = 's71'

def extract_year_month(filename):
    pattern = rf"{post_reg_file_type}/(\d{{4}})/(\d{{2}})"
    match = re.search(pattern, filename)
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    return None

extract_year_month_udf = udf(extract_year_month, StringType())

from pyspark.sql.functions import col, current_timestamp, lit
current_year = datetime.datetime.now().year

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

import datetime
try:
    df_trgt_tbl = spark.sql(
        f"select max(year_month) as year_month from {trgt_catalog}.{trgt_catalog}.bronze.teas_{bronze_tbl_name}_xml_file_data"
    ).collect()[0][0]
    if df_trgt_tbl:
        df_trgt_tbl_year_month = df_trgt_tbl.split('-')
        start_year = int(df_trgt_tbl_year_month[0])
        start_month = int(df_trgt_tbl_year_month[1])
        current_year = datetime.datetime.now().year
        current_month = datetime.datetime.now().month

        # If max month is December, move to next year
        if start_month == 12:
            start_year += 1
            start_month = 1

        months = [
            f"{m:02d}" for m in range(
                start_month,
                13 if start_year < current_year else current_month + 1
            )
        ]
    else:
        months = [f"{m:02d}" for m in range(1, datetime.datetime.now().month + 1)]
        start_year = datetime.datetime.now().year
except Exception:
    months = [f"{m:02d}" for m in range(1, datetime.datetime.now().month + 1)]
    start_year = datetime.datetime.now().year

print(start_year, months)

# COMMAND ----------

#dbx_env = 'prod' # for unit testing only to read files from s3 prod

# COMMAND ----------



for year in range(start_year, current_year + 1):
    for month in months:
        if year == 2023 and int(month) <=5: 
            s3_path = f's3://{cdc_bucket}/eds/trademark/TEAS/datasync/{post_reg_file_type}/{year}/{month}/*/teas.xml'
        else:
            s3_path = f's3://{cdc_bucket}/eds/trademark/TEAS/datasync/{post_reg_file_type}/{year}/{month}/*/*/teas.xml'
        print(s3_path)
        try:
            df_teas = spark.read.format("xml") \
            .option("rootTag", "uspto-tm-document") \
            .option("rowTag", "uspto-tm-document") \
            .load(s3_path)
        except:
            print(f"No file found for {year} {month}")
            continue


        # Add filename column to identify the source
        df_teas_with_filenames = df_teas.withColumn("filename", input_file_name())

        df_teas_with_filenames = df_teas_with_filenames.withColumn("year-month", extract_year_month_udf(df_teas_with_filenames.filename))

        #union of teas and eteas files
        df_all_files = df_teas_with_filenames

        # Clean column names
        df_all_files_renamed = df_all_files.select(
            *[col(c).alias(c.replace("_", "").replace("-", "_").lower()) for c in df_all_files.columns]
        ).withColumn("create_ts", current_timestamp()).withColumn("create_user_id", lit("etl"))


        # Flatten the XML structure in df_all_BAS_files2 DataFrame
        flattened_df = df_all_files_renamed.select(
            col("description").alias("description"),
            col("document_type"),
            col("filing.filing-identifier"),
            col("filing.xml-create-date"),
            #col("filing.submit-date"),
            f.to_date(f.trim(f.regexp_replace(f.regexp_replace("filing.submit-date", " \\d{2}:\\d{2}:\\d{2} ET ", " "), "\\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\\b", "")),"MMM dd yyyy").alias("submit-date"),
            lit(None).alias("filing-date"),
            col("trademark_case_files.trademark-case-file.case-file-header.registration-number"),
            col("trademark_case_files.trademark-case-file.case-file-header.serial-number"),
            col("trademark_case_files.trademark-case-file.case-file-header.registration-date"),
            col("trademark_case_files.trademark-case-file.section-form._attorney-filing-indicator").alias("attorney_filing"),
            col("trademark_case_files.trademark-case-file.section-form.case-file-owners.case-file-owner.name").alias("case_file_owner_name"),
            col("trademark_case_files.trademark-case-file.section-form.case-file-owners.case-file-owner.citizenship-country-name").alias("case_file_owner_citizenship_country_name"),
            col("trademark_case_files.trademark-case-file.section-form.case-file-owners.case-file-owner.country-name").alias("case_file_owner_country_name"),
            col("trademark_case_files.trademark-case-file.correspondence-form.correspondences.correspondence.new-address"),
            col("trademark_case_files.trademark-case-file.section-form.goods-services.*"),
            col("trademark_case_files.fee-types.fee-type.*"),
            col("create_ts"),
            col("create_user_id"),
            lit("y").alias("pay-additional-fee"),
            col("year_month")
            ).drop("_action-code","_version")
        #flattened_df.display()

        flattened_df = flattened_df.withColumn("case_file_owner_name", expr("element_at(case_file_owner_name, -1)"))\
            .withColumn("case_file_owner_citizenship_country_name_last", expr("element_at(case_file_owner_citizenship_country_name, -1)"))\
            .withColumn(
            "case_file_owner_citizenship_country_name",
            when(
                col("case_file_owner_citizenship_country_name_last").isNull(),
                expr("element_at(case_file_owner_citizenship_country_name, 1)")
            ).otherwise(col("case_file_owner_citizenship_country_name_last"))
        )\
            .withColumn("case_file_owner_country_name_last", expr("element_at(case_file_owner_country_name, -1)"))\
            .withColumn(
            "case_file_owner_country_name",
            when(
                col("case_file_owner_country_name_last").isNull(),
                expr("element_at(case_file_owner_country_name, 1)")
            ).otherwise(col("case_file_owner_country_name_last"))
        ).drop("case_file_owner_citizenship_country_name_last", "case_file_owner_country_name_last")
            
        # Flatten the signature structure in the flattened_df_expanded_flat DataFrame
        try:
            flattened_df_explode = flattened_df\
            .withColumn("goods_service_flat", explode_outer("goods-service")
                ).select(
                "*",
                "goods_service_flat.*"
                ).drop("goods_service_flat","goods-service","specimen","specimen-website-info","_action-code","_version")
        except:
             flattened_df_explode = flattened_df.select(
                "*"
                ).drop("specimen","specimen-website-info","_action-code","_version")



        try:
            flattened_df_explode_attorney = flattened_df_explode \
                .withColumn("new_address_flat", explode_outer("new-address")) \
                .select(
                "*",
                f.trim(col("new_address_flat.attorney-docket-number")).cast("string").alias("attorney_docket_number"),
                f.when(f.length(col("new_address_flatt.attorney-docket-number")) == 0, lit("None")).otherwise(f.col("new_address_flat.attorney-credential.bar-membership-number")).alias("attorney_credential_bar_membership_number")\
                ).drop("new_address_flat","new-address")
        except:
            flattened_df_explode_attorney = flattened_df_explode \
                .withColumn("new_address_flat", explode_outer("new-address")) \
                .select(
                "*",
                f.trim(col("new_address_flat.attorney-docket-number")).cast("string").alias("attorney_docket_number"),
                f.when(f.length(col("new_address_flat.attorney-docket-number")) == 0, lit("None")).alias("attorney_credential_bar_membership_number")\
                ).drop("new_address_flat","new-address")

        flattened_df_explode_filter = flattened_df_explode_attorney.filter("`keep-description-text-flag` != 'YES'")


        flattened_df_explode_filter_renamed = flattened_df_explode_filter.select(
            *[col(c).alias(c.replace("-", "_").lower()) for c in flattened_df_explode_filter.columns])
        

        # Casting data types in dataframe
        flattened_df_explode_filter_renamed = flattened_df_explode_filter_renamed.select(
        col("description").cast(StringType()).alias("description"),
        col("document_type").cast(StringType()).alias("document_type"),
        col("filing_identifier").cast(StringType()).alias("filing_identifier"),
        #col("xml_create_date").alias("xml_create_date_o"),
        to_timestamp(col("xml_create_date"),'yyyyMMdd HH:mm:ss').alias("xml_create_date"),
        col("submit_date").alias("submit_date"),
        to_date(col("filing_date"),'yyyyMMdd').alias("filing_date"),
        #to_date(col("submit_date"), "EEE MMM dd HH:mm:ss z yyyy").alias("submit_date"),  # Adjusted pattern
        col("registration_number").cast(IntegerType()).alias("registration_number"),
        col("serial_number").cast(IntegerType()).alias("serial_number"),
        #col("registration_date").alias("registration_date_o"),
        to_date((col("registration_date")),'yyyyMMdd').alias("registration_date"),
        col("pay_additional_fee").cast(StringType()).alias("pay_additional_fee"),
        col("attorney_filing").cast(StringType()).alias("attorney_filing"),
        col("case_file_owner_name").cast(StringType()).alias("case_file_owner_name"),
        col("case_file_owner_citizenship_country_name").cast(StringType()).alias("case_file_owner_citizenship_country_name"),
        col("case_file_owner_country_name").cast(StringType()).alias("case_file_owner_country_name"),
        col("attorney_docket_number").cast(StringType()).alias("attorney_docket_number"),
        col("attorney_credential_bar_membership_number").cast(StringType()).alias("attorney_credential_bar_membership_number"),
        col("fee_code").cast(StringType()).alias("fee_code"),
        col("grace_period").cast(IntegerType()).alias("grace_period"),
        col("number_of_classes").cast(IntegerType()).alias("number_of_classes"),
        col("number_of_classes_paid").cast(IntegerType()).alias("number_of_classes_paid"),
        col("subtotal_amount").cast(IntegerType()).alias("subtotal_amount"),
        col("class_code").cast(StringType()).alias("class_code"),
        col("deleted_description_text").cast(StringType()).alias("deleted_description_text"),
        col("description_text").cast(StringType()).alias("description_text"),
        col("final_description_text").cast(StringType()).alias("final_description_text"),
        col("keep_description_text_flag").cast(StringType()).alias("keep_description_text_flag"),
        col("create_ts").cast(StringType()).alias("create_ts"),
        col("create_user_id").cast(StringType()).alias("create_user_id"),
        col("year_month").cast(StringType()).alias("year_month"))


        flattened_df_explode_filter_renamed.write.format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"year_month = '{year}-{month}'") \
            .save(f"s3://{cdc_bucket}/delta_tables/{trgt_catalog}/bronze/teas_{bronze_tbl_name}_xml_file_data")



        count = flattened_df_explode_filter_renamed.count()
        print(f"Month: {year}-{month}, Count: {count}")


# COMMAND ----------

# MAGIC %sql
# MAGIC --select year_month, count(*) from trm_reporting_dev.bronze.teas_s71_xml_file_data group by year_month

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit(f"Completed loading teas_sect_xml_file_data Table ")
