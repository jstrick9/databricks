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
# MAGIC Track use of deleted goods/services field and classes deleted in TEAS Section 7/8/71/8&15/71&15/8&9 form over time
# MAGIC Show important dates like:
# MAGIC •	(1) the permanent audit program (November 1, 2017) and 
# MAGIC •	(2) the deletion fee (January 2, 2021)
# MAGIC </pre>

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

# DBTITLE 1,Define env parameter
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Run common functions and param ntbk
# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
#src_catalog = common_configs['schema']['tmngpdb_src_catalog']
print(f"{trgt_catalog=}")
spark.conf.set('conf.catalog', trgt_catalog)
#spark.conf.set('conf.src_catalog', src_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

#trgt_catalog= 'trm_reporting_dev'

# COMMAND ----------

# DBTITLE 1,Read Bronze table into Dataframe
df_s08n15 = spark.sql(f"""select * from {trgt_catalog}.bronze.teas_s08n15_xml_file_data""")

df_s07 = spark.sql(f"""select * from {trgt_catalog}.bronze.teas_s07_xml_file_data""")

df_s08n09 = spark.sql(f"""select * from {trgt_catalog}.bronze.teas_s08n09_xml_file_data""")

df_s71n15 = spark.sql(f"""select * from {trgt_catalog}.bronze.teas_s71n15_xml_file_data""")

df_s08 = spark.sql(f"""select * from {trgt_catalog}.bronze.teas_s08_xml_file_data""")

df_s71 = spark.sql(f"""select * from {trgt_catalog}.bronze.teas_s71_xml_file_data""")


df_all_teas = df_s08n15.unionByName(df_s07, allowMissingColumns=True).unionByName(df_s08n09, allowMissingColumns=True).unionByName(df_s71n15, allowMissingColumns=True).unionByName(df_s08, allowMissingColumns=True).unionByName(df_s71, allowMissingColumns=True)
#df_all_teas.display()

# COMMAND ----------

# DBTITLE 1,Clean column names
from pyspark.sql.functions import col

# Creating alias for dataframe column names
df_all_teas_col = df_all_teas.withColumn("update_ts", current_timestamp()).withColumn("update_user_id", lit("etl"))

#display(flattened_df_explode_filter_renamed)

# COMMAND ----------

#spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

from pyspark.sql.functions import col,from_unixtime,to_date,to_timestamp,expr
from pyspark.sql.types import StringType, IntegerType, TimestampType,DateType


# Casting data types in dataframe
df_all_teas_col_cast = df_all_teas_col.select(
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
    col("update_ts").cast(StringType()).alias("update_ts"),
    col("update_user_id").cast(StringType()).alias("update_user_id"),
    col("year_month").cast(StringType()).alias("year_month")
)

#display(df_all_teas_col_cast)

# COMMAND ----------


df_all_teas_col_cast.write.format("delta") \
            .mode("overwrite") \
            .save(f"s3://bdr-databricks-app-{dbx_env}/eds/delta_tables/{trgt_catalog}/gold/teas_goods_services_deleted")

# COMMAND ----------

dbutils.notebook.exit(f"Completed loading teas_goods_services_deleted Table ")

# COMMAND ----------

# MAGIC %md
# MAGIC select count(gsd.* )
# MAGIC from trm_reporting_dev.gold.teas_goods_services_deleted gsd
# MAGIC group by gsd.document_type
# MAGIC left outer join (select distinct ser_num, filing_basis_grp from trm_reporting_dev.gold.filings_dashboard) fda
# MAGIC on gsd.serial_number = fda.ser_num
# MAGIC --and gsd.class_code = fda.class
# MAGIC
