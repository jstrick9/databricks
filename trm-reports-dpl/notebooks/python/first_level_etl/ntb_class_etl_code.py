# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook produces the silver-layer `class` table by transforming two input
# MAGIC dataframes (ip1 – class data, ip2 – goods & services text), joining them, and
# MAGIC writing the result as a Delta table.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import (
    col,
    collect_list,
    concat_ws,
    current_timestamp,
    expr,
    lit,
    regexp_extract,
    regexp_replace,
    sort_array,
    struct,
    transform,
)
from pyspark.sql.types import DateType, IntegerType

# COMMAND ----------

# DBTITLE 1,Environment & Config
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file = f"../../config/{dbx_env}/trmreports-conf.yaml"
print(f"{config_file=}, {dbx_env=}")

# COMMAND ----------

# DBTITLE 1,Class Input Dataframes
# MAGIC %run ./ntb_class_etl_input $config_file=config_file

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformation

# COMMAND ----------

# DBTITLE 1,ip1 — Select, derive class_status, clean cl_cls_us, sort
ip1_df = (
    ip1_df.select(
        col("cl_ser_num").alias("ser_num"),
        col("cl_prime_cls"),
        col("cl_cls_us_ct"),
        col("cl_cls_stat"),
        col("cl_cls_us"),
        col("cl_dt_stat"),
        col("cl_flg_anoth_form"),
    )
    .select(
        col("*"),
        expr("""
            case
                when cl_cls_stat = '1' then 'Sec.7(d)-Cancelled'
                when cl_cls_stat = '2' then 'Sec.8-Cancelled'
                when cl_cls_stat = '3' then 'Sec.18-Cancelled'
                when cl_cls_stat = '4' then 'Sec.24-Cancelled'
                when cl_cls_stat = '5' then 'Sec.37-Cancelled'
                when cl_cls_stat = '6' then 'ACTIVE'
                when cl_cls_stat = '7' then 'INACTIVE-Insufficient Fee Received'
                when cl_cls_stat = '8' then 'Abandoned'
                when cl_cls_stat = '9' then 'Expired'
                when cl_cls_stat = 'A' then 'Sec.7(d)-Cancelled'
                when cl_cls_stat = 'B' then 'Sec.8-Cancelled'
                when cl_cls_stat = 'C' then 'Sec.1-Cancelled'
                when cl_cls_stat = 'D' then 'Sec.24-Cancelled'
                when cl_cls_stat = 'E' then 'Sec.37-Cancelled'
                when cl_cls_stat = 'F' then 'Sec.70-Canceled'
                when cl_cls_stat = 'G' then 'Sec.71-Cancelled'
                when cl_cls_stat = 'H' then 'Sec.70-Cancelled'
                when cl_cls_stat = 'P' then 'Partially Paid'
                when cl_cls_stat = 'W' then 'FEE WAIVED'
            end
        """).alias("class_status"),
    )
    .drop("cl_cls_stat")
    .select(
        col("class_status"),
        col("cl_prime_cls").alias("class"),
        col("ser_num"),
        col("cl_cls_us_ct"),
        col("cl_cls_us"),
        col("cl_dt_stat"),
        col("cl_flg_anoth_form"),
    )
    .withColumn("cl_cls_us", regexp_replace(col("cl_cls_us"), "\¿", ""))
    .withColumn("cl_cls_us", regexp_replace(col("cl_cls_us"), "\ÿ", ""))
    .withColumn("cl_cls_us", regexp_replace(col("cl_cls_us"), r"(\d{3})", "$1,"))
)

# COMMAND ----------

# DBTITLE 1,ip2 — Parse text type, aggregate goods & services text, sort
ip2_df = (
    ip2_df.withColumn("vt_class", regexp_extract("vt_text_type", r"\w{2}(\d{3})\d", 1))
    .groupBy(
        col("vt_ser_num"),
        col("vt_class"),
    )
    .agg(
        concat_ws(
            "",
            transform(
                sort_array(collect_list(struct("vt_text_type", "vt_ent_num", "vt_text"))),
                lambda x: x["vt_text"]
            )
        ).alias("goods_and_services_desc")
    )
)

# COMMAND ----------

# DBTITLE 1,Join ip1 and ip2, add audit columns
final_class_df = (
    ip1_df.join(
        ip2_df,
        on=(col("class") == col("vt_class")) & (col("ser_num") == col("vt_ser_num")),
        how="left",
    )
    .select(
        col("class_status"),
        col("class"),
        col("ser_num").cast(IntegerType()),
        col("cl_cls_us_ct"),
        col("cl_cls_us"),
        col("cl_dt_stat").cast(DateType()),
        col("cl_flg_anoth_form"),
        col("vt_ser_num"),
        col("vt_class"),
        expr("nullif(goods_and_services_desc, '') as goods_and_services_desc"),
    )
    .withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit("-1"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit("-1"))
)

# COMMAND ----------

# DBTITLE 1,Handle Alteryx "1400-01-01" date validation and fill null class_status
final_class_df_updated = replace_null_with_condition(
    final_class_df, "cl_dt_stat"
).fillna("", subset=["class_status"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Layer

# COMMAND ----------

# DBTITLE 1,Write to Silver
(
    final_class_df_updated.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "false")
    .saveAsTable(f"{reporting_catalog}.{schema_silver}.{table_silver}")
)
