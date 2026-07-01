# Databricks notebook source
# DBTITLE 1,Load Libraries
# Load Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType, DateType, LongType

# COMMAND ----------

# DBTITLE 1,Set config file
# Load widgets. What are these for?

dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=},{dbx_env=}")

# COMMAND ----------

# DBTITLE 1,Execute common function ntbk
# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,Set parameter values
# Enables usage of tmngpdb and reporting without hard coding them.

common_configs = read_yaml(config_file)
tmngpdb_src_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
reporting_catalog = common_configs["schema"]["reporting_catalog"]

# COMMAND ----------

# DBTITLE 1,Start Job Control
# Does this cell need to be updated?
# start job control
job_name = "third_level_hold_monitoring_detail_h"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Process Records

# COMMAND ----------

# DBTITLE 1,Create Detail Dataframe
df_detail = spark.sql(f"""
  SELECT ah.hold_docket_no AS hold_docket,
  ah.cfk_hold_category_cd AS category_cd,
  ah.cfk_hold_status_cd AS hold_status_cd,
  ah.DN_SERIAL_NUM_TX AS serial_number,
  m.am_cls_ct_actv AS classes,
  CASE 
    WHEN 
      MONTH(CAST(ah.PLACED_ON_HOLD_DT AS DATE)) = MONTH(current_date)
      AND YEAR(CAST(ah.PLACED_ON_HOLD_DT AS DATE)) = YEAR(current_date)
      THEN 1
      ELSE 0
      END AS new_case,
  b.mark_nm AS mark_name,
  date(ah.PLACED_ON_HOLD_DT) AS placed_on_hold_date,
  date_diff(current_date,ah.PLACED_ON_HOLD_DT) As days_on_hold,
  b.am_stat AS status_cd,
  m.abandonment_dt,
  current_date as run_date,
  month(current_date) as run_month,
  date_format(current_date, 'MMM') AS run_month_abbr,
  CASE
    WHEN month(current_date) >= 10 THEN year(current_date) + 1
    ELSE year(current_date)
  END AS run_fy
  FROM {tmngpdb_src_catalog}.bronze.attorney_hold ah
  JOIN {reporting_catalog}.silver.bibliography b  ON b.ser_num = ah.dn_serial_num_tx
  JOIN {reporting_catalog}.silver.milestone m  ON m.ser_num = ah.dn_serial_num_tx
  WHERE CFK_HOLD_STATUS_CD = 'ON_HOLD'
  AND ah.hold_docket_no < 16
  ORDER BY days_on_hold
""")
df_detail.printSchema()

# COMMAND ----------

# DBTITLE 1,Data Types
# convert data types
df_detail_typed = df_detail.select(
    col("hold_docket").cast(IntegerType()).alias("hold_docket"),
    col("category_cd").cast(StringType()).alias("category_cd"),
    col("hold_status_cd").cast(StringType()).alias("hold_status_cd"),
    col("serial_number").cast(StringType()).alias("serial_number"),
    col("classes").cast(IntegerType()).alias("classes"),
    col("new_case").cast(IntegerType()).alias("new_case"),
    col("mark_name").cast(StringType()).alias("mark_name"),
    col("placed_on_hold_date").cast(DateType()).alias("placed_on_hold_date"),
    col("days_on_hold").cast(IntegerType()).alias("days_on_hold"),
    col("status_cd").cast(StringType()).alias("status_cd"),
    col("abandonment_dt").cast(DateType()).alias("abandonment_dt"),
    col("run_date").cast(DateType()).alias("run_date"),
    col("run_month").cast(IntegerType()).alias("run_month"),
    col("run_month_abbr").cast(StringType()).alias("run_month_abbr"),
    col("run_fy").cast(IntegerType()).alias("run_fy"),
)
df_detail_typed.printSchema()

# COMMAND ----------

# DBTITLE 1,Add Timestamps
df_detail_typed = (
    df_detail_typed.withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit("ETL"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit("ETL"))
)
df_detail_typed.printSchema()
df_detail_typed.createOrReplaceTempView("staged")
display(spark.sql("select * from staged").limit(10))

# COMMAND ----------

display(
    spark.sql(f"""
        insert into {reporting_catalog}.gold.hold_monitoring_detail_h
        select 
            * 
        from 
            staged
    """)
)

# COMMAND ----------

# DBTITLE 1,Write To Table
recs_count: int = spark.sql("select * from staged").count()

end_job_cntl(
    f"{reporting_catalog}.silver",
    job_name,
    job_start_ts,
    "completed",
    recs_count,
    "job completed successfully",
)
dbutils.notebook.exit(f"Completed Loading on_hold Table")
