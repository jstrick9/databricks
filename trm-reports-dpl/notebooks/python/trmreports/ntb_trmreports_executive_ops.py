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
trm_tmbuscalendar_catalog = common_configs["schema"]["trm_tmbuscalendar_catalog"]
spark.conf.set('conf.dbx_env', dbx_env)

print(trgt_catalog, src_catalog, trm_tmbuscalendar_catalog)

# COMMAND ----------

# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trmreports_executive_ops'

control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,starttime)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lets Calculate Projections if its the  First Day of the Fiscal year

# COMMAND ----------

# Refresh projections if today is the first day of the new fiscal year (October 1)
if curntdt.month == 10 and curntdt.day == 1:
    # Load previous projections
    df_old_projections = spark.sql(f"SELECT * FROM {trgt_catalog}.gold.executive_ops_percent_projections")
    
    # Retrieve actual BDs, FAs, and Disposals for last fiscal year
    last_year = spark.sql(f"""
        SELECT DISTINCT
            range_nm AS range,
            fk_start_calendar_dt,
            fk_end_calendar_dt,
            ROUND((SUM(`Net First Actions Init Exam`) OVER (PARTITION BY range_nm) / SUM(`Net First Actions Init Exam`) OVER (PARTITION BY 1)) * 100, 2) AS PERCENT_FA,
            ROUND((SUM(`Total_Balance_Disposals`) OVER (PARTITION BY range_nm) / SUM(`Total_Balance_Disposals`) OVER (PARTITION BY 1)) * 100, 2) AS PERCENT_BD,
            ROUND((SUM(Disposals) OVER (PARTITION BY range_nm) / SUM(Disposals) OVER (PARTITION BY 1)) * 100, 2) AS PERCENT_Disposals
        FROM (
            SELECT
                fy,
                max_fy,
                range_nm,
                fk_start_calendar_dt,
                fk_end_calendar_dt,
                (APP_PUB_CT_PP + ABAN_CT_PP + TOT_FA_INIT_PP_CL) AS `Total_Balance_Disposals`,
                TOT_FA_INIT_PP_CL AS `Net First Actions Init Exam`,
                ((APP_PUB_CT_PP + ABAN_CT_PP + TOT_FA_INIT_PP_CL) - TOT_FA_INIT_PP_CL) AS Disposals
            FROM (
                SELECT *,
                    FY_PROCESS_SW,
                    CASE
                        WHEN FY_PROCESS_SW = 1 AND APP_PUB_CT_FY != 0 THEN WK_ACTV_CLS
                        ELSE 0
                    END AS APP_PUB_CT_PP,
                    CASE
                        WHEN FY_PROCESS_SW = 1 AND ABAN_CT_FY != 0 THEN WK_ACTV_CLS
                        ELSE 0
                    END AS ABAN_CT_PP,
                    CASE
                        WHEN FY_PROCESS_SW = 1 AND TOT_FA_INIT_FY_CL != 0 THEN WK_ACTV_CLS
                        ELSE 0
                    END AS TOT_FA_INIT_PP_CL
                FROM (
                    SELECT
                        range_nm,
                        fk_start_calendar_dt,
                        fk_end_calendar_dt,
                        APP_PUB_CT_FY,
                        ABAN_CT_FY,
                        TOT_FA_INIT_FY_CL,
                        WK_ACTV_CLS,
                        TRANSACTION_EFFECTIVE_DT,
                        CASE
                            WHEN month(TRANSACTION_EFFECTIVE_DT) >= 10 THEN year(TRANSACTION_EFFECTIVE_DT) + 1
                            ELSE year(TRANSACTION_EFFECTIVE_DT)
                        END AS fy,
                        max(
                            CASE
                                WHEN month(TRANSACTION_EFFECTIVE_DT) >= 10 THEN year(TRANSACTION_EFFECTIVE_DT) + 1
                                ELSE year(TRANSACTION_EFFECTIVE_DT)
                            END
                        ) OVER (PARTITION BY 1) AS max_fy,
                        CASE
                            WHEN (EP_PP_PERIOD = range_nm) THEN 1
                            ELSE 0
                        END AS FY_PROCESS_SW
                    FROM {trgt_catalog}.silver.epquery_stg3
                    INNER JOIN {trm_tmbuscalendar_catalog}.bronze.business_calendar_range
                        ON date(TRANSACTION_EFFECTIVE_DT) BETWEEN fk_start_calendar_dt AND fk_end_calendar_dt
                )
            ) sub_fy_sw
        )
        WHERE fy = max_fy - 1
    """)

    # Join on range_nm and fiscal_year_pay_period
    joined = df_old_projections.join(
        last_year,
        last_year.range == df_old_projections.fiscal_year_pay_period,
        how="inner"
    )

    # Average corresponding columns row by row
    avg_df = joined.select(
        col("fiscal_year_pay_period"),
        ((col("PERCENT_FA") + col("projected_fa_percent")) / 2).alias("avg_fa"),
        ((col("PERCENT_Disposals") + col("projected_disposal_percent")) / 2).alias("avg_disposal"),
        ((col("PERCENT_BD") + col("projected_bd_percent")) / 2).alias("avg_bd")
    )

    from pyspark.sql.functions import sum as spark_sum, round as spark_round,lit, current_timestamp

    # Calculate totals for each column
    totals = avg_df.agg(
        spark_sum("avg_fa").alias("total_fa"),
        spark_sum("avg_disposal").alias("total_disposal"),
        spark_sum("avg_bd").alias("total_bd")
    ).collect()[0]

    total_fa = totals["total_fa"]
    total_disposal = totals["total_disposal"]
    total_bd = totals["total_bd"]

    # Normalize each column by its total and multiply by 100, rounded
    normalized_df = avg_df.select(
        "fiscal_year_pay_period",
        spark_round((col("avg_fa") / total_fa) * 100, 2).alias("norm_fa"),
        spark_round((col("avg_disposal") / total_disposal) * 100, 2).alias("norm_disposal"),
        spark_round((col("avg_bd") / total_bd) * 100, 2).alias("norm_bd")
    )

    normalized_df = normalized_df.select(
        col("fiscal_year_pay_period").alias("fiscal_year_pay_period"),
        col("norm_fa").alias("projected_fa_percent"),
        col("norm_disposal").alias("projected_disposal_percent"),
        col("norm_bd").alias("projected_bd_percent")
    ) .withColumn("created_at", current_timestamp()) \
    .withColumn("updated_at", current_timestamp())
    display(normalized_df)

    normalized_df.write.mode("overwrite").saveAsTable(f"{trgt_catalog}.gold.executive_ops_percent_projections")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Lets now calculate Actuals

# COMMAND ----------

Actuals = spark.sql(f"""
SELECT
  range_nm,
  fk_start_calendar_dt,
  fk_end_calendar_dt,
  FA FA_Actual,
  SUM(FA) OVER (
      PARTITION BY substr(range_nm, 1, 4) ORDER BY range_nm ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS FA_Cumulative,
  BD BD_Actual,
  SUM(BD) OVER (
      PARTITION BY substr(range_nm, 1, 4) ORDER BY range_nm ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS BD_Cumulative,
  Disposals Disposals_Actual,
  SUM(Disposals) OVER (
      PARTITION BY substr(range_nm, 1, 4) ORDER BY range_nm ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Disposals_Cumulative
FROM
  (
    SELECT DISTINCT
      range_nm,
      fk_start_calendar_dt,
      fk_end_calendar_dt,
      SUM(`Net First Actions Init Exam`) OVER (PARTITION BY range_nm) AS FA,
      SUM(`Total_Balance_Disposals`) OVER (PARTITION BY range_nm) AS BD,
      SUM(Disposals) OVER (PARTITION BY range_nm) AS Disposals
    FROM
      (
        SELECT
          fy,
          range_nm,
          fk_start_calendar_dt,
          fk_end_calendar_dt,
          (APP_PUB_CT_PP + ABAN_CT_PP + TOT_FA_INIT_PP_CL) AS `Total_Balance_Disposals`,
          TOT_FA_INIT_PP_CL AS `Net First Actions Init Exam`,
          ((APP_PUB_CT_PP + ABAN_CT_PP + TOT_FA_INIT_PP_CL) - TOT_FA_INIT_PP_CL) AS Disposals
        FROM
          (
            SELECT
              *,
              FY_PROCESS_SW,
              CASE
                WHEN
                  FY_PROCESS_SW = 1
                  AND APP_PUB_CT_FY != 0
                THEN
                  WK_ACTV_CLS
                ELSE 0
              END AS APP_PUB_CT_PP,
              CASE
                WHEN
                  FY_PROCESS_SW = 1
                  AND ABAN_CT_FY != 0
                THEN
                  WK_ACTV_CLS
                ELSE 0
              END AS ABAN_CT_PP,
              CASE
                WHEN
                  FY_PROCESS_SW = 1
                  AND TOT_FA_INIT_FY_CL != 0
                THEN
                  WK_ACTV_CLS
                ELSE 0
              END AS TOT_FA_INIT_PP_CL
            FROM
              (
                SELECT
                  range_nm,
                  fk_start_calendar_dt,
                  fk_end_calendar_dt,
                  APP_PUB_CT_FY,
                  ABAN_CT_FY,
                  TOT_FA_INIT_FY_CL,
                  WK_ACTV_CLS,
                  TRANSACTION_EFFECTIVE_DT,
                  CASE
                    WHEN
                      month(TRANSACTION_EFFECTIVE_DT) >= 10
                    THEN
                      year(TRANSACTION_EFFECTIVE_DT) + 1
                    ELSE year(TRANSACTION_EFFECTIVE_DT)
                  END AS fy,
                  CASE
                    WHEN (EP_PP_PERIOD = range_nm) THEN 1
                    ELSE 0
                  END AS FY_PROCESS_SW
                FROM
                  {trgt_catalog}.silver.epquery_stg3
                    INNER JOIN {trm_tmbuscalendar_catalog}.bronze.business_calendar_range
                      ON date(TRANSACTION_EFFECTIVE_DT) BETWEEN
                        fk_start_calendar_dt
                      AND
                        fk_end_calendar_dt
              )
          ) sub_fy_sw
      )
  )
""")

# COMMAND ----------

target_table_name = f"{trgt_catalog}.gold.executive_ops_actuals"
Actuals.write.mode("overwrite").format("delta").insertInto(target_table_name)

# COMMAND ----------

recs_count = Actuals.count()
end_job_cntl(f"{trgt_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")
