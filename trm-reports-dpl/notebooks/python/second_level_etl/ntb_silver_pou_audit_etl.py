# Databricks notebook source
# MAGIC %md
# MAGIC #### Configs, Imports & Job Control

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

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

# COMMAND ----------

# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_silver_pou_audit_etl'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inputs

# COMMAND ----------

cancellation_codes = ['C71P', 'C71T', 'C8..', 'C8.T', 'C7..']
acceptance_codes = ['NA75', 'NAS8', 'NA89', 'NA71', 'NA85', 'NP89']
audit_codes = ['PUM1', 'PUM2', 'PUM3', 'PUMI', 'PUNQ']

# Unified query for business events
df_ip_cmn_bus_event = spark.sql(f"""
    SELECT
        sber.legacy_cm_ent_cd AS CM_ENT_CD,
        split(be.cfk_object_gid, ':')[2] AS CM_SER_NUM,
        be.effective_ts AS CM_ENT_DT,
        be.last_mod_user_id AS CM_PRCD_NUM,
        sber.title_tx
    FROM {tmngpdb_catalog}.bronze.business_event be
    JOIN {tmngpdb_catalog}.bronze.stnd_business_event_reason sber
        ON be.fk_business_event_reason_id = sber.business_event_reason_id
""")

# Filtered DataFrames
df_ip141 = df_ip_cmn_bus_event.filter(col("CM_ENT_CD").isin(audit_codes))
df_cancellations = df_ip_cmn_bus_event.filter(col("CM_ENT_CD").isin(cancellation_codes))
df_acceptance = df_ip_cmn_bus_event.filter(col("CM_ENT_CD").isin(acceptance_codes))
df_ip150 = df_ip_cmn_bus_event.filter(col("CM_ENT_CD") == "REIN")
df_ip151 = df_ip_cmn_bus_event.filter(col("CM_ENT_CD") == "EROP")

# Worker, post reg, pou, correspondence, bibliography, and sales
df_ip139 = spark.sql(f"""
    SELECT worker_no AS EM_EMPE_NUM, worker_nm AS EM_EMPE_NAME
    FROM {tmworker_catalog}.bronze.worker
""")
df_post_reg = spark.sql(f"SELECT * FROM {reporting_catalog}.gold.post_reg_dashboard")  
df_pou = spark.sql(f"""SELECT *,all_deletion_dates AS deletion_dt FROM {reporting_catalog}.silver.proof_of_use_audit""")
df_corr = spark.sql(f"SELECT * FROM {reporting_catalog}.silver.correspondence")
df_biblo = spark.sql(f"SELECT * FROM {reporting_catalog}.silver.bibliography")
df_ip55 = read_data_from_oracle_conn_dsu_cmn(f"""
    SELECT * FROM DW.VW_FPNG_SALE
    WHERE FEE_CD IN ('7012', '7013')
""", edw_scope).withColumn("rev_src_cd1", col("FEE_CD"))

# Acceptance before PUM1 (no PUM1)
df_events_accept = df_ip_cmn_bus_event.filter(
    col("CM_ENT_CD").isin(acceptance_codes + ['PUM1'])
)
df_accept_no_pum1 = df_events_accept.withColumn(
    "AcceptFlag_noPUM1",
    when(
        (col("CM_ENT_CD").isin(acceptance_codes)) &
        (col("CM_ENT_DT") > "2020-10-01") &
        (~col("CM_ENT_CD").isin(['PUM1'])),
        1
    ).otherwise(0)
)
accept_count_no_pum1 = df_accept_no_pum1.filter(col("AcceptFlag_noPUM1") == 1).count()

# Acceptance after PUM1 and cancellation codes
df_events_pum1 = df_ip_cmn_bus_event.filter(
    col("CM_ENT_CD").isin(acceptance_codes + ['PUM1'] + cancellation_codes) &
    (col("CM_ENT_DT") >= "2020-10-01")
).withColumn(
    "PUM1Date", when(col("CM_ENT_CD") == "PUM1", col("CM_ENT_DT"))
).withColumn(
    "AfterPum1Date", when(col("CM_ENT_CD").isin(acceptance_codes), col("CM_ENT_DT"))
).withColumn(
    "CancellationCode", when(col("CM_ENT_CD").isin(cancellation_codes), col("CM_ENT_CD"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Start ETL
# MAGIC DF numbers align to activity numbers in original Alteryx ETL - POU Audit Table DBX.yxmd

# COMMAND ----------

# Optimize window and join operations by minimizing intermediate DataFrames and chaining transformations

cm_ent_cd_dict = {
    "PUM1": "first_audit_office_action_dt",
    "PUM2": "second_audit_office_action_dt",
    "PUM3": "third_audit_office_action_dt",
    "PUMI": "audit_interim_office_action_dt",
    "PUNQ": "audit_no_response_office_action_dt"
}

# Get latest PUM1 for each CM_SER_NUM and join with worker info
df_65 = (
    df_ip141.filter(col("CM_ENT_CD") == 'PUM1')
    .withColumn("rn", row_number().over(Window().partitionBy("CM_SER_NUM").orderBy(col("CM_ENT_DT").desc())))
    .filter(col("rn") == 1)
    .drop("rn")
    .join(df_ip139, col("CM_PRCD_NUM") == df_ip139.EM_EMPE_NUM, "inner")
    .select("cm_ser_num", "em_empe_name")
)

# Replace codes, convert date, and get first cm_ent_dt for each unique ser_num + ent_cd, then pivot
df_16 = (
    df_ip141.replace(cm_ent_cd_dict, subset=["CM_ENT_CD"])
    .withColumn("CM_ENT_DT", col("CM_ENT_DT").astype(DateType()))
    .withColumn("rn", row_number().over(Window().partitionBy("cm_ser_num", "cm_ent_cd").orderBy("cm_ent_dt")))
    .filter(col("rn") == 1)
    .drop("rn")
    .groupBy("cm_ser_num")
    .pivot("cm_ent_cd")
    .agg(first("cm_ent_dt"))
)

# Join post_reg with pivoted audit dates
df_6 = (
    df_post_reg.select(
        "serial_number",
        "registration_number",
        "owner_name",
        "country_or_area_name",
        "active_class_count",
        col("concat_class").alias("reg_classes")
    )
    .join(df_16, col("serial_number") == df_16.cm_ser_num, "inner")
    .drop("cm_ser_num")
)

# Get first attorney/firm for each ser_num
df_22 = (
    df_corr.filter((col("atty_nm").isNotNull()) & (col("atty_nm") != ''))
    .withColumn("rn", row_number().over(Window().partitionBy("ser_num").orderBy('atty_nm', 'firm_nm')))
    .filter(col("rn") == 1)
    .drop("rn")
    .select("ser_num", "atty_nm", "firm_nm")
)

# Get first filing_basis_cur for each ser_num
df_23 = (
    df_biblo.withColumn("rn", row_number().over(Window().partitionBy("ser_num").orderBy('filing_basis_cur')))
    .filter(col("rn") == 1)
    .drop("rn")
    .select("ser_num", "filing_basis_cur")
)

# Join attorney/firm and filing_basis_cur info
df_29 = df_23.join(df_22, "ser_num", "left")

# Final join and column renaming
df_17 = (
    df_6.join(df_29, col("serial_number") == df_29.ser_num, "inner")
    .drop("active_class_count", "ser_num")
    .withColumnsRenamed({'atty_nm': 'attorney_name', 'firm_nm': 'firm_name'})
)

# COMMAND ----------

from pyspark.sql.types import TimestampType

# Select relevant columns from df_pou and join with df_17, add audit columns
df_34_L = (
    df_17.join(
        df_pou.select(
            "serial_number",
            "em_empe_name",
            "response_oa_rec_in",
            "deletions_after_audit_in",
            "deletions_after_audit_count",
            "cancellation_in",
            "deletion_dt",
            "termination_dt",
            "review_fy",
            "review_fy_quarter",
            "review_month",
            "review_month_int"
        ),
        "serial_number",
        "left"
    )
    .withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit('ETL'))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit('ETL'))
)

# Anti join for unmatched serial_numbers, then union
#df_34 = df_34_L.unionByName(
    #df_pou.withColumnRenamed("rn", "registration_number")
    #.join(df_17, "serial_number", "anti")
#)

# Anti join for unmatched serial_numbers, then union with aligned columns

# Get columns from df_34_L
df_34_L_cols = df_34_L.columns

# Select only matching columns from anti-join DataFrame
df_34_anti = (
    df_pou.join(df_17, "serial_number", "anti")
    .select([col(c) for c in df_34_L_cols])
)

# Union with matching columns
df_34 = df_34_L.unionByName(df_34_anti)

# Window and refund_flag in one step, then filter and aggregate
win_41 = Window().orderBy("PRJCT_CD")
df_42 = (
    df_ip55.withColumn(
        "PRJCT_CD",
        when(col("TRAN_PSTNG_REF_TX").rlike("^[a-zA-Z.]*$"), col("SALE_PSTG_REF_TX")).otherwise(col("TRAN_PSTNG_REF_TX"))
    )
    .withColumn(
        "refund_flag",
        when(
            ((lead(col("PRJCT_CD")).over(win_41) == col("PRJCT_CD")) & (lead(col("TRAN_AM")).over(win_41) == col("TRAN_AM"))) | (col("TRAN_AM") < 0),
            lit("NO")
        ).otherwise(
            when(
                ((lag(col("PRJCT_CD")).over(win_41) == col("PRJCT_CD")) & (lag(col("TRAN_AM")).over(win_41) == col("TRAN_AM"))) | (col("TRAN_AM") < 0),
                lit("NO")
            ).otherwise("YES")
        )
    )
)

df_44 = df_42.groupBy("PRJCT_CD").agg(
    sum("QTY").alias("sum_unit_qt"),
    max("ACCTG_DT").alias("deletion_dt")
)

# Join post_reg and deletion aggregates, then group and rename
df_52 = (
    df_post_reg.select(
        "serial_number",
        "next_10yr_renewal",
        "ten_yr_fy",
        "pctram_link",
        "country_or_area_name",
        "mark_nm_short",
        "registration_number"
    )
    .join(df_44, df_post_reg.registration_number == df_44.PRJCT_CD, "right")
    .groupBy("PRJCT_CD")
    .agg(
        sum("sum_unit_qt").alias("deletions_after_audit_count"),
        max(col("deletion_dt")).alias("deletion_dt")
    )
    .withColumnRenamed("PRJCT_CD", "serial_number")
)

# Join deletion counts to df_34, update columns, and optimize chaining

df_69 = (
    df_34.join(
        df_52.withColumnRenamed('deletions_after_audit_count', 'del_ct1').withColumnRenamed('deletion_dt', 'del_dt1'),
        "serial_number",
        "left"
    )
    .withColumn(
        'deletions_after_audit_count',
        when(col('del_ct1').isNotNull(), col('del_ct1')).otherwise(col('deletions_after_audit_count'))
    )
    .withColumn(
        'deletion_dt',
        when(col('del_dt1').isNotNull(), col('del_dt1').cast(TimestampType()))
        .otherwise(
            when(
                col('deletion_dt').isNotNull() & (size(col('deletion_dt')) > 0) & (col('deletion_dt')[0].isNotNull()),
                col('deletion_dt')[0].cast(TimestampType())
            ).otherwise(lit(None).cast(TimestampType()))
        )
    )
    .drop('del_ct1', 'del_dt1')
)

# Set deletions_after_audit_in flag
df_57 = df_69.withColumn(
    "deletions_after_audit_in",
    when((col("deletions_after_audit_in") == True) | (col("deletions_after_audit_count") > 0), lit(True)).otherwise(lit(False))
)

# Join worker info and update em_empe_name
df_77 = (
    df_57.join(df_65.withColumnRenamed('em_empe_name', 'em_nm1'), df_65.cm_ser_num == df_57.serial_number, "left")
    .withColumn(
        'em_empe_name',
        when(col('em_nm1').isNotNull(), col('em_nm1')).otherwise(col('em_empe_name'))
    )
    .drop('em_nm1', 'cm_ser_num')
)

# Aggregate cancellation and acceptance dates, then join with PUM1 date
df_81 = df_cancellations.groupBy("cm_ser_num").agg(max(col("cm_ent_dt")).alias("cancellation_dt"))
df_acc = df_acceptance.groupBy("cm_ser_num").agg(max(col("cm_ent_dt")).alias("acceptance_dt"))
df_83 = df_pou.groupBy("serial_number").agg(min(col("first_audit_office_action_dt")).alias("PUM1_DT")).withColumnRenamed("serial_number", "cm_ser_num")

df_86 = (
    df_81.join(df_83, "cm_ser_num")
    .withColumn(
        "cancellation_in",
        when(
            (months_between(col("cancellation_dt"), col("pum1_dt")) <= 42) & (months_between(col("cancellation_dt"), col("pum1_dt")) >= 0),
            lit(True)
        ).otherwise(lit(False))
    )
)

# COMMAND ----------

# Optimize joins, chaining, and minimize intermediate DataFrames

# Acceptance join and flag in one step
df_acceptance_joined = (
    df_acc.join(df_83, "cm_ser_num")
    .withColumn(
        "acceptance_in",
        when(
            (months_between(col("acceptance_dt"), col("PUM1_DT")) <= 42) &
            (months_between(col("acceptance_dt"), col("PUM1_DT")) >= 0),
            lit(True)
        ).otherwise(lit(False))
    )
    .filter(col("acceptance_in"))
    .withColumnRenamed("PUM1_DT", "ACCEPT_AUDIT_DT")
)

# Termination calculation
df_termination = (
    df_86.filter(col("cancellation_in"))
    .join(df_acceptance_joined, "cm_ser_num", "outer")
    .withColumn(
        "termination_dt",
        least(col("cancellation_dt"), col("acceptance_dt"))
    )
    .select("cm_ser_num", "termination_dt")
)

# Rein join and cancellation logic
df_98 = df_ip150.groupBy("cm_ser_num").agg(max(col("cm_ent_dt")).alias("rein_dt"))
df_99 = (
    df_86.join(df_98, "cm_ser_num", "left")
    .join(df_termination, "cm_ser_num", "outer")
)
df_100 = (
    df_99.withColumn(
        "cancellation_in",
        when(col("rein_dt").isNull(), col("cancellation_in"))
        .otherwise(when(col("rein_dt") > col("cancellation_dt"), lit(False)).otherwise(col("cancellation_in")))
    )
    .select("cm_ser_num", "cancellation_in", "termination_dt")
)

# Join to main, update cancellation and termination flags
df_92 = (
    df_77.join(
        df_100.withColumnRenamed('cancellation_in', 'canc_1').withColumnRenamed('termination_dt', 'term_dt1'),
        df_77.serial_number == df_100.cm_ser_num,
        "left"
    )
    .withColumn(
        'cancellation_in',
        when(col('canc_1').isNotNull(), col('canc_1')).otherwise(col('cancellation_in'))
    )
    .withColumn(
        'termination_dt',
        when(col('term_dt1').isNotNull(), col('term_dt1')).otherwise(col('termination_dt'))
    )
    .drop('canc_1', 'term_dt1', 'cm_ser_num')
)

# EROP join and response flag
df_110 = (
    df_83.join(df_ip151.select("cm_ser_num", col("cm_ent_dt").alias("erop_dt")), "cm_ser_num")
    .withColumn(
        "response_oa_rec_in",
        when(
            (floor(months_between(col("erop_dt"), col("PUM1_DT"))) <= 24) &
            (ceiling(months_between(col("erop_dt"), col("PUM1_DT"))) >= 0),
            lit(True)
        ).otherwise(lit(False))
    )
    .select("cm_ser_num", "response_oa_rec_in")
    .dropDuplicates(["cm_ser_num"])
)

# Join response flag to main
df_107 = (
    df_92.join(
        df_110.withColumnRenamed('response_oa_rec_in', 'oa_rec1'),
        df_92.serial_number == df_110.cm_ser_num,
        "left"
    )
    .withColumn(
        'response_oa_rec_in',
        when(col('oa_rec1').isNotNull(), col('oa_rec1')).otherwise(col('response_oa_rec_in'))
    )
    .drop('oa_rec1', 'cm_ser_num')
)

# Final chaining and column calculations
df_94 = (
    df_107.fillna(False, subset=["cancellation_in", "response_oa_rec_in"])
    .withColumn(
        "filing_basis_cur",
        when((col("filing_basis_cur").isNull()) | (col("filing_basis_cur") == ""), lit("USE")).otherwise(col("filing_basis_cur"))
    )
    .withColumn(
        "review_month_int", month(col("first_audit_office_action_dt"))
    )
    .withColumn(
        "review_fy",
        when(col("review_month_int") > 9, year(col("first_audit_office_action_dt")) + 1).otherwise(year(col("first_audit_office_action_dt")))
    )
    .withColumn(
        "review_fy_quarter",
        when(col("review_month_int") < 4, lit("Q2"))
        .otherwise(when(col("review_month_int") < 7, lit("Q3"))
        .otherwise(when(col("review_month_int") < 10, lit("Q4")).otherwise(lit("Q1"))))
    )
    .withColumn(
        "review_month", date_format(col("first_audit_office_action_dt"), 'MMMM')
    )
)

# COMMAND ----------

from pyspark.sql.functions import when, col, to_date, lit, array_sort, collect_list, min, max, size, split

may_1_2026 = to_date(lit("2026-05-01"))

# Optimize PRJCT_CD update and deletion event aggregation in a single chained transformation
df_deletion_events = (
    df_42.withColumn(
        "PRJCT_CD",
        when(
            (col("PRJCT_CD") == "OTHER") & (col("ACCTG_DT") >= may_1_2026),
            col("TRAN_PSTNG_REF_TX")
        ).otherwise(col("PRJCT_CD"))
    )
    .groupBy("PRJCT_CD")
    .agg(
        array_sort(collect_list("ACCTG_DT")).alias("all_deletion_dates"),
        min("ACCTG_DT").alias("first_deletion_dt"),
        max("ACCTG_DT").alias("latest_deletion_dt"),
        size(collect_list("ACCTG_DT")).alias("deletion_event_count")
    )
    .withColumnRenamed("PRJCT_CD", "registration_number")
)

df_94 = df_94.join(df_deletion_events, "registration_number", "left")

# Efficient event flag calculation: filter first, then aggregate
acceptance_codes = ['NA75', 'NAS8', 'NA89', 'NA71', 'NA85', 'NP89']
df_events = (
    spark.table("trm_tmngpdb.bronze.business_event").alias("be")
    .join(
        spark.table("trm_tmngpdb.bronze.stnd_business_event_reason").alias("sber"),
        col("be.fk_business_event_reason_id") == col("sber.business_event_reason_id"),
        "inner"
    )
    .filter(col("sber.legacy_cm_ent_cd").isin(acceptance_codes + ['PUM1']))
    .select(
        col("sber.legacy_cm_ent_cd").alias("CM_ENT_CD"),
        split(col("be.cfk_object_gid"), ":")[2].alias("CM_SER_NUM"),
        col("be.effective_ts").alias("CM_ENT_DT"),
        col("sber.title_tx")
    )
)

df_events_flag = (
    df_events
    .groupBy("CM_SER_NUM")
    .agg(
        max(
            when(
                (col("CM_ENT_CD").isin(acceptance_codes)) &
                (col("CM_ENT_DT") > lit("2020-10-01 00:00:00")) &
                (~col("CM_ENT_CD").isin(['PUM1'])),
                lit(True)
            ).otherwise(lit(False))
        ).alias("AcceptFlag_noPUM1")
    )
)

df_94 = df_94.join(
    df_events_flag.select("CM_SER_NUM", "AcceptFlag_noPUM1"),
    df_94.serial_number == df_events_flag.CM_SER_NUM,
    "left"
)

df_final = df_94.select(
    'serial_number',
    'registration_number',
    'em_empe_name',
    'first_audit_office_action_dt',
    'second_audit_office_action_dt',
    'third_audit_office_action_dt',
    'audit_interim_office_action_dt',
    'audit_no_response_office_action_dt',
    'response_oa_rec_in',
    'deletions_after_audit_in',
    'deletions_after_audit_count',
    'deletion_event_count',
    'all_deletion_dates',
    'first_deletion_dt',
    'latest_deletion_dt',
    'cancellation_in',
    'AcceptFlag_noPUM1',
    'owner_name',
    'attorney_name',
    'firm_name',
    'filing_basis_cur',
    'country_or_area_name',
    'reg_classes',
    'review_fy',
    'review_fy_quarter',
    'review_month',
    'review_month_int',
    'termination_dt',
    'create_ts',
    'create_user_id',
    'update_ts',
    'update_user_id'
)

# COMMAND ----------

try:
    # Use saveAsTable with mergeSchema for schema evolution
    df_final.write.mode("overwrite").option("mergeSchema", "true").format("delta").insertInto(f"{reporting_catalog}.silver.proof_of_use_audit")        
    recs_count = df_final.count()
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts, 'completed', recs_count, "job completed successfully")
    dbutils.notebook.exit(f"Completed Loading {reporting_catalog}.silver.proof_of_use_audit Table")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts, 'failed', 0, e)
    raise