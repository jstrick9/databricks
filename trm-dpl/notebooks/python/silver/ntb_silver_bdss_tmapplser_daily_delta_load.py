# Databricks notebook source
from delta.tables import DeltaTable

from pyspark.sql.functions import (
    date_format,
    current_timestamp,
    from_utc_timestamp
)

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text(name="dbx_env", defaultValue="dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env")

config_file = f"../../config/{dbx_env}/tmngpdb-conf.yaml"
tmintltm_config_file = f"../../config/{dbx_env}/tmintltm-conf.yaml"

print(f"{dbx_env=}")

print(f"{config_file=}")
print(f"{tmintltm_config_file=}")

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

tmngpdb_common_configs = read_yaml(config_file)
tmintltm_common_configs = read_yaml(tmintltm_config_file)

tmngpdb_dbx_catalog = tmngpdb_common_configs["schema"]["trgt_catalog"]
tmintltm_dbx_catalog = tmintltm_common_configs["schema"]["trgt_catalog"]
s3_action_key_business_event_table_path = tmngpdb_common_configs["schema"]["s3_action_key_business_event_table_path"]

# Delta feed date is always the day before of pipeline run date.
delta_feed_date = (datetime.datetime.today() - timedelta(days=1)).date().strftime("%Y-%m-%d")

print(f"{tmngpdb_dbx_catalog=}")
print(f"{tmintltm_dbx_catalog=}")
print(f"{s3_action_key_business_event_table_path=}")
print(f"{delta_feed_date=}")

# COMMAND ----------

tmappl_action_key_business_event_df = (
    spark.read.format("com.crealytics.spark.excel")
    .option("dataAddress", "'Mapping_Table'!A1")
    .option("header", True)
    .option("inferSchema", True)
    .load(s3_action_key_business_event_table_path)
)

tmappl_action_key_business_event_df = tmappl_action_key_business_event_df.withColumn(
    "INGESTION_TS",
    date_format(
        from_utc_timestamp(current_timestamp(), "America/New_York"),
        "yyyy-MM-dd'T'HH:mm:ss.SSS",
    ),
)

for column in tmappl_action_key_business_event_df.columns:
    tmappl_action_key_business_event_df = tmappl_action_key_business_event_df.withColumnRenamed(column, column.lower().replace(" ", ""))

tmappl_action_key_business_event_df.write.mode("overwrite").option(
    "mergeSchema", True
).saveAsTable(f"{tmngpdb_dbx_catalog}.silver.tmappl_action_key_business_event")

print(f"Action keys to be inserted: {tmappl_action_key_business_event_df.count()}")
tmappl_action_key_business_event_df.display()

# COMMAND ----------

sernum_ib_df = spark.sql(
    f"""
        SELECT
            tsde.serial_num_tx AS sernum,
            tsde.event_dt AS pulldt,
            'IB' AS actcd
        FROM {tmngpdb_dbx_catalog}.silver.temp_summary_daily_event_pull tsde
        JOIN {tmintltm_dbx_catalog}.silver.temp_summary_daily_ia_event_pull tsdie ON tsde.serial_num_tx = tsdie.serial_num_tx
        WHERE
        tsde.event_dt = '{delta_feed_date}'
        OR (
            tsde.create_ts >= '{delta_feed_date}T00:00:00'
            AND tsde.create_ts <= '{delta_feed_date}T23:59:59'
        )
    """
)

print(f"Serial numbers 'IB' processed: {sernum_ib_df.count()}")
sernum_ib_df.display()

# COMMAND ----------

sernum_action_key_df = spark.sql(
    f"""
        SELECT DISTINCT
            tsde.serial_num_tx AS sernum,
            tsde.event_dt AS pulldt,
            takb.action_key_code AS actcd
            FROM {tmngpdb_dbx_catalog}.silver.temp_summary_daily_event_pull tsde
            JOIN {tmngpdb_dbx_catalog}.bronze.business_event be ON tsde.trademark_gid = be.cfk_object_gid
            JOIN {tmngpdb_dbx_catalog}.silver.tmappl_action_key_business_event takb ON takb.business_event_reason_id = be.fk_business_event_reason_id
            WHERE tsde.event_dt = '{delta_feed_date}'
            AND be.fk_business_event_reason_id IN (
                125, 127, 191, 221, 222, 223, 224, 225, 226, 227, 228, 232, 233, 342, 343, 344, 345, 346, 347, 534, 547, 552, 554, 555, 624, 655, 677, 839, 871, 1023,
                -- IB codes
                133, 146, 193, 194, 204, 205, 210, 211, 235, 315, 316, 319, 320, 340, 349, 359, 360, 364, 448, 450, 451, 455, 456, 457, 458, 459, 460, 465, 509, 510, 515, 646, 672, 673, 674, 718, 719, 757, 758, 814, 815, 816, 817, 818, 819, 821, 822, 823, 824, 842, 846, 848, 853, 883, 890, 911, 912, 915, 921, 942, 943, 946, 949, 951, 952, 953, 954, 955, 956, 957, 958, 959, 960, 961, 962, 963, 964, 967, 968, 969, 970, 971, 972, 973, 975, 976, 979, 980, 981, 983, 985, 987, 988, 990, 992, 993, 994, 995, 996, 997, 998, 999, 1000, 1001, 1002, 1003, 1004, 1005, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 1019, 1020, 1024, 1025, 1026, 1028, 1029, 1031, 1032, 1033, 1034, 1037, 1053, 1082, 1084, 342572, 1108740, 1108818, 1547192, 1547193, 1547195, 1547196
            ) AND to_date(be.last_mod_ts) = '{delta_feed_date}';
    """
)

print(f"Serial numbers that are not 'IB' and 'TX' processed: {sernum_action_key_df.count()}")
sernum_action_key_df.display()

# COMMAND ----------

sernum_tx_df = spark.sql(
    f"""
        WITH tx_serials AS (
            SELECT
                tsde.serial_num_tx AS sernum,
                tsde.event_dt AS pulldt,
                'TX' AS actcd,
                tsde.create_ts
            FROM {tmngpdb_dbx_catalog}.silver.temp_summary_daily_event_pull tsde
            WHERE
                tsde.event_dt = '{delta_feed_date}'
                OR (
                    tsde.create_ts >= '{delta_feed_date}T00:00:00'
                    AND tsde.create_ts <= '{delta_feed_date}T23:59:59'
                )
        ),
        existing_serials AS (
            SELECT DISTINCT sernum
            FROM {tmngpdb_dbx_catalog}.silver.tmappl_daily_delta
            WHERE
                pulldt = '{delta_feed_date}'
                OR (
                    create_ts >= '{delta_feed_date}T00:00:00'
                    AND create_ts <= '{delta_feed_date}T23:59:59'
                )
        )
        SELECT sernum, pulldt, actcd
        FROM tx_serials
        WHERE sernum NOT IN (SELECT sernum FROM existing_serials)
    """
)

print(f"Serial numbers 'TX' processed: {sernum_tx_df.count()}")
display(sernum_tx_df)

# COMMAND ----------

final_df = sernum_ib_df.union(sernum_action_key_df).union(sernum_tx_df).distinct()
print(f"Processed serial numbers: {final_df.count()}")

final_df = (
    final_df.withColumn(
        "create_ts",
        date_format(
            from_utc_timestamp(current_timestamp(), "America/New_York"),
            "yyyy-MM-dd'T'HH:mm:ss.SSS"
        )
    )
)

if final_df.isEmpty():
    dbutils.notebook.exit(f"No delta data for {delta_feed_date}.")

# COMMAND ----------

if spark.catalog.tableExists(f"{tmngpdb_dbx_catalog}.silver.tmappl_daily_delta"):
    tmappl_daily_delta_dt = DeltaTable.forName(
        spark, f"{tmngpdb_dbx_catalog}.silver.tmappl_daily_delta"
    )

    columns_to_update = [col for col in final_df.columns if col not in ["sernum", "pulldt", "actcd"]]
    
    (
        tmappl_daily_delta_dt.alias("trgt")
        .merge(
            final_df.alias("src"),
            "trgt.sernum = src.sernum AND trgt.pulldt = src.pulldt AND trgt.actcd = src.actcd",
        )
        .whenMatchedUpdate(set={col: f"src.{col}" for col in columns_to_update})
        .whenNotMatchedInsert(values={col: f"src.{col}" for col in final_df.columns})
        .execute()
    )
else:
    final_df.write.mode("append").option("mergeSchema", True).saveAsTable(
        f"{tmngpdb_dbx_catalog}.silver.tmappl_daily_delta"
    )
