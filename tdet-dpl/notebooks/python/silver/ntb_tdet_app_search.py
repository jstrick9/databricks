# Databricks notebook source
import pytz
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, lit, coalesce, sha2, concat_ws,
    current_date, current_timestamp, when,
    max, first, array_join, collect_set,
    trim, concat, broadcast, floor, dense_rank, split
)

# COMMAND ----------

dbutils.widgets.text("dbx_env", "dev", "Database Environment")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

dbutils.widgets.text("load_method", "Incremental", "Load Method")
load_method = dbutils.widgets.get("load_method").rstrip()

config_file = f"../../config/{dbx_env}/tdet-conf.yaml"

job_name = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
    .split("/")[-1]
)
job_start_ts = datetime.now(pytz.timezone('US/Eastern'))
print(f"{config_file=}\n\n{job_name=}\n\n{job_start_ts=}")

# COMMAND ----------

# MAGIC %run ../../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# Create checkpoint directory if it doesn't exist
checkpoint_dir = '/dbfs/tmp/tdet_checkpoints'

try:
    dbutils.fs.mkdirs(checkpoint_dir)
    print(f"\033[1mCreated checkpoint directory:\033[0m {checkpoint_dir}") 
except Exception as e:
    print(f"Checkpoint directory already exists or error creating: {e}")

# Set the checkpoint directory
spark.sparkContext.setCheckpointDir(checkpoint_dir)

# COMMAND ----------

configs = read_yaml(config_file)
tmngpdb_catalog = configs["schema"]["source_tmngpdb_catalog"]
tmintltm_catalog = configs["schema"]["source_tmintltm_catalog"]
reporting_catalog = configs["schema"]["source_reporting_catalog"]
worker_catalog = configs["schema"]["source_worker_catalog"]
tdet_catalog = configs["schema"]["trgt_catalog"]

# COMMAND ----------

target_table = tdet_catalog + ".silver.tdet_app_search" 

print(f"\033[1mTarget table:\033[0m {target_table}\n")

table_exists = spark.catalog.tableExists(target_table)

if table_exists:
    load_method = 'Incremental'
else:
    load_method = 'Initial'

if dbutils.widgets.get("load_method") != '':
    load_method = dbutils.widgets.get("load_method").rstrip()

print(f"\033[1mLoad method:\033[0m {load_method}")

# COMMAND ----------

def add_hashes(df):
    """
    Hash function that uses ALL data columns to generate a unique Row ID.
    
    Since the input DataFrame uses DISTINCT, hashing all columns guarantees 
    uniqueness per row. This allows us to detect ANY change (current or historical)
    and manage it as a Version Change (Deactivate Old / Insert New).
    """
    merge_timestamp = current_timestamp()
    
    # Only exclude the metadata columns we are about to create
    exclude_cols = {"_natural_key_hash", "_record_data_hash", "_created_date", 
                    "_created_timestamp", "_updated_timestamp", "_is_record_active"}
    
    # Hash EVERY column in the dataframe to ensure exact data match
    cols_to_hash = sorted([c for c in df.columns if c not in exclude_cols])
    
    # Create the hash expression
    hash_expr = concat_ws(
        "||",
        *[
            when(col(c).isNull(), lit("NULL"))
            .otherwise(trim(col(c).cast("string")))
            for c in cols_to_hash
        ]
    )
    
    # _natural_key_hash and _record_data_hash are identical in this strategy.
    # We keep both column names to maintain compatibility with the table schema.
    df = (df
          .withColumn("_natural_key_hash", sha2(hash_expr, 256))
          .withColumn("_record_data_hash", sha2(hash_expr, 256))
          .withColumn("_created_date", current_date())
          .withColumn("_created_timestamp", merge_timestamp)
          .withColumn("_updated_timestamp", merge_timestamp)
          .withColumn("_is_record_active", lit(True))
          )
    
    return df

# COMMAND ----------

# ============================================================
# STAGE 1: Spark Configuration & Base Table Setup
# ============================================================

print("Stage 1: Configuring Spark and reading base tables...\n")

# ---- Spark Configuration ----
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "auto")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

# ---- Small lookups (broadcast candidates) ----
stnd_legacy_status_df = spark.table(f"{tmngpdb_catalog}.bronze.stnd_legacy_status").cache()
workers_df = spark.table(f"{worker_catalog}.bronze.worker").select("worker_no", "worker_nm").cache()

# ---- tm_party_role: read ONCE, filter ONCE ----
all_party_roles_df = (
    spark.table(f"{tmngpdb_catalog}.bronze.tm_party_role")
    .filter(col("fk_tm_party_role_cd").isin("OWNER", "AT", "COR", "DR"))
    .select(
        "fk_trademark_gid", "tm_party_role_id", "bar_information_tx",
        "fk_interested_party_gid", "fk_tm_party_role_cd", "party_role_sequence_no"
    )
    .cache()
)
all_party_roles_count = all_party_roles_df.count()
print(f"  all_party_roles_df cached: {all_party_roles_count:,} rows")

# ---- interested_party: read ONCE ----
interested_party_base_df = (
    spark.table(f"{tmngpdb_catalog}.bronze.interested_party")
    .select("interested_party_gid", "interested_party_nm", "country_cd")
    .cache()
)
ip_count = interested_party_base_df.count()
print(f"  interested_party_base_df cached: {ip_count:,} rows")

# ---- Mailing address: read ONCE, join ONCE ----
mailing_info_df = (
    spark.table(f"{tmngpdb_catalog}.bronze.tm_mailing_addr").alias("tmma")
    .join(
        spark.table(f"{tmngpdb_catalog}.bronze.mailing_address")
        .filter(
            (col("address_type_ct") == "S") &
            ~(
                col("street_line_1_tx").isNull() &
                col("street_line_2_tx").isNull() &
                col("city_nm").isNull() &
                col("geographic_region_cd").isNull() &
                col("postal_cd").isNull() &
                col("country_cd").isNull()
            )
        )
        .alias("ma"),
        col("tmma.fk_mailing_address_gid") == col("ma.mailing_address_gid"),
        "inner"
    )
    .select(
        col("tmma.fk_tm_party_role_id"),
        col("ma.name_line_1_tx"),
        col("ma.name_line_2_tx").alias("firm_name"),
        trim(concat_ws(" ",
                        col("ma.street_line_1_tx"),
                        col("ma.street_line_2_tx"),
                        col("ma.city_nm"),
                        col("ma.geographic_region_cd"),
                        col("ma.postal_cd"),
                        col("ma.country_cd")
                        )).alias("address")
    )
    .distinct()
    .persist()
)
mi_count = mailing_info_df.count()
print(f"  mailing_info_df persisted: {mi_count:,} rows")

# ---- Electronic address: read ONCE, join ONCE ----
email_joined_df = (
    spark.table(f"{tmngpdb_catalog}.bronze.tm_electronic_addr").alias("tmea")
    .join(
        spark.table(f"{tmngpdb_catalog}.bronze.electronic_address")
        .filter(col("fk_electronic_addr_type_cd") == "EMAIL")
        .alias("ea"),
        col("tmea.fk_electronic_address_gid") == col("ea.electronic_address_gid"),
        "inner"
    )
    .select(
        col("tmea.fk_tm_party_role_id"),
        col("tmea.primary_in"),
        col("ea.electronic_address_gid"),
        col("ea.electronic_addr_locator_tx").alias("email")
    )
    .persist()
)
ej_count = email_joined_df.count()
print(f"  email_joined_df persisted: {ej_count:,} rows")

# ---- Telecom: read ONCE, join ONCE ----
telecom_info_df = (
    spark.table(f"{tmngpdb_catalog}.bronze.tm_telecom_addr").alias("tmta")
    .join(
        spark.table(f"{tmngpdb_catalog}.bronze.telecom_address")
        .filter(
            (col("fk_telecom_type_cd") == "OFC") &
            (col("fk_telecom_format_cd") == "US") &
            col("telecom_no").isNotNull()
        )
        .alias("ta"),
        col("tmta.fk_telecom_address_gid") == col("ta.telecom_address_gid"),
        "inner"
    )
    .select(
        col("tmta.fk_tm_party_role_id"),
        col("ta.telecom_no")
    )
    .distinct()
    .persist()
)
ti_count = telecom_info_df.count()
print(f"  telecom_info_df persisted: {ti_count:,} rows")

print(f"\n  \033[1mTotal rows cached/persisted: {all_party_roles_count + ip_count + mi_count + ej_count + ti_count:,}\033[0m")
print(f"\n\033[1m\033[32mCOMPLETED\033[0m - Base tables read, filtered, and cached/persisted")
print(f"  \033[1mNote:\033[0m Historical tables will be read directly in Stages 3 and 5")

# COMMAND ----------

# ============================================================
# STAGE 2: Trademarks + Reporting tables
# ============================================================

print("Stage 2: Processing trademarks with reporting data...\n")

trademarks_base_df = (
    spark.table(f"{tmngpdb_catalog}.bronze.trademark").alias("tm")
    .join(
        broadcast(stnd_legacy_status_df).alias("sls"),
        col("tm.legacy_status_cd") == col("sls.status_no"),
        "inner"
    )
    .select(
        col("tm.trademark_gid"),
        col("tm.serial_num_tx"),
        col("tm.registration_num").alias("registration_number"),
        concat(col("tm.legacy_status_cd"), lit(" - "), col("sls.description_tx")).alias("legacy_status_cd"),
        F.to_date("tm.status_dt").alias("status_date"),
        col("tm.external_reference_tx").alias("docket_number"),
        col("tm.standard_character_tx").alias("mark_tx")
    )
)

class_list_df = (
    spark.table(f"{reporting_catalog}.silver.class")
    .groupBy("ser_num")
    .agg(array_join(collect_set("class"), ";").alias("class_list"))
)

milestone_df = (
    spark.table(f"{reporting_catalog}.silver.milestone")
    .select(
        col("ser_num"),
        col("filing_dt").cast("date").alias("filing_date"),
        col("registration_dt").cast("date").alias("registration_date")
    )
)

bibliography_df = (
    spark.table(f"{reporting_catalog}.silver.bibliography")
    .select("ser_num", "exmr_eid", "law_office")
)

trademarks_with_reporting_df = (
    trademarks_base_df.alias("tm")
    .join(milestone_df.alias("m"),
          col("tm.serial_num_tx") == col("m.ser_num"), "inner")
    .join(bibliography_df.alias("b"),
          col("tm.serial_num_tx") == col("b.ser_num"), "inner")
    .join(class_list_df.alias("cl"),
          col("tm.serial_num_tx") == col("cl.ser_num"), "left")
    .join(broadcast(workers_df).alias("w"),
          col("b.exmr_eid") == col("w.worker_no"), "left")
    .select(
        col("tm.trademark_gid"),
        col("tm.serial_num_tx"),
        col("tm.registration_number"),
        col("tm.legacy_status_cd"),
        col("tm.status_date"),
        col("tm.docket_number"),
        col("tm.mark_tx"),
        col("m.filing_date"),
        col("m.registration_date"),
        col("b.exmr_eid").alias("examiner_number"),
        col("b.law_office"),
        col("w.worker_nm").alias("examiner_name"),
        col("cl.class_list")
    )
    .checkpoint()
)

# Release lookup tables no longer needed
stnd_legacy_status_df.unpersist()
workers_df.unpersist()

print("\033[1m\033[32mCOMPLETED\033[0m - Trademarks with reporting data\n")
print("\033[1m***CHECKPOINT CREATED***\033[0m")

# COMMAND ----------

# ============================================================
# STAGE 3: Owner processing
# ============================================================

print("Stage 3: Processing owner data...\n")

owner_roles_df = all_party_roles_df.filter(col("fk_tm_party_role_cd") == "OWNER")

owner_base_df = (
    owner_roles_df.alias("tpr")
    .join(
        spark.table(f"{tmngpdb_catalog}.bronze.tm_party_role_owner").alias("tpro"),
        (col("tpr.fk_trademark_gid") == col("tpro.fk_trademark_gid")) &
        (col("tpr.party_role_sequence_no") == col("tpro.fk_party_role_sequence_no")),
        "inner"
    )
    .select(
        col("tpr.fk_trademark_gid"),
        col("tpr.tm_party_role_id"),
        col("tpr.fk_interested_party_gid"),
        col("tpr.party_role_sequence_no")
    )
    .distinct()
)

max_seq_window = Window.partitionBy("fk_trademark_gid")
owner_base_df = (
    owner_base_df
    .withColumn("seq_group", floor(col("party_role_sequence_no") / 100))
    .withColumn("max_seq_group", F.max("seq_group").over(max_seq_window))
    .withColumn("latest", col("seq_group") == col("max_seq_group"))
    .drop("seq_group", "max_seq_group", "party_role_sequence_no")
    .persist()
)
owner_base_df.count()

latest_owners_df = owner_base_df.filter(col("latest") == True)
historical_owners_df = owner_base_df.filter(col("latest") == False)

# Owner names (latest only)
owner_names_df = (
    latest_owners_df.alias("ob")
    .join(
        interested_party_base_df.alias("ip"),
        col("ob.fk_interested_party_gid") == col("ip.interested_party_gid"),
        "inner"
    )
    .select(
        col("ob.fk_trademark_gid"),
        col("ob.tm_party_role_id"),
        trim(col("ip.interested_party_nm")).alias("owner_name"),
        col("ip.country_cd").alias("owner_country")
    )
)

# Historic owner names (latest = false) - read interested_party_h directly
hist_owner_names_df = (
    historical_owners_df.alias("ob")
    .join(
        spark.table(f"{tmngpdb_catalog}.bronze.interested_party_h")
        .select("interested_party_gid", "interested_party_nm")
        .alias("iph"),
        col("ob.fk_interested_party_gid") == col("iph.interested_party_gid"),
        "inner"
    )
    .groupBy("ob.fk_trademark_gid")
    .agg(array_join(collect_set(trim(col("iph.interested_party_nm"))), ";").alias("hist_owner_nm"))
)

# Owner email (latest)
owner_email_df = (
    latest_owners_df.alias("ob")
    .join(
        email_joined_df.alias("ej"),
        col("ob.tm_party_role_id") == col("ej.fk_tm_party_role_id"),
        "inner"
    )
    .select(
        col("ob.fk_trademark_gid"),
        col("ob.tm_party_role_id"),
        col("ej.email").alias("owner_email")
    )
    .distinct()
)

# Historic owner email (all owners) - read _h tables directly
hist_owner_email_df = (
    owner_base_df.alias("ob")
    .join(
        spark.table(f"{tmngpdb_catalog}.bronze.tm_electronic_addr_h")
        .select("fk_tm_party_role_id", "fk_electronic_address_gid")
        .alias("tmeah"),
        col("ob.tm_party_role_id") == col("tmeah.fk_tm_party_role_id"),
        "inner"
    )
    .join(
        spark.table(f"{tmngpdb_catalog}.bronze.electronic_address_h")
        .filter((col("action_ct") != "D") & (col("fk_electronic_addr_type_cd") == "EMAIL"))
        .select("electronic_address_gid", "electronic_addr_locator_tx")
        .alias("eah"),
        col("tmeah.fk_electronic_address_gid") == col("eah.electronic_address_gid"),
        "inner"
    )
    .groupBy("ob.fk_trademark_gid")
    .agg(array_join(collect_set(col("eah.electronic_addr_locator_tx")), ";").alias("hist_owner_email"))
)

# Owner address (latest)
owner_address_df = (
    owner_names_df.alias("wn")
    .join(
        mailing_info_df.alias("mi"),
        col("wn.tm_party_role_id") == col("mi.fk_tm_party_role_id"),
        "inner"
    )
    .select(
        col("wn.fk_trademark_gid"),
        col("mi.fk_tm_party_role_id"),
        col("mi.address").alias("owner_address")
    )
    .distinct()
)

# Owner phone (latest)
owner_phone_df = (
    owner_names_df.alias("wn")
    .join(
        telecom_info_df.alias("ti"),
        col("wn.tm_party_role_id") == col("ti.fk_tm_party_role_id"),
        "inner"
    )
    .select(
        col("wn.fk_trademark_gid"),
        col("ti.fk_tm_party_role_id"),
        col("ti.telecom_no").alias("owner_phone")
    )
    .distinct()
)

print("\033[1m\033[32mCOMPLETED\033[0m - Owner data processing complete")

# COMMAND ----------

# ============================================================
# STAGE 4: Non-owner party processing (AT, COR, DR)
# ============================================================

print("Stage 4: Processing non-owner party data (AT, COR, DR)...\n")

non_owner_roles_df = (
    all_party_roles_df
    .filter(col("fk_tm_party_role_cd").isin("AT", "COR", "DR"))
    .drop("party_role_sequence_no")
    .distinct()
)

# Interested party with COR fallback
interested_party_df = (
    non_owner_roles_df.alias("tpr")
    .join(
        interested_party_base_df.alias("ip"),
        col("tpr.fk_interested_party_gid") == col("ip.interested_party_gid"),
        "left"
    )
    .join(
        mailing_info_df.alias("mi"),
        col("tpr.tm_party_role_id") == col("mi.fk_tm_party_role_id"),
        "left"
    )
    .filter(
        col("ip.interested_party_nm").isNotNull() | col("mi.name_line_1_tx").isNotNull()
    )
    .select(
        col("tpr.fk_trademark_gid"),
        col("tpr.tm_party_role_id"),
        col("tpr.bar_information_tx"),
        col("tpr.fk_tm_party_role_cd"),
        when(col("tpr.fk_tm_party_role_cd") == "COR",
             trim(coalesce(col("ip.interested_party_nm"), col("mi.name_line_1_tx")))
             ).otherwise(trim(col("ip.interested_party_nm"))).alias("interested_party_nm")
    )
    .distinct()
)

# Non-owner emails with party_no
non_owner_email_base_df = (
    non_owner_roles_df.alias("tpr")
    .join(
        email_joined_df.alias("ej"),
        col("tpr.tm_party_role_id") == col("ej.fk_tm_party_role_id"),
        "inner"
    )
    .select(
        col("tpr.fk_trademark_gid"),
        col("tpr.tm_party_role_id"),
        col("tpr.fk_tm_party_role_cd"),
        col("ej.email"),
        col("ej.primary_in"),
        col("ej.electronic_address_gid")
    )
    .distinct()
)

# Window to count emails per party role
# This is used to determine if an email is the only one for a party role
count_per_role_window = Window.partitionBy(
    "tm_party_role_id",
    split(col("electronic_address_gid"), ":")[1].cast("int")
)

party_no_window = Window.partitionBy(
    "tm_party_role_id",
    split(col("electronic_address_gid"), ":")[1].cast("int")
).orderBy(split(col("electronic_address_gid"), ":")[2].cast("int"))

non_owner_email_base_df = (
    non_owner_email_base_df
    # Count total emails per party role partition to detect single-email roles
    .withColumn("email_count_in_partition", F.count("email").over(count_per_role_window))
    .withColumn(
        "party_no",
        when(
            # Explicitly marked as primary
            col("primary_in").eqNullSafe("Y"),
            lit(0)
        )
        .when(
            # Only one email exists for this party role — treat as primary regardless of primary_in flag
            col("email_count_in_partition") == 1,
            lit(0)
        )
        .otherwise(
            # Multiple emails and not explicitly primary — rank them
            dense_rank().over(party_no_window)
        )
    )
    .drop("email_count_in_partition")
)

attorney_email_df = (
    non_owner_email_base_df
    .filter(col("fk_tm_party_role_cd") == "AT")
    .select("fk_trademark_gid", col("email").alias("attorney_email"))
    .distinct()
)

cor_email_window = Window.partitionBy("fk_trademark_gid", "tm_party_role_id")

correspondent_email_agg_df = (
    non_owner_email_base_df
    .filter((col("fk_tm_party_role_cd") == "COR") & (col("party_no") < 6))
    .withColumn(
        "correspondent_email",
        array_join(
            collect_set(
                when((col("party_no") == 0) & col("email").isNotNull(), col("email"))
            ).over(cor_email_window),
            ";"
        )
    )
    .withColumn(
        "secondary_cor_email",
        array_join(
            collect_set(
                when(
                    (col("party_no") != 0) &
                    col("party_no").isNotNull() &
                    col("email").isNotNull(),
                    col("email")
                )
            ).over(cor_email_window),
            ";"
        )
    )
    .select("fk_trademark_gid", "tm_party_role_id", "correspondent_email", "secondary_cor_email")
    .distinct()
)

dr_email_df = (
    non_owner_email_base_df
    .filter(col("fk_tm_party_role_cd") == "DR")
    .select("fk_trademark_gid", col("email").alias("domestic_representative_email"))
    .distinct()
)

correspondent_names_df = (
    interested_party_df
    .filter(col("fk_tm_party_role_cd") == "COR")
    .select("fk_trademark_gid", "tm_party_role_id",
            col("interested_party_nm").alias("correspondent_name"))
    .distinct()
)

attorney_names_df = (
    interested_party_df
    .filter(col("fk_tm_party_role_cd") == "AT")
    .select("fk_trademark_gid", "tm_party_role_id", "bar_information_tx",
            col("interested_party_nm").alias("attorney_name"))
    .distinct()
)

dr_names_df = (
    interested_party_df
    .filter(col("fk_tm_party_role_cd") == "DR")
    .select("fk_trademark_gid", "tm_party_role_id",
            col("interested_party_nm").alias("domestic_representative_name"))
    .distinct()
)

# Release persisted DataFrames - no longer needed after this stage
all_party_roles_df.unpersist()
interested_party_base_df.unpersist()
email_joined_df.unpersist()

print("\033[1m\033[32mCOMPLETED\033[0m - Non-owner party data processing complete")

# COMMAND ----------

# ============================================================
# STAGE 5: Historical non-owner party data
# ============================================================

print("Stage 5: Processing historical party data...\n")

# Read historical party roles directly for non-owner processing
hist_non_owner_roles_df = (
    spark.table(f"{tmngpdb_catalog}.bronze.tm_party_role_h")
    .filter(
        (col("action_ct") != "D") &
        col("fk_tm_party_role_cd").isin("AT", "COR", "DR")
    )
    .select("fk_trademark_gid", "tm_party_role_id",
            "fk_interested_party_gid", "fk_tm_party_role_cd")
    .distinct()
)

# Historic non-owner emails with party_no - read _h tables directly
historic_non_owner_email_base_df = (
    hist_non_owner_roles_df.alias("tpr")
    .join(
        spark.table(f"{tmngpdb_catalog}.bronze.tm_electronic_addr_h")
        .select("fk_tm_party_role_id", "fk_electronic_address_gid", "primary_in")
        .alias("tmeah"),
        col("tpr.tm_party_role_id") == col("tmeah.fk_tm_party_role_id"),
        "inner"
    )
    .join(
        spark.table(f"{tmngpdb_catalog}.bronze.electronic_address_h")
        .filter((col("action_ct") != "D") & (col("fk_electronic_addr_type_cd") == "EMAIL"))
        .select("electronic_address_gid", "electronic_addr_locator_tx")
        .alias("eah"),
        col("tmeah.fk_electronic_address_gid") == col("eah.electronic_address_gid"),
        "inner"
    )
    .select(
        col("tpr.fk_trademark_gid"),
        col("tpr.tm_party_role_id"),
        col("tpr.fk_tm_party_role_cd"),
        col("eah.electronic_addr_locator_tx").alias("email"),
        col("tmeah.primary_in"),
        col("eah.electronic_address_gid")
    )
    .distinct()
)

# Window to count emails per party role partition
# Detects single-email roles so they are always treated as primary
hist_count_per_role_window = Window.partitionBy(
    "tm_party_role_id",
    split(col("electronic_address_gid"), ":")[1].cast("int")
)

hist_party_no_window = Window.partitionBy(
    "tm_party_role_id",
    split(col("electronic_address_gid"), ":")[1].cast("int")
).orderBy(split(col("electronic_address_gid"), ":")[2].cast("int"))

# FIXED: Same logic as Stage 4
historic_non_owner_email_base_df = (
    historic_non_owner_email_base_df
    .withColumn("email_count_in_partition", F.count("email").over(hist_count_per_role_window))
    .withColumn(
        "party_no",
        when(
            # Explicitly marked as primary
            col("primary_in").eqNullSafe("Y"),
            lit(0)
        )
        .when(
            # Only one email exists for this party role — treat as primary
            col("email_count_in_partition") == 1,
            lit(0)
        )
        .otherwise(
            # Multiple emails and not explicitly primary — rank them
            dense_rank().over(hist_party_no_window)
        )
    )
    .drop("email_count_in_partition")
)

hist_attorney_email_df = (
    historic_non_owner_email_base_df
    .filter(col("fk_tm_party_role_cd") == "AT")
    .groupBy("fk_trademark_gid")
    .agg(array_join(collect_set("email"), ";").alias("hist_at_email"))
)

hist_cor_email_window = Window.partitionBy("fk_trademark_gid", "tm_party_role_id")

hist_correspondent_email_df = (
    historic_non_owner_email_base_df
    .filter((col("fk_tm_party_role_cd") == "COR") & (col("party_no") < 6))
    .withColumn(
        "hist_cr_email",
        array_join(
            collect_set(when(col("party_no") == 0, col("email"))).over(hist_cor_email_window),
            ";"
        )
    )
    .withColumn(
        "hist_secondary_cr_email",
        array_join(
        collect_set(
            when(
                (col("party_no") != 0) & 
                col("party_no").isNotNull() &
                col("email").isNotNull(),
                col("email")
            )
        ).over(hist_cor_email_window),
        ";")
    )

    .select("fk_trademark_gid", "hist_cr_email", "hist_secondary_cr_email")
    .distinct()
)

hist_dr_email_df = (
    historic_non_owner_email_base_df
    .filter(col("fk_tm_party_role_cd") == "DR")
    .groupBy("fk_trademark_gid")
    .agg(array_join(collect_set("email"), ";").alias("hist_dr_email"))
)

# Historic names - read interested_party_h directly
interested_party_history_df = (
    hist_non_owner_roles_df.alias("tpr")
    .join(
        spark.table(f"{tmngpdb_catalog}.bronze.interested_party_h")
        .filter((col("action_ct") != "D") & col("interested_party_nm").isNotNull())
        .select("interested_party_gid", "interested_party_nm", "last_mod_ts")
        .alias("iph"),
        col("tpr.fk_interested_party_gid") == col("iph.interested_party_gid"),
        "inner"
    )
    .select(
        col("tpr.fk_trademark_gid"),
        col("tpr.tm_party_role_id"),
        col("tpr.fk_tm_party_role_cd"),
        trim(col("iph.interested_party_nm")).alias("interested_party_nm"),
        col("iph.last_mod_ts")
    )
    .distinct()
)

# Historic correspondent name with mailing_address_h fallback
correspondent_addr_name_history_df = (
    hist_non_owner_roles_df
    .filter(col("fk_tm_party_role_cd") == "COR")
    .alias("tpr")
    .join(
        spark.table(f"{tmngpdb_catalog}.bronze.tm_mailing_addr_h")
        .filter(col("action_ct") != "D")
        .alias("tmma"),
        col("tpr.tm_party_role_id") == col("tmma.fk_tm_party_role_id"),
        "inner"
    )
    .join(
        spark.table(f"{tmngpdb_catalog}.bronze.mailing_address_h")
        .filter((col("action_ct") != "D") & col("name_line_1_tx").isNotNull())
        .alias("mah"),
        col("tmma.fk_mailing_address_gid") == col("mah.mailing_address_gid"),
        "inner"
    )
    .select(
        col("tpr.fk_trademark_gid"),
        col("tpr.tm_party_role_id"),
        trim(col("mah.name_line_1_tx")).alias("interested_party_nm")
    )
    .distinct()
)

hist_correspondent_names_df = (
    hist_non_owner_roles_df
    .filter(col("fk_tm_party_role_cd") == "COR")
    .alias("tpr")
    .join(
        interested_party_history_df
        .filter(col("fk_tm_party_role_cd") == "COR")
        .alias("iph"),
        col("tpr.tm_party_role_id") == col("iph.tm_party_role_id"),
        "left"
    )
    .join(
        correspondent_addr_name_history_df.alias("tma"),
        col("tpr.tm_party_role_id") == col("tma.tm_party_role_id"),
        "left"
    )
    .select(
        col("tpr.fk_trademark_gid"),
        coalesce(col("iph.interested_party_nm"), col("tma.interested_party_nm")).alias("hist_cr_name")
    )
    .groupBy("fk_trademark_gid")
    .agg(array_join(collect_set("hist_cr_name"), ";").alias("hist_cr_nm"))
)

hist_attorney_names_df = (
    interested_party_history_df
    .filter(col("fk_tm_party_role_cd") == "AT")
    .groupBy("fk_trademark_gid")
    .agg(array_join(collect_set("interested_party_nm"), ";").alias("hist_attorney_nm"))
)

hist_dr_names_df = (
    interested_party_history_df
    .filter(col("fk_tm_party_role_cd") == "DR")
    .groupBy("fk_trademark_gid")
    .agg(array_join(collect_set("interested_party_nm"), ";").alias("hist_dr_nm"))
)

print("\033[1m\033[32mCOMPLETED\033[0m - Historical party data processing complete")

# COMMAND ----------

# ============================================================
# STAGE 6: Build trademark_party_history (the big join)
# ============================================================

print("Stage 6: Building trademark_party_history...\n")

trademark_party_history_df = (
    trademarks_with_reporting_df.alias("tm")

    # Owner data
    .join(owner_names_df.alias("wn"),
          col("tm.trademark_gid") == col("wn.fk_trademark_gid"), "left")
    .join(hist_owner_names_df.alias("hwn"),
          col("tm.trademark_gid") == col("hwn.fk_trademark_gid"), "left")
    .join(owner_email_df.alias("oe"),
          col("wn.tm_party_role_id") == col("oe.tm_party_role_id"), "left")
    .join(hist_owner_email_df.alias("oeh"),
          col("tm.trademark_gid") == col("oeh.fk_trademark_gid"), "left")
    .join(owner_phone_df.alias("ota"),
          col("wn.tm_party_role_id") == col("ota.fk_tm_party_role_id"), "left")
    .join(owner_address_df.alias("oma"),
          col("wn.tm_party_role_id") == col("oma.fk_tm_party_role_id"), "left")

    # Correspondent data
    .join(correspondent_names_df.alias("cn"),
          col("tm.trademark_gid") == col("cn.fk_trademark_gid"), "left")
    .join(hist_correspondent_names_df.alias("hcn"),
          col("tm.trademark_gid") == col("hcn.fk_trademark_gid"), "left")
    .join(correspondent_email_agg_df.alias("ce"),
          col("tm.trademark_gid") == col("ce.fk_trademark_gid"), "left")
    .join(hist_correspondent_email_df.alias("hce"),
          col("tm.trademark_gid") == col("hce.fk_trademark_gid"), "left")
    .join(mailing_info_df.alias("cma"),
          col("cn.tm_party_role_id") == col("cma.fk_tm_party_role_id"), "left")
    .join(telecom_info_df.alias("cta"),
          col("cn.tm_party_role_id") == col("cta.fk_tm_party_role_id"), "left")

    # Attorney data
    .join(attorney_names_df.alias("an"),
          col("tm.trademark_gid") == col("an.fk_trademark_gid"), "left")
    .join(hist_attorney_names_df.alias("han"),
          col("tm.trademark_gid") == col("han.fk_trademark_gid"), "left")
    .join(attorney_email_df.alias("ae"),
          col("tm.trademark_gid") == col("ae.fk_trademark_gid"), "left")
    .join(hist_attorney_email_df.alias("hae"),
          col("tm.trademark_gid") == col("hae.fk_trademark_gid"), "left")
    .join(mailing_info_df.alias("ama"),
          col("an.tm_party_role_id") == col("ama.fk_tm_party_role_id"), "left")
    .join(telecom_info_df.alias("ata"),
          col("an.tm_party_role_id") == col("ata.fk_tm_party_role_id"), "left")

    # Domestic representative data
    .join(dr_names_df.alias("drn"),
          col("tm.trademark_gid") == col("drn.fk_trademark_gid"), "left")
    .join(hist_dr_names_df.alias("hdrn"),
          col("tm.trademark_gid") == col("hdrn.fk_trademark_gid"), "left")
    .join(dr_email_df.alias("dre"),
          col("tm.trademark_gid") == col("dre.fk_trademark_gid"), "left")
    .join(hist_dr_email_df.alias("hdre"),
          col("tm.trademark_gid") == col("hdre.fk_trademark_gid"), "left")
    .join(telecom_info_df.alias("drta"),
          col("drn.tm_party_role_id") == col("drta.fk_tm_party_role_id"), "left")

    .select(
        col("tm.trademark_gid"),
        col("tm.serial_num_tx"),
        col("tm.registration_number"),
        col("tm.legacy_status_cd"),
        col("tm.status_date"),
        col("tm.docket_number"),
        col("tm.mark_tx"),
        col("tm.filing_date"),
        col("tm.registration_date"),
        col("tm.examiner_number"),
        col("tm.law_office"),
        col("tm.examiner_name"),
        col("tm.class_list"),

        when(col("wn.owner_name") == "", lit(None)).otherwise(col("wn.owner_name")).alias("owner_name"),
        when(col("hwn.hist_owner_nm") == "", lit(None)).otherwise(col("hwn.hist_owner_nm")).alias("hist_owner_nm"),
        col("oma.owner_address"),
        col("wn.owner_country"),
        when(col("oe.owner_email") == "", lit(None)).otherwise(col("oe.owner_email")).alias("owner_email"),
        when(col("oeh.hist_owner_email") == "", lit(None)).otherwise(col("oeh.hist_owner_email")).alias("hist_owner_email"),
        col("ota.owner_phone"),

        col("an.attorney_name"),
        col("an.bar_information_tx").alias("attorney_membership_no"),
        when(col("han.hist_attorney_nm") == "", lit(None)).otherwise(col("han.hist_attorney_nm")).alias("hist_attorney_nm"),
        col("ama.address").alias("attorney_address"),
        col("ata.telecom_no").alias("attorney_phone"),
        when(col("ae.attorney_email") == "", lit(None)).otherwise(col("ae.attorney_email")).alias("attorney_email"),
        when(col("hae.hist_at_email") == "", lit(None)).otherwise(col("hae.hist_at_email")).alias("hist_at_email"),

        col("cn.correspondent_name"),
        col("cma.address").alias("correspondent_address"),
        col("cma.firm_name"),
        when(col("hcn.hist_cr_nm") == "", lit(None)).otherwise(col("hcn.hist_cr_nm")).alias("hist_cr_nm"),
        when(col("ce.correspondent_email") == "", lit(None)).otherwise(col("ce.correspondent_email")).alias("correspondent_email"),
        when(col("hce.hist_cr_email") == "", lit(None)).otherwise(col("hce.hist_cr_email")).alias("hist_cr_email"),
        col("cta.telecom_no").alias("correspondent_phone"),
        when(col("ce.secondary_cor_email") == "", lit(None)).otherwise(col("ce.secondary_cor_email")).alias("secondary_cor_email"),

        col("drn.domestic_representative_name"),
        when(col("hdrn.hist_dr_nm") == "", lit(None)).otherwise(col("hdrn.hist_dr_nm")).alias("hist_dr_nm"),
        when(col("dre.domestic_representative_email") == "", lit(None)).otherwise(col("dre.domestic_representative_email")).alias("domestic_representative_email"),
        when(col("hdre.hist_dr_email") == "", lit(None)).otherwise(col("hdre.hist_dr_email")).alias("hist_dr_email"),
        col("drta.telecom_no").alias("domestic_rep_phone")
    )
    .distinct()
    .checkpoint()
)

# Release remaining persisted DataFrames
owner_base_df.unpersist()
mailing_info_df.unpersist()
telecom_info_df.unpersist()

print("\033[1m\033[32mCOMPLETED\033[0m - trademark_party_history built\n")
print("\033[1m***CHECKPOINT CREATED***\033[0m")

# COMMAND ----------

# ============================================================
# STAGE 7: Supplementary tables
# ============================================================

print("Stage 7: Processing supplementary tables...\n")

tmfb_df = (
    spark.table(f"{tmngpdb_catalog}.bronze.tm_filing_basis")
    .withColumn("serial_num_tx", split(col("fk_trademark_gid"), ":")[2])
    .groupBy("serial_num_tx")
    .agg(
        F.max(when(col("current_in") == "Y", col("fk_filing_basis_cd"))).alias("current_bases"),
        F.max(when(col("filed_in") == "Y", col("fk_filing_basis_cd"))).alias("filed_bases")
    )
)

iar_df = (
    spark.table(f"{tmintltm_catalog}.bronze.base_appl_intl_reg")
    .select(
        col("cfk_trademark_gid"),
        when(col("fk_international_reg_gid").isNotNull(), "Y").otherwise("N").alias("fk_international_reg_gid"),
        when(col("fk_international_appl_gid").isNotNull(), "Y").otherwise("N").alias("fk_international_appl_gid")
    )
    .unionByName(
        spark.table(f"{tmintltm_catalog}.bronze.international_reg_tm")
        .select(
            col("cfk_trademark_gid"),
            when(col("fk_international_reg_gid").isNotNull(), "Y").otherwise("N").alias("fk_international_reg_gid"),
            lit("N").alias("fk_international_appl_gid")
        )
    )
    .groupBy("cfk_trademark_gid")
    .agg(
        F.max("fk_international_reg_gid").alias("intl_reg_num"),
        F.max("fk_international_appl_gid").alias("international_us_ref_no")
    )
)

og_df = (
    spark.table(f"{tmngpdb_catalog}.bronze.tm_publication").alias("tmp")
    .join(
        spark.table(f"{tmngpdb_catalog}.bronze.tm_publication_subct").alias("tmpsc"),
        col("tmp.tm_publication_gid") == col("tmpsc.fk_tm_publication_gid"),
        "left"
    )
    .join(
        spark.table(f"{tmngpdb_catalog}.bronze.og_publication_tm").alias("ogptm"),
        col("tmp.tm_publication_gid") == col("ogptm.fk_tm_publication_gid"),
        "left"
    )
    .join(
        spark.table(f"{tmngpdb_catalog}.bronze.og_publication").alias("ogp"),
        col("ogptm.fk_og_publication_gid") == col("ogp.og_publication_gid"),
        "left"
    )
    .select(
        col("tmp.fk_trademark_gid"),
        col("ogp.publication_dt").alias("og_issue_date"),
        col("tmp.legacy_og_status_cd").alias("og_status"),
        col("tmpsc.legacy_des_cd").alias("og_catg")
    )
    .distinct()
)

gs_df = (
    spark.table("tm.silver.goods_service")
    .filter(col("specimen_website_address").isNotNull())
    .groupBy("serial_num_tx")
    .agg(
        concat_ws("; ",
                  collect_set(
                      when(
                          (col("specimen_website_address") != "") &
                          (col("specimen_website_address") != " "),
                          col("specimen_website_address")
                      )
                  )
                  ).alias("specimen_url")
    )
)

print("\033[1m\033[32mCOMPLETED\033[0m - Supplementary tables processed")

# COMMAND ----------

# ============================================================
# STAGE 8: Final output DataFrame
# ============================================================

print("Stage 8: Building final output DataFrame...\n")

tdet_output_df = (
    trademark_party_history_df.alias("hp")
    .join(tmfb_df.alias("tmfb"),
          col("hp.serial_num_tx") == col("tmfb.serial_num_tx"), "left")
    .join(iar_df.alias("iar"),
          col("hp.trademark_gid") == col("iar.cfk_trademark_gid"), "left")
    .join(og_df.alias("og"),
          col("hp.trademark_gid") == col("og.fk_trademark_gid"), "left")
    .join(spark.table(f"{tmngpdb_catalog}.bronze.tm_literal").alias("tml"),
          col("hp.trademark_gid") == col("tml.fk_trademark_gid"), "left")
    .join(gs_df.alias("gs"),
          col("hp.serial_num_tx") == col("gs.serial_num_tx"), "left")
    .select(
        col("hp.serial_num_tx").alias("serial_number"),
        coalesce(col("hp.mark_tx"), col("tml.literal_element_tx")).alias("mark_tx"),
        col("hp.filing_date"),
        col("tmfb.filed_bases"),
        col("tmfb.current_bases"),
        col("hp.registration_number"),
        col("hp.registration_date"),
        col("hp.owner_name"),
        col("hp.hist_owner_nm").alias("owner_name_historical"),
        col("hp.owner_address"),
        col("hp.owner_country"),
        col("hp.owner_email"),
        col("hp.hist_owner_email").alias("owner_email_historical"),
        col("hp.owner_phone"),
        col("hp.attorney_membership_no").alias("attorney_membership_number"),
        col("hp.attorney_name"),
        col("hp.hist_attorney_nm").alias("attorney_name_historical"),
        col("hp.attorney_address"),
        col("hp.attorney_email"),
        col("hp.hist_at_email").alias("attorney_email_historical"),
        col("hp.attorney_phone"),
        col("hp.correspondent_name"),
        col("hp.hist_cr_nm").alias("correspondent_name_historical"),
        col("hp.correspondent_address"),
        col("hp.correspondent_email"),
        col("hp.secondary_cor_email").alias("correspondent_email_secondary"),
        col("hp.hist_cr_email").alias("correspondent_email_historical"),
        col("hp.correspondent_phone"),
        col("hp.domestic_representative_name"),
        col("hp.hist_dr_nm").alias("domestic_representative_name_historical"),
        col("hp.domestic_representative_email"),
        col("hp.hist_dr_email").alias("domestic_representative_email_historical"),
        col("hp.domestic_rep_phone").alias("domestic_representative_phone"),
        col("hp.examiner_number"),
        col("hp.examiner_name"),
        col("hp.docket_number"),
        col("hp.firm_name"),
        col("hp.law_office"),
        col("hp.class_list"),
        col("hp.legacy_status_cd").alias("status"),
        col("hp.status_date"),
        col("og.og_issue_date"),
        col("og.og_status"),
        col("og.og_catg").alias("og_category"),
        coalesce(
            when(
                (col("tmfb.current_bases") == "66(a)") |
                col("hp.serial_num_tx").startswith("79"),
                "Y"
            ).otherwise("N"),
            lit("N")
        ).alias("international_registration_number"),
        coalesce(col("iar.international_us_ref_no"), lit("N")).alias("international_us_reference_number"),
        col("gs.specimen_url")
    )
    .distinct()
)

print("\033[1m\033[32mCOMPLETED\033[0m - Final DataFrame built")

# COMMAND ----------

# ============================================================
# STAGE 9: Hash and Write
# ============================================================

# Apply hashing to all columns
source_df = add_hashes(tdet_output_df)

if load_method == 'Initial':
    print("\n" + "=" * 80)
    print("PERFORMING INITIAL LOAD")
    print("=" * 80)

    (
        source_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "false")
        .partitionBy("_created_date")
        .saveAsTable(target_table)
    )

    print(f"\033[1mInitial load completed successfully.\033[0m\n\nRunning post-write optimization...\n")
    spark.sql(f"OPTIMIZE {target_table}")
    print(f"Post-write optimization completed")

else:
    print("\n" + "=" * 80)
    print("PERFORMING INCREMENTAL LOAD")
    print("=" * 80)

    target_df = spark.table(target_table).filter("_is_record_active = true")

    print(f"\n1. Target table active record count: {target_df.count()}")
    print(f"2. Source data record count: {source_df.count()}")

    # Join on the unique row hash
    # Since the hash represents the entire row's data, we only need to check:
    # 1. Is the Hash in Source but not Target? -> INSERT (New Version)
    # 2. Is the Hash in Target but not Source? -> DELETE (Old Version to Deactivate)
    comparison_df = (
        source_df.alias("src")
        .join(
            target_df.alias("tgt"),
            col("src._natural_key_hash") == col("tgt._natural_key_hash"),
            "full_outer"
        )
        .select(
            col("src._natural_key_hash").alias("src_hash"),
            col("tgt._natural_key_hash").alias("tgt_hash"),
            *[col(f"src.{c}").alias(c) for c in source_df.columns]
        )
    )

    changes_df = (
        comparison_df.withColumn(
            "_change_type",
            when(col("tgt_hash").isNull(), "INSERT")
            .when(col("src_hash").isNull(), "DELETE")
            .otherwise("NO_CHANGE") 
        )
        .persist()
    )

    # Calculate counts
    change_counts = changes_df.groupBy("_change_type").count().collect()
    change_count_map = {row["_change_type"]: row["count"] for row in change_counts}

    insert_count = change_count_map.get("INSERT", 0)
    delete_count = change_count_map.get("DELETE", 0)
    no_change_count = change_count_map.get("NO_CHANGE", 0)

    print(f"\n3. Change Summary:")
    print(f"   INSERT (New Data/Version): {insert_count} records")
    print(f"   DELETE (Deactivate Old):   {delete_count} records")
    print(f"   NO_CHANGE:                 {no_change_count} records")

    total_changes = insert_count + delete_count

    if total_changes == 0:
        print("\n✓ No changes detected - incremental load complete")
    else:
        print(f"\n⚠ Found {total_changes} total changes to process")

        # Process DELETES (Deactivate records that no longer exist exactly as is in source)
        if delete_count > 0:
            (
                changes_df
                .filter(col("_change_type") == "DELETE")
                .select(
                    col("tgt_hash").alias("_natural_key_hash"),
                    current_timestamp().alias("_updated_timestamp")
                )
                .createOrReplaceTempView("deletes_view")
            )
            spark.sql(f"""
                MERGE INTO {target_table} AS tgt
                USING deletes_view AS src
                ON tgt._natural_key_hash = src._natural_key_hash
                   AND tgt._is_record_active = true
                WHEN MATCHED THEN UPDATE SET
                    tgt._is_record_active = false,
                    tgt._updated_timestamp = src._updated_timestamp
            """)
            print(f"   Deactivated {delete_count} old records")

        # Process INSERTS (Add new records)
        if insert_count > 0:
            records_to_insert = (
                changes_df
                .filter(col("_change_type") == "INSERT")
                .select(*[c for c in source_df.columns])
            )
            (
                records_to_insert
                .write
                .format("delta")
                .mode("append")
                .saveAsTable(target_table)
            )
            print(f"   Inserted {insert_count} new records")

    changes_df.unpersist()

# COMMAND ----------

# ============================================================
# STAGE 10: Post-processing and verification
# ============================================================

print("\n" + "=" * 80)
print("POST-PROCESSING AND VERIFICATION")
print("=" * 80)

duplicate_check = spark.sql(f"""
    SELECT _natural_key_hash, COUNT(*) as active_count
    FROM {target_table}
    WHERE _is_record_active = true
    GROUP BY _natural_key_hash
    HAVING COUNT(*) > 1
""")

if duplicate_check.count() > 0:
    print("\n⚠ WARNING: Duplicate active records found:")
    duplicate_check.show(20, truncate=False)

    print("\nSample duplicate serial numbers:")
    spark.sql(f"""
        SELECT t.serial_number, t._natural_key_hash, t._record_data_hash
        FROM {target_table} t
        INNER JOIN (
            SELECT _natural_key_hash
            FROM {target_table}
            WHERE _is_record_active = true
            GROUP BY _natural_key_hash
            HAVING COUNT(*) > 1
        ) d ON t._natural_key_hash = d._natural_key_hash
        WHERE t._is_record_active = true
        ORDER BY t.serial_number
        LIMIT 20
    """).show(truncate=False)
else:
    print("\n✓ No duplicate active records found")

print("\nFinal Table Metrics:")
spark.sql(f"""
    SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT serial_number) as unique_serials,
        COUNT(DISTINCT _natural_key_hash) as unique_natural_keys,
        SUM(CASE WHEN _is_record_active = true THEN 1 ELSE 0 END) as active_records,
        SUM(CASE WHEN _is_record_active = false THEN 1 ELSE 0 END) as inactive_records
    FROM {target_table}
""").show()

spark.catalog.clearCache()
from datetime import datetime
end_time = datetime.now(pytz.timezone('US/Eastern'))
total_seconds = (end_time - job_start_ts).total_seconds()
minutes = int(total_seconds // 60)
seconds = int(total_seconds % 60)

print(f"\nProcess completed at {end_time}")
print(f"\nTotal execution time: {minutes} minutes and {seconds} seconds")