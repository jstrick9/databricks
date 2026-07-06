# Databricks notebook source
# MAGIC %md
# MAGIC # Silver – build_case_timeline
# MAGIC ### TRMNG Bronze → Silver case_milestones – USPTO Trademark Wait Times
# MAGIC
# MAGIC Environment-aware – NO hardcoded catalog names
# MAGIC - Catalog / schema from: config/{dbx_env}/tmngpdb-conf.yaml
# MAGIC - Event codes from: config/wait_time_source_mapping.yml
# MAGIC - Widget overrides allowed for event codes only
# MAGIC
# MAGIC Source: trm_tmngpdb.bronze (prod) / trm_tmngpdb_dev.bronze (dev)
# MAGIC Target: {trgt_catalog}.silver.case_milestones

# COMMAND ----------

dbutils.widgets.text("dbx_env", "dev")
# Optional event code overrides – comma separated – leave blank to use wait_time_source_mapping.yml
dbutils.widgets.text("oa_event_codes", "")
dbutils.widgets.text("reg_event_codes", "")
dbutils.widgets.text("abn_event_codes", "")
dbutils.widgets.text("reg_status_codes", "")
dbutils.widgets.text("abn_status_codes", "")
dbutils.widgets.text("config_file", "")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file_name = "tmngpdb-conf.yaml"
default_config = f"../../../config/{dbx_env}/{config_file_name}"
config_file = dbutils.widgets.get("config_file") or default_config

print(f"dbx_env={dbx_env}, config_file={config_file}")

# COMMAND ----------

# MAGIC %run ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------


import yaml, os
common_configs = read_yaml(config_file)

tmngpdb_catalog = common_configs['schema']['trgt_catalog']
silver_schema_name = common_configs['schema'].get('silver_schema', 'silver')
gold_schema_name = common_configs['schema'].get('gold_schema', 'gold')

bronze_catalog = common_configs.get('unity_catalog', {}).get('bronze_catalog', tmngpdb_catalog)
bronze_schema_name = common_configs.get('unity_catalog', {}).get('bronze_schema', 'bronze')

print(f"catalog={tmngpdb_catalog}, bronze={bronze_catalog}.{bronze_schema_name}, silver={silver_catalog}.{silver_schema_name}")

spark.conf.set('conf.catalog', tmngpdb_catalog)
spark.conf.set('conf.database', bronze_schema_name)
spark.conf.set('conf.dbx_env', dbx_env)

bronze_fqn = f"`{bronze_catalog}`.`{bronze_schema_name}`"
silver_table = f"`{tmngpdb_catalog}`.`{silver_schema_name}`.case_milestones"

# ---------- Load event codes from wait_time_source_mapping.yml ----------
# Config is environment-agnostic – same codes in dev/prod
# Path: ../../config/wait_time_source_mapping.yml relative to this notebook
wait_time_cfg_paths = [
    "../../../config/wait_time_source_mapping.yml",
    "../../config/wait_time_source_mapping.yml",
    "../config/wait_time_source_mapping.yml"
]
wait_cfg = {}
for p in wait_time_cfg_paths:
    try:
        if os.path.exists(p):
            with open(p) as f:
                wait_cfg = yaml.safe_load(f)
                print(f"Loaded event codes from {p}")
                break
    except Exception:
        pass

if not wait_cfg:
    print("WARNING: wait_time_source_mapping.yml not found – using widget defaults / empty lists")

def get_codes(yaml_path, widget_value):
    # widget overrides YAML
    if widget_value and widget_value.strip():
        return [x.strip() for x in widget_value.split(",") if x.strip()]
    cur = wait_cfg.get("event_codes", {})
    for part in yaml_path.split("."):
        cur = cur.get(part, {}) if isinstance(cur, dict) else []
    if isinstance(cur, dict) and "active" in cur:
        return cur["active"]
    return cur if isinstance(cur, list) else []

oa_events = get_codes("trademark_last_event_type_cd.first_office_action", dbutils.widgets.get("oa_event_codes"))
reg_events = get_codes("trademark_last_event_type_cd.registration", dbutils.widgets.get("reg_event_codes"))
abn_events = get_codes("trademark_last_event_type_cd.abandonment", dbutils.widgets.get("abn_event_codes"))
reg_status = get_codes("legacy_status_cd.registered", dbutils.widgets.get("reg_status_codes"))
abn_status = get_codes("legacy_status_cd.abandoned", dbutils.widgets.get("abn_status_codes"))

print(f"Event codes active:")
print(f"  OA events ({len(oa_events)}): {oa_events[:5]}{'...' if len(oa_events)>5 else ''}")
print(f"  REG events: {reg_events}")
print(f"  ABN events: {abn_events}")
print(f"  REG status ({len(reg_status)} codes): {reg_status[:5]}{'...' if len(reg_status)>5 else ''}")
print(f"  ABN status ({len(abn_status)} codes): {abn_status}")
if not oa_events:
    print("WARNING: No OA event codes – first_oa_date will be NULL – check wait_time_source_mapping.yml")

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ---------- Helper: auto-detect PK ----------
def find_pk(table_fqn):
    try:
        cols = [r.col_name for r in spark.sql(f"DESCRIBE TABLE {table_fqn}").collect() if r.col_name]
        cols_l = [c.lower() for c in cols]
        for cand in ["serial_number","serial_no","trademark_id","application_number","case_serial_number","application_id"]:
            if cand in cols_l:
                return cols[cols_l.index(cand)]
        return cols[0]
    except Exception:
        return "serial_number"

def qn(catalog, schema, table):
    return f"`{catalog}`.`{schema}`.`{table}`"

# Resolve bronze table names from wait_time_source_mapping.yml if present
def tbl(logical_name, default):
    # wait_time_source_mapping.yml tables section contains logical names like "trademark_h"
    # strip any catalog.schema prefix if present in the config
    t = wait_cfg.get("tables", {}).get(logical_name, default)
    # if t contains dots, take last part
    return t.split(".")[-1]

tm_h_table = tbl("trademark_history", "trademark_h")
tm_h_fqn = f"{bronze_fqn}.{tm_h_table}"
tm_pk = find_pk(tm_h_fqn)
print(f"trademark_h PK: {tm_pk}  from {tm_h_fqn}")

def event_in(col, codes):
    return F.col(col).isin(codes) if codes else F.lit(False)

# ---------- 1. Core trademark timeline – trademark_h ----------
th = spark.table(tm_h_fqn)

oa_cond = event_in("last_event_type_cd", oa_events)
reg_cond = event_in("last_event_type_cd", reg_events) | event_in("legacy_status_cd", reg_status)
abn_cond = event_in("last_event_type_cd", abn_events) | event_in("legacy_status_cd", abn_status)

from pyspark.sql import Window
w = Window.partitionBy(tm_pk).orderBy(F.col("last_action_dt").desc_nulls_last())

core = th.withColumn("rn", F.row_number().over(w))

oa_dates = core.filter(oa_cond).groupBy(tm_pk).agg(F.min(F.to_date("last_action_dt")).alias("first_oa_date"))
reg_dates = core.filter(reg_cond).groupBy(tm_pk).agg(F.min(F.to_date("status_dt")).alias("registration_date"))
abn_dates = core.filter(abn_cond).groupBy(tm_pk).agg(F.min(F.to_date("status_dt")).alias("abandonment_date"))

base = (core.filter(F.col("rn") == 1)
  .select(
    F.col(tm_pk).alias("serial_number"),
    F.to_date("filing_dt").alias("filing_date"),
    F.to_date("effective_filing_dt").alias("effective_filing_date")
  ).dropDuplicates(["serial_number"])
)

# Filing basis – tram_am.am_flg_action
# filing_basis_values from wait_time_source_mapping.yml
teas_codes = get_codes("filing_basis_values.teas", "")
madrid_codes = get_codes("filing_basis_values.madrid_66a", "")
try:
    tram_am_table = tbl("tram_am", "tram_am")
    tram_am_fqn = f"{bronze_fqn}.`{tram_am_table}`"
    tram_am_df = spark.table(tram_am_fqn)
    am_pk = find_pk(tram_am_fqn)
    # find filing_basis column – from config or auto-detect
    am_cols = [r.col_name for r in spark.sql(f"DESCRIBE TABLE {tram_am_fqn}").collect()]
    basis_col = None
    cfg_basis_col = (wait_cfg.get("event_codes", {}).get("filing_basis_values", {}).get("filing_basis_col", "am_flg_action")
                     if isinstance(wait_cfg.get("event_codes", {}), dict) else "am_flg_action")
    for cand in [cfg_basis_col, "am_flg_action", "filing_basis_cd", "filing_basis", "application_basis_cd"]:
        if cand in am_cols:
            basis_col = cand
            break
        if cand.lower() in [c.lower() for c in am_cols]:
            basis_col = [c for c in am_cols if c.lower() == cand.lower()][0]
            break
    if basis_col:
        basis_df = tram_am_df.select(
          F.col(am_pk).alias("serial_number"),
          F.col(basis_col).cast("string").alias("filing_basis_raw")
        ).dropDuplicates(["serial_number"])
        if teas_codes or madrid_codes:
            basis_df = basis_df.withColumn("filing_basis",
              F.when(F.col("filing_basis_raw").isin(teas_codes), F.lit("TEAS"))
               .when(F.col("filing_basis_raw").isin(madrid_basis_codes := madrid_codes or []), F.lit("MADRID"))
               .otherwise(F.col("filing_basis_raw"))
            )
        else:
            basis_df = basis_df.withColumnRenamed("filing_basis_raw", "filing_basis")
        base = base.join(basis_df.select("serial_number", "filing_basis"), "serial_number", "left")
        print(f"Filing basis joined from tram_am.{basis_col} – TEAS codes={teas_basis}, MADRID codes={madrid_basis}")
    else:
        base = base.withColumn("filing_basis", F.lit(None).cast("string"))
        print("WARNING: filing_basis column not found in tram_am")
except Exception as e:
    print(f"tram_am join skipped: {e}")
    base = base.withColumn("filing_basis", F.lit(None).cast("string"))

milestones = (base
  .join(oa_dates.withColumnRenamed(tm_pk, "serial_number"), "serial_number", "left")
  .join(reg_dates.withColumnRenamed(tm_pk, "serial_number"), "serial_number", "left")
  .join(abn_dates.withColumnRenamed(tm_pk, "serial_number"), "serial_number", "left")
  .withColumn("disposal_date", F.coalesce("registration_date", "abandonment_date"))
)

# ---------- Helper: left-join milestone tables ----------
def left_join_milestone(df_left, bronze_table_logical, date_map):
    """
    date_map: {output_col: source_col, ...}
    bronze_table_logical: key in wait_time_source_mapping.yml tables{} – e.g. "tm_itu_h"
    Resolves to: {bronze_catalog}.{bronze_schema}.<actual_table>
    """
    # resolve physical table name
    table_name = tbl(bronze_table_logical, bronze_table_logical)
    full_name = f"{bronze_fqn}.`{table_name}`"
    try:
        t = spark.table(full_name)
        pk = find_pk(full_name)
        agg_exprs = [F.min(F.to_date(F.col(src))).alias(out) for out, src in date_map.items()]
        agg_df = t.groupBy(pk).agg(*agg_exprs).withColumnRenamed(pk, "serial_number")
        return df_left.join(agg_df, "serial_number", "left"), True
    except Exception as e:
        print(f"  {full_name} join skipped: {e}")
        out_df = df_left
        for out_col in date_map.keys():
            if out_col not in out_df.columns:
                out_df = out_df.withColumn(out_col, F.lit(None).cast("date"))
        return out_df, False

# ITU SOU
milestones, _ = left_join_milestone(milestones, "tm_itu_h", {
  "sou_filing_date": "LATEST_ITU_FILNG_RECEIVED_DT",
  "sou_processed_date": "SOU_RECEIVED_DT"
})
# ITU Extension
milestones, _ = left_join_milestone(milestones, "tm_itu_extension_h", {
  "extension_request_date": "CREATE_TS",
  "extension_processed_date": "EXPIRATION_DT"
})
# Divisional
milestones, _ = left_join_milestone(milestones, "tm_divisional_child_h", {
  "divisional_request_date": "mailroom_received_dt",
  "divisional_processed_date": "tm_divisional_status_dt"
})
# Post-Reg Affidavit / Amendment
milestones, _ = left_join_milestone(milestones, "tm_post_registration", {
  "affidavit_filing_date": "latest_correspondence_rcvd_dt",
  "affidavit_processed_date": "post_reg_audit_begin_dt",
  "amendment_filing_date": "latest_correspondence_rcvd_dt",
  "amendment_processed_date": "post_reg_audit_begin_dt"
})
# Renewal
milestones, _ = left_join_milestone(milestones, "tm_renewal_h", {
  "renewal_filing_date": "renewal_filed_dt",
  "renewal_processed_date": "renewal_begin_effective_dt"
})

# ESU / LOP – not mapped yet in event_codes – create NULL columns
for c in ["esu_response_date","esu_processed_date","lop_filing_date","lop_processed_date"]:
    if c not in milestones.columns:
        milestones = milestones.withColumn(c, F.lit(None).cast("date"))

# ---------- SCD2 metadata ----------
milestones = (milestones
  .withColumn("_updated_ts", F.current_timestamp())
  .withColumn("_valid_from", F.current_timestamp())
  .withColumn("_valid_to", F.lit(None).cast("timestamp"))
  .withColumn("_is_current", F.lit(True))
)

expected_cols = [
"serial_number","filing_date","filing_basis",
"first_oa_date","registration_date","abandonment_date","disposal_date",
"sou_filing_date","sou_processed_date",
"renewal_filing_date","renewal_processed_date",
"extension_request_date","extension_processed_date",
"divisional_request_date","divisional_processed_date",
"lop_filing_date","lop_processed_date",
"affidavit_filing_date","affidavit_processed_date",
"amendment_filing_date","amendment_processed_date",
"esu_response_date","esu_processed_date",
"_updated_ts","_valid_from","_valid_to","_is_current"
]
for c in expected_cols:
    if c not in milestones.columns:
        milestones = milestones.withColumn(c, F.lit(None).cast("date") if "date" in c or c.startswith("filing_") else F.lit(None))

milestones = milestones.select(*[c for c in expected_cols if c in milestones.columns])

target = DeltaTable.forName(spark, f"{tmngpdb_catalog}.{silver_schema_name}.case_milestones")

target.alias("t").merge(
    milestones.alias("s"),
    "t.serial_number = s.serial_number AND t._is_current = true"
  ).whenMatchedUpdate(
    condition = "COALESCE(s.first_oa_date, DATE'1900-01-01') <> COALESCE(t.first_oa_date, DATE'1900-01-01') OR COALESCE(s.disposal_date, DATE'1900-01-01') <> COALESCE(t.disposal_date, DATE'1900-01-01')",
    set = {"_is_current": "false", "_valid_to": "current_timestamp()"}
  ).whenNotMatchedInsertAll().execute()

# Insert new versions
changed_keys = spark.table(f"{tmngpdb_catalog}.{silver_schema_name}.case_milestones").filter("_is_current = false AND _valid_to >= current_timestamp() - INTERVAL 10 minutes").select("serial_number").distinct()
new_versions = milestones.join(changed_keys, "serial_number", "inner")
if new_versions.count() > 0:
    new_versions.write.mode("append").saveAsTable(f"{tmngpdb_catalog}.{silver_schema_name}.case_milestones")

print(f"Silver case_milestones upsert complete → {silver_table}")
display(spark.sql(f"SELECT COUNT(*) as current_cases, COUNT(first_oa_date) as with_first_oa, COUNT(disposal_date) as with_disposal FROM {tmngpdb_catalog}.{silver_schema_name}.case_milestones WHERE _is_current = true"))
display(spark.sql(f"SELECT filing_basis, COUNT(*) as n FROM {tmngpdb_catalog}.{silver_schema_name}.case_milestones WHERE _is_current = true GROUP BY filing_basis ORDER BY n DESC"))