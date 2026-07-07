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

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file_name = "tmngpdb-conf.yaml"
config_file = "../../config/" + dbx_env + "/" + config_file_name

print(f"dbx_env={dbx_env}, config_file={config_file}")

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

import yaml, os
common_configs = read_yaml(config_file)

tmngpdb_catalog = common_configs['schema']['trgt_catalog']
silver_schema_name = common_configs['schema'].get('silver_schema', 'silver')
gold_schema_name = common_configs['schema'].get('gold_schema', 'gold')
reporting_catalog = common_configs['schema']['reporting_catalog']

bronze_catalog = common_configs.get('unity_catalog', {}).get('bronze_catalog', tmngpdb_catalog)
bronze_schema_name = common_configs.get('unity_catalog', {}).get('bronze_schema', 'bronze')

print(f"catalog={tmngpdb_catalog}, bronze={bronze_catalog}.{bronze_schema_name}, silver={tmngpdb_catalog}.{silver_schema_name}")

spark.conf.set('conf.catalog', tmngpdb_catalog)
spark.conf.set('conf.database', bronze_schema_name)
spark.conf.set('conf.dbx_env', dbx_env)

bronze_fqn = f"`{bronze_catalog}`.`{bronze_schema_name}`"
silver_table = f"`{tmngpdb_catalog}`.`{silver_schema_name}`.case_milestones"

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ---------- Load event codes and table mappings from wait_time_source_mapping.yml ----------
wait_time_cfg_path = "../../config/wait_time_source_mapping.yml"
wait_cfg = {}
try:
    if os.path.exists(wait_time_cfg_path):
        with open(wait_time_cfg_path) as f:
            wait_cfg = yaml.safe_load(f)
            print(f"Loaded event codes and table mappings from {wait_time_cfg_path}")
except Exception as e:
    print(f"Note: Could not load {wait_time_cfg_path}: {e}")

if not wait_cfg:
    print("WARNING: wait_time_source_mapping.yml not loaded – using defaults")

def get_codes(yaml_path):
    cur = wait_cfg.get("event_codes", {})
    for part in yaml_path.split("."):
        cur = cur.get(part, {}) if isinstance(cur, dict) else []
    if isinstance(cur, dict) and "active" in cur:
        return cur["active"]
    return cur if isinstance(cur, list) else []

oa_events = get_codes("trademark_last_event_type_cd.first_office_action")
reg_status = get_codes("legacy_status_cd.registered")
abn_status = get_codes("legacy_status_cd.abandoned")

def tbl(logical_name, default):
    t = wait_cfg.get("tables", {}).get(logical_name, default)
    return t.split(".")[-1]

# ---------- 1. Core trademark timeline – silver.milestone & silver.bibliography ----------
ml_table = f"{reporting_catalog}.silver.milestone"
bib_table = f"{reporting_catalog}.silver.bibliography"
toa_table = f"{bronze_catalog}.{bronze_schema_name}.TM_OFFICE_ACTIONS"

print(f"Reading core timeline from {ml_table}")
ml_df = spark.table(ml_table)

print(f"Reading bibliography from {bib_table}")
bib_df = spark.table(bib_table)

print(f"Reading first office actions 100% from {toa_table}")
toa_df = spark.table(toa_table)

# Prepare Office Action Dates relying 100% on TM_OFFICE_ACTIONS
toa_clean = (toa_df
    .withColumn("serial_number", F.when(F.col("FK_TRADEMARK_GID").cast("string").contains(":"), F.element_at(F.split(F.col("FK_TRADEMARK_GID").cast("string"), ":"), -1)).otherwise(F.col("FK_TRADEMARK_GID")).cast("string"))
    .filter(F.col("FIRST_EA_ACTION_COUNTED_DT").isNotNull())
    .groupBy("serial_number")
    .agg(F.min(F.to_date("FIRST_EA_ACTION_COUNTED_DT")).alias("first_oa_date_toa"))
)

# Optional fallback from trademark_h using wait_time_source_mapping.yml event codes
tm_h_table = tbl("trademark_history", "trademark_h")
tm_h_cands = [tm_h_table, "TRADEMARK", "trademark"]
tm_h_df = None
for cand in tm_h_cands:
    try:
        tm_h_df = spark.table(f"{bronze_fqn}.`{cand}`")
        print(f"Loaded {cand} for event code validation from wait_time_source_mapping.yml")
        break
    except Exception:
        pass

if tm_h_df is not None:
    cols_l = [c.lower() for c in tm_h_df.columns]
    pk_col = "TRADEMARK_GID" if "trademark_gid" in cols_l else ("serial_number" if "serial_number" in cols_l else tm_h_df.columns[0])
    tm_h_clean = tm_h_df.withColumn("serial_number", F.when(F.col(pk_col).cast("string").contains(":"), F.element_at(F.split(F.col(pk_col).cast("string"), ":"), -1)).otherwise(F.col(pk_col)).cast("string"))
    evt_col = "last_event_type_cd" if "last_event_type_cd" in cols_l else ("event_type_cd" if "event_type_cd" in cols_l else None)
    dt_col = "last_action_dt" if "last_action_dt" in cols_l else ("status_dt" if "status_dt" in cols_l else ("milestone_dt" if "milestone_dt" in cols_l else None))
    if evt_col and dt_col and oa_events:
        oa_from_yml = tm_h_clean.filter(F.col(evt_col).isin(oa_events) & F.col(dt_col).isNotNull()).groupBy("serial_number").agg(F.min(F.to_date(dt_col)).alias("first_oa_date_yml"))
    else:
        oa_from_yml = None
else:
    oa_from_yml = None

# Build base from silver.milestone and silver.bibliography
base = (ml_df.alias("ml")
    .join(bib_df.alias("bb"), F.col("ml.ser_num") == F.col("bb.SER_NUM"), "left")
    .select(
        F.col("ml.ser_num").cast("string").alias("serial_number"),
        F.to_date("ml.filing_dt").alias("filing_date"),
        F.to_date("ml.filing_dt").alias("effective_filing_date"),
        F.coalesce(F.to_date("ml.first_action_dt_ph"), F.to_date("ml.am_1_actn_ct_dt")).alias("first_oa_date_ml"),
        F.when(F.col("bb.AM_FLG_66A_FIL") == 1, F.lit("MADRID"))
         .when(F.col("bb.FILING_BASIS_CUR").rlike("(?i)66a|madrid|ir"), F.lit("MADRID"))
         .when(F.col("bb.FILING_BASIS_CUR").rlike("(?i)1a|1b|44d|44e|teas"), F.lit("TEAS"))
         .otherwise(F.coalesce(F.col("bb.FILING_BASIS_CUR"), F.lit("TEAS"))).alias("filing_basis"),
        F.to_date("ml.registration_dt").alias("registration_date"),
        F.to_date("ml.abandonment_dt").alias("abandonment_date"),
        F.coalesce(F.to_date("ml.registration_dt"), F.to_date("ml.abandonment_dt"), F.to_date("ml.disposal_dt")).alias("disposal_date")
    ).dropDuplicates(["serial_number"])
)

milestones = base.join(toa_clean, "serial_number", "left")
if oa_from_yml is not None:
    milestones = milestones.join(oa_from_yml, "serial_number", "left").withColumn("first_oa_date", F.coalesce("first_oa_date_toa", "first_oa_date_ml", "first_oa_date_yml")).drop("first_oa_date_toa", "first_oa_date_ml", "first_oa_date_yml")
else:
    milestones = milestones.withColumn("first_oa_date", F.coalesce("first_oa_date_toa", "first_oa_date_ml")).drop("first_oa_date_toa", "first_oa_date_ml")

# ---------- Helper: left-join milestone tables with smart fallbacks ----------
def left_join_milestone(df_left, bronze_table_logical, date_map, candidate_names=None):
    table_name = tbl(bronze_table_logical, bronze_table_logical)
    cands = [table_name] + (candidate_names or [])
    t = None
    matched_name = None
    for cand in cands:
        for prefix in [f"`{tmngpdb_catalog}`.`silver`", bronze_fqn, f"`{tmngpdb_catalog}`.`bronze`"]:
            fn = f"{prefix}.`{cand}`"
            try:
                t = spark.table(fn)
                matched_name = fn
                break
            except Exception:
                pass
        if t is not None:
            break
            
    if t is None:
        print(f"  WARNING: Could not find table for {bronze_table_logical} among {cands}")
        out_df = df_left
        for out_col in date_map.keys():
            if out_col not in out_df.columns:
                out_df = out_df.withColumn(out_col, F.lit(None).cast("date"))
        return out_df, False

    print(f"  Joining {bronze_table_logical} from {matched_name}")
    try:
        cols_l = [c.lower() for c in t.columns]
        fk_col = None
        for cand_col in ["fk_trademark_gid", "cfk_trademark_gid", "fk_child_trademark_gid", "trademark_gid", "serial_number", "ser_num"]:
            if cand_col in cols_l:
                fk_col = t.columns[cols_l.index(cand_col)]
                break
        if not fk_col:
            fk_col = t.columns[0]
            
        t_clean = t.withColumn("join_serial", F.when(F.col(fk_col).cast("string").contains(":"), F.element_at(F.split(F.col(fk_col).cast("string"), ":"), -1)).otherwise(F.col(fk_col)).cast("string"))
        
        # Build aggregation expressions matching target column names if present
        agg_exprs = []
        for out_col, src_col in date_map.items():
            if src_col in t.columns:
                agg_exprs.append(F.min(F.to_date(F.col(src_col))).alias(out_col))
            elif src_col.lower() in cols_l:
                actual_col = t.columns[cols_l.index(src_col.lower())]
                agg_exprs.append(F.min(F.to_date(F.col(actual_col))).alias(out_col))
                
        if not agg_exprs:
            print(f"  WARNING: none of {date_map.values()} found in {matched_name}")
            out_df = df_left
            for out_col in date_map.keys():
                if out_col not in out_df.columns:
                    out_df = out_df.withColumn(out_col, F.lit(None).cast("date"))
            return out_df, False
            
        agg_df = t_clean.groupBy("join_serial").agg(*agg_exprs).withColumnRenamed("join_serial", "serial_number")
        return df_left.join(agg_df, "serial_number", "left"), True
    except Exception as e:
        print(f"  {matched_name} join skipped: {e}")
        out_df = df_left
        for out_col in date_map.keys():
            if out_col not in out_df.columns:
                out_df = out_df.withColumn(out_col, F.lit(None).cast("date"))
        return out_df, False

# ---------- 2. Divisionals (from silver.divisionals or bronze.TM_DIVISIONAL_CHILD) ----------
for cand in [f"`{tmngpdb_catalog}`.`silver`.`divisionals`", f"{bronze_fqn}.`TM_DIVISIONAL_CHILD`", f"{bronze_fqn}.`tm_divisional_child`"]:
    try:
        t_div = spark.table(cand)
        cols_l = [c.lower() for c in t_div.columns]
        req_col = "dv_dt_rqst" if "dv_dt_rqst" in cols_l else ("mailroom_received_dt" if "mailroom_received_dt" in cols_l else ("unit_received_dt" if "unit_received_dt" in cols_l else None))
        prc_col = "dv_dt_complete" if "dv_dt_complete" in cols_l else ("tm_divisional_status_dt" if "tm_divisional_status_dt" in cols_l else None)
        if req_col and prc_col:
            pk_col = "ser_num" if "ser_num" in cols_l else ("serial_number" if "serial_number" in cols_l else ("fk_child_trademark_gid" if "fk_child_trademark_gid" in cols_l else t_div.columns[0]))
            t_clean = t_div.withColumn("join_serial", F.when(F.col(pk_col).cast("string").contains(":"), F.element_at(F.split(F.col(pk_col).cast("string"), ":"), -1)).otherwise(F.col(pk_col)).cast("string"))
            div_agg = t_clean.groupBy("join_serial").agg(F.min(F.to_date(req_col)).alias("divisional_request_date"), F.min(F.to_date(prc_col)).alias("divisional_processed_date")).withColumnRenamed("join_serial", "serial_number")
            milestones = milestones.join(div_agg, "serial_number", "left")
            print(f"Joined divisionals from {cand}")
            break
    except Exception:
        pass

if "divisional_request_date" not in milestones.columns:
    milestones = milestones.withColumn("divisional_request_date", F.lit(None).cast("date")).withColumn("divisional_processed_date", F.lit(None).cast("date"))

# ---------- 3. Post-Registration (from silver.post_reg_detail) ----------
for cand in [f"`{tmngpdb_catalog}`.`silver`.`post_reg_detail`", f"{bronze_fqn}.`TM_POST_REGISTRATION`"]:
    try:
        t_pr = spark.table(cand)
        cols_l = [c.lower() for c in t_pr.columns]
        if "postreg_category" in cols_l and "start_action_date" in cols_l and "end_action_date" in cols_l:
            pk_col = "serial_number" if "serial_number" in cols_l else ("ser_num" if "ser_num" in cols_l else t_pr.columns[0])
            t_clean = t_pr.withColumn("join_serial", F.when(F.col(pk_col).cast("string").contains(":"), F.element_at(F.split(F.col(pk_col).cast("string"), ":"), -1)).otherwise(F.col(pk_col)).cast("string"))
            
            aff_agg = t_clean.filter(F.col("postreg_category").rlike("(?i)6 YEAR|AFFIDAVIT")).groupBy("join_serial").agg(F.min(F.to_date("start_action_date")).alias("affidavit_filing_date"), F.min(F.to_date("end_action_date")).alias("affidavit_processed_date")).withColumnRenamed("join_serial", "serial_number")
            ren_agg = t_clean.filter(F.col("postreg_category").rlike("(?i)10 YEAR|RENEWAL")).groupBy("join_serial").agg(F.min(F.to_date("start_action_date")).alias("renewal_filing_date"), F.min(F.to_date("end_action_date")).alias("renewal_processed_date")).withColumnRenamed("join_serial", "serial_number")
            amd_agg = t_clean.filter(F.col("postreg_category").rlike("(?i)AMEND|CORRECT|SECTION 7")).groupBy("join_serial").agg(F.min(F.to_date("start_action_date")).alias("amendment_filing_date"), F.min(F.to_date("end_action_date")).alias("amendment_processed_date")).withColumnRenamed("join_serial", "serial_number")
            
            milestones = milestones.join(aff_agg, "serial_number", "left").join(ren_agg, "serial_number", "left").join(amd_agg, "serial_number", "left")
            print(f"Joined Post-Registration from {cand}")
            break
    except Exception:
        pass

for c in ["affidavit_filing_date","affidavit_processed_date","renewal_filing_date","renewal_processed_date","amendment_filing_date","amendment_processed_date"]:
    if c not in milestones.columns:
        milestones = milestones.withColumn(c, F.lit(None).cast("date"))

# ---------- 4. ITU (SOU / Extension) and ESU / LOP from BUSINESS_EVENT & STND_BUSINESS_EVENT_REASON ----------
try:
    be_df = spark.table(f"{bronze_fqn}.`BUSINESS_EVENT`")
    ber_df = spark.table(f"{bronze_fqn}.`STND_BUSINESS_EVENT_REASON`")
    be_joined = (be_df.alias("be")
        .join(ber_df.alias("ber"), F.col("be.FK_BUSINESS_EVENT_REASON_ID") == F.col("ber.BUSINESS_EVENT_REASON_ID"), "inner")
        .select(
            F.element_at(F.split(F.col("be.CFK_OBJECT_GID").cast("string"), ":"), -1).alias("serial_number"),
            F.to_date("be.EFFECTIVE_TS").alias("evt_dt"),
            F.col("ber.LEGACY_CM_ENT_CD").cast("string").alias("leg_cd")
        ).filter(F.col("serial_number").isNotNull() & F.col("evt_dt").isNotNull())
    )
    
    sou_agg = be_joined.filter(F.col("leg_cd").rlike("(?i)^SOU")).groupBy("serial_number").agg(F.min("evt_dt").alias("sou_filing_date"), F.max("evt_dt").alias("sou_processed_date"))
    ext_agg = be_joined.filter(F.col("leg_cd").rlike("(?i)^EXT_")).groupBy("serial_number").agg(F.min("evt_dt").alias("extension_request_date"), F.max("evt_dt").alias("extension_processed_date"))
    lop_agg = be_joined.filter(F.col("leg_cd").rlike("(?i)LOP|PET_LOP")).groupBy("serial_number").agg(F.min("evt_dt").alias("lop_filing_date"), F.max("evt_dt").alias("lop_processed_date"))
    esu_agg = be_joined.filter(F.col("leg_cd").rlike("(?i)ESU|RCSCS|RCCKS")).groupBy("serial_number").agg(F.min("evt_dt").alias("esu_response_date"), F.max("evt_dt").alias("esu_processed_date"))
    
    milestones = milestones.join(sou_agg, "serial_number", "left").join(ext_agg, "serial_number", "left").join(lop_agg, "serial_number", "left").join(esu_agg, "serial_number", "left")
    print("Joined ITU (SOU/EXT), LOP, and ESU from BUSINESS_EVENT")
except Exception as e:
    print(f"Note: BUSINESS_EVENT join skipped: {e}")

for c in ["sou_filing_date","sou_processed_date","extension_request_date","extension_processed_date","lop_filing_date","lop_processed_date","esu_response_date","esu_processed_date"]:
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
"serial_number","filing_date","effective_filing_date","filing_basis",
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

from datetime import datetime, timezone
batch_run_ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

target = DeltaTable.forName(spark, f"{tmngpdb_catalog}.{silver_schema_name}.case_milestones")

target.alias("t").merge(
    milestones.alias("s"),
    "t.serial_number = s.serial_number AND t._is_current = true"
  ).whenMatchedUpdate(
    condition = "COALESCE(s.first_oa_date, DATE'1900-01-01') <> COALESCE(t.first_oa_date, DATE'1900-01-01') OR COALESCE(s.disposal_date, DATE'1900-01-01') <> COALESCE(t.disposal_date, DATE'1900-01-01')",
    set = {"_is_current": "false", "_valid_to": f"CAST('{batch_run_ts}' AS TIMESTAMP)"}
  ).whenNotMatchedInsertAll().execute()

# Insert new versions (deterministic restart-safe match using batch_run_ts)
changed_keys = spark.table(f"{tmngpdb_catalog}.{silver_schema_name}.case_milestones").filter(f"_is_current = false AND _valid_to = CAST('{batch_run_ts}' AS TIMESTAMP)").select("serial_number").distinct()
new_versions = milestones.join(changed_keys, "serial_number", "inner")
if new_versions.count() > 0:
    new_versions.write.mode("append").saveAsTable(f"{tmngpdb_catalog}.{silver_schema_name}.case_milestones")

print(f"Silver case_milestones upsert complete: {silver_table}")
display(spark.sql(f"SELECT COUNT(*) as current_cases, COUNT(first_oa_date) as with_first_oa, COUNT(disposal_date) as with_disposal FROM {tmngpdb_catalog}.{silver_schema_name}.case_milestones WHERE _is_current = true"))
display(spark.sql(f"SELECT filing_basis, COUNT(*) as n FROM {tmngpdb_catalog}.{silver_schema_name}.case_milestones WHERE _is_current = true GROUP BY filing_basis ORDER BY n DESC"))

# COMMAND ----------

# spark.sql(f"DELETE FROM {tmngpdb_catalog}.{silver_schema_name}.case_milestones WHERE TRUE")

# COMMAND ----------

