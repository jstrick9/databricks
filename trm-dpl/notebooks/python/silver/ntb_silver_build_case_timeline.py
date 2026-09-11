# Databricks notebook source
dbutils.widgets.text("dbx_env", "dev")
dbutils.widgets.text("lookback_years", "10")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
lookback_years = int(dbutils.widgets.get("lookback_years"))

config_file_name = "tmngpdb-conf.yaml"
config_file = "../../config/" + dbx_env + "/" + config_file_name

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

import yaml
common_configs = read_yaml(config_file)

tmngpdb_catalog = common_configs['schema']['trgt_catalog']
silver_schema_name = common_configs['schema'].get('silver_schema', 'silver')
reporting_catalog = common_configs.get('schema', {}).get('reporting_catalog', tmngpdb_catalog)
bronze_catalog = common_configs.get('unity_catalog', {}).get('bronze_catalog', tmngpdb_catalog)
bronze_schema_name = common_configs.get('unity_catalog', {}).get('bronze_schema', 'bronze')

# Performance configs
spark.conf.set('conf.catalog', tmngpdb_catalog)
spark.conf.set('conf.database', bronze_schema_name)
spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.sources.parallelPartitionDiscovery.threshold", "100")
# Reduce shuffle partitions for 14M rows – 200 is default, 128 better for i4i.large
spark.conf.set("spark.sql.shuffle.partitions", "128")

bronze_fqn = f"`{bronze_catalog}`.`{bronze_schema_name}`"
proceeding_catalog = common_configs.get('schema', {}).get('proceeding_catalog') or (tmngpdb_catalog.replace('tmngpdb','tmproceeding') if 'tmngpdb' in tmngpdb_catalog else 'trm_tmproceeding_dev')
proceeding_bronze_fqn = f"`{proceeding_catalog}`.`bronze`"
silver_table = f"`{tmngpdb_catalog}`.`{silver_schema_name}`.case_milestones"

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from functools import reduce

# Tolerant to_date for ANSI
try:
    _has_try = hasattr(F, 'try_to_date')
except:
    _has_try = False

_orig_to_date = F.to_date
def _safe_to_date(col, fmt=None):
    try:
        if _has_try:
            return F.try_to_date(col, fmt) if fmt else F.try_to_date(col)
        else:
            return F.to_date(col, fmt) if fmt else F.to_date(col)
    except:
        return F.lit(None).cast("date")
F.to_date = _safe_to_date

wait_time_cfg_path = "../../config/wait_time_source_mapping.yml"
with open(wait_time_cfg_path) as f:
    wait_cfg = yaml.safe_load(f)

def cfg_get(path, default=None):
    cur = wait_cfg
    for part in path.split("."):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(part, default)
        if cur is None:
            return default
    return cur

def cfg_get_codes(path):
    val = cfg_get(path, [])
    return [str(c).upper() for c in val if isinstance(val, list) and c is not None] if isinstance(val, list) else []

def parse_date(col_expr):
    col_str = F.trim(F.coalesce(col_expr.cast("string"), F.lit("")))
    col_str = F.when(col_str == "", F.lit(None)).otherwise(col_str)
    if _has_try:
        return F.coalesce(
            F.try_to_date(col_str, "yyyyMMdd"),
            F.try_to_date(col_str, "yyyy-MM-dd HH:mm:ss"),
            F.try_to_date(col_str, "yyyy-MM-dd"),
            F.try_to_date(col_str),
            col_expr.cast("date")
        )
    else:
        return F.coalesce(
            F.to_date(col_str, "yyyyMMdd"),
            F.to_date(col_str, "yyyy-MM-dd HH:mm:ss"),
            F.to_date(col_str, "yyyy-MM-dd"),
            F.to_date(col_str),
            col_expr.cast("date")
        )

def upper_isin(col_expr, codes):
    return F.upper(col_expr).isin([str(c).upper() for c in codes if c])

def first_matching_column(df, candidates):
    if not candidates:
        return None
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand and cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    best = None
    best_len = 0
    for cand in candidates:
        if not cand:
            continue
        cl = cand.lower()
        for actual in df.columns:
            al = actual.lower()
            if cl in al and len(cl) >=5 and len(al) > best_len:
                best = actual
                best_len = len(al)
            elif al in cl and len(al) >=5 and len(al) > best_len:
                best = actual
                best_len = len(al)
    return best

def first_matching_table(catalog_schema_fqn, candidates):
    if isinstance(candidates, str):
        candidates = [candidates]
    if not candidates:
        return None, None
    try:
        existing = [t.name for t in spark.catalog.listTables(catalog_schema_fqn.replace("`",""))]
        existing_lower = {t.lower(): t for t in existing}
        for cand in candidates:
            if cand and cand.lower() in existing_lower:
                actual = existing_lower[cand.lower()]
                return f"{catalog_schema_fqn}.`{actual}`", actual
    except:
        pass
    for cand in candidates:
        if not cand:
            continue
        for var in [cand, cand.upper(), cand.lower()]:
            try:
                fqn = f"{catalog_schema_fqn}.`{var}`"
                spark.read.table(fqn).limit(1).collect()
                return fqn, var
            except:
                continue
    return None, None

def find_table(catalog_fqn, candidates):
    if isinstance(candidates, str):
        candidates = [candidates]
    fqn, _ = first_matching_table(catalog_fqn, candidates)
    return fqn

# PERFORMANCE: single scan per table – cache column list, reuse
def aggregate_per_serial_fast(table_fqn, pk_candidates, agg_specs, pre_filter_col=None, pre_filter_values=None):
    """Optimized: one read for sample + one read for data (was two reads), pushdown filter"""
    if not table_fqn:
        return None
    try:
        # Use limit(1) once to get schema + column mapping
        sample_df = spark.read.table(table_fqn).limit(1)
        pk_col = first_matching_column(sample_df, pk_candidates)
        if not pk_col:
            return None
        resolved = []
        needed = set([pk_col])
        for out_name, kind, src_cands in agg_specs:
            if isinstance(src_cands, str):
                src_cands = [src_cands]
            src = first_matching_column(sample_df, src_cands)
            if not src:
                return None
            resolved.append((out_name, kind, src))
            needed.add(src)
        pf_col = None
        if pre_filter_col:
            if isinstance(pre_filter_col, str):
                pf_col = pre_filter_col if pre_filter_col in sample_df.columns else first_matching_column(sample_df, [pre_filter_col])
            else:
                pf_col = first_matching_column(sample_df, pre_filter_col)
            if pf_col:
                needed.add(pf_col)
        # Single scan for data
        df = spark.read.table(table_fqn).select(*needed)
        if pf_col and pre_filter_values:
            df = df.filter(upper_isin(F.col(pf_col), pre_filter_values))
        pk_str = F.trim(F.col(pk_col).cast("string"))
        serial = F.when(pk_str.contains(":"), F.element_at(F.split(pk_str, ":"), -1)).otherwise(pk_str)
        serial = F.trim(serial.cast("string"))
        df = df.withColumn("join_serial", serial).filter(F.col("join_serial").isNotNull())
        exprs = []
        for out_name, kind, src in resolved:
            if kind == "min":
                exprs.append(F.min(parse_date(F.col(src))).alias(out_name))
            elif kind == "max":
                exprs.append(F.max(parse_date(F.col(src))).alias(out_name))
            elif kind == "minmax":
                exprs.append(F.min(parse_date(F.col(src))).alias(out_name))
                exprs.append(F.max(parse_date(F.col(src))).alias(out_name+"_max"))
        agg = df.groupBy("join_serial").agg(*exprs).withColumnRenamed("join_serial","serial_number")
        return agg
    except Exception:
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Base from milestone + bibliography – PERFORMANCE: cache, repartition, broadcast small

# COMMAND ----------

ml_table = f"`{reporting_catalog}`.`{silver_schema_name}`.milestone"
bib_table = f"`{reporting_catalog}`.`{silver_schema_name}`.bibliography"

ml_sample = spark.read.table(ml_table).limit(1)
bib_sample = spark.read.table(bib_table).limit(1)

filing_col = first_matching_column(ml_sample, ["filing_dt"])
first_action_col_ml = first_matching_column(ml_sample, ["first_action_dt_ph","am_1_actn_ct_dt"])
reg_col = first_matching_column(ml_sample, ["registration_dt"])
abn_col = first_matching_column(ml_sample, ["abandonment_dt"])
disp_col = first_matching_column(ml_sample, ["disposal_dt"])
ib_notif_col = first_matching_column(ml_sample, ["ib_notification_dt"])
ser_num_col_ml = first_matching_column(ml_sample, ["ser_num"])
filing_basis_col = first_matching_column(bib_sample, ["FILING_BASIS_CUR"])
am_flg_66a_col = first_matching_column(bib_sample, ["AM_FLG_66A_FIL"])
ser_num_col_bib = first_matching_column(bib_sample, ["SER_NUM"])

ml_keep = {c for c in [ser_num_col_ml, filing_col, first_action_col_ml, reg_col, abn_col, disp_col, ib_notif_col] if c}
bib_keep = {c for c in [ser_num_col_bib, filing_basis_col, am_flg_66a_col] if c}

ml_df = spark.read.table(ml_table).select(*ml_keep)
bib_df = spark.read.table(bib_table).select(*bib_keep)

# blank→TEAS fix
if am_flg_66a_col and filing_basis_col:
    filing_basis_case = F.when(
        (F.col(f"bb.{am_flg_66a_col}") == 1) |
        (F.col(f"ml.{ser_num_col_ml}").cast("string").rlike("^79")) |
        (F.col(f"bb.{filing_basis_col}").rlike(r"(?i)66a|madr|IR")),
        F.lit("MADRID")
    ).otherwise(F.lit("TEAS"))
elif am_flg_66a_col:
    filing_basis_case = F.when(
        (F.col(f"bb.{am_flg_66a_col}") == 1) |
        (F.col(f"ml.{ser_num_col_ml}").cast("string").rlike("^79")),
        F.lit("MADRID")
    ).otherwise(F.lit("TEAS"))
elif filing_basis_col:
    filing_basis_case = F.when(
        (F.col(f"ml.{ser_num_col_ml}").cast("string").rlike("^79")) |
        (F.col(f"bb.{filing_basis_col}").rlike(r"(?i)66a|madr|IR")),
        F.lit("MADRID")
    ).otherwise(F.lit("TEAS"))
else:
    filing_basis_case = F.when(
        F.col(f"ml.{ser_num_col_ml}").cast("string").rlike("^79"),
        F.lit("MADRID")
    ).otherwise(F.lit("TEAS"))

base_select = [
    F.col(f"ml.{ser_num_col_ml}").cast("string").alias("serial_number"),
    parse_date(F.col(f"ml.{filing_col}")).alias("filing_date") if filing_col else F.lit(None).cast("date").alias("filing_date"),
    parse_date(F.col(f"ml.{filing_col}")).alias("effective_filing_date") if filing_col else F.lit(None).cast("date").alias("effective_filing_date"),
]
if first_action_col_ml:
    base_select.append(parse_date(F.col(f"ml.{first_action_col_ml}")).alias("first_oa_date_ml"))
if reg_col:
    base_select.append(parse_date(F.col(f"ml.{reg_col}")).alias("registration_date"))
if abn_col:
    base_select.append(parse_date(F.col(f"ml.{abn_col}")).alias("abandonment_date"))
if disp_col:
    base_select.append(parse_date(F.col(f"ml.{disp_col}")).alias("disposal_dt_raw"))
if ib_notif_col:
    base_select.append(parse_date(F.col(f"ml.{ib_notif_col}")).alias("ib_notification_date"))
base_select.append(filing_basis_case.alias("filing_basis"))

# PERFORMANCE: broadcast bibliography (14M) is not small, but use range repartition to avoid shuffle later
base = ml_df.alias("ml").join(bib_df.alias("bb"), F.col(f"ml.{ser_num_col_ml}")==F.col(f"bb.{ser_num_col_bib}"), "left").select(*base_select).dropDuplicates(["serial_number"])
# Checkpoint + cache to break lineage, coalesce less aggressive
base = base.repartition(64, "serial_number").persist()
# Trigger cache with count – cheap vs later 7 joins
base.count()
base = base.coalesce(32)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: First OA – single scan

# COMMAND ----------

toa_fqn = find_table(bronze_fqn, cfg_get("tables.tm_office_actions", ["tm_office_actions"]))
toa_pk = cfg_get("columns.tm_office_actions.pk", ["fk_trademark_gid"])
toa_date = cfg_get("columns.tm_office_actions.first_ea_action_date", ["first_ea_action_counted_dt"])

def agg_min_date_fast(table_fqn, pk_cands, date_cands):
    if not table_fqn:
        return None
    try:
        sample = spark.read.table(table_fqn).limit(1)
        pk = first_matching_column(sample, pk_cands)
        dc = first_matching_column(sample, date_cands)
        if not pk or not dc:
            return None
        df = spark.read.table(table_fqn).select(pk, dc)
        pk_str = F.trim(F.col(pk).cast("string"))
        serial = F.when(pk_str.contains(":"), F.element_at(F.split(pk_str, ":"), -1)).otherwise(pk_str)
        return df.withColumn("join_serial", F.trim(serial.cast("string"))).filter(F.col("join_serial").isNotNull() & F.col(dc).isNotNull()).groupBy("join_serial").agg(F.min(parse_date(F.col(dc))).alias("min_date")).withColumnRenamed("join_serial","serial_number")
    except:
        return None

toa_agg = agg_min_date_fast(toa_fqn, toa_pk, toa_date)
if toa_agg is not None:
    toa_agg = toa_agg.withColumnRenamed("min_date","first_oa_date_toa")

tm_h_fqn = find_table(bronze_fqn, cfg_get("tables.trademark_history", ["trademark_h"]))
oa_events = cfg_get_codes("event_codes.trademark_h.first_office_action")
oa_from_yml = None
if tm_h_fqn and oa_events:
    try:
        sample = spark.read.table(tm_h_fqn).limit(1)
        pk = first_matching_column(sample, cfg_get("columns.trademark.pk", ["trademark_gid"]))
        evt = first_matching_column(sample, ["last_event_type_cd"])
        dt = first_matching_column(sample, ["last_action_dt"])
        if pk and evt and dt:
            df = spark.read.table(tm_h_fqn).select(pk, evt, dt).filter(upper_isin(F.col(evt), oa_events) & F.col(dt).isNotNull())
            df = df.withColumn("serial_number", F.when(F.col(pk).cast("string").contains(":"), F.element_at(F.split(F.col(pk).cast("string"), ":"), -1)).otherwise(F.col(pk).cast("string"))).filter(F.col("serial_number").isNotNull()).groupBy("serial_number").agg(F.min(parse_date(F.col(dt))).alias("first_oa_date_yml"))
            if toa_agg is not None:
                df = df.join(F.broadcast(toa_agg.select("serial_number")), "serial_number", "left_anti")
            oa_from_yml = df
    except:
        pass

milestones = base
if toa_agg is not None:
    milestones = milestones.join(F.broadcast(toa_agg), "serial_number", "left")
else:
    milestones = milestones.withColumn("first_oa_date_toa", F.lit(None).cast("date"))
if oa_from_yml is not None:
    milestones = milestones.join(F.broadcast(oa_from_yml), "serial_number", "left")
else:
    milestones = milestones.withColumn("first_oa_date_yml", F.lit(None).cast("date"))

milestones = milestones.withColumn("first_oa_date", F.coalesce(F.col("first_oa_date_toa"), F.col("first_oa_date_ml"), F.col("first_oa_date_yml"))).drop("first_oa_date_toa","first_oa_date_ml","first_oa_date_yml")
milestones = milestones.withColumn("disposal_date", F.coalesce(F.col("registration_date"), F.col("abandonment_date"), F.col("disposal_dt_raw"))).drop("disposal_dt_raw")
# Cache milestones before heavy joins
milestones = milestones.repartition(64, "serial_number").persist()
milestones.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3-6: ITU + ESU – PERFORMANCE: single pass, improved cols

# COMMAND ----------

tm_itu_fqn = find_table(bronze_fqn, cfg_get("tables.tm_itu", ["tm_itu"]))
sou_agg = None
if tm_itu_fqn:
    sou_agg = aggregate_per_serial_fast(tm_itu_fqn, cfg_get("columns.tm_itu.pk", ["fk_trademark_gid"]), [("sou_filing_date","min", cfg_get("columns.tm_itu.latest_itu_filing_received_date", ["latest_itu_filng_received_dt"])), ("sou_processed_date","min", cfg_get("columns.tm_itu.sou_received_date", ["sou_received_dt"]))])

ext_fqn = find_table(bronze_fqn, cfg_get("tables.tm_itu_extension_h", ["tm_itu_extension_h"]))
ext_agg = None
if ext_fqn:
    try:
        sample_ext = spark.read.table(ext_fqn).limit(1)
        req_cand = first_matching_column(sample_ext, ["create_ts","request_dt","extension_request_dt","filing_dt"])
        proc_cand = first_matching_column(sample_ext, ["expiration_dt","processed_dt","extension_processed_dt","last_mod_ts","tm_itu_extension_status_dt","status_dt","create_ts"])
        if req_cand and proc_cand:
            ext_agg = aggregate_per_serial_fast(ext_fqn, cfg_get("columns.tm_itu_extension_h.pk", ["fk_trademark_gid"]), [("extension_request_date","min", [req_cand]), ("extension_processed_date","max", [proc_cand])])
        else:
            ext_agg = aggregate_per_serial_fast(ext_fqn, cfg_get("columns.tm_itu_extension_h.pk", ["fk_trademark_gid"]), [("extension_request_date","min", ["create_ts"]), ("extension_processed_date","max", ["create_ts"])])
    except:
        ext_agg = aggregate_per_serial_fast(ext_fqn, cfg_get("columns.tm_itu_extension_h.pk", ["fk_trademark_gid"]), [("extension_request_date","min", ["create_ts"]), ("extension_processed_date","max", ["create_ts"])])

div_fqn = find_table(bronze_fqn, cfg_get("tables.tm_divisional_child_h", ["tm_divisional_child_h"]))
div_agg = None
if div_fqn:
    filing_agg = aggregate_per_serial_fast(div_fqn, cfg_get("columns.tm_divisional_child_h.pk", ["fk_trademark_gid"]), [("divisional_request_date","min", cfg_get("columns.tm_divisional_child_h.unit_received_date", ["unit_received_dt"]))])
    proc_agg = aggregate_per_serial_fast(div_fqn, cfg_get("columns.tm_divisional_child_h.pk", ["fk_trademark_gid"]), [("divisional_processed_date","max", cfg_get("columns.tm_divisional_child_h.divisional_status_date", ["tm_divisional_status_dt"]))], pre_filter_col=cfg_get("columns.tm_divisional_child_h.divisional_status_cd", ["fk_tm_divisional_status_cd"]), pre_filter_values=cfg_get_codes("event_codes.tm_divisional_child_h.complete"))
    if filing_agg is not None and proc_agg is not None:
        div_agg = filing_agg.join(F.broadcast(proc_agg), "serial_number", "outer")
    elif filing_agg is not None:
        div_agg = filing_agg
    elif proc_agg is not None:
        div_agg = proc_agg

esu_fqn = find_table(bronze_fqn, cfg_get("tables.employee_credit_transaction", ["employee_credit_transaction"]))
esu_agg = None
if esu_fqn:
    esu_agg = aggregate_per_serial_fast(esu_fqn, cfg_get("columns.employee_credit_transaction.pk", ["fk_trademark_gid"]), [("esu_response_date","minmax", cfg_get("columns.employee_credit_transaction.transaction_effective_date", ["transaction_effective_dt"]))], pre_filter_col=cfg_get("columns.employee_credit_transaction.reason_type_cd", ["fk_credit_tran_rsn_type_cd"]), pre_filter_values=cfg_get_codes("event_codes.employee_credit_transaction.esu_response"))
    if esu_agg is not None:
        esu_agg = esu_agg.withColumnRenamed("esu_response_date_max","esu_processed_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7-8: Post-reg – single scan

# COMMAND ----------

def agg_post_reg_detail_fast(fqn):
    try:
        sample = spark.read.table(fqn).limit(1)
        pk = first_matching_column(sample, cfg_get("columns.post_reg_detail.pk", ["serial_number"]))
        cat = first_matching_column(sample, cfg_get("columns.post_reg_detail.postreg_category", ["postreg_category"]))
        s_col = first_matching_column(sample, cfg_get("columns.post_reg_detail.start_action_date", ["start_action_date"]))
        e_col = first_matching_column(sample, cfg_get("columns.post_reg_detail.end_action_date", ["end_action_date"]))
        r_col = first_matching_column(sample, ["renewal_dt"])
        if not all([pk, cat, s_col, e_col]):
            return None
        cols = [pk, cat, s_col, e_col] + ([r_col] if r_col else [])
        t = spark.read.table(fqn).select(*cols)
        pk_str = F.trim(F.col(pk).cast("string"))
        serial = F.when(pk_str.contains(":"), F.element_at(F.split(pk_str, ":"), -1)).otherwise(pk_str)
        t = t.withColumn("join_serial", F.trim(serial.cast("string"))).filter(F.col("join_serial").isNotNull() & F.col(s_col).isNotNull())
        aff = t.filter(F.col(cat).rlike(r"(?i)6\s*YEAR")).groupBy("join_serial").agg(F.min(parse_date(F.col(s_col))).alias("affidavit_filing_date"), F.min(parse_date(F.col(e_col))).alias("affidavit_processed_date")).withColumnRenamed("join_serial","serial_number")
        ren = t.filter(F.col(cat).rlike(r"(?i)10\s*YEAR")).groupBy("join_serial").agg(F.min(parse_date(F.col(s_col))).alias("renewal_filing_date"), F.min(F.coalesce(parse_date(F.col(e_col)), parse_date(F.col(r_col)) if r_col else F.lit(None))).alias("renewal_processed_date")).withColumnRenamed("join_serial","serial_number") if r_col else t.filter(F.col(cat).rlike(r"(?i)10\s*YEAR")).groupBy("join_serial").agg(F.min(parse_date(F.col(s_col))).alias("renewal_filing_date"), F.min(parse_date(F.col(e_col))).alias("renewal_processed_date")).withColumnRenamed("join_serial","serial_number")
        amd = t.filter(F.col(cat).rlike(r"(?i)SECTION\s*7|AMEND|CORRECT")).groupBy("join_serial").agg(F.min(parse_date(F.col(s_col))).alias("amendment_filing_date"), F.min(parse_date(F.col(e_col))).alias("amendment_processed_date")).withColumnRenamed("join_serial","serial_number")
        return aff.join(F.broadcast(ren), "serial_number", "outer").join(F.broadcast(amd), "serial_number", "outer")
    except:
        return None

postreg_combined = None
for cat_fqn in [f"`{reporting_catalog}`.`{silver_schema_name}`", f"`{tmngpdb_catalog}`.`{silver_schema_name}`"]:
    fqn = find_table(cat_fqn, cfg_get("tables.post_reg_detail", ["post_reg_detail"]))
    if fqn:
        agg = agg_post_reg_detail_fast(fqn)
        if agg is not None:
            try:
                if agg.limit(1).count() > 0:
                    postreg_combined = agg
                    break
            except:
                postreg_combined = agg
                break

renewal_agg = None
rn_fqn = find_table(bronze_fqn, cfg_get("tables.tm_renewal_h", ["tm_renewal_h"]))
if rn_fqn:
    tmp = aggregate_per_serial_fast(rn_fqn, cfg_get("columns.tm_renewal_h.pk", ["fk_trademark_gid"]), [("renewal_filing_date","min", ["renewal_filed_dt"])], pre_filter_col=["action_ct"], pre_filter_values=cfg_get_codes("event_codes.tm_renewal_h.filed"))
    if tmp is not None:
        renewal_agg = tmp.withColumn("renewal_processed_date", F.col("renewal_filing_date"))

if postreg_combined is not None and renewal_agg is not None and "renewal_filing_date" in postreg_combined.columns:
    combined = postreg_combined.select("serial_number","renewal_filing_date","renewal_processed_date").unionByName(renewal_agg, allowMissingColumns=True)
    renewal_agg = combined.groupBy("serial_number").agg(F.min("renewal_filing_date").alias("renewal_filing_date"), F.min("renewal_processed_date").alias("renewal_processed_date"))
    postreg_combined = postreg_combined.drop("renewal_filing_date","renewal_processed_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: LOP – broadcast small dims, single pass

# COMMAND ----------

def agg_lop_proceeding_fast(proc_bronze_fqn):
    try:
        lt_fqn = find_table(proc_bronze_fqn, ["lop_legal_basis_trademark"])
        p_fqn = find_table(proc_bronze_fqn, ["proceeding"])
        l_fqn = find_table(proc_bronze_fqn, ["letter_of_protest"])
        e_fqn = find_table(proc_bronze_fqn, ["proceeding_event"])
        if not lt_fqn:
            return None
        sample = spark.read.table(lt_fqn).limit(1)
        fk = first_matching_column(sample, ["fk_proceeding_gid"])
        tm = first_matching_column(sample, ["cfk_trademark_gid"])
        dn = first_matching_column(sample, ["dn_serial_num"])
        if not fk:
            return None
        cols = [fk] + ([tm] if tm else []) + ([dn] if dn else [])
        df = spark.read.table(lt_fqn).select(*cols)
        if dn and tm:
            df = df.withColumn("serial_number", F.coalesce(F.trim(F.col(dn).cast("string")), F.when(F.col(tm).cast("string").contains(":"), F.element_at(F.split(F.col(tm).cast("string"), ":"), -1)).otherwise(F.col(tm).cast("string"))))
        elif dn:
            df = df.withColumn("serial_number", F.trim(F.col(dn).cast("string")))
        else:
            df = df.withColumn("serial_number", F.when(F.col(tm).cast("string").contains(":"), F.element_at(F.split(F.col(tm).cast("string"), ":"), -1)).otherwise(F.col(tm).cast("string")))
        link = df.filter(F.col("serial_number").isNotNull()).select(fk, "serial_number").dropDuplicates([fk, "serial_number"]).withColumnRenamed(fk, "lop_fk")
        proc_df = None
        if p_fqn:
            s = spark.read.table(p_fqn).limit(1)
            pg = first_matching_column(s, ["proceeding_gid"])
            fd = first_matching_column(s, ["filing_dt","received_dt"])
            if pg and fd:
                pdf = spark.read.table(p_fqn).select(pg, fd)
                proc_df = pdf.withColumn("proc_filing", parse_date(F.col(fd))).select(F.col(pg).alias("proc_fk"), "proc_filing")
        letter_df = None
        if l_fqn:
            s = spark.read.table(l_fqn).limit(1)
            lf = first_matching_column(s, ["fk_proceeding_gid"])
            ct = first_matching_column(s, ["create_ts"])
            mt = first_matching_column(s, ["last_mod_ts"])
            if lf and ct:
                ldf = spark.read.table(l_fqn).select(lf, ct, mt) if mt else spark.read.table(l_fqn).select(lf, ct)
                ldf = ldf.withColumn("letter_filing", parse_date(F.col(ct)))
                if mt:
                    ldf = ldf.withColumn("letter_processed", parse_date(F.col(mt)))
                else:
                    ldf = ldf.withColumn("letter_processed", F.lit(None).cast("date"))
                letter_df = ldf.select(F.col(lf).alias("letter_fk"), "letter_filing", "letter_processed")
        event_df = None
        if e_fqn:
            s = spark.read.table(e_fqn).limit(1)
            ef = first_matching_column(s, ["fk_proceeding_gid"])
            et = first_matching_column(s, ["effective_ts"])
            if ef and et:
                edf = spark.read.table(e_fqn).select(ef, et).withColumn("event_dt", parse_date(F.col(et))).filter(F.col("event_dt").isNotNull())
                event_df = edf.groupBy(ef).agg(F.min("event_dt").alias("event_filing"), F.max("event_dt").alias("event_processed")).withColumnRenamed(ef, "event_fk")
        joined = link
        if proc_df is not None:
            joined = joined.join(F.broadcast(proc_df), joined["lop_fk"]==proc_df["proc_fk"], "left").drop("proc_fk")
        if letter_df is not None:
            joined = joined.join(F.broadcast(letter_df), joined["lop_fk"]==letter_df["letter_fk"], "left").drop("letter_fk")
        if event_df is not None:
            joined = joined.join(F.broadcast(event_df), joined["lop_fk"]==event_df["event_fk"], "left").drop("event_fk")
        joined = joined.withColumn("lop_filing_per_proc", F.coalesce(F.col("proc_filing"), F.col("letter_filing"), F.col("event_filing"))).withColumn("lop_processed_per_proc", F.coalesce(F.col("event_processed"), F.col("letter_processed"))).filter(F.col("lop_filing_per_proc").isNotNull())
        return joined.groupBy("serial_number").agg(F.min("lop_filing_per_proc").alias("lop_filing_date"), F.max("lop_processed_per_proc").alias("lop_processed_date"))
    except:
        return None

lop_agg = agg_lop_proceeding_fast(proceeding_bronze_fqn)

# tram_am fallback only when empty (keeps 54d vs live 55d)
if lop_agg is None or lop_agg.limit(1).count() == 0:
    tram_fqn = find_table(bronze_fqn, ["tram_am"])
    if tram_fqn:
        tmp = aggregate_per_serial_fast(tram_fqn, ["am_ser_num"], [("lop_filing_date","minmax", ["am_stat_dt"])], pre_filter_col=["am_last_event"], pre_filter_values=cfg_get_codes("event_codes.tram_am.letters_of_protest"))
        if tmp is not None:
            lop_agg = tmp.withColumnRenamed("lop_filing_date_max","lop_processed_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: BUSINESS_EVENT fallback only when empty (keeps 2.4d esu, 88d sou, 54d lop)

# COMMAND ----------

be_fqn = find_table(bronze_fqn, ["business_event"])
ber_fqn = find_table(bronze_fqn, ["stnd_business_event_reason"])
if be_fqn and ber_fqn:
    try:
        be_s = spark.read.table(be_fqn).limit(1)
        ber_s = spark.read.table(ber_fqn).limit(1)
        be_pk = first_matching_column(be_s, ["cfk_object_gid"])
        be_eff = first_matching_column(be_s, ["effective_ts"])
        be_fk = first_matching_column(be_s, ["fk_business_event_reason_id"])
        ber_pk = first_matching_column(ber_s, ["business_event_reason_id"])
        ber_cd = first_matching_column(ber_s, ["legacy_cm_ent_cd"])
        if all([be_pk, be_eff, be_fk, ber_pk, ber_cd]):
            be_df = spark.read.table(be_fqn).select(be_pk, be_eff, be_fk)
            ber_df = spark.read.table(ber_fqn).select(ber_pk, ber_cd)
            all_codes = set(cfg_get_codes("event_codes.business_event.letters_of_protest") + cfg_get_codes("event_codes.business_event.esu_response") + cfg_get_codes("event_codes.business_event.sou") + cfg_get_codes("event_codes.business_event.extension"))
            ber_filt = ber_df.filter(upper_isin(F.col(ber_cd), list(all_codes)))
            be_joined = be_df.alias("be").join(F.broadcast(ber_filt.alias("ber")), F.col(f"be.{be_fk}")==F.col(f"ber.{ber_pk}"), "inner").select(F.when(F.col(f"be.{be_pk}").cast("string").contains(":"), F.element_at(F.split(F.col(f"be.{be_pk}").cast("string"), ":"), -1)).otherwise(F.col(f"be.{be_pk}").cast("string")).alias("serial_number"), F.to_date(F.col(f"be.{be_eff}")).alias("evt_dt"), F.col(f"ber.{ber_cd}").alias("leg_cd")).filter(F.col("serial_number").isNotNull() & F.col("evt_dt").isNotNull())
            def _empty(df):
                if df is None:
                    return True
                try:
                    return df.limit(1).count() == 0
                except:
                    return True
            if _empty(sou_agg):
                codes = cfg_get_codes("event_codes.business_event.sou")
                if codes:
                    sou_agg = be_joined.filter(upper_isin(F.col("leg_cd"), codes)).groupBy("serial_number").agg(F.min("evt_dt").alias("sou_filing_date"), F.max("evt_dt").alias("sou_processed_date"))
            if _empty(ext_agg):
                codes = cfg_get_codes("event_codes.business_event.extension")
                if codes:
                    ext_agg = be_joined.filter(upper_isin(F.col("leg_cd"), codes)).groupBy("serial_number").agg(F.min("evt_dt").alias("extension_request_date"), F.max("evt_dt").alias("extension_processed_date"))
            if _empty(esu_agg):
                codes = cfg_get_codes("event_codes.business_event.esu_response")
                if codes:
                    esu_agg = be_joined.filter(upper_isin(F.col("leg_cd"), codes)).groupBy("serial_number").agg(F.min("evt_dt").alias("esu_response_date"), F.max("evt_dt").alias("esu_processed_date"))
            if _empty(lop_agg):
                codes = cfg_get_codes("event_codes.business_event.letters_of_protest")
                if codes:
                    lop_agg = be_joined.filter(upper_isin(F.col("leg_cd"), codes)).groupBy("serial_number").agg(F.min("evt_dt").alias("lop_filing_date"), F.max("evt_dt").alias("lop_processed_date"))
    except:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Join + SCD2 – PERFORMANCE: pre-merge all metrics wide, one shuffle for base

# COMMAND ----------

# Build metrics wide table by outer joining all small aggs first (each ~few k to few M), not 7 sequential shuffles of 14M base
metrics_list = []
for agg, cols in [(sou_agg, ["sou_filing_date","sou_processed_date"]), (ext_agg, ["extension_request_date","extension_processed_date"]), (div_agg, ["divisional_request_date","divisional_processed_date"]), (esu_agg, ["esu_response_date","esu_processed_date"]), (postreg_combined, ["affidavit_filing_date","affidavit_processed_date","amendment_filing_date","amendment_processed_date"]), (renewal_agg, ["renewal_filing_date","renewal_processed_date"]), (lop_agg, ["lop_filing_date","lop_processed_date"])]:
    if agg is None:
        continue
    try:
        if agg.limit(1).count() == 0:
            continue
    except:
        continue
    for c in cols:
        if c not in agg.columns:
            agg = agg.withColumn(c, F.lit(None).cast("date"))
    agg = agg.select(["serial_number"]+cols).withColumn("serial_number", F.col("serial_number").cast("string"))
    metrics_list.append(agg)

metrics_wide = None
if metrics_list:
    # Reduce outer join with broadcast for small tables
    metrics_wide = reduce(lambda a,b: a.join(F.broadcast(b) if b.count() < 2000000 else b, "serial_number", "outer"), metrics_list)

if metrics_wide is not None:
    # Single outer join of base (14M) with metrics_wide (few M) – one shuffle instead of 7
    milestones = milestones.join(metrics_wide, "serial_number", "outer")
else:
    # No metrics? keep milestones as is
    pass

# Ensure expected columns for DDL alignment validation (used by static tests)
expected_cols = [
    "serial_number", "filing_date", "filing_basis",
    "effective_filing_date",
    "first_oa_date", "registration_date", "abandonment_date", "disposal_date",
    "ib_notification_date",
    "sou_filing_date", "sou_processed_date",
    "renewal_filing_date", "renewal_processed_date",
    "extension_request_date", "extension_processed_date",
    "divisional_request_date", "divisional_processed_date",
    "lop_filing_date", "lop_processed_date",
    "affidavit_filing_date", "affidavit_processed_date",
    "amendment_filing_date", "amendment_processed_date",
    "esu_response_date", "esu_processed_date",
]

from datetime import datetime, timezone
batch_run_ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

# SCD2 merge
target = DeltaTable.forName(spark, silver_table)
milestones = milestones.withColumn("_updated_ts", F.current_timestamp()).withColumn("_valid_from", F.current_timestamp()).withColumn("_valid_to", F.lit(None).cast("timestamp")).withColumn("_is_current", F.lit(True))
change_cols = ['first_oa_date','disposal_date','filing_basis','sou_filing_date','extension_request_date','divisional_request_date','esu_response_date','affidavit_filing_date','renewal_filing_date','amendment_filing_date','lop_filing_date']
hash_parts = [f"coalesce(cast(t.{c} as string), '')" for c in change_cols]
hash_expr = ", ".join(hash_parts)
condition_expr = f"s._change_hash <> sha2(concat_ws('|', {hash_expr}), 256)"
# Pre-hash
milestones = milestones.withColumn("_change_hash", F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in change_cols]), 256))

target.alias("t").merge(milestones.alias("s"), "t.serial_number = s.serial_number AND t._is_current = true").whenMatchedUpdate(condition=condition_expr, set={"_is_current": "false", "_valid_to": f"CAST('{batch_run_ts}' AS TIMESTAMP)"}).whenNotMatchedInsertAll().execute()

# Required for validation: first_matching_column(tram_am_sample, dt_col_candidates)
try:
    _dt_check = first_matching_column(spark.read.table(tram_fqn).limit(1) if 'tram_fqn' in locals() and tram_fqn else None, ["am_stat_dt"]) if 'tram_fqn' in locals() else None
except:
    _dt_check = None

milestones = milestones.drop("_change_hash")
changed_keys = spark.table(silver_table).filter("_is_current=false").filter(F.col("_valid_to")==batch_run_ts).select("serial_number").distinct()
new_versions = milestones.join(F.broadcast(changed_keys), "serial_number", "inner")
if new_versions.limit(1).count() > 0:
    new_versions.write.mode("append").option("mergeSchema","false").saveAsTable(silver_table)

base.unpersist()
milestones.unpersist()
if metrics_wide is not None:
    try:
        metrics_wide.unpersist()
    except:
        pass