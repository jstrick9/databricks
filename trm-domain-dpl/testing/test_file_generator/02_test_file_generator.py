# Databricks notebook source
# =============================================================================
# NOTEBOOK  : 02_test_file_generator
# PURPOSE   : THE main Test File Generator notebook. Users only need this.
#             Supports all modes: SIMPLE, SCENARIO, ADVANCED, FULL.
#             One required input: source_table. Everything else has defaults.
# TARGET    : trm_domain_dev.testing
# RUNTIME   : DBR 16.4 LTS (Spark 3.5.2, Scala 2.12)
# =============================================================================

# COMMAND ----------

# DBTITLE 1,Load Helpers
# MAGIC %run ./01_helpers

# COMMAND ----------

# DBTITLE 1,TEST FILE GENERATOR
print("""

🧪  TEST FILE GENERATOR  🧪                              
                                                                      
Target: trm_domain_dev.testing                                     
                                                                      
HOW TO USE:                                                        
─────────────────────────────────────────────────────              
1. Enter your source table (catalog.schema.table)       ← REQUIRED 
2. Pick a generation mode (default: SIMPLE)             ← OPTIONAL 
3. Click "Run All"                                                 
4. That's it! Everything else has smart defaults.                  
                                                                      
MODES:                                                             
─────────────────────────────────────────────────────              
SIMPLE   → Quick: grab N rows → output file                        
SCENARIO → QA: positive + negative test cases per field            
ADVANCED → Edge cases: nulls, boundaries, dupes, schema drift      
FULL     → All of the above combined                               
                                                                     
""")

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Step 1: Enter Source Table and Pick Mode
dbutils.widgets.text(    "source_table",    "", "1) Source Table (catalog.schema.table):")
dbutils.widgets.dropdown("generation_mode", "SIMPLE", ["SIMPLE", "SCENARIO", "ADVANCED", "FULL"], "2) Mode:")

# COMMAND ----------

# DBTITLE 1,Step 2: Run This Cell to Build Widgets for Your Mode
# ═══════════════════════════════════════════════════════════════════════════════
# This cell reads your selected mode, removes irrelevant widgets, and creates
# only the widgets you need. Re-run this cell any time you change the mode.
# ═══════════════════════════════════════════════════════════════════════════════

gen_mode = dbutils.widgets.get("generation_mode").strip().upper()

# ── Master list of all optional widget names ─────────────────────────────────
ALL_OPTIONAL_WIDGETS = [
    "row_count", "output_format", "output_path", "include_header",
    "scenario_types",
    "include_nulls", "include_boundary", "include_duplicates",
    "duplicate_count", "include_schema_drift", "schema_drift_json",
    "apply_masking", "masking_json",
    "volume_multiplier", "columns_to_exclude", "extra_columns_json",
    "profile_name",
]

# ── Remove all optional widgets first ────────────────────────────────────────
for w in ALL_OPTIONAL_WIDGETS:
    try:
        dbutils.widgets.remove(w)
    except Exception:
        pass

# Small pause to let widget removal take effect in the UI
import time
time.sleep(0.5)

# ═══════════════════════════════════════════════════════════════════════════════
# CREATE WIDGETS BASED ON SELECTED MODE
# ═══════════════════════════════════════════════════════════════════════════════

# ── COMMON: All modes need these ─────────────────────────────────────────────
dbutils.widgets.text(    "row_count",     "100", "3) Number of Rows:")
dbutils.widgets.dropdown("output_format", "CSV", ["CSV", "JSON", "NDJSON", "TXT", "XLSX", "PARQUET", "DELTA"], "4) Output Format:")
dbutils.widgets.text(    "output_path",   "",    "5) Output Path (blank = auto):")
dbutils.widgets.dropdown("include_header","True",["True", "False"], "6) Include Header:")

# Track the next widget number dynamically
widget_num = 7

# ── SCENARIO widgets ─────────────────────────────────────────────────────────
if gen_mode in ("SCENARIO", "FULL"):
    dbutils.widgets.dropdown(
        "scenario_types", "BOTH", ["BOTH", "POSITIVE_ONLY", "NEGATIVE_ONLY"],
        f"{widget_num}) Test Case Types (positive/negative):"
    )
    widget_num += 1

# ── ADVANCED widgets ─────────────────────────────────────────────────────────
if gen_mode in ("ADVANCED", "FULL"):
    dbutils.widgets.dropdown("include_nulls",       "No", ["No", "Yes"], f"{widget_num}) Inject Null Values:")
    widget_num += 1
    dbutils.widgets.dropdown("include_boundary",    "No", ["No", "Yes"], f"{widget_num}) Inject Boundary Values:")
    widget_num += 1
    dbutils.widgets.dropdown("include_duplicates",  "No", ["No", "Yes"], f"{widget_num}) Add Duplicate Rows:")
    widget_num += 1
    dbutils.widgets.text(    "duplicate_count",     "1",                 f"{widget_num}) Number of Duplicate Copies:")
    widget_num += 1
    dbutils.widgets.dropdown("include_schema_drift","No", ["No", "Yes"],f"{widget_num}) Simulate Schema Drift:")
    widget_num += 1
    dbutils.widgets.text(    "schema_drift_json",   "",                  f"{widget_num}) Schema Drift Config JSON:")
    widget_num += 1

# ── MASKING: available for all modes ─────────────────────────────────────────
dbutils.widgets.dropdown("apply_masking", "Auto", ["Auto", "Yes", "No"], f"{widget_num}) PII Masking (Auto = use mapping):")
widget_num += 1

# Only show manual masking JSON if user might need it
dbutils.widgets.text("masking_json", "", f"{widget_num}) Manual Masking JSON (only if above = Yes):")
widget_num += 1

# ── EXTRAS: available for all modes ──────────────────────────────────────────
dbutils.widgets.text("volume_multiplier",  "1", f"{widget_num}) Volume Multiplier (1 = no multiply):")
widget_num += 1
dbutils.widgets.text("columns_to_exclude", "",  f"{widget_num}) Exclude Columns (comma-separated):")
widget_num += 1
dbutils.widgets.text("extra_columns_json", "",  f"{widget_num}) Add Columns JSON:")
widget_num += 1
dbutils.widgets.text("profile_name",       "",  f"{widget_num}) Load Saved Profile (optional):")
widget_num += 1

# ── Print what was built ─────────────────────────────────────────────────────
mode_labels = {
    "SIMPLE":   "Widgets 1-6 + Masking + Extras",
    "SCENARIO": "Widgets 1-6 + Test Case Types + Masking + Extras",
    "ADVANCED": "Widgets 1-6 + Nulls/Boundary/Dupes/Drift + Masking + Extras",
    "FULL":     "All widgets (every option available)",
}

print(f"""
{'='*60}
  ✔ Widgets built for: {gen_mode} MODE
  → {mode_labels.get(gen_mode, '')}
  → Fill in the widgets above, then Run All Below ▼
{'='*60}
""")

# COMMAND ----------

# DBTITLE 1,Step 3: Resolve Parameters
source_table_input = dbutils.widgets.get("source_table").strip()
if not source_table_input:
    raise ValueError("⛔ 'Source Table' is required. Enter catalog.schema.table and re-run.")

src_cat, src_schema, src_table = parse_source_table(source_table_input)
source_fqn = f"{src_cat}.{src_schema}.{src_table}"

gen_mode       = dbutils.widgets.get("generation_mode").strip().upper()
row_count      = int(dbutils.widgets.get("row_count").strip() or "100")
output_format  = dbutils.widgets.get("output_format").strip().upper()
output_path    = dbutils.widgets.get("output_path").strip()
include_header = dbutils.widgets.get("include_header").strip().lower() == "true"

# ── Helper: safe widget read (returns default if widget doesn't exist) ───────
def get_widget(name: str, default: str = "") -> str:
    try:
        return dbutils.widgets.get(name).strip()
    except Exception:
        return default

# ── SCENARIO: only read if mode uses it ──────────────────────────────────────
scenario_types = "BOTH"
if gen_mode in ("SCENARIO", "FULL"):
    scenario_types = get_widget("scenario_types", "BOTH")

# ── ADVANCED: only read if mode uses it ──────────────────────────────────────
include_nulls    = False
include_boundary = False
include_dupes    = False
dupe_count       = 1
include_drift    = False
drift_config     = []

if gen_mode in ("ADVANCED", "FULL"):
    include_nulls    = get_widget("include_nulls", "No").lower() == "yes"
    include_boundary = get_widget("include_boundary", "No").lower() == "yes"
    include_dupes    = get_widget("include_duplicates", "No").lower() == "yes"
    dupe_count       = int(get_widget("duplicate_count", "1") or "1")
    include_drift    = get_widget("include_schema_drift", "No").lower() == "yes"
    drift_json       = get_widget("schema_drift_json", "")
    drift_config     = json.loads(drift_json) if drift_json else []

# ── MASKING + EXTRAS: always available ───────────────────────────────────────
masking_mode    = get_widget("apply_masking", "Auto")
masking_json    = get_widget("masking_json", "")
masking_config  = json.loads(masking_json) if masking_json else []

volume_mult     = int(get_widget("volume_multiplier", "1") or "1")
cols_excl_str   = get_widget("columns_to_exclude", "")
cols_to_exclude = [c.strip() for c in cols_excl_str.split(",") if c.strip()] if cols_excl_str else []
extra_cols_str  = get_widget("extra_columns_json", "")
extra_columns   = json.loads(extra_cols_str) if extra_cols_str else []
profile_name    = get_widget("profile_name", "") or None

# ── Auto-generate output path ────────────────────────────────────────────────
if not output_path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    if output_format == "DELTA":
        safe_name = f"tfg_output_{src_table}_{gen_mode.lower()}_{ts}".replace("-", "_")
        output_path = f"{FW}.{safe_name}"
    else:
        output_path = f"{FW_OUTPUT_VOLUME}/{src_table}_{gen_mode.lower()}_{ts}"
    print(f"  ℹ Auto-generated output path: {output_path}")

# ── Print only what's relevant ───────────────────────────────────────────────
print(f"""
{'='*60}
  ✅ CONFIGURATION — {gen_mode} MODE
{'='*60}
  Source          : {source_fqn}
  Rows            : {row_count}
  Output          : {output_format} → {output_path}
  Header          : {include_header}
  Masking         : {masking_mode}""")

if volume_mult > 1:
    print(f"  Volume          : {volume_mult}x")

if gen_mode in ("SCENARIO", "FULL"):
    print(f"  Scenario Types  : {scenario_types}")

if gen_mode in ("ADVANCED", "FULL"):
    print(f"  Inject Nulls    : {include_nulls}")
    print(f"  Boundary Values : {include_boundary}")
    print(f"  Duplicates      : {include_dupes}" + (f" ({dupe_count}x)" if include_dupes else ""))
    print(f"  Schema Drift    : {include_drift}")

if cols_to_exclude:
    print(f"  Exclude Cols    : {cols_to_exclude}")
if extra_columns:
    print(f"  Extra Cols      : {len(extra_columns)} columns")
if profile_name:
    print(f"  Profile         : {profile_name}")

print(f"{'='*60}")

# COMMAND ----------

# DBTITLE 1,Load Profile Override
if profile_name:
    print(f"Loading profile: {profile_name}")
    prof = spark.sql(f"""
        SELECT * FROM {FW}.tfg_test_profiles
        WHERE profile_name='{profile_name}' AND is_active=TRUE
    """).first()

    if prof is None:
        raise ValueError(f"Profile '{profile_name}' not found or inactive.")

    # Override from profile
    source_fqn      = f"{prof['source_catalog']}.{prof['source_schema']}.{prof['source_table']}"
    src_cat, src_schema, src_table = prof['source_catalog'], prof['source_schema'], prof['source_table']
    gen_mode        = prof["generation_mode"].upper()
    row_count       = prof["row_count"] or 100
    volume_mult     = prof["volume_multiplier"] or 1
    masking_mode    = "Auto" if prof["apply_pii_masking"] else "No"
    include_nulls   = prof["include_nulls"]
    include_boundary= prof["include_boundary_values"]
    include_dupes   = prof["include_duplicates"]
    dupe_count      = prof["duplicate_count"] or 1
    include_drift   = prof["include_schema_drift"]
    output_format   = prof["output_format"].upper()
    output_path     = prof["output_path"] or output_path
    include_header  = prof["include_header"]
    cols_to_exclude = [c.strip() for c in (prof["columns_to_exclude"] or "").split(",") if c.strip()]
    extra_columns   = json.loads(prof["extra_columns"] or "[]")
    drift_config    = json.loads(prof["schema_drift_config"] or "[]")
    print(f"  ✔ Profile loaded: {profile_name} → source: {source_fqn} mode: {gen_mode}")

# COMMAND ----------

# DBTITLE 1,Start Run Log
run_uuid = start_run_log(
    generation_mode = gen_mode,
    source_table_fqn= source_fqn,
    output_format   = output_format,
    output_path     = output_path,
    profile_name    = profile_name
)

# COMMAND ----------

# DBTITLE 1,Load Source Data
try:
    source_df    = spark.read.table(source_fqn)
    total_source = source_df.count()
    print(f"✔ Loaded: {source_fqn} | {total_source:,} rows | {len(source_df.columns)} cols")
    if total_source == 0:
        raise ValueError(f"Source table '{source_fqn}' is empty.")
except Exception as e:
    update_run_log(run_uuid, "FAILED", error_message=str(e))
    raise

# COMMAND ----------

# DBTITLE 1,Sample Source Data
sampled_df      = sample_source_data(source_df, row_count)
records_sampled = sampled_df.count()

# COMMAND ----------

# DBTITLE 1,SIMPLE Mode
if gen_mode in ("SIMPLE", "FULL"):
    print("\n─── SIMPLE ─────────────────────────────────────────────────────")
    simple_df = sampled_df
    if volume_mult > 1:
        simple_df = multiply_rows(simple_df, volume_mult)
    print(f"  ✔ Simple: {simple_df.count():,} rows")

# COMMAND ----------

# DBTITLE 1,SCENARIO Mode
scenario_df     = spark.createDataFrame([], schema=source_df.schema)
scenario_errors = 0

if gen_mode in ("SCENARIO", "FULL"):
    print("\n─── SCENARIO ───────────────────────────────────────────────────")

    # Load field mappings for this source table
    mapping_df = spark.sql(f"""
        SELECT sm.source_field_name, re.regex_pattern_name,
               re.positive_scenario_values, re.negative_scenario_values
        FROM {FW}.tfg_regex_patterns re
        INNER JOIN {FW}.tfg_source_field_mapping sm
            ON sm.regex_pattern_name = re.regex_pattern_name
        WHERE sm.source_catalog='{src_cat}' AND sm.source_schema='{src_schema}'
          AND sm.source_table='{src_table}' AND sm.is_active=TRUE AND re.is_active=TRUE
    """)

    if mapping_df.count() == 0:
        print(f"  ⚠ No field mappings found for {source_fqn}")
        print(f"    → Add rows to {FW}.tfg_source_field_mapping to enable SCENARIO mode.")
        print(f"    → Continuing with other modes...")
    else:
        # Build scenarios
        all_scenarios_df = spark.createDataFrame(
            [], "source_field_name string, regex_pattern_name string, "
                "scenario_input string, scenario_label string, "
                "scenario_type string, expected_outcome string"
        )

        if scenario_types in ("POSITIVE_ONLY", "BOTH"):
            pos = (mapping_df
                .withColumn("sl", F.from_json(F.col("positive_scenario_values"), "array<struct<input:string,label:string>>"))
                .select("source_field_name", "regex_pattern_name", F.explode("sl").alias("s"))
                .select("source_field_name", "regex_pattern_name",
                        F.col("s.input").alias("scenario_input"),
                        F.col("s.label").alias("scenario_label"),
                        F.lit("positive").alias("scenario_type"),
                        F.lit("PASS").alias("expected_outcome")))
            all_scenarios_df = all_scenarios_df.unionAll(pos)

        if scenario_types in ("NEGATIVE_ONLY", "BOTH"):
            neg = (mapping_df
                .withColumn("sl", F.from_json(F.col("negative_scenario_values"), "array<struct<input:string,label:string>>"))
                .select("source_field_name", "regex_pattern_name", F.explode("sl").alias("s"))
                .select("source_field_name", "regex_pattern_name",
                        F.col("s.input").alias("scenario_input"),
                        F.col("s.label").alias("scenario_label"),
                        F.lit("negative").alias("scenario_type"),
                        F.lit("FAIL").alias("expected_outcome")))
            all_scenarios_df = all_scenarios_df.unionAll(neg)

        scenarios_list = all_scenarios_df.collect()
        source_rows    = sampled_df.collect()
        src_iter       = iter(source_rows)
        total_sc       = len(scenarios_list)
        print(f"  Generating {total_sc} scenario rows...")

        for i, sc in enumerate(scenarios_list):
            try:
                # Cycle through source rows
                try:
                    src_row = next(src_iter)
                except StopIteration:
                    src_iter = iter(source_rows)
                    src_row  = next(src_iter)

                base_df  = spark.createDataFrame([src_row], schema=source_df.schema)
                fld      = sc.source_field_name
                val      = sc.scenario_input

                if fld not in source_df.columns:
                    scenario_errors += 1
                    continue

                col_type = [f.dataType for f in source_df.schema.fields if f.name == fld][0]
                test_df  = base_df.withColumn(
                    fld,
                    F.lit(val).cast(col_type) if val is not None else F.lit(None).cast(col_type)
                )

                # Log
                log_test_result(
                    run_uuid=run_uuid, source_table_fqn=source_fqn,
                    field_name=fld, scenario_type=sc.scenario_type,
                    value=str(val) if val is not None else None,
                    description=sc.scenario_label, expected_outcome=sc.expected_outcome,
                    regex_pattern_name=sc.regex_pattern_name
                )

                scenario_df = scenario_df.unionAll(test_df)

                if (i + 1) % 50 == 0:
                    print(f"    {i+1}/{total_sc} scenarios...")

            except Exception as e:
                print(f"  ✗ Scenario {i} error: {e}")
                scenario_errors += 1

        print(f"  ✔ Scenario: {scenario_df.count():,} rows | {scenario_errors} errors")

# COMMAND ----------

# DBTITLE 1,ADVANCED Mode
advanced_df = spark.createDataFrame([], schema=source_df.schema)

if gen_mode in ("ADVANCED", "FULL"):
    print("\n─── ADVANCED ───────────────────────────────────────────────────")
    non_sys = get_non_system_columns(source_df, cols_to_exclude)

    # Null injection
    if include_nulls:
        nullable = [c for c in get_nullable_columns(source_df) if c in non_sys]
        if nullable:
            null_rows = generate_null_rows(sampled_df, nullable)
            advanced_df = advanced_df.unionAll(null_rows)
            for nc in nullable:
                log_test_result(run_uuid, source_fqn, nc, "null", None,
                               f"Null injection: '{nc}'", "FAIL")

    # Boundary values
    if include_boundary:
        num_c = [c for c in get_numeric_columns(source_df) if c in non_sys]
        str_c = [c for c in get_string_columns(source_df)  if c in non_sys]
        dt_c  = [c for c in get_date_columns(source_df)    if c in non_sys]
        if num_c or str_c or dt_c:
            bv_rows = generate_boundary_rows(sampled_df, num_c, str_c, dt_c)
            advanced_df = advanced_df.unionAll(bv_rows)
            log_test_result(run_uuid, source_fqn, "MULTIPLE", "boundary",
                           "various", "Boundary/edge values across columns", "VARIES")

    # Duplicates
    if include_dupes:
        dup_rows = generate_duplicate_rows(sampled_df, dupe_count)
        advanced_df = advanced_df.unionAll(dup_rows)
        log_test_result(run_uuid, source_fqn, "ALL", "duplicate",
                       str(dupe_count), f"Exact duplicates ({dupe_count}x)", "FAIL")

    # Schema drift
    if include_drift and drift_config:
        advanced_df = apply_schema_drift(advanced_df, drift_config)
        log_test_result(run_uuid, source_fqn, "SCHEMA", "schema_drift",
                       json.dumps(drift_config)[:500], "Schema drift simulation", "VARIES")

    print(f"  ✔ Advanced: {advanced_df.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Combine Outputs
print("\n─── COMBINING ──────────────────────────────────────────────────")

if gen_mode == "SIMPLE":
    final_df = simple_df
elif gen_mode == "SCENARIO":
    final_df = scenario_df
elif gen_mode == "ADVANCED":
    final_df = advanced_df
elif gen_mode == "FULL":
    frames = []
    if 'simple_df' in dir() and simple_df is not None and simple_df.count() > 0:
        frames.append(simple_df)
    if scenario_df.count() > 0:
        frames.append(scenario_df)
    if advanced_df.count() > 0:
        # Align schemas for union
        common = [c for c in source_df.columns if c in advanced_df.columns]
        frames.append(advanced_df.select(*common) if common else advanced_df)
    final_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames) if frames else sampled_df

print(f"✔ Combined: {final_df.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Apply PII Masking
columns_masked = 0

if masking_mode == "Auto":
    # Auto-detect from mapping table
    masking_config = auto_detect_masking_config(source_fqn)
    if masking_config:
        final_df, columns_masked = apply_masking(final_df, masking_config)
    else:
        print("  ℹ No PII columns flagged in mapping table — no masking applied.")

elif masking_mode == "Yes":
    if masking_config:
        final_df, columns_masked = apply_masking(final_df, masking_config)
    else:
        print("  ⚠ Masking=Yes but no masking_json provided. Skipping.")

else:
    print("  ℹ Masking disabled.")

# COMMAND ----------

# DBTITLE 1,Post-Process Columns
if cols_to_exclude:
    final_df = drop_columns_by_pattern(final_df, cols_to_exclude)

for ec in extra_columns:
    name = ec.get("name", "").strip()
    after = ec.get("after_column", "").strip()
    default = ec.get("default_value")
    if name:
        final_df = insert_column_after(final_df, name, after, default)
        print(f"  ✔ Added column '{name}' after '{after}'")

# COMMAND ----------

# DBTITLE 1,Preview
records_generated = final_df.count()
print(f"\n📊 OUTPUT: {records_generated:,} rows × {len(final_df.columns)} columns")
display(final_df)

# COMMAND ----------

# DBTITLE 1,Write Output
try:
    if output_format == "DELTA":
        write_output_delta(final_df, output_path)
    else:
        write_output_file(final_df, output_format, output_path, include_header)
except Exception as e:
    update_run_log(run_uuid, "FAILED", error_message=str(e))
    raise

# COMMAND ----------

# DBTITLE 1,Complete Run
update_run_log(
    run_uuid          = run_uuid,
    status            = "SUCCESS",
    records_sampled   = records_sampled,
    records_generated = records_generated,
    records_masked    = columns_masked,
    records_error     = scenario_errors if gen_mode in ("SCENARIO", "FULL") else 0
)

show_report(
    run_uuid      = run_uuid,
    source_fqn    = source_fqn,
    output_df     = final_df,
    gen_mode      = gen_mode,
    masking_applied = columns_masked > 0,
    records_gen   = records_generated,
    cols_masked   = columns_masked,
    fmt           = output_format,
    path          = output_path
)

# COMMAND ----------

# DBTITLE 1,Done — Return for Workflow Use
dbutils.notebook.exit(json.dumps({
    "run_uuid":          run_uuid,
    "source_table":      source_fqn,
    "mode":              gen_mode,
    "records_generated": records_generated,
    "output_path":       output_path,
    "output_format":     output_format,
    "status":            "SUCCESS"
}))