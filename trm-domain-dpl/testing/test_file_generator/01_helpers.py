# Databricks notebook source
# =============================================================================
# NOTEBOOK  : 01_helpers
# PURPOSE   : All helper functions for the Test File Generator.
#             %run this from 02_test_file_generator to load all functions.
# TARGET    : trm_domain_dev.testing (hardcoded)
# RUNTIME   : DBR 16.4 LTS (Spark 3.5.2, Scala 2.12)
# =============================================================================

# COMMAND ----------

# DBTITLE 1,Framework Constants
FRAMEWORK_CATALOG = "trm_domain_dev"
FRAMEWORK_SCHEMA  = "testing"
FW = f"{FRAMEWORK_CATALOG}.{FRAMEWORK_SCHEMA}"
FW_OUTPUT_VOLUME  = f"/Volumes/{FRAMEWORK_CATALOG}/{FRAMEWORK_SCHEMA}/tfg_output_files"

# COMMAND ----------

# DBTITLE 1,Core Imports
import uuid
import json
import os
import subprocess
import importlib
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, FloatType, DecimalType, DateType, TimestampType,
    BooleanType, ShortType, ByteType
)

# COMMAND ----------

# DBTITLE 1,Install Faker
try:
    importlib.import_module("faker")
except ImportError:
    subprocess.check_call(["pip", "install", "faker", "--quiet"])

from faker import Faker

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1: RUN LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

def start_run_log(generation_mode: str, source_table_fqn: str,
                  output_format: str = None, output_path: str = None,
                  profile_name: str = None) -> str:
    """Start a new run log entry. Returns run_uuid."""
    run_uuid = str(uuid.uuid4())
    spark.sql(f"""
        INSERT INTO {FW}.tfg_run_log
        (run_uuid, profile_name, generation_mode, source_table_fqn,
         output_format, output_path, status, started_by, started_ts)
        VALUES (
            '{run_uuid}',
            {f"'{profile_name}'" if profile_name else "NULL"},
            '{generation_mode}',
            '{source_table_fqn}',
            {f"'{output_format}'" if output_format else "NULL"},
            {f"'{output_path}'" if output_path else "NULL"},
            'STARTED', current_user(), current_timestamp()
        )
    """)
    print(f"✔ Run started: {run_uuid}")
    return run_uuid


def update_run_log(run_uuid: str, status: str, records_sampled: int = 0,
                   records_generated: int = 0, records_masked: int = 0,
                   records_error: int = 0, error_message: str = None) -> None:
    """Update run log on completion or failure."""
    started = spark.sql(f"SELECT started_ts FROM {FW}.tfg_run_log WHERE run_uuid='{run_uuid}'").first()
    dur = (datetime.now(timezone.utc) - started["started_ts"].replace(tzinfo=timezone.utc)).total_seconds() if started and started["started_ts"] else None
    err = (error_message or "").replace("'", "''")[:4000]
    spark.sql(f"""
        UPDATE {FW}.tfg_run_log SET
            status='{status}', records_sampled={records_sampled},
            records_generated={records_generated}, records_masked={records_masked},
            records_error={records_error},
            error_message={f"'{err}'" if error_message else "NULL"},
            completed_ts=current_timestamp(),
            duration_seconds={dur if dur else "NULL"}
        WHERE run_uuid='{run_uuid}'
    """)
    print(f"✔ Run {status}: {run_uuid} | generated={records_generated}")


def log_test_result(run_uuid: str, source_table_fqn: str, field_name: str,
                    scenario_type: str, value: str, description: str,
                    expected_outcome: str, regex_pattern_name: str = None) -> None:
    """Log a single test scenario to tfg_test_result_log."""
    def q(v): return f"'{(str(v) or '').replace(chr(39), chr(39)*2)[:2000]}'" if v is not None else "NULL"
    spark.sql(f"""
        INSERT INTO {FW}.tfg_test_result_log
        (run_uuid, source_table_fqn, source_field_name, regex_pattern_name,
         scenario_type, test_scenario_value, test_scenario_description,
         expected_outcome, created_ts)
        VALUES ('{run_uuid}', {q(source_table_fqn)}, {q(field_name)}, {q(regex_pattern_name)},
                {q(scenario_type)}, {q(value)}, {q(description)}, {q(expected_outcome)},
                current_timestamp())
    """)

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2: SAMPLING
# ═══════════════════════════════════════════════════════════════════════════════

def sample_source_data(source_df: DataFrame, row_count: int = 100) -> DataFrame:
    """Take the first N rows from the source DataFrame."""
    sampled = source_df.limit(row_count)
    print(f"  ✔ Sampled {sampled.count():,} rows (requested {row_count})")
    return sampled


def multiply_rows(df: DataFrame, multiplier: int) -> DataFrame:
    """Multiply rows for stress/volume testing."""
    if multiplier <= 1:
        return df
    result = reduce(lambda a, b: a.unionAll(b), [df for _ in range(multiplier)])
    print(f"  ✔ Volume ×{multiplier}: {result.count():,} rows")
    return result

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3: SCHEMA INSPECTION
# ═══════════════════════════════════════════════════════════════════════════════

def get_non_system_columns(df: DataFrame, extra_exclusions: List[str] = None) -> List[str]:
    """Return columns excluding system/audit columns (_ prefix, data_source, source_, RUN_*)."""
    skip = ["data_source", "source_", "run_dt", "run_ts"]
    extra = [c.lower() for c in (extra_exclusions or [])]
    return [c for c in df.columns
            if not c.startswith("_")
            and not any(p in c.lower() for p in skip)
            and c.lower() not in extra]


def get_nullable_columns(df: DataFrame) -> List[str]:
    return [f.name for f in df.schema.fields if f.nullable]


def get_numeric_columns(df: DataFrame) -> List[str]:
    return [f.name for f in df.schema.fields
            if isinstance(f.dataType, (IntegerType, LongType, DoubleType, FloatType,
                                       DecimalType, ShortType, ByteType))]

def get_string_columns(df: DataFrame) -> List[str]:
    return [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]


def get_date_columns(df: DataFrame) -> List[str]:
    return [f.name for f in df.schema.fields if isinstance(f.dataType, (DateType, TimestampType))]

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4: COLUMN MANIPULATION
# ═══════════════════════════════════════════════════════════════════════════════

def insert_column_after(df: DataFrame, new_col: str, after_col: str, default=None) -> DataFrame:
    """Insert a new column immediately after a specified column."""
    cols = df.columns
    new_df = df.withColumn(new_col, F.lit(default))
    if after_col not in cols:
        return new_df
    pos = cols.index(after_col) + 1
    ordered = cols[:pos] + [new_col] + [c for c in cols[pos:] if c != new_col]
    return new_df.select(*ordered)


def drop_columns_by_pattern(df: DataFrame, patterns: List[str]) -> DataFrame:
    """Drop columns matching any substring pattern (case-insensitive)."""
    to_drop = [c for c in df.columns if any(p.lower() in c.lower() for p in patterns)]
    return df.drop(*to_drop) if to_drop else df

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5: PII MASKING ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

def auto_detect_masking_config(source_table_fqn: str) -> List[Dict[str, str]]:
    """
    Auto-detect masking config from tfg_source_field_mapping where is_pii=TRUE.
    Returns list of {column_name, masking_strategy_name} — no user input needed.
    """
    parts = source_table_fqn.split(".")
    if len(parts) != 3:
        return []
    rows = spark.sql(f"""
        SELECT source_field_name, masking_strategy
        FROM {FW}.tfg_source_field_mapping
        WHERE source_catalog = '{parts[0]}'
          AND source_schema  = '{parts[1]}'
          AND source_table   = '{parts[2]}'
          AND is_pii = TRUE AND masking_strategy IS NOT NULL AND is_active = TRUE
    """).collect()
    config = [{"column_name": r.source_field_name, "masking_strategy_name": r.masking_strategy} for r in rows]
    if config:
        print(f"  ✔ Auto-detected {len(config)} PII columns to mask from mapping table.")
    return config


def apply_sql_masking(df: DataFrame, col_name: str, expression: str) -> DataFrame:
    return df.withColumn(col_name, F.expr(expression.replace("{col}", f"`{col_name}`")))


def apply_faker_masking(df: DataFrame, col_name: str, faker_provider: str) -> DataFrame:
    from pyspark.sql.functions import pandas_udf
    import pandas as pd
    method_name = faker_provider.split(".")[-1] if "." in faker_provider else faker_provider

    @pandas_udf(StringType())
    def faker_udf(series: pd.Series) -> pd.Series:
        fake = Faker("en_US")
        Faker.seed(0)
        method = getattr(fake, method_name)
        return pd.Series([str(method()) for _ in range(len(series))])

    return df.withColumn(col_name, faker_udf(F.col(col_name)))


def apply_shuffle_masking(df: DataFrame, col_name: str) -> DataFrame:
    shuffled = df.select(F.col(col_name).alias(f"_s_{col_name}"), F.rand(42).alias("_r")).orderBy("_r").drop("_r")
    shuffled = shuffled.withColumn("_si", F.monotonically_increasing_id())
    original = df.withColumn("_oi", F.monotonically_increasing_id())
    joined = original.join(shuffled, original._oi == shuffled._si) \
                     .drop(col_name, "_oi", "_si") \
                     .withColumnRenamed(f"_s_{col_name}", col_name)
    return joined


def apply_masking(df: DataFrame, masking_config: List[Dict[str, str]]) -> Tuple[DataFrame, int]:
    """Apply masking rules to a DataFrame. Returns (masked_df, columns_masked_count)."""
    rules = {r.masking_strategy_name: r.asDict() for r in
             spark.sql(f"SELECT * FROM {FW}.tfg_masking_rules WHERE is_active=TRUE").collect()}
    masked_df = df
    count = 0
    for item in masking_config:
        col_name = item["column_name"]
        strategy = item["masking_strategy_name"]
        if col_name not in df.columns:
            print(f"  ⚠ Column '{col_name}' not found — skipping.")
            continue
        if strategy not in rules:
            print(f"  ⚠ Strategy '{strategy}' not found — skipping.")
            continue
        rule = rules[strategy]
        print(f"  → Masking '{col_name}' with '{strategy}'")
        try:
            if strategy == "SHUFFLE_COLUMN":
                masked_df = apply_shuffle_masking(masked_df, col_name)
            elif rule["faker_provider"]:
                masked_df = apply_faker_masking(masked_df, col_name, rule["faker_provider"])
            elif rule["masking_expression"]:
                masked_df = apply_sql_masking(masked_df, col_name, rule["masking_expression"])
            else:
                continue
            count += 1
        except Exception as e:
            print(f"  ✗ Error masking '{col_name}': {e}")
    print(f"  ✔ {count} columns masked.")
    return masked_df, count

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6: EDGE CASE GENERATORS
# ═══════════════════════════════════════════════════════════════════════════════

def generate_null_rows(source_df: DataFrame, nullable_cols: List[str]) -> DataFrame:
    """Generate one row per nullable column, with that column set to NULL."""
    null_df = spark.createDataFrame([], schema=source_df.schema)
    for col_name in nullable_cols:
        if col_name not in source_df.columns:
            continue
        col_type = [f.dataType for f in source_df.schema.fields if f.name == col_name][0]
        row = source_df.limit(1).withColumn(col_name, F.lit(None).cast(col_type))
        null_df = null_df.unionAll(row)
    print(f"  ✔ Null rows: {null_df.count()}")
    return null_df


def generate_boundary_rows(source_df: DataFrame, num_cols: List[str],
                           str_cols: List[str], dt_cols: List[str]) -> DataFrame:
    """Generate rows with boundary/edge values for numeric, string, and date columns."""
    bv_df = spark.createDataFrame([], schema=source_df.schema)
    base = source_df.limit(1)

    # Numeric boundaries
    for val in [0, -1, 1, 2147483647, -2147483648, 99999999.99]:
        for c in num_cols:
            ct = [f.dataType for f in source_df.schema.fields if f.name == c][0]
            bv_df = bv_df.unionAll(base.withColumn(c, F.lit(val).cast(ct)))

    # String boundaries
    for val in ["", " ", "X"*255, "!@#$%^&*()", "你好世界", "line1\nline2",
                "'; DROP TABLE t; --", "<script>alert(1)</script>",
                "   leading", "trailing   ", "1234567890"]:
        for c in str_cols:
            bv_df = bv_df.unionAll(base.withColumn(c, F.lit(val)))

    # Date boundaries
    for val in ["1900-01-01", "9999-12-31", "2000-02-29",
                datetime.now(timezone.utc).strftime("%Y-%m-%d"), "2024-12-31"]:
        for c in dt_cols:
            ct = [f.dataType for f in source_df.schema.fields if f.name == c][0]
            bv_df = bv_df.unionAll(base.withColumn(c, F.to_date(F.lit(val)).cast(ct)))

    print(f"  ✔ Boundary rows: {bv_df.count()}")
    return bv_df


def generate_duplicate_rows(source_df: DataFrame, copies: int = 1) -> DataFrame:
    """Generate exact duplicate rows."""
    dupe_df = reduce(lambda a, b: a.unionAll(b), [source_df for _ in range(copies)])
    print(f"  ✔ Duplicate rows: {dupe_df.count()} ({copies}x copies)")
    return dupe_df

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7: SCHEMA DRIFT SIMULATOR
# ═══════════════════════════════════════════════════════════════════════════════

def apply_schema_drift(df: DataFrame, drift_config: List[Dict[str, str]]) -> DataFrame:
    """Simulate schema drift: ADD, DROP, RENAME, RETYPE columns."""
    for op in drift_config:
        action = op.get("action", "").upper()
        col = op.get("column", "")
        if action == "ADD":
            dt = op.get("data_type", "string")
            after = op.get("after_column")
            df = insert_column_after(df, col, after, None) if after else df.withColumn(col, F.lit(None).cast(dt))
            print(f"  → Drift ADD: '{col}' ({dt})")
        elif action == "DROP" and col in df.columns:
            df = df.drop(col)
            print(f"  → Drift DROP: '{col}'")
        elif action == "RENAME" and col in df.columns:
            new = op["new_name"]
            df = df.withColumnRenamed(col, new)
            print(f"  → Drift RENAME: '{col}' → '{new}'")
        elif action == "RETYPE" and col in df.columns:
            dt = op["data_type"]
            df = df.withColumn(col, F.col(col).cast(dt))
            print(f"  → Drift RETYPE: '{col}' → {dt}")
    return df

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8: OUTPUT WRITERS
# ═══════════════════════════════════════════════════════════════════════════════

def write_output_file(df: DataFrame, fmt: str, path: str, header: bool = True) -> None:
    """Write DataFrame to file. Auto-appends extension. Creates directory if needed."""
    fmt = fmt.upper().strip()
    file_path = f"{path}.{fmt.lower()}" if not path.lower().endswith(f".{fmt.lower()}") else path

    # ── Auto-create parent directory if it doesn't exist ──
    parent_dir = os.path.dirname(file_path)
    if parent_dir and not os.path.exists(parent_dir):
        os.makedirs(parent_dir, exist_ok=True)
        print(f"  ℹ Created directory: {parent_dir}")

    print(f"  → Writing: {fmt} → {file_path} (header={header})")

    if fmt in ("JSON", "NDJSON", "CSV", "TXT", "XLSX"):
        pdf = df.toPandas()
        if fmt == "JSON":
            pdf.to_json(file_path, orient="records", lines=False, indent=2)
        elif fmt == "NDJSON":
            pdf.to_json(file_path, orient="records", lines=True)
        elif fmt == "CSV":
            pdf.to_csv(file_path, index=False, header=header)
        elif fmt == "TXT":
            pdf.to_csv(file_path, sep="|", index=False, header=header)
        elif fmt == "XLSX":
            try:
                pdf.to_excel(file_path, index=False, header=header)
            except ImportError:
                subprocess.check_call(["pip", "install", "openpyxl", "--quiet"])
                pdf.to_excel(file_path, index=False, header=header)
    elif fmt == "PARQUET":
        df.write.mode("overwrite").parquet(path)
    else:
        raise ValueError(f"Unsupported format: {fmt}")
    print(f"  ✔ File written: {file_path}")


def write_output_delta(df: DataFrame, fqn: str, mode: str = "overwrite") -> None:
    """Write DataFrame as a Delta table in Unity Catalog."""
    df.write.format("delta").mode(mode).saveAsTable(fqn)
    cnt = spark.sql(f"SELECT COUNT(*) c FROM {fqn}").first()["c"]
    print(f"  ✔ Delta table: {fqn} ({cnt:,} rows)")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 9: VALIDATION REPORT
# ═══════════════════════════════════════════════════════════════════════════════

def show_report(run_uuid: str, source_fqn: str, output_df: DataFrame,
                gen_mode: str, masking_applied: bool, records_gen: int,
                cols_masked: int, fmt: str = None, path: str = None) -> None:
    """Display a summary report for the run."""
    d = "=" * 65
    t = "-" * 65
    print(f"\n{d}")
    print(f"  📋  VALIDATION REPORT")
    print(f"{d}")
    print(f"  Run UUID         : {run_uuid}")
    print(f"  Source            : {source_fqn}")
    print(f"  Mode              : {gen_mode}")
    print(f"  Records Generated : {records_gen:,}")
    print(f"  PII Masking       : {'✔ ' + str(cols_masked) + ' columns' if masking_applied else '✗ None'}")
    if fmt: print(f"  Output Format     : {fmt}")
    if path: print(f"  Output Path       : {path}")
    print(t)

    # Scenario breakdown
    if gen_mode in ("SCENARIO", "FULL"):
        summary = spark.sql(f"""
            SELECT scenario_type, COUNT(*) rows, COUNT(DISTINCT source_field_name) fields
            FROM {FW}.tfg_test_result_log WHERE run_uuid='{run_uuid}'
            GROUP BY scenario_type ORDER BY scenario_type
        """)
        if summary.count() > 0:
            print(f"  {'Scenario Type':<25} {'Rows':>8} {'Fields':>8}")
            print(f"  {'-'*25} {'-'*8} {'-'*8}")
            for r in summary.collect():
                print(f"  {r.scenario_type:<25} {r.rows:>8,} {r.fields:>8}")
        print(t)

    # Schema
    print(f"  OUTPUT SCHEMA ({len(output_df.columns)} columns):")
    for f in output_df.schema.fields:
        n = "nullable" if f.nullable else "not null"
        print(f"    {f.name:<40} {f.dataType.simpleString():<15} ({n})")
    print(d)

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 10: SOURCE TABLE PARSER
# ═══════════════════════════════════════════════════════════════════════════════

def parse_source_table(source_table: str) -> Tuple[str, str, str]:
    """Parse 'catalog.schema.table' into components. Raises ValueError if invalid."""
    parts = [p.strip() for p in source_table.strip().split(".")]
    if len(parts) != 3 or any(not p for p in parts):
        raise ValueError(
            f"Source table must be in 'catalog.schema.table' format.\n"
            f"  You entered: '{source_table}'\n"
            f"  Example: my_catalog.my_schema.my_table"
        )
    return parts[0], parts[1], parts[2]

# COMMAND ----------

print("✅ 01_helpers.py loaded — all functions ready.")