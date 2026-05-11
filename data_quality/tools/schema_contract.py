"""
Schema Contract Enforcement.

Compares the live schema of a source table against the expected schema defined
in the runtime repo configs:
  - data_quality/hash_configs/<catalog>/<schema>/<table>_hash_config.yml
  - data_quality/checks/<catalog>/<schema>/<table>_checks.yml

This is designed to fail fast BEFORE processing begins, catching:
  - missing columns referenced by configs
  - (optional) extra columns not referenced anywhere

IMPORTANT:
- Your checks YAML may be nested:
    - check:
        function: ...
        arguments: ...
      criticality: warn
- Or legacy/flat:
    - function: ...
      arguments: ...
      criticality: warning

This module supports both.
"""
from __future__ import annotations

import os
import yaml
from typing import Any, Dict, Iterable, List, Optional, Set, Union

from pyspark.sql import SparkSession

from utils.path_utils import get_repo_root
from engine.dq_engine import load_env_config


def _get_spark() -> SparkSession:
    active = SparkSession.getActiveSession()
    return active if active is not None else SparkSession.builder.getOrCreate()


def _safe_lower(x: Any) -> Optional[str]:
    if x is None:
        return None
    return str(x).strip().lower()


def _coerce_to_list(x: Any) -> List[str]:
    if x is None:
        return []
    if isinstance(x, list):
        return [str(v) for v in x if v is not None]
    return [str(x)]


def _extract_column_refs_from_args(args: Dict[str, Any]) -> Set[str]:
    """
    Extract column references from a check's arguments dict.

    Supported patterns:
      - col_name / column: <str>
      - col_names / columns: <list[str]> or <str>
      - create_col / modified_col / date_col / fiscal_year_col: <str>
      - any key ending with "_col": <str>

    Returns lowercase column names.
    """
    expected: Set[str] = set()
    if not isinstance(args, dict):
        return expected

    # Common single-column keys
    for key in ("col_name", "column"):
        if key in args and args[key] is not None:
            expected.add(_safe_lower(args[key]))

    # Common multi-column keys
    for key in ("col_names", "columns"):
        if key in args and args[key] is not None:
            for v in _coerce_to_list(args[key]):
                lv = _safe_lower(v)
                if lv:
                    expected.add(lv)

    # Known multi-column check patterns
    for key in ("create_col", "modified_col", "date_col", "fiscal_year_col"):
        if key in args and args[key] is not None:
            expected.add(_safe_lower(args[key]))

    # Generic: any *_col argument is treated as a column reference
    for k, v in args.items():
        if k.endswith("_col") and v is not None:
            expected.add(_safe_lower(v))

    # Remove None/empty
    expected.discard(None)
    expected.discard("")
    return expected


def _load_yaml_file(path: str) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r") as f:
        doc = yaml.safe_load(f)
    return doc or {}


def enforce_contract(
    catalog: str,
    schema: str,
    table_name: str,
    dbx_env: str = "dev",
    fail_on_missing: bool = True,
    fail_on_extra: bool = False,
    ignore_extra_prefixes: Optional[List[str]] = None,
) -> dict:
    """
    Compare live table schema against DQ configuration expectations.

    Args:
        fail_on_missing: Raise if columns referenced in configs don't exist in table
        fail_on_extra:   Raise if table has columns not referenced anywhere (optional)
        ignore_extra_prefixes: optional list of prefixes to ignore when evaluating "extra" columns
                               (example: ["_", "sys_"])

    Returns:
        dict summary of contract check results
    """
    spark = _get_spark()
    repo_root = get_repo_root()

    ignore_extra_prefixes = ignore_extra_prefixes or []

    # Load env config to resolve physical catalog
    config = load_env_config(dbx_env, catalog)
    catalog_physical = config["trgt_catalog"]
    full_table = f"{catalog_physical}.{schema}.{table_name}"

    # Load live schema
    try:
        live_cols = {f.name.strip().lower() for f in spark.table(full_table).schema.fields}
    except Exception as e:
        raise RuntimeError(f"Unable to read table schema for {full_table}: {e}") from e

    # Load hash_config
    hash_path = os.path.join(repo_root, "hash_configs", catalog, schema, f"{table_name}_hash_config.yml")
    hash_cfg = _load_yaml_file(hash_path)

    expected_from_hash: Set[str] = set()
    for col in (hash_cfg.get("natural_key_columns") or []):
        lc = _safe_lower(col)
        if lc:
            expected_from_hash.add(lc)

    for col in (hash_cfg.get("deterministic_columns_for_data_hash") or []):
        lc = _safe_lower(col)
        if lc:
            expected_from_hash.add(lc)

    # Load checks YAML
    checks_path = os.path.join(repo_root, "checks", catalog, schema, f"{table_name}_checks.yml")
    checks_raw = _load_yaml_file(checks_path)

    checks = checks_raw.get("checks", checks_raw) if isinstance(checks_raw, dict) else checks_raw
    if checks is None:
        checks = []
    if not isinstance(checks, list):
        raise ValueError(f"Invalid checks YAML structure in {checks_path}: expected list or dict with 'checks' list")

    expected_from_checks: Set[str] = set()
    for i, check_block in enumerate(checks):
        if not isinstance(check_block, dict):
            continue

        # Support both nested and flat forms
        check_def = check_block.get("check", check_block)
        if not isinstance(check_def, dict):
            continue

        args = check_def.get("arguments") or {}
        expected_from_checks |= _extract_column_refs_from_args(args)

    all_expected: Set[str] = expected_from_hash | expected_from_checks

    # Compare
    missing = sorted(list(all_expected - live_cols))

    # "Extra" columns are those in table not referenced anywhere
    extra_candidates = live_cols - all_expected
    extra = sorted([
        c for c in extra_candidates
        if not any(c.startswith(pfx) for pfx in ignore_extra_prefixes)
    ])

    result = {
        "table": full_table,
        "config_paths": {
            "checks_yml": checks_path,
            "hash_config_yml": hash_path,
        },
        "live_columns": len(live_cols),
        "expected_columns": len(all_expected),
        "expected_from_hash": sorted(list(expected_from_hash)),
        "expected_from_checks": sorted(list(expected_from_checks)),
        "missing_columns": missing,
        "extra_columns": extra,
        "status": "PASS"
    }

    if missing:
        result["status"] = "SCHEMA_DRIFT"
        msg = f"SCHEMA DRIFT: {full_table} is missing expected columns: {missing}"
        print(f"🚨 {msg}")
        if fail_on_missing:
            raise SchemaContractViolation(msg)

    if extra and fail_on_extra:
        result["status"] = "SCHEMA_DRIFT"
        msg = f"SCHEMA DRIFT: {full_table} has unexpected extra columns: {extra}"
        print(f"⚠️ {msg}")
        raise SchemaContractViolation(msg)

    if result["status"] == "PASS":
        print(f"✓ Schema contract verified for {full_table}")

    return result


class SchemaContractViolation(Exception):
    """Raised when a source table's schema doesn't match DQ expectations."""
    pass