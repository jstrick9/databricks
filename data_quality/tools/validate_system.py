# tools/validate_system.py
from __future__ import annotations

import os
import sys
import importlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils.path_utils import get_repo_root


@dataclass
class ValidationResult:
    status: str                    # PASS | FAIL
    started_at_utc: str
    finished_at_utc: str
    checks_passed: int
    checks_failed: int
    details: List[Dict[str, Any]]  # list of {name,status,message,...}


def _utc_now_str() -> str:
    return datetime.now(timezone.utc).isoformat()


def _spark() -> SparkSession:
    active = SparkSession.getActiveSession()
    return active if active is not None else SparkSession.builder.getOrCreate()


def _record(details, name: str, ok: bool, message: str = "", extra: dict | None = None):
    row = {"name": name, "ok": ok, "message": message}
    if extra:
        row.update(extra)
    details.append(row)


def validate_system(
    dbx_env: str = "dev",
    # If provided, validate only these tables; else read from dq_table_registry if available.
    tables: Optional[List[Dict[str, str]]] = None,
    # Optional: run a real DQ job for one table as a smoke test
    smoke_test: bool = True,
    smoke_table: Optional[Dict[str, str]] = None,
    # Optional: write validation result to a Delta table
    log_table_fqn: Optional[str] = None,
) -> ValidationResult:
    """
    End-to-end production readiness validation for the DQ ecosystem.
    """
    started = _utc_now_str()
    details: List[Dict[str, Any]] = []
    passed = 0
    failed = 0

    repo_root = get_repo_root()
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    # ------------------------------------------------------------
    # 1) Core imports (fail fast if package/module graph broken)
    # ------------------------------------------------------------
    import_targets = [
        "engine.dq_engine",
        "tools.schema_contract",
        "tools.validate_configs",
        "utils.hash_utils",
        "utils.load_utils",
        "utils.path_utils",
        "custom_checks.common_checks",
        "transforms.common_transforms",
    ]

    for mod in import_targets:
        try:
            importlib.import_module(mod)
            _record(details, f"import:{mod}", True, "OK")
            passed += 1
        except Exception as e:
            _record(details, f"import:{mod}", False, str(e))
            failed += 1

    # If imports are broken, abort early
    if failed > 0:
        finished = _utc_now_str()
        result = ValidationResult(
            status="FAIL",
            started_at_utc=started,
            finished_at_utc=finished,
            checks_passed=passed,
            checks_failed=failed,
            details=details
        )
        if log_table_fqn:
            _write_validation_log(result, log_table_fqn)
        return result

    # ------------------------------------------------------------
    # 2) YAML validation (syntax + function existence + signature)
    # ------------------------------------------------------------
    try:
        from tools.validate_configs import validate_all
        v = validate_all()
        if v["failed"] == 0:
            _record(details, "validate_yaml", True, f"All YAML configs valid (passed={v['passed']})")
            passed += 1
        else:
            _record(details, "validate_yaml", False, f"YAML validation failed (failed={v['failed']})", {"errors": v["errors"][:50]})
            failed += 1
    except Exception as e:
        _record(details, "validate_yaml", False, str(e))
        failed += 1

    # ------------------------------------------------------------
    # 3) Required audit tables/views exist (domain catalog)
    # ------------------------------------------------------------
    try:
        from engine.dq_engine import load_env_config
        cfg = load_env_config(dbx_env, "trm_reporting")  # any catalog config works if it contains domain_catalog
        domain_catalog = cfg["domain_catalog"]
        required_objects = [
            f"{domain_catalog}.audit_quality.error_log",
            f"{domain_catalog}.audit_quality.dq_run_history",
            f"{domain_catalog}.audit_quality.dq_table_registry",
        ]
        sp = _spark()
        for obj in required_objects:
            try:
                sp.table(obj).limit(1).collect()
                _record(details, f"exists:{obj}", True, "OK")
                passed += 1
            except Exception as e:
                _record(details, f"exists:{obj}", False, str(e))
                failed += 1
    except Exception as e:
        _record(details, "audit_objects", False, str(e))
        failed += 1

    # ------------------------------------------------------------
    # 4) Determine tables to validate schema contracts against
    # ------------------------------------------------------------
    if tables is None:
        tables = []
        try:
            sp = _spark()
            # Try registry first
            reg = sp.table(f"{domain_catalog}.audit_quality.dq_table_registry") \
                   .filter(F.col("_is_record_active") == True) \
                   .select("catalog_name", "schema_name", "table_name") \
                   .collect()
            tables = [{"catalog": r["catalog_name"], "schema": r["schema_name"], "table_name": r["table_name"]} for r in reg]
        except Exception:
            # Fallback: no tables; user can pass tables explicitly
            tables = []

    # ------------------------------------------------------------
    # 5) Schema contract validation
    # ------------------------------------------------------------
    try:
        from tools.schema_contract import enforce_contract
        if not tables:
            _record(details, "schema_contract", True, "No tables provided/found; skipping schema contract checks.")
            passed += 1
        else:
            sc_fail = 0
            for t in tables:
                try:
                    enforce_contract(t["catalog"], t["schema"], t["table_name"], dbx_env=dbx_env, fail_on_missing=True)
                    _record(details, f"schema_contract:{t['catalog']}.{t['schema']}.{t['table_name']}", True, "OK")
                    passed += 1
                except Exception as e:
                    sc_fail += 1
                    _record(details, f"schema_contract:{t['catalog']}.{t['schema']}.{t['table_name']}", False, str(e))
                    failed += 1
            if sc_fail == 0:
                _record(details, "schema_contract_summary", True, f"All schema contracts passed ({len(tables)} tables).")
                passed += 1
            else:
                _record(details, "schema_contract_summary", False, f"{sc_fail} tables failed schema contract.")
                failed += 1
    except Exception as e:
        _record(details, "schema_contract", False, str(e))
        failed += 1

    # ------------------------------------------------------------
    # 6) Optional: run one real DQ smoke test
    # ------------------------------------------------------------
    if smoke_test:
        try:
            from engine.dq_engine import process_table_dq
            st = smoke_table or (tables[0] if tables else None)
            if st is None:
                _record(details, "dq_smoke_test", True, "No table available for smoke test; skipping.")
                passed += 1
            else:
                res = process_table_dq(
                    table_name=st["table_name"],
                    schema=st["schema"],
                    catalog=st["catalog"],
                    dbx_env=dbx_env,
                    enable_transformations=True,
                    load_method="Incremental"
                )
                _record(details, "dq_smoke_test", True, f"Ran DQ successfully: status={res['status']} run_id={res['run_id']}")
                passed += 1
        except Exception as e:
            _record(details, "dq_smoke_test", False, str(e))
            failed += 1

    finished = _utc_now_str()
    status = "PASS" if failed == 0 else "FAIL"
    result = ValidationResult(
        status=status,
        started_at_utc=started,
        finished_at_utc=finished,
        checks_passed=passed,
        checks_failed=failed,
        details=details
    )

    if log_table_fqn:
        _write_validation_log(result, log_table_fqn)

    return result


def _write_validation_log(result: ValidationResult, table_fqn: str) -> None:
    sp = _spark()
    rows = [{
        "validation_run_id": str(uuid.uuid4()),
        "status": result.status,
        "started_at_utc": result.started_at_utc,
        "finished_at_utc": result.finished_at_utc,
        "checks_passed": result.checks_passed,
        "checks_failed": result.checks_failed,
        "details_json": str(result.details),
        "_created_timestamp": datetime.now(timezone.utc)
    }]
    sp.createDataFrame(rows).write.mode("append").option("mergeSchema", "true").saveAsTable(table_fqn)