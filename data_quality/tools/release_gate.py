# tools/release_gate.py
from __future__ import annotations

import os
import sys
import importlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils.path_utils import get_repo_root


@dataclass
class GateResult:
    status: str
    started_at_utc: str
    finished_at_utc: str
    passed: int
    failed: int
    details: List[Dict[str, Any]]


def _utcnow():
    return datetime.now(timezone.utc)

def _utcstr(dt):
    return dt.isoformat()

def _spark() -> SparkSession:
    active = SparkSession.getActiveSession()
    return active if active is not None else SparkSession.builder.getOrCreate()

def _add(details, name: str, ok: bool, message: str = "", extra: dict | None = None):
    row = {"name": name, "ok": ok, "message": message}
    if extra:
        row.update(extra)
    details.append(row)


def _discover_tables_from_checks(repo_root: str) -> List[Dict[str, str]]:
    """
    Discovers tables from runtime checks directory:
      checks/<catalog>/<schema>/<table>_checks.yml
    """
    checks_root = os.path.join(repo_root, "checks")
    out = []
    for root, _, files in os.walk(checks_root):
        for fn in files:
            if not fn.endswith((".yml", ".yaml")):
                continue
            if not fn.endswith(("_checks.yml", "_checks.yaml")):
                continue

            rel = os.path.relpath(os.path.join(root, fn), checks_root).replace("\\", "/")
            parts = rel.split("/")
            if len(parts) < 3:
                continue
            catalog = parts[0]
            schema = parts[1]
            table = fn.replace("_checks.yml", "").replace("_checks.yaml", "")
            out.append({"catalog": catalog, "schema": schema, "table_name": table})
    # de-dupe
    uniq = {(t["catalog"], t["schema"], t["table_name"]) for t in out}
    return [{"catalog": c, "schema": s, "table_name": t} for (c, s, t) in sorted(uniq)]


def run_release_gate(
    dbx_env: str = "dev",
    smoke_test: bool = True,
    smoke_keys_per_table: int = 500,
    smoke_tables_limit: int = 3,
    output_suffix: str = "__smoke",
    skip_notifications: bool = True,
    log_table_fqn: Optional[str] = None,
) -> GateResult:
    """
    Runs a production-readiness gate:
      1) imports
      2) YAML validation (syntax+function existence+signature)
      3) schema contracts for all tables in checks/
      4) optional smoke DQ runs for first N tables

    This should be run on the same type of cluster you’ll use in production.
    """
    started = _utcnow()
    details: List[Dict[str, Any]] = []
    passed = failed = 0

    repo_root = get_repo_root()

    # Ensure imports work from this runtime root
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    # 1) Core imports
    core_modules = [
        "engine.dq_engine",
        "tools.schema_contract",
        "tools.validate_configs",
        "utils.hash_utils",
        "utils.load_utils",
        "utils.path_utils",
        "transforms.common_transforms",
        "custom_checks.common_checks",
    ]
    for m in core_modules:
        try:
            importlib.import_module(m)
            _add(details, f"import:{m}", True, "OK")
            passed += 1
        except Exception as e:
            _add(details, f"import:{m}", False, str(e))
            failed += 1

    if failed:
        return _finalize(started, details, passed, failed, log_table_fqn)

    # 2) YAML validation
    try:
        from tools.validate_configs import validate_all
        res = validate_all()
        if res["failed"] == 0:
            _add(details, "validate_all_yaml", True, f"passed={res['passed']}")
            passed += 1
        else:
            _add(details, "validate_all_yaml", False, f"failed={res['failed']}", {"errors": res["errors"][:100]})
            failed += 1
    except Exception as e:
        _add(details, "validate_all_yaml", False, str(e))
        failed += 1

    # 3) Schema contracts
    tables = _discover_tables_from_checks(repo_root)
    if not tables:
        _add(details, "discover_tables", False, "No tables found under checks/.")
        failed += 1
        return _finalize(started, details, passed, failed, log_table_fqn)

    _add(details, "discover_tables", True, f"found={len(tables)}")
    passed += 1

    try:
        from tools.schema_contract import enforce_contract
        for t in tables:
            name = f"{t['catalog']}.{t['schema']}.{t['table_name']}"
            try:
                enforce_contract(t["catalog"], t["schema"], t["table_name"], dbx_env=dbx_env, fail_on_missing=True)
                _add(details, f"schema_contract:{name}", True, "OK")
                passed += 1
            except Exception as e:
                _add(details, f"schema_contract:{name}", False, str(e))
                failed += 1
    except Exception as e:
        _add(details, "schema_contract_runner", False, str(e))
        failed += 1

    # 4) Smoke DQ runs (limited keys, writes to *_smoke_clean/quarantine)
    if smoke_test:
        try:
            from engine.dq_engine import process_table_dq
            to_run = tables[: max(0, int(smoke_tables_limit))]
            for t in to_run:
                name = f"{t['catalog']}.{t['schema']}.{t['table_name']}"
                try:
                    out = process_table_dq(
                        table_name=t["table_name"],
                        schema=t["schema"],
                        catalog=t["catalog"],
                        dbx_env=dbx_env,
                        load_method="Incremental",
                        scope_natural_keys_limit=int(smoke_keys_per_table),
                        output_suffix=output_suffix,
                        skip_notifications=skip_notifications,
                    )
                    _add(details, f"smoke_dq:{name}", True, f"status={out['status']} run_id={out['run_id']}")
                    passed += 1
                except Exception as e:
                    _add(details, f"smoke_dq:{name}", False, str(e))
                    failed += 1
        except Exception as e:
            _add(details, "smoke_dq_runner", False, str(e))
            failed += 1

    return _finalize(started, details, passed, failed, log_table_fqn)


def _finalize(started_dt, details, passed, failed, log_table_fqn) -> GateResult:
    finished = _utcnow()
    status = "PASS" if failed == 0 else "FAIL"
    result = GateResult(
        status=status,
        started_at_utc=_utcstr(started_dt),
        finished_at_utc=_utcstr(finished),
        passed=passed,
        failed=failed,
        details=details,
    )
    if log_table_fqn:
        _write_log(result, log_table_fqn)
    return result


def _write_log(result: GateResult, table_fqn: str) -> None:
    sp = _spark()
    row = [{
        "gate_run_id": str(os.urandom(8).hex()),
        "status": result.status,
        "started_at_utc": result.started_at_utc,
        "finished_at_utc": result.finished_at_utc,
        "passed": result.passed,
        "failed": result.failed,
        "details_json": str(result.details),
        "_created_timestamp": _utcnow(),
    }]
    sp.createDataFrame(row).write.mode("append").option("mergeSchema", "true").saveAsTable(table_fqn)