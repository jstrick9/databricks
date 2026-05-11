"""
Pre-flight validation of all YAML configurations.

Purpose:
- Catch YAML structural issues before deployment
- Catch unknown check function names
- Catch argument mismatches vs Python check function signatures
- Enforce DQX criticality values: warn|error (accept legacy warning)

Usage:
    from tools.validate_configs import validate_all
    results = validate_all()
    print(results)
"""
from __future__ import annotations

import os
import sys
import yaml
import importlib
import inspect
from typing import Any, Dict, List, Tuple, Optional

from utils.path_utils import get_repo_root

repo_root = get_repo_root()

# Ensure repo_root (data_quality folder) is importable so we can import custom_checks.*
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

VALID_CRITICALITY = {"warn", "error", "warning"}  # accept legacy "warning"


# -----------------------------
# Helpers
# -----------------------------
def _read_yaml(path: str) -> Any:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _is_yaml_file(name: str) -> bool:
    return name.endswith((".yml", ".yaml"))


def _parse_checks_path(yaml_path: str) -> Tuple[str, str, str]:
    """
    Extract (catalog, schema, table_name) from:
      checks/<catalog>/<schema>/<table>_checks.yml
    """
    checks_root = os.path.join(repo_root, "checks")
    rel = os.path.relpath(yaml_path, checks_root).replace("\\", "/")
    parts = rel.split("/")

    if len(parts) < 3:
        raise ValueError(f"Unexpected checks path layout: {yaml_path}")

    catalog = parts[0]
    schema = parts[1]
    filename = parts[-1]
    if not filename.endswith(("_checks.yml", "_checks.yaml")):
        raise ValueError(f"Unexpected checks filename: {yaml_path}")

    table = filename.replace("_checks.yml", "").replace("_checks.yaml", "")
    return catalog, schema, table


def _parse_hash_path(yaml_path: str) -> Tuple[str, str, str]:
    """
    Extract (catalog, schema, table_name) from:
      hash_configs/<catalog>/<schema>/<table>_hash_config.yml
    """
    hash_root = os.path.join(repo_root, "hash_configs")
    rel = os.path.relpath(yaml_path, hash_root).replace("\\", "/")
    parts = rel.split("/")

    if len(parts) < 3:
        raise ValueError(f"Unexpected hash_config path layout: {yaml_path}")

    catalog = parts[0]
    schema = parts[1]
    filename = parts[-1]
    if not filename.endswith(("_hash_config.yml", "_hash_config.yaml")):
        raise ValueError(f"Unexpected hash_config filename: {yaml_path}")

    table = filename.replace("_hash_config.yml", "").replace("_hash_config.yaml", "")
    return catalog, schema, table


def _normalize_criticality(crit: str) -> str:
    """
    Canonical DQX values are: warn|error.
    Accept legacy warning -> warn.
    """
    if crit == "warning":
        return "warn"
    return crit


def _normalize_arguments(args: Any) -> Dict[str, Any]:
    """
    Normalize argument aliases:
      - column  -> col_name
      - columns -> col_names
    """
    if args is None:
        return {}
    if not isinstance(args, dict):
        # caller will error on non-dict
        return {"__invalid_args__": args}

    args = dict(args)  # copy
    if "column" in args and "col_name" not in args:
        args["col_name"] = args.pop("column")
    if "columns" in args and "col_names" not in args:
        args["col_names"] = args.pop("columns")
    return args


def _load_check_registry(catalog: str, schema: str, table: str) -> Dict[str, Any]:
    """
    Load check functions from:
      - custom_checks.common_checks
      - custom_checks.<catalog>.<schema>.<table>_checks  (optional)
    Mirrors engine loader behavior but returns a local dict.
    """
    registry: Dict[str, Any] = {}

    def collect(module_path: str):
        mod = importlib.import_module(module_path)
        for name in dir(mod):
            obj = getattr(mod, name)
            if callable(obj) and not name.startswith("_") and hasattr(obj, "__name__"):
                registry[name] = obj

    # common checks
    collect("custom_checks.common_checks")

    # table-specific checks
    specific = f"custom_checks.{catalog}.{schema}.{table}_checks"
    try:
        collect(specific)
    except ModuleNotFoundError:
        pass

    return registry


def _validate_args_against_signature(func, args: Dict[str, Any]) -> List[str]:
    """
    Validates that the provided YAML arguments can be passed to the function.

    Rules:
    - If function has required params (no default), they must exist in args.
    - If args contains keys not in signature and function doesn't accept **kwargs,
      that's an error.
    """
    errs: List[str] = []
    try:
        sig = inspect.signature(func)
    except Exception:
        # If we can't introspect, skip signature validation
        return errs

    params = sig.parameters
    accepts_kwargs = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())

    required = [
        name for name, p in params.items()
        if p.default is inspect._empty
        and p.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    ]

    # Missing required args
    for r in required:
        if r not in args:
            errs.append(f"missing required argument '{r}'")

    # Extra args not accepted
    if not accepts_kwargs:
        for k in args.keys():
            if k not in params:
                errs.append(f"unknown argument '{k}' (not in function signature)")

    return errs


# -----------------------------
# Validators
# -----------------------------
def validate_checks_yaml(yaml_path: str) -> List[str]:
    """
    Validates a checks YAML file.

    Supports:
      - criticality: warn|error (accept legacy warning)
      - nested 'check': {function, arguments}
      - flat form: {function, arguments, criticality}
      - arguments alias keys: column/columns
    """
    errors: List[str] = []

    raw = _read_yaml(yaml_path)
    if raw is None:
        return [f"Empty YAML: {yaml_path}"]

    checks = raw.get("checks", raw) if isinstance(raw, dict) else raw
    if not isinstance(checks, list):
        return [f"Invalid structure: expected a list or a dict with 'checks:' list in {yaml_path}"]

    # Determine table context so we can validate function existence
    try:
        catalog, schema, table = _parse_checks_path(yaml_path)
        registry = _load_check_registry(catalog, schema, table)
    except Exception as e:
        registry = {}
        errors.append(f"Could not load check registry for {yaml_path}: {e}")
        catalog = schema = table = "unknown"

    seen_names = set()

    for i, item in enumerate(checks):
        if not isinstance(item, dict):
            errors.append(f"Check #{i}: must be a dict")
            continue

        # DQX-style block: {criticality, check:{function,arguments}, name?}
        crit = item.get("criticality")
        if crit is None:
            errors.append(f"Check #{i}: missing 'criticality'")
            crit_norm = None
        else:
            if crit not in VALID_CRITICALITY:
                errors.append(f"Check #{i}: invalid criticality '{crit}' (must be warn|error)")
                crit_norm = None
            else:
                crit_norm = _normalize_criticality(crit)

        # Name is optional but recommended; if present enforce uniqueness
        if "name" in item:
            nm = str(item["name"])
            if nm in seen_names:
                errors.append(f"Check #{i}: duplicate name '{nm}'")
            seen_names.add(nm)

        check_def = item.get("check", item)
        if not isinstance(check_def, dict):
            errors.append(f"Check #{i}: 'check' must be a dict")
            continue

        func_name = check_def.get("function")
        if not func_name:
            errors.append(f"Check #{i}: missing check.function")
            continue

        # Function existence validation
        if registry and func_name not in registry:
            errors.append(
                f"Check #{i}: unknown function '{func_name}' "
                f"(not found in custom_checks.common_checks or custom_checks.{catalog}.{schema}.{table}_checks)"
            )
            func = None
        else:
            func = registry.get(func_name)

        # Arguments shape
        args_raw = check_def.get("arguments") or {}
        if not isinstance(args_raw, dict):
            errors.append(f"Check #{i}: arguments must be a dict (got {type(args_raw).__name__})")
            continue

        args = _normalize_arguments(args_raw)

        # Signature validation
        if func is not None:
            sig_errs = _validate_args_against_signature(func, args)
            for se in sig_errs:
                errors.append(f"Check #{i} ({func_name}): {se}")

        # Optional: enforce canonical criticality values in runtime
        # If you want strict canonicalization (warn|error only), uncomment:
        # if crit is not None and crit not in ("warn", "error"):
        #     errors.append(f"Check #{i}: use DQX canonical criticality 'warn' or 'error' (found '{crit}')")

    return errors


def validate_hash_config(yaml_path: str) -> List[str]:
    errors: List[str] = []
    cfg = _read_yaml(yaml_path) or {}

    if not isinstance(cfg, dict):
        return [f"{yaml_path}: hash_config must be a dict"]

    if not cfg.get("natural_key_columns"):
        errors.append(f"{yaml_path}: missing natural_key_columns")

    use_all = bool(cfg.get("use_all_columns_for_data_hash", False))
    det = cfg.get("deterministic_columns_for_data_hash")

    if not use_all and not det:
        errors.append(
            f"{yaml_path}: missing deterministic_columns_for_data_hash "
            f"(required when use_all_columns_for_data_hash=false)"
        )

    # Optional: ensure list types
    nk = cfg.get("natural_key_columns")
    if nk is not None and not isinstance(nk, list):
        errors.append(f"{yaml_path}: natural_key_columns must be a list")

    if det is not None and not isinstance(det, list):
        errors.append(f"{yaml_path}: deterministic_columns_for_data_hash must be a list")

    return errors


def validate_all() -> dict:
    """
    Validates:
      - checks YAMLs
      - hash_configs YAMLs

    Returns:
      {"passed": int, "failed": int, "errors": [str,...]}
    """
    results = {"passed": 0, "failed": 0, "errors": []}

    checks_root = os.path.join(repo_root, "checks")
    hash_root = os.path.join(repo_root, "hash_configs")

    # checks
    for root, _, files in os.walk(checks_root):
        for fn in files:
            if _is_yaml_file(fn):
                path = os.path.join(root, fn)
                errs = validate_checks_yaml(path)
                if errs:
                    results["failed"] += 1
                    results["errors"].append(f"FILE: {path}")
                    results["errors"].extend([f"  - {e}" for e in errs])
                else:
                    results["passed"] += 1

    # hash configs
    for root, _, files in os.walk(hash_root):
        for fn in files:
            if _is_yaml_file(fn):
                path = os.path.join(root, fn)
                errs = validate_hash_config(path)
                if errs:
                    results["failed"] += 1
                    results["errors"].append(f"FILE: {path}")
                    results["errors"].extend([f"  - {e}" for e in errs])
                else:
                    results["passed"] += 1

    return results