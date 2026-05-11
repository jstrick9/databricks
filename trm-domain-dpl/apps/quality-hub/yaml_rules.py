# app/yaml_rules.py
from __future__ import annotations
import yaml

# Added case-insensitivity support
VALID_CRIT = {"error", "warning", "warn"}

def normalize_checks_doc(doc: dict) -> dict:
    """
    Normalizes variations into the canonical engine schema.
    """
    if not doc:
        return {"checks": []}

    checks = doc.get("checks", doc)
    if not isinstance(checks, list):
        raise ValueError("YAML must be a list or contain a top-level 'checks:' list")

    normalized = []
    for i, item in enumerate(checks):
        if not isinstance(item, dict):
            raise ValueError(f"Check #{i} must be a dict")

        # 1. Normalize Criticality (Handle case-insensitivity)
        raw_crit = str(item.get("criticality", "warning")).lower()
        if raw_crit not in VALID_CRIT:
            raise ValueError(f"Check #{i} invalid criticality '{raw_crit}' (must be error|warning|warn)")
        
        crit = "warning" if raw_crit in ["warn", "warning"] else "error"

        # 2. Extract and preserve 'name' if provided
        check_name = item.get("name")

        # 3. Resolve Check Definition (Handle Nested vs Flat)
        check_def = item.get("check", item).copy()
        
        # Cleanup top-level keys if we are wrapping a flat structure
        for key in ["criticality", "name", "check"]:
            if key in check_def:
                check_def.pop(key)

        if "function" not in check_def:
            raise ValueError(f"Check #{i} missing check.function")

        # 4. Normalize Arguments
        args = check_def.get("arguments", {}) or {}
        if not isinstance(args, dict):
            raise ValueError(f"Check #{i} arguments must be a dict")

        # Standardize column naming for Spark kwargs
        if "column" in args and "col_name" not in args:
            args["col_name"] = args.pop("column")
        if "columns" in args and "col_names" not in args:
            args["col_names"] = args.pop("columns")

        check_def["arguments"] = args

        # 5. Build Canonical Object
        entry = {
            "check": check_def,
            "criticality": crit
        }
        if check_name:
            entry["name"] = check_name
            
        normalized.append(entry)

    return {"checks": normalized}

def validate_yaml_text(yaml_text: str) -> dict:
    """
    Parses YAML, validates, and returns normalized doc.
    """
    try:
        doc = yaml.safe_load(yaml_text) or {}
        return normalize_checks_doc(doc)
    except yaml.YAMLError as e:
        raise ValueError(f"YAML Syntax Error: {str(e)}")