# app/onboard_logic.py
import re
from typing import List, Dict

# Heuristic for detecting primary identifiers
LIKELY_KEY_PATTERNS = [
    r".*_id$", r".*_num$", r".*_key$", r"^id$", r"^gid$", 
    r"serial", r"application_number", r"registration_number"
]

def infer_natural_keys(columns: List[str]) -> List[str]:
    """Heuristic to guess the natural keys of a table."""
    keys = []
    for col in columns:
        if any(re.match(p, col.lower()) for p in LIKELY_KEY_PATTERNS):
            keys.append(col)
    return keys if keys else [columns[0]] # Fallback to first column

def generate_default_checks(columns: List[str]) -> dict:
    """Generates a list of basic 'is_not_null' checks for all columns."""
    checks = []
    for col in columns:
        # Skip internal system columns
        if col.startswith("_"):
            continue
            
        checks.append({
            "name": f"{col}_not_null",
            "criticality": "warn",
            "check": {
                "function": "is_not_null",
                "arguments": {"col_name": col}
            }
        })
    return {"checks": checks}

def generate_default_hash_config(table_name: str, columns: List[str]) -> dict:
    """Generates a standard SCD2 hash configuration."""
    # Filter out internal columns for the data hash
    data_cols = [c for c in columns if not c.startswith("_")]
    
    return {
        "table_name": table_name,
        "natural_key_columns": infer_natural_keys(columns),
        "use_all_columns_for_data_hash": False,
        "deterministic_columns_for_data_hash": data_cols
    }