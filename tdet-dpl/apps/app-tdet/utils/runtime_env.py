import os
import re
import streamlit as st

# Allowed environment names
_ALLOWED = {"lab", "prod"}


def get_runtime_env() -> str:
    """
    Extract environment from DATABRICKS_APP_NAME, assuming you name the app:
      app-tdet-${bundle.target}  -> app-tdet-lab/prod
    
    Falls back to ENVIRONMENT env var, then defaults to 'lab'.
    Always returns a valid string ('lab' or 'prod').
    """
    # Primary: Extract from app name
    name = os.getenv("DATABRICKS_APP_NAME", "")
    if name:
        m = re.search(r"(lab|prod)\b", name, flags=re.IGNORECASE)
        if m:
            return m.group(1).lower()

    # Fallback: Explicit ENVIRONMENT variable
    env = os.getenv("ENVIRONMENT", "lab").lower().strip()
    if env in _ALLOWED:
        return env

    # Safe default
    return "lab"