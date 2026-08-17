import os
import re

_ALLOWED = {"lab", "prod"}


def get_runtime_env() -> str:
    """
    Extract environment from DATABRICKS_APP_NAME, assuming app is named:
      app-ph-action-code-${bundle.target}  -> app-ph-action-code-lab/prod
    
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
