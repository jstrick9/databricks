import os
import re

_ALLOWED = {"dev", "lab", "prod"}

def get_runtime_env() -> str:
    name = os.getenv("DATABRICKS_APP_NAME", "")
    if name:
        m = re.search(r"(dev|lab|prod)\b", name, flags=re.IGNORECASE)
        if m:
            env = m.group(1).lower()
            return "dev" if env == "lab" else env
    env = os.getenv("ENVIRONMENT", "dev").lower().strip()
    if env in _ALLOWED:
        return "dev" if env == "lab" else env
    return "dev"
