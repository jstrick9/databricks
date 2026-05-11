import os
import yaml
from pathlib import Path

def get_repo_root() -> str:
    # engine/config_loader.py is in data_quality/engine/
    return str(Path(__file__).resolve().parents[1])

def load_env_config(dbx_env: str, catalog: str) -> dict:
    repo_root = get_repo_root()
    config_path = os.path.join(repo_root, "config", dbx_env, f"{catalog}-conf.yaml")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f) or {}
    return config["schema"]

def get_dq_configs_root(environment: str) -> str:
    """
    Get the UC Volume root for DQ configs for given environment.
    Reads from env var or falls back to default.
    """
    # Allow override via env var
    env_var = os.environ.get("DQ_CONFIGS_ROOT")
    if env_var:
        return env_var
    
    # Default per environment
    if environment == "prod":
        return "/Volumes/trm_domain_prod/audit_quality/dq_configs"
    else:
        return "/Volumes/trm_domain_dev/audit_quality/dq_configs"