# utils/dq_config_store.py
from __future__ import annotations

import os
import yaml
from dataclasses import dataclass
from typing import Any, Dict

@dataclass(frozen=True)
class DQConfigStore:
    """
    Config store rooted at a UC Volume path.

    Layout:
      <root>/
        checks/<catalog>/<schema>/<table>_checks.yml
        hash_configs/<catalog>/<schema>/<table>_hash_config.yml
    """
    root: str  # e.g. "/Volumes/trm_domain_dev/audit_quality/dq_configs"

    def __post_init__(self):
        if not self.root.startswith("/Volumes/"):
            raise ValueError(f"DQConfigStore.root must start with /Volumes/. Got: {self.root}")

    def checks_path(self, catalog: str, schema: str, table: str) -> str:
        return f"{self.root.rstrip('/')}/checks/{catalog}/{schema}/{table}_checks.yml"

    def hash_path(self, catalog: str, schema: str, table: str) -> str:
        return f"{self.root.rstrip('/')}/hash_configs/{catalog}/{schema}/{table}_hash_config.yml"

    def load_checks_yaml(self, catalog: str, schema: str, table: str) -> Dict[str, Any]:
        path = self.checks_path(catalog, schema, table)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Checks YAML not found in UC volume: {path}")
        with open(path, "r") as f:
            return yaml.safe_load(f) or {}

    def load_hash_config_yaml(self, catalog: str, schema: str, table: str) -> Dict[str, Any]:
        path = self.hash_path(catalog, schema, table)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Hash config YAML not found in UC volume: {path}")
        with open(path, "r") as f:
            return yaml.safe_load(f) or {}