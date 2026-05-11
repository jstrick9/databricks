# app/dq_config_store.py
from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
import yaml
from databricks.sdk import WorkspaceClient


@dataclass(frozen=True)
class DQConfigStore:
    """
    Read/write DQ config YAMLs in a Unity Catalog Volume via Databricks Files API.

    Works in Databricks Apps (required) because Apps cannot use normal file I/O on /Volumes.
    """
    root: str  # e.g. "/Volumes/trm_domain_dev/audit_quality/dq_configs"

    def __post_init__(self):
        if not self.root.startswith("/Volumes/"):
            raise ValueError(f"Expected /Volumes/... root. Got: {self.root}")

    @property
    def w(self) -> WorkspaceClient:
        return WorkspaceClient()

    def checks_path(self, catalog: str, schema: str, table: str) -> str:
        return f"{self.root.rstrip('/')}/checks/{catalog}/{schema}/{table}_checks.yml"

    def hash_path(self, catalog: str, schema: str, table: str) -> str:
        return f"{self.root.rstrip('/')}/hash_configs/{catalog}/{schema}/{table}_hash_config.yml"

    def _ensure_dir(self, directory: str) -> None:
        self.w.files.create_directory(directory)

    def read_text(self, path: str) -> str:
        resp = self.w.files.download(path)
        return resp.contents.read().decode("utf-8")

    def write_text(self, path: str, text: str, overwrite: bool = True) -> None:
        parent = path.rsplit("/", 1)[0]
        self._ensure_dir(parent)
        bio = BytesIO(text.encode("utf-8"))
        self.w.files.upload(file_path=path, contents=bio, overwrite=overwrite)

    def exists(self, path: str) -> bool:
        try:
            self.w.files.get_metadata(path)
            return True
        except Exception:
            return False

    def load_checks(self, catalog: str, schema: str, table: str) -> dict:
        path = self.checks_path(catalog, schema, table)
        return yaml.safe_load(self.read_text(path)) or {}

    def save_checks(self, catalog: str, schema: str, table: str, doc: dict) -> str:
        path = self.checks_path(catalog, schema, table)
        self.write_text(path, yaml.dump(doc, sort_keys=False), overwrite=True)
        return path

    def load_hash_config(self, catalog: str, schema: str, table: str) -> dict:
        path = self.hash_path(catalog, schema, table)
        return yaml.safe_load(self.read_text(path)) or {}

    def save_hash_config(self, catalog: str, schema: str, table: str, doc: dict) -> str:
        path = self.hash_path(catalog, schema, table)
        self.write_text(path, yaml.dump(doc, sort_keys=False), overwrite=True)
        return path