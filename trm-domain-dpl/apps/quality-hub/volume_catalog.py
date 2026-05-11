# app/volume_catalog.py
from __future__ import annotations
from databricks.sdk import WorkspaceClient
from typing import List, Tuple, Set, Dict

def _w() -> WorkspaceClient:
    return WorkspaceClient()

def _discover_tables_in_subfolder(root_path: str, file_suffix: str) -> List[Tuple[str, str, str]]:
    found = set()
    w = _w()
    try:
        catalogs = list(w.files.list_directory_contents(root_path))
    except Exception:
        return []

    for cat_entry in catalogs:
        if not cat_entry.is_directory: continue
        try:
            schemas = list(w.files.list_directory_contents(cat_entry.path))
            for sch_entry in schemas:
                if not sch_entry.is_directory: continue
                try:
                    files = list(w.files.list_directory_contents(sch_entry.path))
                    for f in files:
                        file_path = f.path.rstrip("/")
                        name = file_path.rsplit("/", 1)[-1].lower()
                        if name.endswith(f"{file_suffix}.yml") or name.endswith(f"{file_suffix}.yaml"):
                            parts = file_path.split('/')
                            if len(parts) >= 4:
                                catalog, schema = parts[-3], parts[-2]
                                table = name.replace(f"{file_suffix}.yml", "").replace(f"{file_suffix}.yaml", "")
                                found.add((catalog, schema, table))
                except Exception: continue
        except Exception: continue
    return sorted(list(found))

def list_tables_by_type(config_root: str) -> Dict[str, List[Tuple[str, str, str]]]:
    """Returns separated lists for Checks and Hash Configs."""
    root = config_root.rstrip("/")
    return {
        "Checks": _discover_tables_in_subfolder(f"{root}/checks", "_checks"),
        "Hash Configs": _discover_tables_in_subfolder(f"{root}/hash_configs", "_hash_config")
    }