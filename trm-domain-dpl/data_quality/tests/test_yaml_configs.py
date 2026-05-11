"""
YAML Configuration Tests — validates all checks, hash_configs, and environment configs.
Runnable locally with pytest or in GitLab CI.
"""
import os
import yaml
import pytest

DQ_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _collect_yaml_files(subdirectory: str) -> list:
    base = os.path.join(DQ_ROOT, subdirectory)
    if not os.path.exists(base):
        return []
    results = []
    for root, dirs, files in os.walk(base):
        for f in files:
            if f.endswith((".yml", ".yaml")):
                results.append(os.path.join(root, f))
    return results


# ===================================================================
# SYNTAX TESTS
# ===================================================================
ALL_YAMLS = (
    _collect_yaml_files("checks")
    + _collect_yaml_files("hash_configs")
    + _collect_yaml_files("config")
    + _collect_yaml_files("allowed_values")
)

@pytest.mark.parametrize("yaml_path", ALL_YAMLS, ids=lambda p: os.path.relpath(p, DQ_ROOT))
def test_yaml_is_parseable(yaml_path):
    with open(yaml_path) as f:
        doc = yaml.safe_load(f)
    assert doc is not None, f"YAML file is empty: {yaml_path}"


# ===================================================================
# CHECKS YAML — Structural validation
# ===================================================================
CHECKS_YAMLS = _collect_yaml_files("checks")

@pytest.mark.parametrize("yaml_path", CHECKS_YAMLS, ids=lambda p: os.path.relpath(p, DQ_ROOT))
def test_checks_yaml_structure(yaml_path):
    with open(yaml_path) as f:
        raw = yaml.safe_load(f)

    checks = raw.get("checks", raw) if isinstance(raw, dict) else raw
    assert isinstance(checks, list), f"'checks' must be a list in {yaml_path}"

    for i, c_block in enumerate(checks):
        check_def = c_block.get("check", c_block)

        assert "function" in check_def, f"Check #{i} missing 'function' in {yaml_path}"

        assert "criticality" in c_block, f"Check #{i} missing 'criticality' in {yaml_path}"

        assert c_block["criticality"] in ("error", "warning", "warn"), \
            f"Check #{i} has invalid criticality '{c_block['criticality']}' in {yaml_path}"


# ===================================================================
# HASH CONFIG — Structural validation
# ===================================================================
HASH_YAMLS = _collect_yaml_files("hash_configs")

@pytest.mark.parametrize("yaml_path", HASH_YAMLS, ids=lambda p: os.path.relpath(p, DQ_ROOT))
def test_hash_config_structure(yaml_path):
    with open(yaml_path) as f:
        cfg = yaml.safe_load(f)

    assert cfg.get("natural_key_columns"), f"Missing natural_key_columns in {yaml_path}"

    use_all = cfg.get("use_all_columns_for_data_hash", False)
    det_cols = cfg.get("deterministic_columns_for_data_hash")

    if not use_all:
        assert det_cols, f"Missing deterministic_columns_for_data_hash in {yaml_path}"


# ===================================================================
# CROSS-REFERENCE — Every checks YAML must have a matching hash_config
# ===================================================================
def test_checks_have_matching_hash_configs():
    checks_tables = set()
    for path in CHECKS_YAMLS:
        fname = os.path.basename(path)
        table = fname.replace("_checks.yml", "").replace("_checks.yaml", "")
        rel_dir = os.path.relpath(os.path.dirname(path), os.path.join(DQ_ROOT, "checks"))
        checks_tables.add(os.path.join(rel_dir, table))

    hash_tables = set()
    for path in HASH_YAMLS:
        fname = os.path.basename(path)
        table = fname.replace("_hash_config.yml", "").replace("_hash_config.yaml", "")
        rel_dir = os.path.relpath(os.path.dirname(path), os.path.join(DQ_ROOT, "hash_configs"))
        hash_tables.add(os.path.join(rel_dir, table))

    missing = checks_tables - hash_tables
    assert not missing, f"Tables have checks YAML but NO hash_config: {missing}"


# ===================================================================
# ENV CONFIG — Required keys
# ===================================================================
CONFIG_YAMLS = _collect_yaml_files("config")
REQUIRED_CONFIG_KEYS = ["trgt_catalog", "domain_catalog", "lake_base_url", "workspace_id"]

@pytest.mark.parametrize("yaml_path", CONFIG_YAMLS, ids=lambda p: os.path.relpath(p, DQ_ROOT))
def test_env_config_has_required_keys(yaml_path):
    with open(yaml_path) as f:
        cfg = yaml.safe_load(f)
    assert "schema" in cfg, f"Missing top-level 'schema' key in {yaml_path}"
    schema_cfg = cfg["schema"]
    missing = [k for k in REQUIRED_CONFIG_KEYS if k not in schema_cfg]
    assert not missing, f"Missing required keys {missing} in {yaml_path}"