# Databricks notebook source
current_date, start_date, end_date, send_to_users = spark.sql(
    """
    select
        current_date,
        make_date(year(current_date), 1, 1) start_date,
        make_date(year(current_date) + 1, 1, 1) end_date,
        day(current_date) = 31 and month(current_date) = 12 send_to_users
    """
).collect()[0]
 
print(end_date)

from datetime import date, timedelta
display_end_date = (date.fromisoformat(str(end_date)) - timedelta(days=1)).isoformat()


subject = f"TM5 Report for {start_date} to {display_end_date}"
print(subject)

# COMMAND ----------

catalog = "trm_tmngpdb_dev"
database = "bronze"
tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{database}")
tables_to_drop = tables_df.filter(
    (tables_df.tableName.endswith('_clean')) | (tables_df.tableName.endswith('_quarantine'))
).select("database", "tableName").collect()

for row in tables_to_drop:
    db = row['database']
    tbl = row['tableName']
    spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.`{db}`.`{tbl}`")

# COMMAND ----------

spark.sql("TRUNCATE TABLE trm_domain_dev.audit_quality.error_log")

# COMMAND ----------

import os

root = "/Workspace/Users/joshua.strickland@uspto.gov/data_quality"
hits = []

for dirpath, dirnames, filenames in os.walk(root):
    for f in filenames:
        if f.endswith(".py"):
            path = os.path.join(dirpath, f)
            with open(path, "r") as fh:
                for i, line in enumerate(fh, 1):
                    if "data_quality." in line and not line.strip().startswith("#"):
                        hits.append(f"{path}:{i}  →  {line.strip()}")

if hits:
    print(f"🚨 Found {len(hits)} remaining 'data_quality.' references:\n")
    for h in hits:
        print(f"  {h}")
else:
    print("✓ No remaining 'data_quality.' references found!")

# COMMAND ----------

import os
import shutil

root = "/Workspace/Users/joshua.strickland@uspto.gov/data_quality"

# 1. Delete every __pycache__ directory
deleted = 0
for dirpath, dirnames, filenames in os.walk(root):
    for d in dirnames:
        if d == "__pycache__":
            full_path = os.path.join(dirpath, d)
            shutil.rmtree(full_path)
            print(f"DELETED: {full_path}")
            deleted += 1

# 2. Delete any stray .pyc files
for dirpath, dirnames, filenames in os.walk(root):
    for f in filenames:
        if f.endswith(".pyc"):
            full_path = os.path.join(dirpath, f)
            os.remove(full_path)
            print(f"DELETED: {full_path}")
            deleted += 1

print(f"\nCleaned {deleted} cached items")

# COMMAND ----------

import os

root = "/Workspace/Users/joshua.strickland@uspto.gov/data_quality"
hits = []

for dirpath, dirnames, filenames in os.walk(root):
    # Skip __pycache__ (should be gone now)
    dirnames[:] = [d for d in dirnames if d != "__pycache__"]
    
    for f in filenames:
        if f.endswith(".py"):
            path = os.path.join(dirpath, f)
            with open(path, "r") as fh:
                for i, line in enumerate(fh, 1):
                    stripped = line.strip()
                    # Skip comments
                    if stripped.startswith("#"):
                        continue
                    # Check for any data_quality. reference
                    if "data_quality." in stripped:
                        hits.append(f"{path}:{i}  →  {stripped}")

if hits:
    print(f"🚨 Found {len(hits)} remaining 'data_quality.' references:\n")
    for h in hits:
        print(f"  {h}")
    print("\n⚠️ FIX THESE BEFORE PROCEEDING")
else:
    print("✓ No remaining 'data_quality.' references in any .py file")

# COMMAND ----------

import os

root = "/Workspace/Users/joshua.strickland@uspto.gov/data_quality"

for dirpath, dirnames, filenames in os.walk(root):
    for f in filenames:
        if f == "__init__.py":
            path = os.path.join(dirpath, f)
            with open(path, "r") as fh:
                content = fh.read().strip()
            
            if content:
                print(f"📄 {path}")
                print(f"   Content: {content[:200]}")
                if "data_quality." in content:
                    print(f"   🚨 CONTAINS 'data_quality.' — NEEDS FIX")
                print()
            else:
                print(f"📄 {path} (empty — OK)")

# COMMAND ----------

import sys

# 1. Remove any cached 'data_quality' modules from memory
to_remove = [key for key in sys.modules if "data_quality" in key]
for key in to_remove:
    del sys.modules[key]
    print(f"FLUSHED from sys.modules: {key}")

if not to_remove:
    print("No cached 'data_quality' modules found in sys.modules")

# 2. Ensure the data_quality folder is on sys.path
dq_path = "/Workspace/Users/joshua.strickland@uspto.gov/data_quality"
if dq_path not in sys.path:
    sys.path.insert(0, dq_path)
    print(f"\nADDED to sys.path: {dq_path}")
else:
    print(f"\nAlready on sys.path: {dq_path}")

# 3. Show current sys.path for debugging
print("\nCurrent sys.path:")
for p in sys.path:
    print(f"  {p}")

# COMMAND ----------

import importlib
import traceback
import sys

# Ensure path is set
dq_path = "/Workspace/Users/joshua.strickland@uspto.gov/data_quality"
if dq_path not in sys.path:
    sys.path.insert(0, dq_path)

# Flush old modules
for key in [k for k in sys.modules if k.startswith(("engine", "utils", "transforms", "custom_checks"))]:
    del sys.modules[key]

# Try importing every module in dependency order
modules_to_test = [
    "utils.path_utils",
    "utils.dqx_compat",
    "utils.hash_utils",
    "utils.load_utils",
    "transforms.common_transforms",
    "custom_checks.common_checks",
    "utils.teams_notifier",
    "engine.dq_engine",
]

for mod_name in modules_to_test:
    try:
        mod = importlib.import_module(mod_name)
        print(f"  ✓ {mod_name}")
    except Exception as e:
        print(f"  ✗ {mod_name}")
        print(f"    ERROR: {e}")
        # Print the FULL traceback so we can see exactly which line is failing
        traceback.print_exc()
        print()

# COMMAND ----------

import os

DQ_PATH = "/Workspace/Users/joshua.strickland@uspto.gov/data_quality"

folders = [
    "engine",
    "utils",
    "transforms",
    "custom_checks",
    "tools",
    "transforms/trm_reporting",
    "transforms/trm_reporting/silver",
    "transforms/trm_tmngpdb",
    "transforms/trm_tmngpdb/bronze",
    "custom_checks/trm_reporting",
    "custom_checks/trm_reporting/silver",
    "custom_checks/trm_tmngpdb",
    "custom_checks/trm_tmngpdb/bronze",
]

for folder in folders:
    init_path = os.path.join(DQ_PATH, folder, "__init__.py")
    if not os.path.exists(init_path):
        with open(init_path, "w") as f:
            f.write("# Package init\n")
        print(f"Created: {init_path}")
    else:
        print(f"Already exists: {init_path}")

print("\nDone. All __init__.py files are in place.")

# COMMAND ----------

import sys
import os

# Simulate the App's environment
DQ_PATH = "/Workspace/Users/joshua.strickland@uspto.gov/data_quality"
os.environ["PYTHONPATH"] = DQ_PATH
os.environ["DQ_PATH"] = DQ_PATH

if DQ_PATH not in sys.path:
    sys.path.insert(0, DQ_PATH)

# Test every import the App will try
imports_to_test = [
    "engine.dq_engine",
    "utils.ai_rule_generator",
    "utils.pii_utils",
    "utils.root_cause_analyzer",
    "utils.email_notifier",
    "utils.servicenow_client",
    "utils.hash_utils",
    "utils.load_utils",
    "utils.path_utils",
    "tools.validate_configs",
    "tools.onboard_table",
    "transforms.common_transforms",
    "custom_checks.common_checks",
]

print("Testing all App imports...\n")
all_passed = True
for mod in imports_to_test:
    try:
        __import__(mod)
        print(f"  ✅ {mod}")
    except Exception as e:
        print(f"  ❌ {mod}: {e}")
        all_passed = False

print(f"\n{'✅ All imports passed — safe to deploy!' if all_passed else '❌ Fix the failing imports before deploying.'}")

# COMMAND ----------

from tools.sync_dq_configs import sync_volume_to_workspace

r = sync_volume_to_workspace(
    volume_root="/Volumes/trm_domain_dev/audit_quality/dq_configs",
    workspace_dq_root=get_repo_root(),  # important: uses repo-local runtime root
    archive_old_versions=True,
    archive_volume_root="/Volumes/trm_domain_dev/audit_quality/dq_configs/_runtime_archive",
    log_table_fqn="trm_domain_dev.audit_quality.dq_config_sync_log",
    dry_run=False
)
print(r)

# COMMAND ----------

from tools.sync_dq_configs import purge_archives

summary = purge_archives(
    archive_volume_root="/Volumes/trm_domain_dev/audit_quality/dq_configs/_runtime_archive",
    keep_days=90,
    dry_run=True,
    log_table_fqn="trm_domain_dev.audit_quality.dq_config_archive_purge_log",
    log_detail_rows=True
)

print(summary["purge_run_id"], summary["eligible"])
print("Deleted:", summary["deleted"], "Errors:", summary["errors"])

# COMMAND ----------

from tools.schema_contract import enforce_contract

for t in ["class", "owner", "bibliography"]:
    enforce_contract("trm_reporting", "silver", t, dbx_env="dev", fail_on_missing=True)

# COMMAND ----------

from tools.validate_configs import validate_all

res = validate_all()
print("Passed:", res["passed"], "Failed:", res["failed"])
if res["errors"]:
    print("\n".join(res["errors"][:200]))

# COMMAND ----------

from tools.validate_system import validate_system

res = validate_system(
    dbx_env="dev",
    smoke_test=True,
    log_table_fqn="trm_domain_dev.audit_quality.dq_system_validation_log"
)

print(res.status, res.checks_passed, res.checks_failed)
for d in res.details:
    if not d["ok"]:
        print("FAIL:", d["name"], d["message"])

# COMMAND ----------

from utils.path_utils import get_repo_root
print(get_repo_root())

# COMMAND ----------

import os

# Pin runtime root when running from a repo path (recommended)
os.environ["DQ_RUNTIME_ROOT"] = "/Workspace/Users/joshua.strickland@uspto.gov/data_quality"

from tools.release_gate import run_release_gate

res = run_release_gate(
    dbx_env="dev",
    smoke_test=True,
    smoke_keys_per_table=500,
    smoke_tables_limit=3,
    output_suffix="__smoke",
    skip_notifications=True,
    log_table_fqn="trm_domain_dev.audit_quality.dq_release_gate_log"
)

print(res.status, "passed=", res.passed, "failed=", res.failed)
for d in res.details:
    if not d["ok"]:
        print("FAIL:", d["name"], d["message"])
