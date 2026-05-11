# Databricks notebook source
# MAGIC %pip install databricks-labs-dqx==0.9.3

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
import os

dq_path = "/Workspace/Users/joshua.strickland@uspto.gov/data_quality"
if dq_path not in sys.path:
    sys.path.insert(0, dq_path)
    print(f"Added to sys.path: {dq_path}")

# Capture for thread propagation
_NOTEBOOK_SYS_PATH = list(sys.path)

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# COMMAND ----------

dbutils.widgets.dropdown("dbx_env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.dropdown("load_method", "Incremental", ["Initial", "Incremental"], "Load Method")
dbutils.widgets.text("max_parallel", "4", "Max Parallel Tables")

dbx_env = dbutils.widgets.get("dbx_env").lower()
load_method = dbutils.widgets.get("load_method")
max_parallel = int(dbutils.widgets.get("max_parallel"))

# COMMAND ----------

# Define all tables to process
# In production, this would be read from the dq_table_registry
TABLES = [
    {"catalog": "trm_reporting", "schema": "silver", "table_name": "bibliography"},
    {"catalog": "trm_reporting", "schema": "silver", "table_name": "class"},
    {"catalog": "trm_reporting", "schema": "silver", "table_name": "owner"},
    {"catalog": "trm_reporting", "schema": "silver", "table_name": "divisionals"},
    {"catalog": "trm_reporting", "schema": "silver", "table_name": "prosecution_history"},
    {"catalog": "trm_reporting", "schema": "silver", "table_name": "fpep_fact"},
    {"catalog": "trm_tmngpdb",   "schema": "bronze", "table_name": "mailing_address"},
]

# COMMAND ----------

def run_table(table_config):
    """Thread-safe wrapper that ensures sys.path is correct inside each thread."""
    import sys
    
    # FIX: Inject the notebook's sys.path into this thread
    for p in _NOTEBOOK_SYS_PATH:
        if p not in sys.path:
            sys.path.insert(0, p)
    
    # Import INSIDE the thread (after sys.path is fixed)
    # from data_quality.engine.dq_engine import process_table_dq
    from engine.dq_engine import process_table_dq
    
    try:
        result = process_table_dq(
            table_name=table_config["table_name"],
            schema=table_config["schema"],
            catalog=table_config["catalog"],
            dbx_env=dbx_env,
            load_method=load_method
        )
        return {**table_config, **result, "error": None}
    except Exception as e:
        return {**table_config, "status": "FAILED", "error": str(e)}

# COMMAND ----------

orchestration_start = datetime.now(timezone.utc)
results = []

with ThreadPoolExecutor(max_workers=max_parallel) as executor:
    futures = {executor.submit(run_table, t): t for t in TABLES}
    
    for future in as_completed(futures):
        result = future.result()
        results.append(result)
        status = result.get("status", "UNKNOWN")
        name = f"{result['catalog']}.{result['schema']}.{result['table_name']}"
        
        if result.get("error"):
            print(f"❌ {name}: FAILED — {result['error']}")
        else:
            print(f"{'✓' if status == 'PASS' else '⚠'} {name}: {status}")

# COMMAND ----------

# Summary Report
total_duration = (datetime.now(timezone.utc) - orchestration_start).total_seconds()
passed = sum(1 for r in results if r.get("status") == "PASS")
failed = sum(1 for r in results if r.get("status") == "FAILED")
quarantined = sum(1 for r in results if r.get("status") == "QUARANTINED")

print(f"\n{'='*80}")
print(f"ORCHESTRATION COMPLETE")
print(f"{'='*80}")
print(f"Tables processed: {len(results)}")
print(f"  PASS:        {passed}")
print(f"  QUARANTINED: {quarantined}")
print(f"  FAILED:      {failed}")
print(f"Total duration: {total_duration:.0f}s")

if failed > 0:
    print("\nFAILED TABLES:")
    for r in results:
        if r.get("status") == "FAILED":
            print(f"  ❌ {r['catalog']}.{r['schema']}.{r['table_name']}: {r['error']}")