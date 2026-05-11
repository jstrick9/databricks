# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
import os
import pytest
import importlib

DQ_PATH = "/Workspace/Users/joshua.strickland@uspto.gov/data_quality"

for folder in ["custom_checks", "transforms", "utils", "engine", "tests"]:
    init_path = os.path.join(DQ_PATH, folder, "__init__.py")
    if not os.path.exists(init_path):
        with open(init_path, "w") as f:
            f.write("# Init file to establish package module\n")
        print(f"Created {init_path}")

if DQ_PATH not in sys.path:
    sys.path.insert(0, DQ_PATH)
os.chdir(DQ_PATH)

try:
    import custom_checks.common_checks
    import transforms.common_transforms
    import utils.hash_utils
    print("✅ Python successfully resolved all imports natively.")
except Exception as e:
    print(f"❌ Native import failed: {e}")
    raise

print("\n" + "="*50)
print("STARTING PYTEST")
print("="*50 + "\n")

exit_code = pytest.main([
    "tests/",
    "-v",
    "--tb=short",
    "-p", "no:cacheprovider",
    "--import-mode=importlib"  
])

print(f"\nExit code: {exit_code}")