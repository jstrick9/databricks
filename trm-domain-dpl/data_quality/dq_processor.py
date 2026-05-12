# Databricks notebook source
# MAGIC %pip install databricks-labs-dqx==0.9.3

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
import os

# Dynamically resolve repo path 
current_dir = os.getcwd()
if "data_quality" in current_dir and current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from engine.dq_engine import process_table_dq

# COMMAND ----------

# Define widgets for parameters
dbutils.widgets.dropdown("dbx_env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema", "", "Schema")
dbutils.widgets.text("table_name", "", "Table name")
dbutils.widgets.dropdown("enable_transformations", "true", ["true", "false"], "Enable transformations")
dbutils.widgets.dropdown("load_method", "Incremental", ["Initial", "Incremental"], "Load Method")

# COMMAND ----------

# Read widget values
dbx_env = dbutils.widgets.get("dbx_env").lower()
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_name = dbutils.widgets.get("table_name")
enable_transformations = dbutils.widgets.get("enable_transformations").lower()
load_method = dbutils.widgets.get("load_method")

if not catalog or not schema or not table_name:
    raise ValueError("Parameters 'catalog', 'schema', and 'table_name' are required")

print(f"ENVIRONMENT: {dbx_env}")
print(f"Running DQ for {catalog}.{schema}.{table_name}")
print(f"Enable transformations: {enable_transformations}")
print(f"Load Method: {load_method}")

# COMMAND ----------

# Execute Pipeline
result = process_table_dq(
    table_name=table_name,
    schema=schema,
    catalog=catalog,
    dbx_env=dbx_env,
    enable_transformations=enable_transformations,
    load_method=load_method
)

# COMMAND ----------

# 5) Print a simple summary
print("\nDQ Run Summary")
print("--------------")
print(f"Status:       {result['status']}")
print(f"Run ID:       {result['run_id']}")
print(f"Valid rows:   {result['valid']:,}")
print(f"Quarantined:  {result['quarantined']:,}")