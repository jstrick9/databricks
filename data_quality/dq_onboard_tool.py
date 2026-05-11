# Databricks notebook source
# MAGIC %pip install databricks-labs-dqx==0.9.3

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from tools.onboard_table import onboard

onboard("trm_reporting", "bronze", "address", dbx_env="dev")

# COMMAND ----------

