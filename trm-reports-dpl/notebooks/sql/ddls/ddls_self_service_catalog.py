# Databricks notebook source
# MAGIC %md
# MAGIC # Purpose:
# MAGIC <pre> 
# MAGIC In this notebook contains SQL code to create catalog and schema for Self-service analytics (trademark_db). Also contains SQL code to create Views of table from different catalogs that are needed for users.
# MAGIC <pre>

# COMMAND ----------

# DBTITLE 1,Create Widget
dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
print(f'{dbx_env=}')

# COMMAND ----------

# DBTITLE 1,Read Config File.
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Run common functions notebook.
# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,Set Parameters.
common_configs = read_yaml(config_file)
self_service_catalog = common_configs['schema']['self_service_catalog']
tmdecisions_catalog = common_configs['schema']['tmdecisions_catalog']
trm_reporting_catalog = common_configs['schema']['trm_reporting_catalog']
pqr = common_configs['schema']['pqr_catalog']
print(f"{self_service_catalog=},{tmdecisions_catalog=}")
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set('conf.catalog', self_service_catalog)
spark.conf.set('conf.trm_reporting_catalog', trm_reporting_catalog)
spark.conf.set('conf.tmdecisions_catalog', tmdecisions_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

# DBTITLE 1,Creating Catalog
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${conf.catalog} MANAGED LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}';

# COMMAND ----------

# DBTITLE 1,Creating Schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self_service_catalog}.gold COMMENT 'For self service analytics gold layer data'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self_service_catalog}.silver COMMENT 'For self service analytics silver layer data'")

# COMMAND ----------

# DBTITLE 1,Views of Tables from tm_decisions Gold.
#Tables from tm_decisions_gold.
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.tmdecisions_expungement_reexam_vw AS SELECT * FROM {tmdecisions_catalog}.gold.tm_decisions_expungement_reexam")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.tmdecisions_expungement_reexam_tma_dashboard_vw AS SELECT * FROM {tmdecisions_catalog}.gold.tm_decisions_expungement_reexam_tma_dashboard")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.tmdecisions_tm_decisions_petition_to_director_vw AS SELECT * FROM {tmdecisions_catalog}.gold.tm_decisions_petition_to_director") 

# COMMAND ----------

# DBTITLE 1,Views of Tables from trm_reporting Gold.
#Tables from trm_reporting_gold.
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_filings_dashboard_vw AS SELECT * FROM {trm_reporting_catalog}.gold.filings_dashboard")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_form_paragraph_dashboard_vw AS SELECT * FROM {trm_reporting_catalog}.gold.form_paragraph_dashboard")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_goods_services_dashboard_vw AS SELECT * FROM {trm_reporting_catalog}.gold.goods_services_dashboard")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_inventory_dashboard_bd_occurrence_vw AS SELECT * FROM {trm_reporting_catalog}.gold.inventory_dashboard_bd_occurrence")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_inventory_dashboard_ea_counts_vw AS SELECT * FROM {trm_reporting_catalog}.gold.inventory_dashboard_ea_counts")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_inventory_dashboard_filings_vw AS SELECT * FROM {trm_reporting_catalog}.gold.inventory_dashboard_filings")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_inventory_dashboard_pendency_vw AS SELECT * FROM {trm_reporting_catalog}.gold.inventory_dashboard_pendency")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_inventory_dashboard_ratio_vw AS SELECT * FROM {trm_reporting_catalog}.gold.inventory_dashboard_ratio")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_inventory_dashboard_running_vw AS SELECT * FROM {trm_reporting_catalog}.gold.inventory_dashboard_running")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_inventory_madrid_vw AS SELECT * FROM {trm_reporting_catalog}.gold.inventory_madrid")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_inventory_unexamined_hstry_vw AS SELECT * FROM {trm_reporting_catalog}.gold.inventory_unexamined_hstry")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_pendency_dashboard_vw AS SELECT * FROM {trm_reporting_catalog}.gold.pendency_dashboard")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_post_reg_dashboard_vw AS SELECT * FROM {trm_reporting_catalog}.gold.post_reg_dashboard")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_post_reg_dashboard_running_vw AS SELECT * FROM {trm_reporting_catalog}.gold.post_reg_dashboard_running")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_post_reg_detail_dashboard_vw AS SELECT * FROM {trm_reporting_catalog}.gold.post_reg_detail_dashboard")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_post_reg_workforce_vw AS SELECT * FROM {trm_reporting_catalog}.gold.post_reg_workforce")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_quality_dashboard_vw AS SELECT * FROM {trm_reporting_catalog}.gold.quality_dashboard")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_quality_dashboard_pivot_vw AS SELECT * FROM {trm_reporting_catalog}.gold.quality_dashboard_pivot")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_ttab_decision_rates_vw AS SELECT * FROM {trm_reporting_catalog}.gold.ttab_decision_rates")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_ttab_detail_vw AS SELECT * FROM {trm_reporting_catalog}.gold.ttab_detail")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_ttab_workloads_vw AS SELECT * FROM {trm_reporting_catalog}.gold.ttab_workloads")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.trm_reporting_naics_fasttext_vw AS SELECT * FROM {trm_reporting_catalog}.gold.naics_fasttext")

# COMMAND ----------

# DBTITLE 1,Views of Tables from trm_reporting silver.
#Tables from trm_reporting_silver.
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.silver.trm_reporting_bibliography_vw AS SELECT * FROM {trm_reporting_catalog}.silver.bibliography")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.silver.trm_reporting_class_vw AS SELECT * FROM {trm_reporting_catalog}.silver.class")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.silver.trm_reporting_correspondence_vw AS SELECT * FROM {trm_reporting_catalog}.silver.correspondence")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.silver.trm_reporting_owner_vw AS SELECT * FROM {trm_reporting_catalog}.silver.owner")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.silver.trm_reporting_milestone_vw AS SELECT * FROM {trm_reporting_catalog}.silver.milestone")
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.silver.trm_reporting_prosecution_history_vw AS SELECT * FROM {trm_reporting_catalog}.silver.prosecution_history")

# COMMAND ----------

# DBTITLE 1,Views of Table from pqr
spark.sql(f"CREATE VIEW IF NOT EXISTS {self_service_catalog}.gold.pqr_trm_reports_pqr_quality_review_vw AS SELECT * FROM {pqr}.gold.trm_reports_pqr_quality_review")
