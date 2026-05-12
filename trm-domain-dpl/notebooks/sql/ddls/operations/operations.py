# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmdomain-conf.yaml" 
config_file = "../../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run  ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
domain_catalog = common_configs['schema']['trgt_catalog']
spark.conf.set('config.domain_catalog', domain_catalog)
print(f'{domain_catalog=}')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the operations schema in trm_domain_dev catalog
# MAGIC CREATE SCHEMA IF NOT EXISTS ${config.catalog}.operations
# MAGIC COMMENT 'Centralized Operations for Trademark assets - data productions, data sources, business entities/domains, etc.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Registry supports multiple apps
# MAGIC CREATE TABLE IF NOT EXISTS ${config.catalog}.operations.app_permission_registry (
# MAGIC     app_id          STRING NOT NULL COMMENT 'The unique ID of the app (e.g., dq_hub, tdet)',
# MAGIC     group_name      STRING NOT NULL COMMENT 'The Databricks Group Name',
# MAGIC     capability      STRING NOT NULL COMMENT 'The specific permission within that app',
# MAGIC     data_scope      STRING COMMENT 'Comma-separated catalogs or * for all access',
# MAGIC     description     STRING COMMENT 'A short description of the permission',
# MAGIC     is_active       BOOLEAN COMMENT 'True if this permission is active',
# MAGIC     updated_at      TIMESTAMP COMMENT 'Timestamp of last update',
# MAGIC     updated_by      STRING COMMENT 'The user who last updated the permission'
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Example: Configuring two different apps in one table
# MAGIC INSERT INTO ${config.catalog}.operations.app_permission_registry 
# MAGIC (app_id, group_name, capability, data_scope, description, is_active, updated_at, updated_by)
# MAGIC VALUES
# MAGIC -- ============================================================
# MAGIC -- APP 1: Data Quality Hub (dq_hub)
# MAGIC -- ============================================================
# MAGIC -- Platform Admins
# MAGIC ('dq_hub', 'dq_admins', 'manage_access', '*', 'Manage App permissions', true, current_timestamp(), 'SYSTEM_INIT'),
# MAGIC ('dq_hub', 'dq_admins', 'view_dashboards', '*', 'Full visibility', true, current_timestamp(), 'SYSTEM_INIT'),
# MAGIC
# MAGIC -- Data Engineers
# MAGIC ('dq_hub', 'dq_engineers', 'onboard_tables', '*', 'Onboard new UC tables', true, current_timestamp(), 'SYSTEM_INIT'),
# MAGIC ('dq_hub', 'dq_engineers', 'admin_sync', '*', 'Sync Volume to Workspace', true, current_timestamp(), 'SYSTEM_INIT'),
# MAGIC ('dq_hub', 'dq_engineers', 'view_violations', '*', 'System-wide error view', true, current_timestamp(), 'SYSTEM_INIT'),
# MAGIC ('dq_hub', 'dq_engineers', 'edit_rules', '*', 'Manage all YAML configs', true, current_timestamp(), 'SYSTEM_INIT'),
# MAGIC
# MAGIC -- Trademark Stewards (Scoped to Trademark Data)
# MAGIC ('dq_hub', 'dq_stewards', 'view_violations', 'trm_reporting, trm_tmngpdb', 'View TRM errors', true, current_timestamp(), 'SYSTEM_INIT'),
# MAGIC ('dq_hub', 'dq_stewards', 'edit_rules', 'trm_reporting, trm_tmngpdb', 'Manage TRM rules', true, current_timestamp(), 'SYSTEM_INIT'),
# MAGIC ('dq_hub', 'dq_stewards', 'view_dashboards', 'trm_reporting, trm_tmngpdb', 'View TRM health', true, current_timestamp(), 'SYSTEM_INIT'),
# MAGIC
# MAGIC -- Stakeholders (Read-Only)
# MAGIC ('dq_hub', 'dq_stakeholders', 'view_dashboards', '*', 'Read-only health summary', true, current_timestamp(), 'SYSTEM_INIT'),
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- APP 2: TDET App 
# MAGIC -- ============================================================
# MAGIC ('TDET', 'dq_engineers', 'access_app', '*', 'User access', true, current_timestamp(), 'SYSTEM_INIT')