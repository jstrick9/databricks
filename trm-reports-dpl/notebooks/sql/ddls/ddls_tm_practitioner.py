# Databricks notebook source
# DBTITLE 1,Environment
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=}, {dbx_env=}")

# COMMAND ----------

# DBTITLE 1,Functions
# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,Configurations
configs = read_yaml(config_file)
target_catalog = configs["schema"]["tm_practitioner_catalog"]
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set("config.target_catalog", target_catalog)
spark.conf.set("config.dbx_env", dbutils.widgets.get("dbx_env"))
print(f"{target_catalog=}")

# COMMAND ----------

# DBTITLE 1,Bronze: dim_patron
# MAGIC %sql
# MAGIC create table if not exists ${config.target_catalog}.bronze.dim_patron (
# MAGIC   dim_patron_id BIGINT,
# MAGIC   patron_id STRING,
# MAGIC   user_acct_nm STRING,
# MAGIC   user_acct_status_tx STRING,
# MAGIC   given_nm STRING,
# MAGIC   family_nm STRING,
# MAGIC   middle_nm STRING,
# MAGIC   nickname_nm STRING,
# MAGIC   emp_no STRING,
# MAGIC   emp_type_nm STRING,
# MAGIC   patron_org_nm STRING,
# MAGIC   electronic_addr_locator_tx STRING,
# MAGIC   acct_type_cd STRING,
# MAGIC   src_create_ts TIMESTAMP,
# MAGIC   src_last_mod_ts TIMESTAMP,
# MAGIC   bgn_dt TIMESTAMP,
# MAGIC   end_dt TIMESTAMP,
# MAGIC   load_no BIGINT,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   source_nm STRING,
# MAGIC   distinguished_nm STRING
# MAGIC ) USING delta
# MAGIC COMMENT 'Historical dimension for patron IDs'
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${config.target_catalog}/bronze/dim_patron'
# MAGIC TBLPROPERTIES ('delta.minWriterVersion' = '7');

# COMMAND ----------

# DBTITLE 1,Silver: dim_account
# MAGIC %sql
# MAGIC create or replace table ${config.target_catalog}.silver.dim_account(
# MAGIC   account_id string not null primary key,
# MAGIC   account_patron_name string,
# MAGIC   account_patron_nickname string,
# MAGIC   account_status string,
# MAGIC   account_email string,
# MAGIC   account_creation_timestamp timestamp,
# MAGIC   account_created_before_verification_enforced boolean,
# MAGIC   last_modified_timestamp timestamp default current_timestamp,
# MAGIC   begin_effective_timestamp timestamp default current_timestamp,
# MAGIC   end_effective_timestamp timestamp default null
# MAGIC )
# MAGIC comment 'Dimension for the latest MyUSPTO accounts'
# MAGIC location
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${config.target_catalog}/silver/dim_account'
# MAGIC tblproperties ('delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

# DBTITLE 1,Silver: dim_practitioner
# MAGIC %sql
# MAGIC create or replace table ${config.target_catalog}.silver.dim_practitioner(
# MAGIC   practitioner_id string not null primary key,
# MAGIC   fk_account_id string,
# MAGIC   fk_telecom_id string,
# MAGIC   fk_email_id string,
# MAGIC   fk_address_id string,
# MAGIC   role_type string,
# MAGIC   `name` string,
# MAGIC   suffix string,
# MAGIC   professional_title string,
# MAGIC   bar_identity string,
# MAGIC   bar_state string,
# MAGIC   bar_identity_enforced string,
# MAGIC   last_modified_timestamp timestamp default current_timestamp,
# MAGIC   begin_effective_timestamp timestamp default current_timestamp,
# MAGIC   end_effective_timestamp timestamp default null
# MAGIC )
# MAGIC comment 'Dimension for practitioners'
# MAGIC location
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${config.target_catalog}/silver/dim_practitioner'
# MAGIC tblproperties ('delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

# DBTITLE 1,Silver: dim_telecom
# MAGIC %sql
# MAGIC create or replace table ${config.target_catalog}.silver.dim_telecom(
# MAGIC   telecom_id string not null,
# MAGIC   fk_practitioner_id string,
# MAGIC   telecom_number string not null,
# MAGIC   telecom_extension_number string,
# MAGIC   telecom_format_code string,
# MAGIC   telecom_type_code string,
# MAGIC   last_modified_timestamp timestamp default current_timestamp,
# MAGIC   begin_effective_timestamp timestamp default current_timestamp,
# MAGIC   end_effective_timestamp timestamp default null
# MAGIC )
# MAGIC comment 'Dimension for telephone and fax numbers'
# MAGIC location
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${config.target_catalog}/silver/dim_telecom'
# MAGIC tblproperties ('delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

# DBTITLE 1,Silver: dim_address
# MAGIC %sql
# MAGIC create or replace table ${config.target_catalog}.silver.dim_address(
# MAGIC   address_id string not null,
# MAGIC   fk_practitioner_id string,
# MAGIC   country_code string,
# MAGIC   state_code string,
# MAGIC   city_name string,
# MAGIC   postal_code string,
# MAGIC   street_line_one string,
# MAGIC   street_line_two string,
# MAGIC   last_modified_timestamp timestamp default current_timestamp,
# MAGIC   begin_effective_timestamp timestamp default current_timestamp,
# MAGIC   end_effective_timestamp timestamp default null
# MAGIC )
# MAGIC comment 'Dimension for mailing addresses of practitioners'
# MAGIC location
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${config.target_catalog}/silver/dim_address'
# MAGIC tblproperties ('delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

# DBTITLE 1,Silver: dim_email
# MAGIC %sql
# MAGIC create or replace table ${config.target_catalog}.silver.dim_email(
# MAGIC   email_id string not null primary key,
# MAGIC   fk_practitioner_id string,
# MAGIC   email string not null,
# MAGIC   email_domain string not null,
# MAGIC   email_code string not null,
# MAGIC   last_modified_timestamp timestamp default current_timestamp,
# MAGIC   begin_effective_timestamp timestamp default current_timestamp,
# MAGIC   end_effective_timestamp timestamp default null
# MAGIC )
# MAGIC comment 'Dimension for email addresses of practitioners'
# MAGIC location
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${config.target_catalog}/silver/dim_email'
# MAGIC tblproperties ('delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

# DBTITLE 1,Gold: practitioner
# MAGIC %sql
# MAGIC create table if not exists ${config.target_catalog}.gold.practitioner (
# MAGIC   `name` string,
# MAGIC   role_type string,
# MAGIC   suffix string,
# MAGIC   professional_title string,
# MAGIC   bar_identity string,
# MAGIC   bar_state string,
# MAGIC   bar_identity_enforced string,
# MAGIC   telecom_number string,
# MAGIC   telecom_extension_number string,
# MAGIC   telecom_format_code string,
# MAGIC   telecom_type_code string,
# MAGIC   country_code string,
# MAGIC   state_code string,
# MAGIC   city_name string,
# MAGIC   postal_code string,
# MAGIC   street_line_one string,
# MAGIC   street_line_two string,
# MAGIC   email string,
# MAGIC   email_domain string,
# MAGIC   email_code string,
# MAGIC   account_id string,
# MAGIC   account_patron_name string,
# MAGIC   account_patron_nickname string,
# MAGIC   account_status string,
# MAGIC   account_email string,
# MAGIC   account_creation_timestamp timestamp,
# MAGIC   account_created_before_verification_enforced boolean,
# MAGIC   has_link boolean
# MAGIC ) USING delta
# MAGIC COMMENT 'The aggregated TM Practitioners table.'
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${config.target_catalog}/gold/practitioner'

# COMMAND ----------

# DBTITLE 1,Verify Bronze Tables
# MAGIC %sql
# MAGIC use ${config.target_catalog}.bronze;
# MAGIC
# MAGIC show tables;

# COMMAND ----------

# DBTITLE 1,Verify Silver Tables
# MAGIC %sql
# MAGIC use ${config.target_catalog}.silver;
# MAGIC
# MAGIC show tables;

# COMMAND ----------

# DBTITLE 1,Verify Gold Tables
# MAGIC %sql
# MAGIC use ${config.target_catalog}.gold;
# MAGIC
# MAGIC show tables;
