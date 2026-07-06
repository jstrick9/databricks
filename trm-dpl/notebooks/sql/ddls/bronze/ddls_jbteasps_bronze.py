# Databricks notebook source
# MAGIC %md
# MAGIC <pre>
# MAGIC Purpose: This not ntbk executes DDL scripts to create jbteasps bronze layer tables
# MAGIC </pre>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT dbx_env DEFAULT "dev"

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file = "../../../config/"+dbutils.widgets.get("dbx_env").rstrip()+"/jbteasps-conf.yaml"
print(f'{config_file=}')
if dbx_env == "qa":
    dbutils.widgets.text("env", "test")
else:
    dbutils.widgets.text("env", dbx_env) 


# COMMAND ----------

# MAGIC %run ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# schema variables
common_configs = read_yaml(config_file)
jbteasps_catalog = common_configs["schema"]["trgt_catalog"]
data_quality_catalog = common_configs["schema"]["data_quality_catalog"]
print(f"{jbteasps_catalog=}, {data_quality_catalog=} ")
src_folder = common_configs["cdc"]["src_csv_files"]
src_database = common_configs["cdc"]["src_database"]
src_db_name = common_configs["schema"]["src_db_name"]
spark.conf.set("config.data_quality_catalog", data_quality_catalog.lower())
spark.conf.set("config.jbteasps_catalog", jbteasps_catalog.lower())
common_configs

# COMMAND ----------

database = "bronze"
control_table = "cdc_batch_job_control"
job_history_table = "cdc_batch_job_history"
catalog = jbteasps_catalog
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set("conf.catalog", jbteasps_catalog)
spark.conf.set("conf.database", database)
spark.conf.set("conf.control_table", control_table)
spark.conf.set("conf.job_history_table", job_history_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${config.jbteasps_catalog}
# MAGIC MANAGED LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/trm_jbteasps';

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${conf.catalog};
# MAGIC create schema if not exists  ${conf.database};
# MAGIC use ${conf.database};

# COMMAND ----------

# DBTITLE 1,STND_SOURCE_SYSTEM
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.jbteasps_catalog}.${conf.database}.stnd_source_system(
# MAGIC   source_system_id int,
# MAGIC   short_nm string,
# MAGIC   full_nm string,
# MAGIC   description_tx string,
# MAGIC   begin_effective_dt timestamp,
# MAGIC   end_effective_dt timestamp,
# MAGIC   create_ts timestamp,
# MAGIC   create_user_id string,
# MAGIC   last_mod_ts timestamp,
# MAGIC   last_mod_user_id string
# MAGIC ) USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_jbteasps/bronze/STND_SOURCE_SYSTEM'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled' = true, 'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# DBTITLE 1,AUDIT_LOG
# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE ${config.jbteasps_catalog}.${conf.database}.audit_log(
# MAGIC   audit_log_id int,
# MAGIC   reference_no string,
# MAGIC   serial_no int,
# MAGIC   cfk_patron_id string,
# MAGIC   ip_address_tx string,
# MAGIC   fk_transaction_type_cd string,
# MAGIC   fk_source_system_id DECIMAL(22, 5),
# MAGIC   registration_no DECIMAL(22, 15),
# MAGIC   fk_form_cd string,
# MAGIC   submission_id string,
# MAGIC   filing_id string,
# MAGIC   create_user_id string,
# MAGIC   create_ts timestamp,
# MAGIC   signatory_nm string,
# MAGIC   signatory_position_nm string,
# MAGIC   fk_signature_type_cd string,
# MAGIC   filing_dt timestamp,
# MAGIC   dn_patron_first_nm string,
# MAGIC   dn_patron_last_nm string,
# MAGIC   dn_patron_email_address_tx string,
# MAGIC   fk_proof_cd string,
# MAGIC   cfk_proceeding_no string,
# MAGIC   cfk_proceeding_type_cd string,
# MAGIC   dn_patron_middle_nm string,
# MAGIC   dn_approver_email_address_tx string,
# MAGIC   dn_patron_tm_role string
# MAGIC ) USING delta location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_jbteasps/bronze/AUDIT_LOG'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# DBTITLE 1,STND_TRANSACTION_TYPE
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.jbteasps_catalog}.${conf.database}.stnd_transaction_type(
# MAGIC   transaction_type_cd string,
# MAGIC   description_tx string,
# MAGIC   begin_effective_dt timestamp,
# MAGIC   end_effective_dt timestamp,
# MAGIC   create_ts timestamp,
# MAGIC   create_user_id string,
# MAGIC   last_mod_ts timestamp,
# MAGIC   last_mod_user_id string
# MAGIC ) USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_jbteasps/bronze/STND_TRANSACTION_TYPE'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled' = true, 'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# DBTITLE 1,INTERESTED_PARTY
# MAGIC %sql
# MAGIC create or replace table ${config.jbteasps_catalog}.${conf.database}.interested_party(
# MAGIC   cfk_patron_id string not null,
# MAGIC   cfk_individual_proofing_id string not null,
# MAGIC   begin_effective_dt timestamp not null,
# MAGIC   end_effective_dt timestamp,
# MAGIC   create_ts timestamp not null,
# MAGIC   create_user_id string not null,
# MAGIC   last_mod_ts timestamp not null,
# MAGIC   last_mod_user_id string not null,
# MAGIC   identity_proof_given_nm string,
# MAGIC   identity_proof_middle_nm string,
# MAGIC   identity_proof_family_nm string,
# MAGIC   selected_role_nm string,
# MAGIC   proofing_data_update_in string
# MAGIC ) using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_jbteasps/bronze/INTERESTED_PARTY'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled' = true, 'delta.enablechangedatafeed' = true);

# COMMAND ----------

# DBTITLE 1,SPONSORSHIP
# MAGIC %sql
# MAGIC create or replace table ${config.jbteasps_catalog}.${conf.database}.sponsorship(
# MAGIC   sponsorship_id decimal(22, 10) not null,
# MAGIC   cfk_sponsorer_id string not null,
# MAGIC   cfk_sponsoree_id string not null,
# MAGIC   cfk_approver_user_id string,
# MAGIC   cfk_initiator_id string not null,
# MAGIC   fk_sponsorship_status_id decimal(22, 5) not null,
# MAGIC   begin_effective_dt timestamp not null,
# MAGIC   end_effective_dt timestamp,
# MAGIC   create_ts timestamp not null,
# MAGIC   create_user_id string not null,
# MAGIC   last_mod_ts timestamp not null,
# MAGIC   last_mod_user_id string not null,
# MAGIC   lock_control_no decimal(22, 10) not null
# MAGIC ) using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_jbteasps/bronze/SPONSORSHIP'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled' = true, 'delta.enablechangedatafeed' = true);

# COMMAND ----------

# DBTITLE 1,SPONSORSHIP_H
# MAGIC %sql
# MAGIC create or replace table ${config.jbteasps_catalog}.${conf.database}.sponsorship_h(
# MAGIC   fk_sponsorship_id decimal(22, 10) not null,
# MAGIC   cfk_sponsorer_id string not null,
# MAGIC   cfk_sponsoree_id string not null,
# MAGIC   cfk_approver_user_id string,
# MAGIC   cfk_initiator_id string not null,
# MAGIC   fk_sponsorship_status_id decimal(22, 5) not null,
# MAGIC   begin_effective_dt timestamp not null,
# MAGIC   end_effective_dt timestamp,
# MAGIC   create_ts timestamp not null,
# MAGIC   create_user_id string not null,
# MAGIC   last_mod_ts timestamp not null,
# MAGIC   last_mod_user_id string not null,
# MAGIC   lock_control_no decimal(22, 10) not null
# MAGIC ) using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_jbteasps/bronze/SPONSORSHIP_H'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled' = true, 'delta.enablechangedatafeed' = true);

# COMMAND ----------

# DBTITLE 1,STND_FORM
# MAGIC %sql
# MAGIC create or replace table ${config.jbteasps_catalog}.${conf.database}.stnd_form(
# MAGIC   form_cd string not null,
# MAGIC   rank_no decimal(22, 10) not null,
# MAGIC   form_full_nm string not null,
# MAGIC   description_tx string,
# MAGIC   begin_effective_dt timestamp not null,
# MAGIC   end_effective_dt timestamp,
# MAGIC   create_ts timestamp not null,
# MAGIC   create_user_id string not null,
# MAGIC   last_mod_ts timestamp not null,
# MAGIC   last_mod_user_id string not null,
# MAGIC   lock_control_no decimal(22, 10) not null
# MAGIC ) using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_jbteasps/bronze/STND_FORM'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled' = true, 'delta.enablechangedatafeed' = true);

# COMMAND ----------

# DBTITLE 1,STND_PROOF_TYPE
# MAGIC %sql
# MAGIC create or replace table ${config.jbteasps_catalog}.${conf.database}.stnd_proof_type(
# MAGIC   proof_cd string not null,
# MAGIC   description_tx string,
# MAGIC   begin_effective_dt timestamp not null,
# MAGIC   end_effective_dt timestamp,
# MAGIC   create_ts timestamp not null,
# MAGIC   create_user_id string not null,
# MAGIC   last_mod_ts timestamp not null,
# MAGIC   last_mod_user_id string not null,
# MAGIC   lock_control_no decimal(22, 10) not null
# MAGIC ) using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_jbteasps/bronze/STND_PROOF_TYPE'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled' = true, 'delta.enablechangedatafeed' = true);

# COMMAND ----------

# DBTITLE 1,STND_SIGNATURE_TYPE
# MAGIC %sql
# MAGIC create or replace table ${config.jbteasps_catalog}.${conf.database}.stnd_signature_type(
# MAGIC   signature_type_cd string not null,
# MAGIC   description_tx string not null,
# MAGIC   begin_effective_dt timestamp not null,
# MAGIC   end_effective_dt timestamp,
# MAGIC   create_ts timestamp not null,
# MAGIC   create_user_id string not null,
# MAGIC   last_mod_ts timestamp not null,
# MAGIC   last_mod_user_id string not null,
# MAGIC   lock_control_no decimal(22, 10) not null
# MAGIC ) using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_jbteasps/bronze/STND_SIGNATURE_TYPE'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled' = true, 'delta.enablechangedatafeed' = true);

# COMMAND ----------

# DBTITLE 1,SPONSORSHIP_STATUS
# MAGIC %sql
# MAGIC create or replace table ${config.jbteasps_catalog}.${conf.database}.stnd_sponsorship_status(
# MAGIC   sponsorship_status_id decimal(22, 5) not null,
# MAGIC   sponsorship_status_cd string not null,
# MAGIC   description_tx string,
# MAGIC   display_order_sequence_no decimal(22, 10) not null,
# MAGIC   begin_effective_dt timestamp not null,
# MAGIC   end_effective_dt timestamp,
# MAGIC   create_ts timestamp not null,
# MAGIC   create_user_id string not null,
# MAGIC   last_mod_ts timestamp not null,
# MAGIC   last_mod_user_id string not null,
# MAGIC   lock_control_no decimal(22, 10) not null
# MAGIC ) using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_jbteasps/bronze/STND_SPONSORSHIP_STATUS'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled' = true, 'delta.enablechangedatafeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table if exists ${config.jbteasps_catalog}.${conf.database}.${conf.control_table};
# MAGIC
# MAGIC create table if not exists ${config.jbteasps_catalog}.${conf.database}.${conf.control_table} (
# MAGIC   src_folder string,
# MAGIC   catalog_name string,
# MAGIC   database_name string,
# MAGIC   table_name string,
# MAGIC   source_db_name string,
# MAGIC   source_table_name string,
# MAGIC   primary_keys string,
# MAGIC   full_load string,
# MAGIC   initial_load_finished boolean
# MAGIC ) USING delta location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_jbteasps/bronze/${conf.control_table}'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC #Initialize the dms-cdc-batch-job-control table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

table_schema = spark.table(f"{catalog}.{database}.{control_table}").schema

table_data = [
    (
        src_folder + "/" + "STND_TRANSACTION_TYPE",
        catalog,
        database,
        "stnd_transaction_type",
        src_db_name,
        "STND_TRANSACTION_TYPE",
        "TRANSACTION_TYPE_CD",
        "N",
        False,
    ),
    (
        src_folder + "/" + "AUDIT_LOG",
        catalog,
        database,
        "audit_log",
        src_db_name,
        "AUDIT_LOG",
        "fk_trademark_gid,fk_tm_employee_role_cd",
        "N",
        False,
    ),
    (
        src_folder + "/" + "STND_SOURCE_SYSTEM",
        catalog,
        database,
        "stnd_source_system",
        src_db_name,
        "STND_SOURCE_SYSTEM",
        "SOURCE_SYSTEM_ID",
        "N",
        False,
    ),
]


df = spark.createDataFrame(data=table_data, schema=table_schema)

display(df)

df.write.mode("append").saveAsTable(f"{catalog}.{database}.{control_table}")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

table_schema = spark.table(f"{catalog}.{database}.{control_table}").schema

table_data = [
    (
        src_folder + "/" + "INTERESTED_PARTY",
        catalog,
        database,
        "interested_party",
        src_db_name,
        "INTERESTED_PARTY",
        "CFK_PATRON_ID",
        "N",
        False,
    ),
    (
        src_folder + "/" + "SPONSORSHIP",
        catalog,
        database,
        "sponsorship",
        src_db_name,
        "SPONSORSHIP",
        "SPONSORSHIP_ID",
        "N",
        False,
    ),
    (
        src_folder + "/" + "SPONSORSHIP_H",
        catalog,
        database,
        "sponsorship_h",
        src_db_name,
        "SPONSORSHIP_H",
        "",
        "Y",
        False,
    ),
    (
        src_folder + "/" + "STND_FORM",
        catalog,
        database,
        "stnd_form",
        src_db_name,
        "STND_FORM",
        "FORM_CD",
        "N",
        False,
    ),
    (
        src_folder + "/" + "STND_PROOF_TYPE",
        catalog,
        database,
        "stnd_proof_type",
        src_db_name,
        "STND_PROOF_TYPE",
        "PROOF_CD",
        "N",
        False,
    ),
    (
        src_folder + "/" + "STND_SIGNATURE_TYPE",
        catalog,
        database,
        "stnd_signature_type",
        src_db_name,
        "STND_SIGNATURE_TYPE",
        "SIGNATURE_TYPE_CD",
        "N",
        False,
    ),
    (
        src_folder + "/" + "STND_SPONSORSHIP_STATUS",
        catalog,
        database,
        "sponsorship_status",
        src_db_name,
        "STND_SPONSORSHIP_STATUS",
        "SPONSORSHIP_STATUS_ID",
        "N",
        False,
    ),
]


df = spark.createDataFrame(data=table_data, schema=table_schema)

display(df)

df.write.mode("append").saveAsTable(f"{catalog}.{database}.{control_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Initialize the dms-cdc-batch-job-history table

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists ${conf.catalog}.${conf.database}.${conf.job_history_table};
# MAGIC
# MAGIC create table if not exists ${conf.catalog}.${conf.database}.${conf.job_history_table} (
# MAGIC   cdc_file_path string, meta_src_time long, cdc_file_date date, processing_time TIMESTAMP
# MAGIC ) USING delta
# MAGIC location
# MAGIC 's3://${conf.cdc_bucket}/eds/delta_tables/trm_jbteasps/bronze/${conf.job_history_table}'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled' = true, 'delta.enableChangeDataFeed' = true);
