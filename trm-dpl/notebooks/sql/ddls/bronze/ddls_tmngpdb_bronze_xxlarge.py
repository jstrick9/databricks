# Databricks notebook source
# MAGIC %md
# MAGIC <pre>
# MAGIC Purpose: This ntbk executes DDL scripts to create tmngpvtdb bronze layer tables
# MAGIC </pre>

# COMMAND ----------

# DBTITLE 1,Delta_load_xlarge_tables
# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.${conf.database}.tm_filing_basis_h(
# MAGIC fk_trademark_gid string, 
# MAGIC fk_filing_basis_cd string, 
# MAGIC current_in string, 
# MAGIC amended_in string, 
# MAGIC filed_in string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC action_ct string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_filing_basis_h'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'= true,'delta.enablechangedatafeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.${conf.database}.fsm_instance_h(
# MAGIC   fsm_instance_gid string, 
# MAGIC   fk_parent_fsm_instance_gid string, 
# MAGIC   fk_root_fsm_instance_gid string, 
# MAGIC   fk_fsm_type_id int, 
# MAGIC   fk_current_fsm_type_state_id int, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   begin_effective_ts timestamp, 
# MAGIC   end_effective_ts timestamp, 
# MAGIC   suspended_no int, 
# MAGIC   depth_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   terminated_in string)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/fsm_instance_h'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'= true,'delta.enablechangedatafeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.${conf.database}.tm_class_reference_h(
# MAGIC   fk_trademark_gid string, 
# MAGIC   fk_class_id int, 
# MAGIC   fk_referenced_class_id int, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   begin_effective_ts timestamp, 
# MAGIC   end_effective_ts timestamp, 
# MAGIC   action_ct string)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_class_reference_h'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'= true,'delta.enablechangedatafeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.${conf.database}.tm_filing_basis(
# MAGIC   fk_trademark_gid string, 
# MAGIC   fk_filing_basis_cd string, 
# MAGIC   current_in string, 
# MAGIC   amended_in string, 
# MAGIC   filed_in string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_filing_basis'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'= true,'delta.enablechangedatafeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.${conf.database}.work_item_h(
# MAGIC   work_item_gid string, 
# MAGIC   fk_work_item_type_cd string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   begin_effective_ts timestamp, 
# MAGIC   end_effective_ts timestamp, 
# MAGIC   action_ct string)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/work_item_h'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'= true,'delta.enablechangedatafeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.${conf.database}.work_item(
# MAGIC   work_item_gid string, 
# MAGIC   fk_work_item_type_cd string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/work_item'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'= true,'delta.enablechangedatafeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.${conf.database}.tm_mailing_addr_h(
# MAGIC   fk_tm_party_role_id int, 
# MAGIC   fk_mailing_address_gid string, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   action_ct string, 
# MAGIC   primary_in string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   begin_effective_ts timestamp, 
# MAGIC   end_effective_ts timestamp)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_mailing_addr_h'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'= true,'delta.enablechangedatafeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.${conf.database}.mailing_address_h(
# MAGIC   mailing_address_gid string, 
# MAGIC   name_line_1_tx string, 
# MAGIC   name_line_2_tx string, 
# MAGIC   street_line_1_tx string, 
# MAGIC   street_line_2_tx string, 
# MAGIC   city_nm string, 
# MAGIC   geographic_region_cd string, 
# MAGIC   geographic_region_nm string, 
# MAGIC   postal_cd string, 
# MAGIC   country_cd string, 
# MAGIC   country_nm string, 
# MAGIC   department_nm string, 
# MAGIC   address_type_ct string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   begin_effective_ts timestamp, 
# MAGIC   end_effective_ts timestamp, 
# MAGIC   action_ct string)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/mailing_address_h'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'= true,'delta.enablechangedatafeed' = true); 

# COMMAND ----------

# DBTITLE 1,Delta_load_xxlarge_tables
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.transaction_instance(
# MAGIC   fk_legacy_transaction_cd string, 
# MAGIC   cfk_employee_no string, 
# MAGIC   transaction_instance_gid string, 
# MAGIC   transaction_instance_id string, 
# MAGIC   effective_ts timestamp, 
# MAGIC   details_tx string, 
# MAGIC   terminated_in string, 
# MAGIC   origin_location_tx string, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/transaction_instance'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'= true,'delta.enablechangedatafeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.business_event(
# MAGIC   business_event_id int, 
# MAGIC   cfk_domain_cd string, 
# MAGIC   fk_object_type_cd string, 
# MAGIC   cfk_object_gid string, 
# MAGIC   order_no int, 
# MAGIC   effective_ts timestamp, 
# MAGIC   fk_business_event_reason_id int, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   cfk_fsm_instance_h_id int, 
# MAGIC   cfk_proceeding_no int, 
# MAGIC   document_id string, 
# MAGIC   paper_in string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/business_event'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'= true,'delta.enablechangedatafeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.work_item_object_h(
# MAGIC   fk_work_item_gid string, 
# MAGIC   fk_object_type_cd string, 
# MAGIC   cfk_object_gid string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   begin_effective_ts timestamp, 
# MAGIC   end_effective_ts timestamp, 
# MAGIC   action_ct string)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/work_item_object_h'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'= true,'delta.enablechangedatafeed' = true);
