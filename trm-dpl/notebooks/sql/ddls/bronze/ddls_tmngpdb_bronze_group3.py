# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.international_appl_reg (
# MAGIC fk_international_reg_gid string, 
# MAGIC fk_international_appl_gid string, 
# MAGIC status_cd string, 
# MAGIC status_dt timestamp, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC ib_renewal_dt timestamp, 
# MAGIC ib_publication_dt timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/international_appl_reg'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.international_appl_reg_h (
# MAGIC fk_international_reg_gid string, 
# MAGIC fk_international_appl_gid string, 
# MAGIC status_cd string, 
# MAGIC status_dt timestamp, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string, 
# MAGIC ib_renewal_dt timestamp, 
# MAGIC ib_publication_dt timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/international_appl_reg_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.international_application (
# MAGIC international_application_gid string, 
# MAGIC international_us_ref_no string, 
# MAGIC fk_electronic_address_gid string, 
# MAGIC status_cd string, 
# MAGIC status_dt timestamp, 
# MAGIC automatic_certification_in string, 
# MAGIC original_filing_dt timestamp, 
# MAGIC reply_by_dt timestamp, 
# MAGIC payment_reference_no int, 
# MAGIC lock_control_no int, 
# MAGIC payment_type_ct string, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/international_application'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.international_application_h (
# MAGIC international_application_gid string, 
# MAGIC fk_electronic_address_gid string, 
# MAGIC international_us_ref_no string, 
# MAGIC status_cd string, 
# MAGIC status_dt timestamp, 
# MAGIC automatic_certification_in string, 
# MAGIC original_filing_dt timestamp, 
# MAGIC reply_by_dt timestamp, 
# MAGIC payment_reference_no int, 
# MAGIC lock_control_no int, 
# MAGIC payment_type_ct string, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/international_application_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.international_reg_tm_h (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_international_reg_gid string, 
# MAGIC status_cd string, 
# MAGIC status_dt timestamp, 
# MAGIC priority_claimed_dt timestamp, 
# MAGIC auto_protect_dt timestamp, 
# MAGIC notification_dt timestamp, 
# MAGIC cancellation_dt timestamp, 
# MAGIC first_refusal_in string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string, 
# MAGIC ib_renewal_dt timestamp, 
# MAGIC ib_publication_dt timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/international_reg_tm_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.international_registration_h (
# MAGIC international_reg_gid string, 
# MAGIC fk_international_reg_no string, 
# MAGIC international_reg_seq_no string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/international_registration_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.international_registration (
# MAGIC international_reg_gid string, 
# MAGIC fk_international_reg_no string, 
# MAGIC international_reg_seq_no string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/international_registration'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.international_tm (
# MAGIC international_reg_no string, 
# MAGIC international_reg_dt timestamp, 
# MAGIC source_ct string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/international_tm'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.international_tm_h (
# MAGIC international_reg_no string, 
# MAGIC international_reg_dt timestamp, 
# MAGIC source_ct string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/international_tm_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.intrstd_party_relationship (
# MAGIC FK_INTERESTED_PARTY_GID string, 
# MAGIC FK_MEMBER_INTERESTED_PARTY_GID string, 
# MAGIC FK_IP_RELTNSP_TYPE_CD string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/intrstd_party_relationship'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.intrstd_party_relationship_h (
# MAGIC FK_INTERESTED_PARTY_GID string, 
# MAGIC ACTION_CT string, 
# MAGIC FK_MEMBER_INTERESTED_PARTY_GID string, 
# MAGIC FK_IP_RELTNSP_TYPE_CD string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS TIMESTAMP, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS TIMESTAMP, 
# MAGIC LAST_MOD_USER_ID string, 
# MAGIC CFK_TRANSACTION_INSTANCE_GID string, 
# MAGIC BEGIN_EFFECTIVE_TS TIMESTAMP, 
# MAGIC END_EFFECTIVE_TS TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/intrstd_party_relationship_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.ip_electronic_address (
# MAGIC fk_interested_party_gid    string,
# MAGIC fk_electronic_address_gid  string,
# MAGIC lock_control_no            int,
# MAGIC create_ts                  timestamp,
# MAGIC create_user_id             string,
# MAGIC last_mod_ts                timestamp,
# MAGIC last_mod_user_id           string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/ip_electronic_address'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.ip_mailing_address (
# MAGIC fk_interested_party_gid string,
# MAGIC fk_mailing_address_gid  string,
# MAGIC lock_control_no         int,
# MAGIC create_ts               timestamp,
# MAGIC create_user_id          string,
# MAGIC last_mod_ts             timestamp,
# MAGIC last_mod_user_id        string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/ip_mailing_address'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.ip_telecom_address (
# MAGIC FK_INTERESTED_PARTY_GID string, 
# MAGIC FK_TELECOM_ADDRESS_GID string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID timestamp, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/ip_telecom_address'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.ir_mailing_address (
# MAGIC FK_INTERNATIONAL_REG_GID string, 
# MAGIC FK_ADDRESS_TYPE_CT string, 
# MAGIC FK_SEQUENCE_NO int, 
# MAGIC FK_MAILING_ADDRESS_GID string, 
# MAGIC FK_INTERNATIONAL_APPL_GID string, 
# MAGIC FK_EMAIL_ELECTRONIC_ADDR_GID string, 
# MAGIC FK_FAX_TELECOM_ADDRESS_GID string, 
# MAGIC FK_ENTLMNT_MAILING_ADDRESS_GID string, 
# MAGIC ADDRESS_LINE_QT int, 
# MAGIC NAME_LINE_QT int, 
# MAGIC LEGAL_NATURE_TX string, 
# MAGIC NATIONALITY_COUNTRY_CD string, 
# MAGIC INCORPORATION_LOCATION_TX string, 
# MAGIC ENTITLEMENT_TYPE_CT string, 
# MAGIC ENTITLEMENT_ADDRESS_LINE_QT int, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS TIMESTAMP, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS TIMESTAMP, 
# MAGIC LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/ir_mailing_address'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.ir_mailing_address_group (
# MAGIC FK_INTERNATIONAL_REG_GID string, 
# MAGIC ADDRESS_TYPE_CT string, 
# MAGIC SEQUENCE_NO int, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS TIMESTAMP, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS TIMESTAMP, 
# MAGIC LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/ir_mailing_address_group'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

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

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.mailing_address_line (
# MAGIC fk_mailing_address_gid string, 
# MAGIC sequence_no int, 
# MAGIC address_line_ct string, 
# MAGIC address_line_tx string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/mailing_address_line'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.mailing_address_line_h (
# MAGIC fk_mailing_address_gid string, 
# MAGIC sequence_no int, 
# MAGIC address_line_ct string, 
# MAGIC address_line_tx string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/mailing_address_line_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.mv_myuspto_tram_search (
# MAGIC serial_num           string,
# MAGIC registration_num     int,
# MAGIC filing_dt            timestamp,
# MAGIC registration_dt      timestamp,
# MAGIC mark_description_tx  string,
# MAGIC owner_nm             string,
# MAGIC owner_id             string,
# MAGIC attorney_nm          string,
# MAGIC attorney_id          string,
# MAGIC dead_mark_in         string,
# MAGIC mark_drawing_cd      string,
# MAGIC search_mark_tx       string,
# MAGIC search_owner_nm      string,
# MAGIC search_attorney_nm   string,
# MAGIC proceeding_num_list  string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/mv_myuspto_tram_search'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.mv_myuspto_trm_search (
# MAGIC SERIAL_NUM int,
# MAGIC REGISTRATION_NUM int,
# MAGIC FILING_DT timestamp,
# MAGIC REGISTRATION_DT timestamp,
# MAGIC MARK_DESCRIPTION_TX string,
# MAGIC OWNER_ID string,
# MAGIC OWNER_NM string,
# MAGIC ATTORNEY_ID string,
# MAGIC ATTORNEY_NM string,
# MAGIC DEAD_MARK_IN string,
# MAGIC MARK_DRAWING_CD string,
# MAGIC SEARCH_MARK_TX string,
# MAGIC SEARCH_OWNER_NM string,
# MAGIC SEARCH_ATTORNEY_NM string,
# MAGIC PROCEEDING_NUM_LIST string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/mv_myuspto_trm_search'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.myuspto_tram_change_ntfcn (
# MAGIC serial_num int,
# MAGIC status_dt  timestamp,
# MAGIC event_dt   timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/myuspto_tram_change_ntfcn'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.myuspto_tram_event_today (
# MAGIC serial_num  int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/myuspto_tram_event_today'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.myuspto_tram_ph (
# MAGIC serial_num int,
# MAGIC event_dt   timestamp,
# MAGIC event_cd   string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/myuspto_tram_ph'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.myuspto_tram_status_today (
# MAGIC serial_num  int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/myuspto_tram_status_today'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.myuspto_trm_change_ntfcn (
# MAGIC serial_num int,
# MAGIC status_dt  timestamp,
# MAGIC event_dt   timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/myuspto_trm_change_ntfcn'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.myuspto_trm_event_today (
# MAGIC serial_num   int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/myuspto_trm_event_today'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.mv_myuspto_trm_ph (
# MAGIC serial_num  int,
# MAGIC event_cd    string,
# MAGIC event_dt    timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/mv_myuspto_trm_ph'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.mv_myuspto_tram_ph (
# MAGIC serial_num int,
# MAGIC event_cd   string,
# MAGIC event_dt   timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/mv_myuspto_tram_ph'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.mv_myuspto_tram_owner (
# MAGIC ser_num  int,
# MAGIC owner_id int,
# MAGIC owner_nm string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/mv_myuspto_tram_owner'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.mv_myuspto_tram_mark (
# MAGIC ser_num         int,
# MAGIC reg_num         int,
# MAGIC fil_dt          timestamp,
# MAGIC reg_dt          timestamp,
# MAGIC mark_tx         string,
# MAGIC dead_mark_in    string,
# MAGIC mark_drawing_cd string,
# MAGIC pn_list         string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/mv_myuspto_tram_mark'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.mv_myuspto_tram_at (
# MAGIC ser_num   int,
# MAGIC at_id     int,
# MAGIC at_nm     string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/mv_myuspto_tram_at'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.mv_myuspto_trm_owner (
# MAGIC trademark_gid string,
# MAGIC owner_id      string,
# MAGIC owner_nm      string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/mv_myuspto_trm_owner'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.mv_myuspto_trm_mark (
# MAGIC trademark_gid    string,
# MAGIC ser_num          int,
# MAGIC reg_num          int,
# MAGIC fil_dt           timestamp,
# MAGIC reg_dt           timestamp,
# MAGIC mark_tx          string,
# MAGIC dead_mark_in     string,
# MAGIC mark_drawing_cd  string,
# MAGIC pn_list          string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/mv_myuspto_trm_mark'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.mv_myuspto_trm_at (
# MAGIC trademark_gid string,
# MAGIC at_id         string,
# MAGIC at_nm         string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/mv_myuspto_trm_at'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.myuspto_trm_status_today (
# MAGIC serial_num int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/myuspto_trm_status_today'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.myuspto_trm_ph (
# MAGIC serial_num int,
# MAGIC event_dt   timestamp,
# MAGIC event_cd   string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/myuspto_trm_ph'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.object_dispatch (
# MAGIC fk_user_session_gid string, 
# MAGIC fk_object_type_cd string, 
# MAGIC fk_object_dispatch_type_cd string, 
# MAGIC cfk_object_gid string, 
# MAGIC cfk_organization_cd string, 
# MAGIC action_start_dt timestamp, 
# MAGIC action_current_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/object_dispatch'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.object_document_h (
# MAGIC fk_object_type_cd string, 
# MAGIC fk_tm_document_id int, 
# MAGIC cfk_object_gid string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/object_document_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.object_document (
# MAGIC fk_object_type_cd string, 
# MAGIC fk_tm_document_id int, 
# MAGIC cfk_object_gid string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/object_document'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.object_fsm_instance (
# MAGIC fk_cur_ste_ofc_actvty_rsn_cd string, 
# MAGIC fk_object_type_cd string, 
# MAGIC cfk_object_gid string, 
# MAGIC cfk_root_fsm_instance_gid string, 
# MAGIC cfk_current_fsm_type_state_id int, 
# MAGIC current_examination_no int, 
# MAGIC sou_last_extension_no int, 
# MAGIC exparte_appeal_active_in string, 
# MAGIC last_action_no int, 
# MAGIC current_registration_rnwl_no int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/object_fsm_instance'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.office_activity (
# MAGIC fk_work_item_gid string, 
# MAGIC issue_dt timestamp, 
# MAGIC issue_empe_no string, 
# MAGIC examination_no int, 
# MAGIC action_no int, 
# MAGIC partial_refusal_in string, 
# MAGIC full_refusal_override_in string, 
# MAGIC response_received_in string, 
# MAGIC response_on_time_in string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC partial_abandonment_in string, 
# MAGIC partial_abandonment_ovrd_in string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/office_activity'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.office_activity_h (
# MAGIC fk_work_item_gid string, 
# MAGIC issue_dt timestamp, 
# MAGIC issue_empe_no string, 
# MAGIC examination_no int, 
# MAGIC action_no int, 
# MAGIC partial_refusal_in string, 
# MAGIC full_refusal_override_in string, 
# MAGIC response_received_in string, 
# MAGIC response_on_time_in string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string, 
# MAGIC partial_abandonment_in string, 
# MAGIC partial_abandonment_ovrd_in string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/office_activity_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.office_activity_draft_document (
# MAGIC fk_work_item_gid string, 
# MAGIC fk_draft_document_id int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/office_activity_draft_document'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.office_activity_draft_doc_h (
# MAGIC fk_work_item_gid string, 
# MAGIC fk_draft_document_id int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/office_activity_draft_doc_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.office_activity_reason (
# MAGIC fk_work_item_gid string, 
# MAGIC fk_office_activity_reason_cd string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/office_activity_reason'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.office_activity_reason_h (
# MAGIC fk_work_item_gid string, 
# MAGIC fk_office_activity_reason_cd string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/office_activity_reason_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.office_activity_review (
# MAGIC office_activity_review_id decimal(22,0), 
# MAGIC fk_work_item_gid string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC review_type_ct string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/office_activity_review'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------


