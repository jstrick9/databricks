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
target_catalog = "myuspto_dev" if dbx_env != "prod" else "myuspto"
cdc_bucket = configs["cdc"]["cdc_bucket"]
spark.conf.set("config.cdc_bucket", cdc_bucket)
spark.conf.set("config.catalog", target_catalog)
spark.conf.set("config.dbx_env", dbutils.widgets.get("dbx_env"))
print(f"{target_catalog=}, {cdc_bucket=}") 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog Setup

# COMMAND ----------

# DBTITLE 1,Create: MyUSPTO Catalog
# MAGIC %sql
# MAGIC create catalog if not exists ${config.catalog} managed location 's3://${config.cdc_bucket}/delta_tables/${config.catalog}';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Setup

# COMMAND ----------

# DBTITLE 1,Create: Bronze Schema
# MAGIC %sql
# MAGIC create schema if not exists ${config.catalog}.bronze comment 'Bronze layer MyUSPTO derived data.';

# COMMAND ----------

# DBTITLE 1,Create: Silver Schema
# MAGIC %sql
# MAGIC create schema if not exists ${config.catalog}.silver comment 'Silver layer MyUSPTO derived data.';

# COMMAND ----------

# DBTITLE 1,Create: Gold Schema
# MAGIC %sql
# MAGIC create schema if not exists ${config.catalog}.gold comment 'Gold layer MyUSPTO derived data.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze

# COMMAND ----------

# DBTITLE 1,Table: attorney_bar
# MAGIC %sql
# MAGIC create or replace table ${config.catalog}.bronze.attorney_bar (
# MAGIC   attorney_bar_id bigint
# MAGIC     not null
# MAGIC     comment 'The system generated surrogate key that uniquely identifies an instance of an Attorney Bar information.',
# MAGIC   fk_interested_party_id bigint
# MAGIC     not null
# MAGIC     comment 'The system generated surrogate key that uniquely identifies an instance of the identity of a person or organization. Source:  10/21/12 PTO Work Sessions  (9/30/97, 5/16/97, 1/30/97, 10/18/96, 7/16/96)',
# MAGIC   attorney_bar_no string comment 'This is the Attorney bar number.',
# MAGIC   geo_region_id bigint
# MAGIC     comment 'GEO_REGION_ID is the identifier in ICT for the Geographic Region.  Source: MyUSPTO registration meeting February 19, 2014',
# MAGIC   bar_number_assc_dt date comment 'The date the bar number information was inserted.',
# MAGIC   certify_in string not null comment 'Indicates certification status. Y or N.',
# MAGIC   create_ts timestamp
# MAGIC     not null
# MAGIC     comment 'The date and time the record was created in the database. Source: audit field.',
# MAGIC   create_user_id bigint
# MAGIC     not null
# MAGIC     comment 'The user id of the account which created this record in the database. Source: audit field.',
# MAGIC   last_mod_ts timestamp
# MAGIC     not null
# MAGIC     comment 'The date and time the record was last modified in the database. Source: audit field.',
# MAGIC   last_mod_user_id bigint
# MAGIC     not null
# MAGIC     comment 'The user id of the account which last modified this record in the database. Source: audit field.',
# MAGIC   lock_control_no bigint
# MAGIC     not null
# MAGIC     default 0
# MAGIC     comment 'System generated sequential number used for Optimistic Locking ensuring concurrency and preventing lock contention.  Default value is 0. Definition Source: Meeting Minutes 06/06/2012',
# MAGIC   atty_bar_mbrshp_yr_no int comment 'This is the Year of Admission to the Bar Membership'
# MAGIC )
# MAGIC   using delta
# MAGIC   location 's3://${config.cdc_bucket}/delta_tables/${config.catalog}/bronze/attorney_bar'
# MAGIC   tblproperties (
# MAGIC     'databricks.delta.autocompact.enabled' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.enableDeletionVectors' = 'true',
# MAGIC     'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC     'delta.feature.changeDataFeed' = 'supported',
# MAGIC     'delta.feature.deletionVectors' = 'supported',
# MAGIC     'delta.feature.invariants' = 'supported',
# MAGIC     'delta.minReaderVersion' = '3',
# MAGIC     'delta.minWriterVersion' = '7'
# MAGIC   );

# COMMAND ----------

# DBTITLE 1,Table: activity_log
# MAGIC %sql
# MAGIC create or replace table ${config.catalog}.bronze.activity_log (
# MAGIC   activity_log_id bigint
# MAGIC     not null
# MAGIC     comment 'The system generated surrogate key that uniquely identifies an instance of the Activity Log',
# MAGIC   fk_interested_party_id bigint
# MAGIC     not null
# MAGIC     comment 'Foreign key to the record of the Interested Party to which the activity log belongs to.',
# MAGIC   fk_helpdesk_user_id bigint
# MAGIC     comment 'Foreign key to the record of the interested party, a helpdesk user, who helped the user execute their activity.',
# MAGIC   activity_desc_tx string
# MAGIC     not null
# MAGIC     comment 'A concatenated string of readable text, describing the activity.',
# MAGIC   fk_activity_cd string not null comment 'The kind of activity which was performed.',
# MAGIC   mod_user_ip_address_tx string comment 'IP address of the user who did the activity.',
# MAGIC   activity_ts timestamp not null comment 'Timestamp of when the activity took place.',
# MAGIC   fk_activity_status_cd string not null comment 'Activity status code.'
# MAGIC )
# MAGIC   using delta
# MAGIC   location 's3://${config.cdc_bucket}/delta_tables/${config.catalog}/bronze/activity_log'
# MAGIC   tblproperties (
# MAGIC     'databricks.delta.autocompact.enabled' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.enableDeletionVectors' = 'true',
# MAGIC     'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC     'delta.feature.changeDataFeed' = 'supported',
# MAGIC     'delta.feature.deletionVectors' = 'supported',
# MAGIC     'delta.feature.invariants' = 'supported',
# MAGIC     'delta.minReaderVersion' = '3',
# MAGIC     'delta.minWriterVersion' = '7'
# MAGIC   )
# MAGIC   comment 'A log to track the activity of the Interested Party, so that a report can be created and the user has access to all changes to their MyUSPTO account. The log data cannot be updated or deleted. Only new activity log records are added in the table.';

# COMMAND ----------

# DBTITLE 1,Table: login_user
# MAGIC %sql
# MAGIC create or replace table ${config.catalog}.bronze.login_user (
# MAGIC   fk_interested_party_id bigint
# MAGIC     not null
# MAGIC     comment 'The system generated surrogate key that uniquely identifies an instance of the identity of a person or organization. Source:  10/21/12 PTO Work Sessions  (9/30/97, 5/16/97, 1/30/97, 10/18/96, 7/16/96)',
# MAGIC   email_comnctn_id string
# MAGIC     comment 'EMAIL_COMNCTN_ID is a unique string to represent e-mail communication identifier. This value is used for identifying a MyUPSTO user in the MyUSPTO Account activation and Password reset email URL sent to registered interested party to verify their MyUSPTO account. Source: data requirements Meeting - October 9, 2014 and  October 21, 2014',
# MAGIC   email_comnctn_ts timestamp
# MAGIC     comment 'EMAIL_COMNCTN_TS is the date and time the EMAIL_COMNCTN_ID was created in the database. Source: data requirements Meeting - October 9, 2014 and October 21, 2014',
# MAGIC   user_account_nm string
# MAGIC     not null
# MAGIC     comment 'USER_ACCOUNT_NM is the login user account name used when registering with MyUSPTO.  User is required to enter a login user account name and the name must be unique.  Since user has the option to use Email as the login account name, the data length is same as defined in Email. Source: Profile Collection Meeting March 13, 2013',
# MAGIC   uspto_terms_cond_acptnc_in string
# MAGIC     not null
# MAGIC     comment 'USPTO_TERMS_COND_ACPTNC_IN is the indicator to record the acceptance/non acceptance of USPTO Terms and conditions. Source: Alignment to SSPAMS PDM model dated 7/9/2014',  uspto_terms_cond_acptnc_dt date
# MAGIC     comment 'USPTO_TERMS_COND_ACPTNC_DT is the data on which the acceptance/non acceptance of USPTO Terms and conditions was recorded. Source: Alignment to SSPAMS PDM model dated 7/9/2014',
# MAGIC   last_login_ts timestamp
# MAGIC     comment 'Records the timestamp when the user last logged in. The field will remain null until the user verifies the email address and logs in.',
# MAGIC   create_ts timestamp
# MAGIC     not null
# MAGIC     comment 'The date and time the record was created in the database. Source: audit field.',
# MAGIC   create_user_id bigint
# MAGIC     not null
# MAGIC     comment 'The user id of the account which created this record in the database. Source: audit field.',
# MAGIC   last_mod_ts timestamp
# MAGIC     not null
# MAGIC     comment 'The date and time the record was last modified in the database. Source: audit field.',
# MAGIC   last_mod_user_id bigint
# MAGIC     not null
# MAGIC     comment 'The user id of the account which last modified this record in the database. Source: audit field.',
# MAGIC   lock_control_no bigint
# MAGIC     not null
# MAGIC     default 0
# MAGIC     comment 'System generated sequential number used for Optimistic Locking ensuring concurrency and preventing lock contention.  Default value is 0. Definition Source: Meeting Minutes 06/06/2012',
# MAGIC   efs_web_user_in string not null default 'N' comment 'This is the the EFS Web user indicator.',
# MAGIC   account_activated_in string
# MAGIC     comment 'The indicator that indicate the user has activated his/her account to complete the user registration process.'
# MAGIC )
# MAGIC   using delta
# MAGIC   location 's3://${config.cdc_bucket}/delta_tables/${config.catalog}/bronze/login_user'
# MAGIC   tblproperties (
# MAGIC     'databricks.delta.autocompact.enabled' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.enableDeletionVectors' = 'true',
# MAGIC     'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC     'delta.feature.changeDataFeed' = 'supported',
# MAGIC     'delta.feature.deletionVectors' = 'supported',
# MAGIC     'delta.feature.invariants' = 'supported',
# MAGIC     'delta.minReaderVersion' = '3',
# MAGIC     'delta.minWriterVersion' = '7'
# MAGIC   )
# MAGIC   comment 'Login user will be created when a user is registered in MyUSPTO.  User must enter a USER_ACCOUNT_NM (using email address is recommended however is not enforced) and must be a unique name.  An individual can have only one login USER_ACCOUNT_NM if registered or no login USER_ACCOUNT_NM, if not registered.  There may be individuals who are interested parties but not registered users.  For example, an owner is an interested party but may not be a registered user, he/she has attorney handling the case and the attorney is the registered user. Source: Profile Collection Meeting March 13, 2013';

# COMMAND ----------

# DBTITLE 1,Table: interested_party
# MAGIC %sql
# MAGIC create or replace table ${config.catalog}.bronze.interested_party (
# MAGIC   interested_party_id bigint
# MAGIC     not null
# MAGIC     comment 'The system generated surrogate key that uniquely identifies an instance of the identity of a person or organization. Source:  10/21/12 PTO Work Sessions  (9/30/97, 5/16/97, 1/30/97, 10/18/96, 7/16/96)',
# MAGIC   cfk_patron_id string
# MAGIC     not null
# MAGIC     comment 'Patron ID is a 36 byte randomly generated character string to uniquely identify Patrons of USPTO. This ID is generated by a free standing ID generator.',
# MAGIC   fk_legal_entity_type_cd string
# MAGIC     comment 'Uniquely identified code to indicate the type of Entity as defined in TEAS Plus Application: Individual, Corporation, Limited Liability Company, Partnership, Limited Partnership, Joint Venture, Sole Proprietorship, Trust, Estate and Other. Source: Profile Collection Meeting March 13, 2013',
# MAGIC   interested_party_ct string
# MAGIC     not null
# MAGIC     default 'I'
# MAGIC     comment 'INTERESTED_PARTY_CT is the category of Interested Party whether it is an individual (Natural Person), organization (Juristic Entity) or Statutory (State or employee of a State or instrumentality of a State acting in his or her official capacity when a specific identity is not known).  The categories are I (Individual), O (Organization), S (Statutory) and U (USPTO internal user).  Default value is I for user registration.  Set to O or S for user migration. Source: EDB Modeling session November 30, 2012',
# MAGIC   preferred_contact_ct string
# MAGIC     not null
# MAGIC     default 'E'
# MAGIC     comment 'PREFERRED_CONTACT_CT is the category of preferred method for contact whether it is Electronic (E), Telecommunication (T), Mailing address (M) or No preference (N).  In the case of user migration, set the preference to N if no preference is available. Source: EDB Modeling session November 30, 2012',
# MAGIC   primary_country_id bigint
# MAGIC     comment 'PRIMARY_COUNTRY_ID is the identifier that represents a country in the ICT database. ICT web service call is used to get the code and the country name.  Country name is not stored physically, and should be kept in the cache and refreshed as necessary for effecient retrieval.  In the case of Individual it is the country of his or her citizenship (Nationality) as in Trademark or country of residence as in Patent; in the case of Organization it is the country in which it is incorporated (Origin).  This is not the same country that individual and organization are doing business. Source: User Registration UI - Meeting Minutes April 05, 2013',
# MAGIC   legal_statement_tx string
# MAGIC     comment 'The text of legal entity describing the nature of the legal business. Source:  10/21/12 PTO Work Sessions  (9/30/97, 5/16/97, 1/30/97, 10/18/96, 7/16/96)',
# MAGIC   begin_effective_dt date
# MAGIC     not null
# MAGIC     comment 'The date Interested Party is in effect. Source: EDB Modeling session November 30, 2012',
# MAGIC   end_effective_dt date
# MAGIC     comment 'The date Interested Party is no longer in effect. Source: EDB Modeling session November 30, 2012',
# MAGIC   create_ts timestamp
# MAGIC     not null
# MAGIC     comment 'The date and time the record was created in the database. Source: audit field.',
# MAGIC   create_user_id bigint
# MAGIC     not null
# MAGIC     comment 'The user id of the account which created this record in the database. Source: audit field.',
# MAGIC   last_mod_ts timestamp
# MAGIC     not null
# MAGIC     comment 'The date and time the record was last modified in the database. Source: audit field.',
# MAGIC   last_mod_user_id bigint
# MAGIC     not null
# MAGIC     comment 'The user id of the account which last modified this record in the database. Source: audit field.',
# MAGIC   lock_control_no bigint
# MAGIC     not null
# MAGIC     default 0
# MAGIC     comment 'LOCK_CONTROL_NO is a system generated sequential number used for Optimistic Locking ensuring concurrency and preventing lock contention.  Default value is 0. Definition Source: Meeting Minutes 06/06/2012',
# MAGIC   cfk_external_id string
# MAGIC     comment 'External ID is a 50 byte randomly generated character string to uniquely identify ID.ME UUID.'
# MAGIC )
# MAGIC   using delta
# MAGIC   location 's3://${config.cdc_bucket}/delta_tables/${config.catalog}/bronze/interested_party'
# MAGIC   tblproperties (
# MAGIC     'databricks.delta.autocompact.enabled' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.enableDeletionVectors' = 'true',
# MAGIC     'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC     'delta.feature.changeDataFeed' = 'supported',
# MAGIC     'delta.feature.deletionVectors' = 'supported',
# MAGIC     'delta.feature.invariants' = 'supported',
# MAGIC     'delta.minReaderVersion' = '3',
# MAGIC     'delta.minWriterVersion' = '7'
# MAGIC   )
# MAGIC   comment 'A PTO Interested Party is an entity pertaining to a customer doing business with the PTO.  The entity may be an individual or an organization who: 1)  Has business with PTO in regards to U.S. national patent or international patent';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# DBTITLE 1,Verify Bronze Tables
# MAGIC %sql
# MAGIC use ${config.catalog}.bronze;
# MAGIC
# MAGIC show tables;

# COMMAND ----------

# DBTITLE 1,Verify Silver Tables
# MAGIC %sql
# MAGIC use ${config.catalog}.silver;
# MAGIC
# MAGIC show tables;

# COMMAND ----------

# DBTITLE 1,Verify Gold Tables
# MAGIC %sql
# MAGIC use ${config.catalog}.gold;
# MAGIC
# MAGIC show tables;
