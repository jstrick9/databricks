# Databricks notebook source
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file = f"../../config/{dbx_env}/tdet-conf.yaml"

print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ../../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

configs = read_yaml(config_file)
tdet_catalog = configs['schema']['trgt_catalog']
certificate_bucket = configs['s3']['certificate_bucket']
spark.conf.set('config.certificate_bucket', certificate_bucket)
spark.conf.set('config.tdet_catalog', tdet_catalog)
spark.conf.set('config.dbx_env', dbutils.widgets.get('dbx_env'))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${config.tdet_catalog} MANAGED LOCATION 's3://${config.certificate_bucket}/delta_tables/${config.tdet_catalog}';

# COMMAND ----------

# MAGIC %sql 
# MAGIC GRANT ALL PRIVILEGES ON CATALOG ${config.tdet_catalog} TO `e5229241-17a2-4935-8626-0e0db3b81fc7`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${config.tdet_catalog}.gold COMMENT 'For TDET gold layer data';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE ${config.tdet_catalog}.gold.search (
# MAGIC   serial_num INT COMMENT "The number that is assigned to a trademark application at the time of receipt at the Patent Trademark Office",
# MAGIC   mark_tx STRING COMMENT "The text of a design mark if it is a standard character mark or typed drawing",
# MAGIC   filing_date DATE COMMENT "The date on which all the elements set forth in 37 C.F.R. ¿2.21(a) are received in the USPTO",
# MAGIC   filed_bases STRING COMMENT "The code that represents and/or denotes a filing basis if the filing basis was filed with the trademark application",
# MAGIC   current_bases STRING COMMENT "The code that represents and/or denotes a filing basis if the filing basis is currently active for the trademark",
# MAGIC   registration_number STRING COMMENT "The number assigned to a trademark application when the mark is officially registered",
# MAGIC   registration_date DATE COMMENT "The date when an application mark is officially registered",
# MAGIC   owner_name STRING COMMENT "The current owner (or joint owner) name associated with the trademark",
# MAGIC   hist_owner_nm STRING COMMENT "The historical owner(s) (or joint owner(s)) name(s) associated with the trademark; multiple previous historical owners' names are separated by a semi-colon (';')",
# MAGIC   owner_address STRING COMMENT "The current owner's (or joint owner's) mailing address associated with the trademark",
# MAGIC   owner_country STRING COMMENT "The current owner's (or joint owner's) country of citizenship associated with the trademark",
# MAGIC   owner_email STRING COMMENT "The current owner's (or joint owner's) email address associated with the trademark",
# MAGIC   hist_owner_email STRING COMMENT "The historical owner(s) (or joint owner(s)) email(s) associated with the trademark; multiple previous historical owners' emails are separated by a semi-colon (';')",
# MAGIC   owner_phone STRING COMMENT "The current owner's (or joint owner's) office telephone number associated with the trademark",
# MAGIC   attorney_name STRING COMMENT "The current attorney's name associated with the trademark",
# MAGIC   hist_attorney_nm STRING COMMENT "The historical attorneys' name(s) associated with the trademark; multiple previous historical attorneys' names are separated by a semi-colon (';')",
# MAGIC   attorney_membership_no STRING COMMENT "The current attorney's bar membership number associated with the trademark",
# MAGIC   attorney_address STRING COMMENT "The current attorney's mailing address associated with the trademark",
# MAGIC   attorney_email STRING COMMENT "The current attorney's email address associated with the trademark",
# MAGIC   hist_at_email STRING COMMENT "The historical attorney(s) email(s) associated with the trademark; multiple previous historical attorney emails are separated by a semi-colon (';')",
# MAGIC   attorney_phone STRING COMMENT "The current attorney's office telephone number associated with the trademark",
# MAGIC   docket_number STRING COMMENT "The current docket number derived from the external reference text associated with a trademark",
# MAGIC   correspondent_name STRING COMMENT "The current correspondent's name associated with the trademark",
# MAGIC   correspondent_address STRING COMMENT "The current correspondent's mailing address associated with the trademark",
# MAGIC   firm_name STRING COMMENT "The current correspondent's firm name associated with the trademark",
# MAGIC   hist_cr_nm STRING COMMENT "The historical correspondents' name(s) associated with the trademark; multiple previous historical correspondents' names are separated by a semi-colon (';')",
# MAGIC   correspondent_email STRING COMMENT "The current correspondent's email address associated with the trademark",
# MAGIC   hist_cr_email STRING COMMENT "The historical correspondent(s) email(s) associated with the trademark; multiple previous historical correspondent emails are separated by a semi-colon (';')",
# MAGIC   correspondent_phone STRING COMMENT "The current correspondent's office telephone number associated with the trademark",
# MAGIC   secondary_cor_email STRING COMMENT "The current secondary correspondents' email address(es) associated with the trademark; multiple secondary correspondents' email addresses are separated by a semi-colon (';')",
# MAGIC   domestic_representative_name STRING COMMENT "The current domestic representative's name associated with the trademark",
# MAGIC   hist_dr_nm STRING COMMENT "The historical domestic representatives' name(s) associated with the trademark; multiple previous historical domestic representatives' names are separated by a semi-colon (';')",
# MAGIC   domestic_representative_email STRING COMMENT "The current domestic representative's email address",
# MAGIC   hist_dr_email STRING COMMENT "The historical domestic representative(s) email(s) associated with the trademark; multiple previous historical domestic representative emails are separated by a semi-colon (';')",
# MAGIC   domestic_rep_phone STRING COMMENT "The current domestic representative's office telephone number associated with the trademark",
# MAGIC   examiner_number INT COMMENT "The current examining attorney's employee number associated with the trademark",
# MAGIC   examiner_name STRING COMMENT "The current examining attorney's name associated with the trademark",
# MAGIC   law_office STRING COMMENT "The current examining attorney's law office number associated with the trademark",
# MAGIC   class_list STRING COMMENT "The current goods and services class associated with the trademark; multiple classes are separated by a semi-colon (';')",
# MAGIC   status STRING COMMENT "The current TRAM_AM_AM.STAT code associate with the trademark",
# MAGIC   status_date DATE COMMENT "The date on which the current status of the trademark case was reported to the system",
# MAGIC   og_issue_date DATE COMMENT "The date on which the Official Gazette was issued or will issue the associated trademark",
# MAGIC   og_status STRING COMMENT "The unique 3 character alphanumeric code that represents and/or denotes the current state of the trademark in the photocomposition process",
# MAGIC   og_catg INT COMMENT "A unique code used in TRAM that is assigned to a publication category, subcategory combination",
# MAGIC   intl_reg_num STRING COMMENT "The flag ('Y' | 'N') indicating that at least one international registration number has been assigned by the International Bureau (IB)",
# MAGIC   international_us_ref_no STRING COMMENT "The flag ('Y' | 'N') indicating that at least one control number has been assigned to the international application by the USPTO",
# MAGIC   specimen_url STRING COMMENT "A website url associated with current trademark",
# MAGIC   create_user INT COMMENT "The user identifier for inserting a record in the table",
# MAGIC   create_dt DATE COMMENT "The date when a record was inserted into the table"
# MAGIC ) USING delta 
# MAGIC COMMENT 'The search table contains current and historical information related to trademark applications and registrations. It is used as the source table for the TDET API: https://tdet.uspto.gov/.'
# MAGIC LOCATION 's3://${config.certificate_bucket}/delta_tables/${config.tdet_catalog}/gold/search' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = 'true',
# MAGIC   'delta.checkpoint.writeStatsAsJson' = 'false',
# MAGIC   'delta.checkpoint.writeStatsAsStruct' = 'true',
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '4'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC # Datebricks TDET APP Tables

# COMMAND ----------

# DBTITLE 1,TDET APP - File History
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${config.tdet_catalog}.gold.tdet_app_file_history (
# MAGIC     search_id STRING NOT NULL               COMMENT 'Unique ID for each search execution',
# MAGIC     matter_number STRING NOT NULL           COMMENT 'User provided matter number',
# MAGIC     comments STRING NOT NULL                COMMENT 'User-provided notes about the search',
# MAGIC     record_count INT NOT NULL               COMMENT 'Number of records expected from the search',
# MAGIC     input_file_name STRING NOT NULL         COMMENT 'Name of the uploaded input file',
# MAGIC     output_file_name STRING NOT NULL        COMMENT 'Generated output file name',
# MAGIC     created_user_name STRING NOT NULL       COMMENT 'Name of the person who ran the search',
# MAGIC     created_user_email STRING NOT NULL      COMMENT 'Email of the person who ran the search',
# MAGIC     created_timestamp TIMESTAMP NOT NULL    COMMENT 'Timestamp when the search was created',
# MAGIC     search_config_json STRING               COMMENT 'JSON string of search parameters for re-runs',
# MAGIC     CONSTRAINT pk_search_history PRIMARY KEY (search_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'TDET application search execution metadata and file tracking'
# MAGIC LOCATION 's3://${config.certificate_bucket}/delta_tables/${config.tdet_catalog}/gold/tdet_app_file_history';

# COMMAND ----------

# DBTITLE 1,TDET APP - Search History
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${config.tdet_catalog}.gold.tdet_app_search_history (
# MAGIC     id STRING NOT NULL                      COMMENT 'Unique ID for each input record',
# MAGIC     search_id STRING NOT NULL               COMMENT 'Reference to the parent search (tdet_app_file_history.search_id)',
# MAGIC     serial_number INT NOT NULL              COMMENT 'Trademark serial number provided in the input file',
# MAGIC     input_file_name STRING NOT NULL         COMMENT 'Name of the uploaded input file',
# MAGIC     created_user_email STRING NOT NULL      COMMENT 'Email of the person who ran the search',
# MAGIC     created_timestamp TIMESTAMP NOT NULL    COMMENT 'Timestamp when the record was created',
# MAGIC     CONSTRAINT pk_input_file PRIMARY KEY (id),
# MAGIC     CONSTRAINT fk_input_search FOREIGN KEY (search_id)
# MAGIC         REFERENCES ${config.tdet_catalog}.gold.tdet_app_file_history (search_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'TDET application input of serial numbers for each search execution'
# MAGIC LOCATION 's3://${config.certificate_bucket}/delta_tables/${config.tdet_catalog}/gold/tdet_app_search_history';

# COMMAND ----------

# DBTITLE 1,TDET APP - Search History Detail
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${config.tdet_catalog}.gold.tdet_app_search_history_detail (
# MAGIC     id STRING NOT NULL                              COMMENT 'Unique ID for each output record',
# MAGIC     search_id STRING NOT NULL                       COMMENT 'Reference to the parent search (tdet_search_history.search_id)',
# MAGIC     output_file_name STRING NOT NULL                COMMENT 'Name of the output file this record belongs to',
# MAGIC     serial_number INT NOT NULL                      COMMENT 'Trademark application serial number',
# MAGIC     mark_tx STRING                                  COMMENT 'Text of the mark',
# MAGIC     filing_date DATE                                COMMENT 'Trademark filing date',
# MAGIC     filed_bases STRING                              COMMENT 'Original legal bases for filing (e.g., 1A, 1B)',
# MAGIC     current_bases STRING                            COMMENT 'Current legal bases after amendments',
# MAGIC     registration_number INT                         COMMENT 'Trademark registration number',
# MAGIC     registration_date DATE                          COMMENT 'Date of trademark registration',
# MAGIC     owner_name STRING                               COMMENT 'Current owner name',
# MAGIC     owner_name_historical STRING                    COMMENT 'Historical owner name(s)',
# MAGIC     owner_address STRING                            COMMENT 'Current owner address',
# MAGIC     owner_country STRING                            COMMENT 'Country of the owner',
# MAGIC     owner_email STRING                              COMMENT 'Current owner email',
# MAGIC     owner_email_historical STRING                   COMMENT 'Historical owner email(s)',
# MAGIC     owner_phone STRING                              COMMENT 'Owner phone number',
# MAGIC     attorney_membership_number STRING               COMMENT 'Attorney bar membership number',
# MAGIC     attorney_name STRING                            COMMENT 'Current attorney of record',
# MAGIC     attorney_name_historical STRING                 COMMENT 'Historical attorney(s)',
# MAGIC     attorney_address STRING                         COMMENT 'Attorney mailing address',
# MAGIC     attorney_email STRING                           COMMENT 'Attorney email address',
# MAGIC     attorney_email_historical STRING                COMMENT 'Historical attorney email(s)',
# MAGIC     attorney_phone STRING                           COMMENT 'Attorney phone number',
# MAGIC     correspondent_name STRING                       COMMENT 'Current correspondent name',
# MAGIC     correspondent_name_historical STRING            COMMENT 'Historical correspondent(s)',
# MAGIC     correspondent_address STRING                    COMMENT 'Correspondent address',
# MAGIC     correspondent_email STRING                      COMMENT 'Correspondent email',
# MAGIC     correspondent_email_secondary STRING            COMMENT 'Secondary correspondent email',
# MAGIC     correspondent_email_historical STRING           COMMENT 'Historical correspondent email(s)',
# MAGIC     correspondent_phone STRING                      COMMENT 'Correspondent phone number',
# MAGIC     domestic_representative_name STRING             COMMENT 'Domestic representative name (if applicable)',
# MAGIC     domestic_representative_name_historical STRING  COMMENT 'Historical domestic representative(s)',
# MAGIC     domestic_representative_email STRING            COMMENT 'Domestic representative email',
# MAGIC     domestic_representative_email_historical STRING COMMENT 'Historical domestic representative email(s)',
# MAGIC     domestic_representative_phone STRING            COMMENT 'Domestic representative phone',
# MAGIC     examiner_number STRING                          COMMENT 'USPTO examiner number',
# MAGIC     examiner_name STRING                            COMMENT 'USPTO examiner name',
# MAGIC     docket_number STRING                            COMMENT 'Internal firm docket number',
# MAGIC     firm_name STRING                                COMMENT 'Firm name associated with the filing',
# MAGIC     law_office STRING                               COMMENT 'Law office assigned',
# MAGIC     class_list STRING                               COMMENT 'International/Nice classification list',
# MAGIC     status STRING                                   COMMENT 'Trademark case status',
# MAGIC     status_date DATE                                COMMENT 'Date status was last updated',
# MAGIC     og_issue_date DATE                              COMMENT 'Official Gazette issue date',
# MAGIC     og_status STRING                                COMMENT 'Official Gazette status',
# MAGIC     og_category STRING                              COMMENT 'Official Gazette publication category',
# MAGIC     international_registration_number STRING        COMMENT 'International registration number (if Madrid Protocol)',
# MAGIC     international_us_reference_number STRING        COMMENT 'U.S. reference number for international applications',
# MAGIC     specimen_url STRING                             COMMENT 'Link to specimen image/document',
# MAGIC     what_matched STRING                             COMMENT 'Criteria that matched this record',
# MAGIC     created_date DATE                               COMMENT 'Date record was created in silver.tdet_app_search DB table.',
# MAGIC     created_user_email STRING                       COMMENT 'Email of user who created this record',
# MAGIC     natural_key_hash STRING NOT NULL                COMMENT 'Hash of the natural key from the source system',
# MAGIC     record_data_hash STRING NOT NULL                COMMENT 'Hash of the data values from the source record',
# MAGIC     _created_timestamp TIMESTAMP                    COMMENT 'Timestamp when this record was created',
# MAGIC     CONSTRAINT pk_output_file PRIMARY KEY (id),
# MAGIC     CONSTRAINT fk_output_search FOREIGN KEY (search_id)
# MAGIC         REFERENCES ${config.tdet_catalog}.gold.tdet_app_file_history (search_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (search_id)
# MAGIC COMMENT 'TDET application detailed Trademark data for each search result'
# MAGIC LOCATION 's3://${config.certificate_bucket}/delta_tables/${config.tdet_catalog}/gold/tdet_app_search_history_detail';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${config.tdet_catalog}.gold.tdet_app_saved_searches (
# MAGIC   id STRING NOT NULL                      COMMENT 'Unique ID for each record',
# MAGIC   user_email STRING NOT NULL              COMMENT 'Email of user who created this record',
# MAGIC   search_name STRING NOT NULL             COMMENT 'Name of user who created this record',  
# MAGIC   search_type_code STRING NOT NULL        COMMENT 'BASIC, HYBRID, or ADVANCED',
# MAGIC   config_json STRING NOT NULL             COMMENT 'JSON payload of parameters',
# MAGIC   _created_timestamp TIMESTAMP NOT NULL   COMMENT 'Timestamp when this record was created',
# MAGIC   CONSTRAINT pk_saved_search PRIMARY KEY (id)
# MAGIC ) 
# MAGIC USING DELTA
# MAGIC COMMENT 'TDET application saved user search criteria'
# MAGIC LOCATION 's3://${config.certificate_bucket}/delta_tables/${config.tdet_catalog}/gold/tdet_app_saved_searches';

# COMMAND ----------

