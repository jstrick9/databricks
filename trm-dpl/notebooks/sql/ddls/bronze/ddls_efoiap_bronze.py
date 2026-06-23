# Databricks notebook source
# MAGIC %md
# MAGIC <pre>
# MAGIC Purpose: This ntbk executes DDL scripts to create jbteasps bronze layer tables
# MAGIC </pre>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT dbx_env DEFAULT "dev"

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file = "../../../config/"+dbutils.widgets.get("dbx_env").rstrip()+"/efoiap-conf.yaml"
print(f'{config_file=}')
if dbx_env == "qa":
    dbutils.widgets.text("env", "test")
else:
    dbutils.widgets.text("env", dbx_env) 

# COMMAND ----------

# MAGIC %run  ../../../../notebooks/python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

#schema variables
common_configs = read_yaml(config_file)
efoiap_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{efoiap_catalog=}, {data_quality_catalog=} ')
src_folder = common_configs['cdc']['src_csv_files']
src_database = common_configs['cdc']['src_database']
spark.conf.set('config.data_quality_catalog', data_quality_catalog.lower())
spark.conf.set('config.efoiap_catalog', efoiap_catalog.lower()) 

# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'
catalog = efoiap_catalog
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set('conf.catalog', efoiap_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${config.efoiap_catalog} MANAGED LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/trm_efoiap/';

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${conf.catalog};
# MAGIC create schema if not exists  ${conf.database};
# MAGIC use ${conf.database};

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.efoiap_catalog}.${conf.database}.APPEAL_DECISION_ISSUE(
# MAGIC fk_sequence_no             DECIMAL,                   
# MAGIC fk_trademark_proceeding_no DECIMAL,       
# MAGIC level_1_issue_cd           string,                       
# MAGIC level_2_issue_cd           string,                       
# MAGIC create_ts                  timestamp,                           
# MAGIC create_user_id             string,                         
# MAGIC last_modified_ts           timestamp,                    
# MAGIC last_modified_user_id      string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_efoiap/bronze/APPEAL_DECISION_ISSUE'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.efoiap_catalog}.${conf.database}.DOCUMENT_TYPE(
# MAGIC dt_document_type_nm   string,                    
# MAGIC description_tx        string,                         
# MAGIC last_modified_ts      timestamp,                    
# MAGIC last_modified_user_id string,                  
# MAGIC dt_business_short_nm  string )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_efoiap/bronze/DOCUMENT_TYPE'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.efoiap_catalog}.${conf.database}.EFOIA_TRIGGER_EXCEPTIONS(           
# MAGIC proceeding_no int,                    
# MAGIC insert_ts     timestamp,                           
# MAGIC error_num     int,                      
# MAGIC error_msg     string,                              
# MAGIC backtrace     string,                              
# MAGIC callstack     string)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_efoiap/bronze/EFOIA_TRIGGER_EXCEPTIONS'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.efoiap_catalog}.${conf.database}.PROSECUTION_HISTORY_EVENT(
# MAGIC identifier                   DECIMAL,                       
# MAGIC entry_code                   string,                             
# MAGIC entry_date                   timestamp,                          
# MAGIC date_due                     timestamp,                            
# MAGIC exhibits_included_indicator  string,            
# MAGIC confidential_indicator       string,                 
# MAGIC text                         string,                                   
# MAGIC object_id                    string,                              
# MAGIC last_update_userid           string,                     
# MAGIC last_update_timestamp        timestamp,               
# MAGIC fk_proceedingnumber0         DECIMAL,             
# MAGIC fk_entry_informentry_code    string,              
# MAGIC entry_num                    DECIMAL,                        
# MAGIC fk_proceedingtype            string,                      
# MAGIC estta_id                     string,                               
# MAGIC internal_comment_tx          string,                    
# MAGIC external_court_nm            string,                      
# MAGIC external_case_no             string,                       
# MAGIC trial_extension_days_qt      DECIMAL,          
# MAGIC trial_suspension_days_qt     DECIMAL,         
# MAGIC motion_pending_in            string,                      
# MAGIC fk_document_type_cd          string,                    
# MAGIC last_mod_doc_type_cd_ts      timestamp,             
# MAGIC proceeding_resume_dt         timestamp,                
# MAGIC defendant_has_email_in       string,                 
# MAGIC fk_ext_of_time_type_id       DECIMAL,           
# MAGIC relinquishment_attachment_in string,           
# MAGIC fk_party_id                  DECIMAL,                      
# MAGIC plaintiff_has_email_in       string )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_efoiap/bronze/PROSECUTION_HISTORY_EVENT'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.efoiap_catalog}.${conf.database}.PROSECUTION_HISTORY_EVENT2(
# MAGIC identifier                   DECIMAL,                       
# MAGIC entry_code                   string,                             
# MAGIC entry_date                   timestamp,                          
# MAGIC date_due                     timestamp,                            
# MAGIC exhibits_included_indicator  string,            
# MAGIC confidential_indicator       string,                 
# MAGIC text                         string,                                   
# MAGIC object_id                    string,                              
# MAGIC last_update_userid           string,                     
# MAGIC last_update_timestamp        timestamp,               
# MAGIC fk_proceedingnumber0         DECIMAL,             
# MAGIC fk_entry_informentry_code    string,              
# MAGIC entry_num                    DECIMAL,                        
# MAGIC fk_proceedingtype            string,                      
# MAGIC estta_id                     string,                               
# MAGIC internal_comment_tx          string,                    
# MAGIC external_court_nm            string,                      
# MAGIC external_case_no             string,                       
# MAGIC trial_extension_days_qt      DECIMAL,          
# MAGIC trial_suspension_days_qt     DECIMAL,         
# MAGIC motion_pending_in            string,                      
# MAGIC fk_document_type_cd          string,                    
# MAGIC last_mod_doc_type_cd_ts      timestamp,             
# MAGIC proceeding_resume_dt         timestamp,                
# MAGIC defendant_has_email_in       string,                 
# MAGIC fk_ext_of_time_type_id       DECIMAL,           
# MAGIC relinquishment_attachment_in string,           
# MAGIC fk_party_id                  DECIMAL,                      
# MAGIC plaintiff_has_email_in       string )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_efoiap/bronze/PROSECUTION_HISTORY_EVENT2'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.efoiap_catalog}.${conf.database}.STND_DECISION(
# MAGIC decision_cd         string,                            
# MAGIC fk_level_1_issue_cd string,                    
# MAGIC decision_nm         string,                            
# MAGIC description_tx      string,                         
# MAGIC create_user_id      string,                         
# MAGIC create_ts           timestamp,                           
# MAGIC last_mod_user_id    string,                       
# MAGIC last_mod_ts         timestamp,                         
# MAGIC begin_effective_dt  timestamp,                  
# MAGIC end_effective_dt    timestamp
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_efoiap/bronze/STND_DECISION'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.efoiap_catalog}.${conf.database}.STND_LEVEL_1_ISSUE(
# MAGIC level_1_issue_cd      string,                       
# MAGIC description_tx        string,                         
# MAGIC delete_in             string,                              
# MAGIC begin_effective_dt    timestamp,                  
# MAGIC end_effective_dt      timestamp,                    
# MAGIC create_ts             timestamp,                           
# MAGIC create_user_id        string,                         
# MAGIC last_modified_ts      timestamp,                    
# MAGIC last_modified_user_id string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_efoiap/bronze/STND_LEVEL_1_ISSUE'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.efoiap_catalog}.${conf.database}.STND_LEVEL_2_ISSUE(
# MAGIC level_2_issue_cd      string,                       
# MAGIC fk_level_1_issue_cd   string,                    
# MAGIC description_tx        string,                         
# MAGIC delete_in             string,                              
# MAGIC begin_effective_dt    timestamp,                  
# MAGIC end_effective_dt      timestamp,                    
# MAGIC create_ts             timestamp,                           
# MAGIC create_user_id        string,                         
# MAGIC last_modified_ts      timestamp,                    
# MAGIC last_modified_user_id string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_efoiap/bronze/STND_LEVEL_2_ISSUE'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.efoiap_catalog}.${conf.database}.TM_APPEAL_DECISION(
# MAGIC tm_appeal_decision_id        DECIMAL(38,10),          
# MAGIC cfk_panel_id                 DECIMAL(38,10),                   
# MAGIC document_create_dt           timestamp,                  
# MAGIC proceeding_no                DECIMAL,                    
# MAGIC document_image_id            string,                      
# MAGIC proceeding_type_cd           string,                     
# MAGIC issue_code_list_tx           string,                     
# MAGIC fk_decision_cd               string,                         
# MAGIC decision_writer_nm           string,                     
# MAGIC proceeding_decision_file_nm  string,            
# MAGIC party_nm                     string,                               
# MAGIC examining_attorney_nm        string,                  
# MAGIC decision_tx                  string,                            
# MAGIC precedent_citable_in         string,                   
# MAGIC panel_member_tx              string,                        
# MAGIC opposer_mark_good_service_tx string,           
# MAGIC applcnt_mark_good_service_tx string,           
# MAGIC exmg_atty_mark_good_cited_tx string,           
# MAGIC issue_tx                     string,                               
# MAGIC create_ts                    timestamp,                           
# MAGIC create_user_id               string,                         
# MAGIC last_mod_user_id             string,                       
# MAGIC last_mod_ts                  timestamp,                         
# MAGIC dn_phe_document_type_cd      string,                
# MAGIC dn_phe_entry_no              DECIMAL,                  
# MAGIC dn_phe_entry_code            string
# MAGIC
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_efoiap/bronze/TM_APPEAL_DECISION'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.efoiap_catalog}.${conf.database}.TM_APPEAL_DECISION_ERRLOG(
# MAGIC `ORA_ERR_NUMBER$`            int,
# MAGIC `ORA_ERR_MESG$`              string,
# MAGIC `ORA_ERR_ROWID$`             string,
# MAGIC `ORA_ERR_OPTYP$`             string,
# MAGIC `ORA_ERR_TAG$`               string,
# MAGIC TM_APPEAL_DECISION_ID        string,
# MAGIC CFK_PANEL_ID                 string,
# MAGIC DOCUMENT_CREATE_DT           string,
# MAGIC PROCEEDING_NO                string,
# MAGIC DOCUMENT_IMAGE_ID            string,
# MAGIC PROCEEDING_TYPE_CD           string,
# MAGIC ISSUE_CODE_LIST_TX           string,
# MAGIC FK_DECISION_CD               string,
# MAGIC DECISION_WRITER_NM           string,
# MAGIC PROCEEDING_DECISION_FILE_NM  string,
# MAGIC PARTY_NM                     string,
# MAGIC EXAMINING_ATTORNEY_NM        string,
# MAGIC DECISION_TX                  string,
# MAGIC PRECEDENT_CITABLE_IN         string,
# MAGIC PANEL_MEMBER_TX              string,
# MAGIC OPPOSER_MARK_GOOD_SERVICE_TX string,
# MAGIC APPLCNT_MARK_GOOD_SERVICE_TX string,
# MAGIC EXMG_ATTY_MARK_GOOD_CITED_TX string,
# MAGIC ISSUE_TX                     string,
# MAGIC CREATE_TS                    string,
# MAGIC CREATE_USER_ID               string,
# MAGIC LAST_MOD_USER_ID             string,
# MAGIC LAST_MOD_TS                  string,
# MAGIC DN_PHE_DOCUMENT_TYPE_CD      string,
# MAGIC DN_PHE_ENTRY_NO              string,
# MAGIC DN_PHE_ENTRY_CODE            string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_efoiap/bronze/TM_APPEAL_DECISION_ERRLOG'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.efoiap_catalog}.${conf.database}.TM_APPEAL_DECISION_H(
# MAGIC tm_appeal_decision_h_id      DECIMAL(38,10),        
# MAGIC tm_appeal_decision_id        DECIMAL(38,10),          
# MAGIC cfk_panel_id                 DECIMAL(38,10),                   
# MAGIC document_create_dt           timestamp,                  
# MAGIC proceeding_no                DECIMAL,                    
# MAGIC document_image_id            string,                      
# MAGIC proceeding_type_cd           string,                     
# MAGIC issue_code_list_tx           string,                     
# MAGIC fk_decision_cd               string,                         
# MAGIC decision_writer_nm           string,                     
# MAGIC proceeding_decision_file_nm  string,            
# MAGIC party_nm                     string,                               
# MAGIC examining_attorney_nm        string,                  
# MAGIC decision_tx                  string,                            
# MAGIC precedent_citable_in         string,                   
# MAGIC panel_member_tx              string,                        
# MAGIC opposer_mark_good_service_tx string,           
# MAGIC applcnt_mark_good_service_tx string,           
# MAGIC exmg_atty_mark_good_cited_tx string,           
# MAGIC issue_tx                     string,                               
# MAGIC create_ts                    timestamp,                           
# MAGIC create_user_id               string,                         
# MAGIC last_mod_user_id             string,                       
# MAGIC last_mod_ts                  timestamp,                         
# MAGIC begin_effective_ts           timestamp,                  
# MAGIC end_effective_ts             timestamp,                    
# MAGIC action_ct                    string,                              
# MAGIC dn_phe_document_type_cd      string,                
# MAGIC dn_phe_entry_no              DECIMAL,                  
# MAGIC dn_phe_entry_code            string
# MAGIC
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_efoiap/bronze/TM_APPEAL_DECISION_H'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.efoiap_catalog}.${conf.database}.TMNG_GO_LIVE(                       
# MAGIC go_live_ts timestamp
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_efoiap/bronze/TMNG_GO_LIVE'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.efoiap_catalog}.${conf.database}.TRADEMARK_APPEAL_DECISION(                       
# MAGIC sequence_no                  DECIMAL,                      
# MAGIC trademark_proceeding_no      DECIMAL,          
# MAGIC proceeding_type_cd           string,                     
# MAGIC proceeding_decision_file_nm  string,            
# MAGIC party_nm                     string,                               
# MAGIC examining_attorney_nm        string,                  
# MAGIC decision_tx                  string,                            
# MAGIC opposer_mark_good_service_tx string,           
# MAGIC applcnt_mark_good_service_tx string,           
# MAGIC precedent_citable_in         string,                   
# MAGIC status_cd                    string,                              
# MAGIC decision_type_cd             string,                       
# MAGIC panel_member_tx              string,                        
# MAGIC exmg_atty_mark_good_cited_tx string,           
# MAGIC document_image_id            string,                      
# MAGIC issue_tx                     string,                               
# MAGIC delete_in                    string,                              
# MAGIC document_create_dt           timestamp,                  
# MAGIC fk_dt_document_type_nm       string,                 
# MAGIC fk_dt_business_short_nm      string,                
# MAGIC last_modified_user_id        string,                  
# MAGIC last_modified_ts             timestamp
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_efoiap/bronze/TRADEMARK_APPEAL_DECISION'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists ${config.efoiap_catalog}.${conf.database}.${conf.control_table};

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table if not exists ${config.efoiap_catalog}.${conf.database}.${conf.control_table} (
# MAGIC   src_folder string,
# MAGIC   catalog_name string,
# MAGIC   database_name string,
# MAGIC   table_name string,
# MAGIC   source_db_name string,
# MAGIC   source_table_name string,
# MAGIC   primary_keys string,
# MAGIC   full_load string,
# MAGIC   initial_load_finished boolean
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_efoiap/bronze/${conf.control_table}'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %md
# MAGIC #Initialize the dms-cdc-batch-job-control table

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

table_schema = spark.table(f'{catalog}.{database}.{control_table}').schema

table_data = [
    (src_folder+"/"+"APPEAL_DECISION_ISSUE", 
     catalog, 
     database,
     "appeal_decision_issue",
     src_database,
     "APPEAL_DECISION_ISSUE",
     "fk_sequence_no,fk_trademark_proceeding_no,level_1_issue_cd",
     "N",
     False
    ),
    (src_folder+"/"+"DOCUMENT_TYPE", 
     catalog, 
     database,
     "document_type",
     "TMNGPVTDB",
     "DOCUMENT_TYPE",
     "dt_business_short_nm,dt_document_type_nm",     
     "N", 
     False
    ),
    (src_folder+"/"+"STND_DECISION", 
     catalog, 
     database,
     "stnd_decision",
     src_database,
     "STND_DECISION",
     "decision_cd",
      "N",    
     False
    ) ,
    (src_folder+"/"+"STND_LEVEL_1_ISSUE", 
     catalog, 
     database,
     "stnd_level_1_issue",
     src_database,
     "STND_LEVEL_1_ISSUE",
     "level_1_issue_cd",
     "N",    
     False
    ),
    (src_folder+"/"+"STND_LEVEL_2_ISSUE", 
     catalog, 
     database,
     "stnd_level_2_issue",
     "TMNGPVTDB",
     "STND_LEVEL_2_ISSUE",
     "level_2_issue_cd,fk_level_1_issue_cd",
     "N",    
     False
    ),
    (src_folder+"/"+"TM_APPEAL_DECISION", 
     catalog, 
     database,
     "tm_appeal_decision",
     src_database,
     "TM_APPEAL_DECISION",
     "tm_appeal_decision_id",
     "N",     
     False
    ) ,
    (src_folder+"/"+"TM_APPEAL_DECISION_H", 
     catalog, 
     database,
     "tm_appeal_decision_h",
     "TMNGPVTDB",
     "TM_APPEAL_DECISION_H",
     "tm_appeal_decision_h_id",
     "N",     
     False
    )  ,
    (src_folder+"/"+"TRADEMARK_APPEAL_DECISION", 
     catalog, 
     database,
     "trademark_appeal_decision",
     src_database,
     "TRADEMARK_APPEAL_DECISION",
     "sequence_no,trademark_proceeding_no",
     "N",    
     False
    )   ,
    (src_folder+"/"+"EFOIA_TRIGGER_EXCEPTIONS", 
     catalog, 
     database,
     "efoia_trigger_exceptions",
     src_database,
     "EFOIA_TRIGGER_EXCEPTIONS",
     "",
     "Y",    
     False
    )   ,
    (src_folder+"/"+"PROSECUTION_HISTORY_EVENT", 
     catalog, 
     database,
     "prosecution_history_event",
     src_database,
     "PROSECUTION_HISTORY_EVENT",
     "",
     "Y",    
     False
    )   ,
    (src_folder+"/"+"PROSECUTION_HISTORY_EVENT2", 
     catalog, 
     database,
     "prosecution_history_event2",
     src_database,
     "PROSECUTION_HISTORY_EVENT2",
     "",
     "Y",    
     False
    )   ,
    (src_folder+"/"+"TMNG_GO_LIVE", 
     catalog, 
     database,
     "tmng_go_live",
     src_database,
     "TMNG_GO_LIVE",
     "",
     "Y",    
     False
    )   ,
    (src_folder+"/"+" TM_APPEAL_DECISION_ERRLOG", 
     catalog, 
     database,
     "tm_appeal_decision_errlog",
     src_database,
     " TM_APPEAL_DECISION_ERRLOG",
     "",
     "Y",    
     False
    )                              
]

 
df = spark.createDataFrame(data=table_data,schema=table_schema)

display(df)

df.write.mode('overwrite').saveAsTable(f'{catalog}.{database}.{control_table}')

# COMMAND ----------

# MAGIC %md
# MAGIC #Initialize the dms-cdc-batch-job-history table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists ${conf.catalog}.${conf.database}.${conf.job_history_table};
# MAGIC
# MAGIC create table if not exists ${conf.catalog}.${conf.database}.${conf.job_history_table} (
# MAGIC   cdc_file_path string,
# MAGIC   meta_src_time long,
# MAGIC   cdc_file_date date,
# MAGIC   processing_time TIMESTAMP
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_efoiap/bronze/${conf.job_history_table}'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);
