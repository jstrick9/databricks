# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE WIDGET TEXT dbx_env DEFAULT "dev"

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file = "../../../../notebooks/config/"+dbutils.widgets.get("dbx_env").rstrip()+"/tqr-conf.yaml"
print(f'{config_file=}')
if dbx_env == "qa":
    dbutils.widgets.text("env", "test")
else:
    dbutils.widgets.text("env", dbx_env) 

# COMMAND ----------

# MAGIC %run ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
tqr_catalog = common_configs['schema']['tqr_catalog']
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)

# COMMAND ----------

spark.conf.set('conf.catalog', tqr_catalog)
spark.conf.set('conf.database', 'gold')

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${conf.catalog};
# MAGIC create schema if not exists  ${conf.database};
# MAGIC use ${conf.database};

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace table ${conf.catalog}.${conf.database}.event_inventory(
# MAGIC   review_type_cd VARCHAR(15) ,
# MAGIC   serial_num_tx VARCHAR(8) ,
# MAGIC   source_system_nm VARCHAR(100) ,
# MAGIC   search_present_in INT ,
# MAGIC   source_event_dt TIMESTAMP ,
# MAGIC   docket_in INT ,
# MAGIC   mark_literal_element_tx STRING ,
# MAGIC   mark_drawing_type_cd VARCHAR(5) ,
# MAGIC   mark_drawing_type_title_tx VARCHAR(25),
# MAGIC   mark_description_tx STRING ,
# MAGIC   examiner_employee_no VARCHAR(7),
# MAGIC   organization_cd VARCHAR(10) ,
# MAGIC   event_json_doc STRING ,
# MAGIC   inventory_create_ts TIMESTAMP ,
# MAGIC   lock_control_no INT ,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id VARCHAR(36),
# MAGIC   last_mod_ts TIMESTAMP ,
# MAGIC   last_mod_user_id VARCHAR(36) ,
# MAGIC   is_tm_exam BOOLEAN)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/tqr/gold/event_inventory'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace table ${conf.catalog}.${conf.database}.tqr_detail_metrics (
# MAGIC   eventinventoryidentifier BIGINT,
# MAGIC   qualityreviewidentifier BIGINT,
# MAGIC   reviewtypecode VARCHAR(15),
# MAGIC   trademarkserialnumber VARCHAR(8),
# MAGIC   eventdatetime TIMESTAMP,
# MAGIC   examineremployeenumber VARCHAR(7),
# MAGIC   organizationcode VARCHAR(10),
# MAGIC   searchcompleteindicator BOOLEAN,
# MAGIC   revieweremployeenumber VARCHAR(7),
# MAGIC   lastreviewdatetime TIMESTAMP,
# MAGIC   assigndatetime TIMESTAMP,
# MAGIC   completedatetime TIMESTAMP,
# MAGIC   financialyear BIGINT,
# MAGIC   financialquarternumber BIGINT,
# MAGIC   missedtagelementnamebag STRING,
# MAGIC   newtagelementnamebag STRING,
# MAGIC   unsoundtagelementnamebag STRING,
# MAGIC   soundtagelementnamebag STRING,
# MAGIC   evidencedeficienttagelementnamebag STRING,
# MAGIC   evidencesatisfactorytagelementnamebag STRING,
# MAGIC   evidenceexcellenttagelementnamebag STRING,
# MAGIC   writingdeficienttagelementnamebag STRING,
# MAGIC   writingsatisfactorytagelementnamebag STRING,
# MAGIC   writingexcellenttagelementnamebag STRING,
# MAGIC   searchsufficientindicator BOOLEAN,
# MAGIC   qualitymetricdeficientindicator BOOLEAN,
# MAGIC   mississueindicator BOOLEAN,
# MAGIC   newissueindicator BOOLEAN,
# MAGIC   refusalunsoundindicator BOOLEAN,
# MAGIC   substantivedeficientindicator BOOLEAN,
# MAGIC   proceduraldeficientindicator BOOLEAN,
# MAGIC   overalldeficientindicator BOOLEAN,
# MAGIC   overallexcellentindicator BOOLEAN,
# MAGIC   evidencedeficientindicator BOOLEAN,
# MAGIC   evidencesatisfactoryindicator BOOLEAN,
# MAGIC   evidenceexcellentindicator BOOLEAN,
# MAGIC   writingdeficientindicator BOOLEAN,
# MAGIC   writingsatisfactoryindicator BOOLEAN,
# MAGIC   writingexcellentindicator BOOLEAN,
# MAGIC   substantiveerrorindicator BOOLEAN,
# MAGIC   satisfactoryindicator BOOLEAN,
# MAGIC   findingindicator BOOLEAN,
# MAGIC   createdatetime TIMESTAMP,
# MAGIC   createuseridentifier VARCHAR(36),
# MAGIC   lastmodifieddatetime TIMESTAMP,
# MAGIC   lastmodifieduseridentifier VARCHAR(36),
# MAGIC   adminspecimenissuesbag STRING)
# MAGIC USING delta
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/tqr/gold/tqr_detail_metrics'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = 'true',
# MAGIC   'delta.enableChangeDataFeed' = 'true' )
