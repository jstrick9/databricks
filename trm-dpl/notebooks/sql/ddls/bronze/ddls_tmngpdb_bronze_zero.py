# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_mad_birth_rec_ct_type  (
# MAGIC MAD_BIRTH_REC_CT_TYPE_CD string, 
# MAGIC TITLE_TX string, 
# MAGIC DESCRIPTION_TX string, 
# MAGIC BEGIN_EFFECTIVE_DT timestamp, 
# MAGIC END_EFFECTIVE_DT timestamp, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_mad_birth_rec_ct_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.submission_elctrn_addr (
# MAGIC FK_SUBMISSION_GID string, 
# MAGIC FK_ELECTRONIC_ADDRESS_GID string, 
# MAGIC PRIMARY_IN string, LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/submission_elctrn_addr'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.submission_averment (
# MAGIC FK_SUBMISSION_GID string, 
# MAGIC SEQUENCE_NO int, 
# MAGIC FK_AVERMENT_ID int, 
# MAGIC NON_STANDARD_AVERMENT_TX string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/submission_averment'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.submission_averment_h (
# MAGIC FK_SUBMISSION_GID string, 
# MAGIC SEQUENCE_NO int, 
# MAGIC FK_AVERMENT_ID int, 
# MAGIC NON_STANDARD_AVERMENT_TX string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string, 
# MAGIC CFK_TRANSACTION_INSTANCE_GID string, 
# MAGIC BEGIN_EFFECTIVE_TS timestamp, 
# MAGIC END_EFFECTIVE_TS timestamp, 
# MAGIC ACTION_CT string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/submission_averment_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.submission_signature (
# MAGIC FK_SUBMISSION_GID string, 
# MAGIC SEQUENCE_NO int, 
# MAGIC SIGNATURE_METHOD_CT string, 
# MAGIC SIGNATURE_TX string, 
# MAGIC SIGNATURE_DT timestamp, 
# MAGIC SIGNATURE_IMAGE_OBJ string, 
# MAGIC SIGNATORY_NAME_TX string, 
# MAGIC SIGNATORY_POSITION_TX string, 
# MAGIC SIGNATORY_TELECOM_NO string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/submission_signature'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_caselock (
# MAGIC SERIAL_NUM int,
# MAGIC LOCK_STATUS string,
# MAGIC LOCK_REASON string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_caselock'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_tranlog  (
# MAGIC TL_DATE int, 
# MAGIC TL_TIMER int, 
# MAGIC TL_SER_NUM int, 
# MAGIC TL_STATE string, 
# MAGIC TL_TIMESTAMP timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_tranlog'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_addl_stmnt_prior_reg (
# MAGIC FK_TRADEMARK_GID string, 
# MAGIC FK_STATEMENT_TYPE_CD string, 
# MAGIC FK_ORDER_NO int, 
# MAGIC FK_PRIOR_REG_TRADEMARK_GID string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_addl_stmnt_prior_reg'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_addl_stmnt_prior_reg_h (
# MAGIC FK_TRADEMARK_GID string, 
# MAGIC FK_STATEMENT_TYPE_CD string, 
# MAGIC FK_ORDER_NO int, 
# MAGIC FK_PRIOR_REG_TRADEMARK_GID string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string, 
# MAGIC CFK_TRANSACTION_INSTANCE_GID string, 
# MAGIC BEGIN_EFFECTIVE_TS timestamp, 
# MAGIC END_EFFECTIVE_TS timestamp,
# MAGIC action_ct string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_addl_stmnt_prior_reg_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_class_gds_srvc_term (
# MAGIC FK_TRADEMARK_GID string, 
# MAGIC FK_CLASS_ID int, 
# MAGIC SEQUENCE_NO int, 
# MAGIC FK_GDS_SRVC_STATUS_CD string, 
# MAGIC FK_GDS_SRVC_STATUS_RSN_CD string, 
# MAGIC FK_STMNT_ACTVTY_TYPE_CD string, 
# MAGIC FIRST_USE_IN_COMMERCE_MONTH_NO int, 
# MAGIC FIRST_USE_IN_COMMERCE_DAY_NO int, 
# MAGIC FIRST_USE_IN_COMMERCE_YEAR_NO int, 
# MAGIC FIRST_USE_ANYWHERE_MONTH_NO int, 
# MAGIC FIRST_USE_ANYWHERE_DAY_NO int, 
# MAGIC FIRST_USE_ANYWHERE_YEAR_NO int, 
# MAGIC INTENT_TO_USE_DT timestamp, LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string, 
# MAGIC GDS_SRVC_TERM_TX string, 
# MAGIC SUGGESTED_GDS_SRVC_TERM_TX string, 
# MAGIC FK_GOODS_SERVICES_TERM_ID int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_class_gds_srvc_term'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_class_gds_srvc_term_h (
# MAGIC FK_TRADEMARK_GID string, 
# MAGIC FK_CLASS_ID int, 
# MAGIC SEQUENCE_NO int, 
# MAGIC FK_STMNT_ACTVTY_TYPE_CD string, 
# MAGIC FK_GDS_SRVC_STATUS_CD string, 
# MAGIC FK_GDS_SRVC_STATUS_RSN_CD string, 
# MAGIC FIRST_USE_IN_COMMERCE_MONTH_NO int, 
# MAGIC FIRST_USE_IN_COMMERCE_DAY_NO int, 
# MAGIC FIRST_USE_IN_COMMERCE_YEAR_NO int, 
# MAGIC FIRST_USE_ANYWHERE_MONTH_NO int, 
# MAGIC FIRST_USE_ANYWHERE_DAY_NO int, 
# MAGIC FIRST_USE_ANYWHERE_YEAR_NO int, 
# MAGIC INTENT_TO_USE_DT timestamp, 
# MAGIC LOCK_CONTROL_NO int, CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string, 
# MAGIC CFK_TRANSACTION_INSTANCE_GID string, 
# MAGIC BEGIN_EFFECTIVE_TS timestamp, 
# MAGIC END_EFFECTIVE_TS timestamp, 
# MAGIC GDS_SRVC_TERM_TX string, 
# MAGIC SUGGESTED_GDS_SRVC_TERM_TX string, 
# MAGIC ACTION_CT string, 
# MAGIC FK_GOODS_SERVICES_TERM_ID int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_class_gds_srvc_term_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_gds_srvc_term_filg_basis (
# MAGIC FK_TRADEMARK_GID string, 
# MAGIC FK_CLASS_ID int, 
# MAGIC FK_GDS_SRVC_TERM_SEQUENCE_NO int, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string, 
# MAGIC FK_FILING_BASIS_CD string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_gds_srvc_term_filg_basis'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_gds_srvc_term_filg_basis_h (
# MAGIC FK_TRADEMARK_GID string, 
# MAGIC ACTION_CT string, 
# MAGIC FK_CLASS_ID int, 
# MAGIC FK_GDS_SRVC_TERM_SEQUENCE_NO int, 
# MAGIC FK_FILING_BASIS_CD string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string, 
# MAGIC CFK_TRANSACTION_INSTANCE_GID string, 
# MAGIC BEGIN_EFFECTIVE_TS timestamp, 
# MAGIC END_EFFECTIVE_TS timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_gds_srvc_term_filg_basis_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_ct (
# MAGIC CT_RSN int, 
# MAGIC CT_PROG string, 
# MAGIC CT_SEQ int, 
# MAGIC CT_BEGIN_SEQNO_1 int, 
# MAGIC CT_BEGIN_SEQNO_2 int, 
# MAGIC CT_BEGIN_SEQNO_3 int, 
# MAGIC CT_BEGIN_SEQNO_4 int, 
# MAGIC CT_BEGIN_SEQNO_5 int, 
# MAGIC CT_BEGIN_SEQNO_6 int, 
# MAGIC CT_BEGIN_SEQNO_7 int, 
# MAGIC CT_BEGIN_SEQNO_8 int, 
# MAGIC CT_BEGIN_SEQNO_9 int, 
# MAGIC CT_BEGIN_SEQNO_10 int, 
# MAGIC CT_BEGIN_SEQNO_11 int, 
# MAGIC CT_BEGIN_SEQNO_12 int, 
# MAGIC CT_END_SEQNO_1 int, 
# MAGIC CT_END_SEQNO_2 int, 
# MAGIC CT_END_SEQNO_3 int, 
# MAGIC CT_END_SEQNO_4 int, 
# MAGIC CT_END_SEQNO_5 int, 
# MAGIC CT_END_SEQNO_6 int, 
# MAGIC CT_END_SEQNO_7 int, 
# MAGIC CT_END_SEQNO_8 int, 
# MAGIC CT_END_SEQNO_9 int, 
# MAGIC CT_END_SEQNO_10 int, 
# MAGIC CT_END_SEQNO_11 int, 
# MAGIC CT_END_SEQNO_12 int, 
# MAGIC CT_REST_1 int, 
# MAGIC CT_REST_2 int, 
# MAGIC CT_REST_3 int, 
# MAGIC CT_REST_4 int, 
# MAGIC CT_REST_5 int, 
# MAGIC CT_REST_6 int, 
# MAGIC CT_REST_7 int, 
# MAGIC CT_REST_8 int, 
# MAGIC CT_REST_9 int, 
# MAGIC CT_REST_10 int, 
# MAGIC CT_REST_11 int,
# MAGIC CT_REST_12 int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ct'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_ecr (
# MAGIC ECR_NUM int, 
# MAGIC ECR_DT_CREATED int, 
# MAGIC ECR_WBS_CD string, 
# MAGIC ECR_DT_RCVD int, 
# MAGIC ECR_FROM string, 
# MAGIC ECR_SUBJECT string, 
# MAGIC ECR_DT_ASGN int, 
# MAGIC ECR_DT_COMPLTD int, 
# MAGIC ECR_ASGN string, 
# MAGIC ECR_PROGRAMMER string, 
# MAGIC ECR_STATUS int, 
# MAGIC ECR_DT_DUE int, 
# MAGIC ECR_DOCS_CNT int, 
# MAGIC ECR_SUP_DOC_TITLE_1 string, 
# MAGIC ECR_SUP_DOC_TITLE_2 string, 
# MAGIC ECR_SUP_DOC_TITLE_3 string, 
# MAGIC ECR_SUP_DOC_TITLE_4 string, 
# MAGIC ECR_SUP_DOC_TITLE_5 string, 
# MAGIC ECR_SUP_DOC_TITLE_6 string, 
# MAGIC ECR_SUP_DOC_TITLE_7 string, 
# MAGIC ECR_SUP_DOC_TITLE_8 string, 
# MAGIC ECR_SUP_DOC_TITLE_9 string, 
# MAGIC ECR_SUP_DOC_TITLE_10 string, 
# MAGIC ECR_SUP_DOC_DESC_1 string, 
# MAGIC ECR_SUP_DOC_DESC_2 string, 
# MAGIC ECR_SUP_DOC_DESC_3 string, 
# MAGIC ECR_SUP_DOC_DESC_4 string, 
# MAGIC ECR_SUP_DOC_DESC_5 string, 
# MAGIC ECR_SUP_DOC_DESC_6 string, 
# MAGIC ECR_SUP_DOC_DESC_7 string, 
# MAGIC ECR_SUP_DOC_DESC_8 string, 
# MAGIC ECR_SUP_DOC_DESC_9 string, 
# MAGIC ECR_SUP_DOC_DESC_10 string, 
# MAGIC ECR_HRS_WORK int 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ecr'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_gs (
# MAGIC GS_SER_NUM int, 
# MAGIC GS_CLS string, 
# MAGIC GS_CLS_STAT string, 
# MAGIC GS_TEXT string, 
# MAGIC GS_BASIS_IND string, 
# MAGIC GS_ENT_NUM int, 
# MAGIC GS_RSN int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_gs'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_pd (
# MAGIC PD_ENT_NUM int, 
# MAGIC PD_EMPE_LO string, 
# MAGIC PD_EMPE_NUM int, 
# MAGIC PD_SER_NUM int, 
# MAGIC PD_TRAN_CD int, 
# MAGIC PD_OFFICE_TYPE string, 
# MAGIC PD_SUB_TYPE string, 
# MAGIC PD_START_EVENT string, 
# MAGIC PD_START_DT int, 
# MAGIC PD_START_TIME int, 
# MAGIC PD_START_FY_PP int, 
# MAGIC PD_FA_EVENT string, 
# MAGIC PD_FA_DT int, 
# MAGIC PD_FA_TIME int, 
# MAGIC PD_FA_FY_PP int, 
# MAGIC PD_END_EVENT string, 
# MAGIC PD_END_DT int, 
# MAGIC PD_END_TIME int, 
# MAGIC PD_END_FY_PP int, 
# MAGIC PD_PEND_DAYS int, 
# MAGIC PD_UPtimestamp_DT int, 
# MAGIC PD_UPtimestamp_TIME int, 
# MAGIC PD_RSN int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_pd'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_qe (
# MAGIC QE_SER_NUM int, 
# MAGIC QE_QUEUE_TYPE string, 
# MAGIC QE_QUEUE string, 
# MAGIC QE_FLG_ASGN int, 
# MAGIC QE_EMPE_NUM int, 
# MAGIC QE_EMPE_ASGN_DT int, 
# MAGIC QE_EMPE_ASGN_TI int, 
# MAGIC QE_ENTER_DT int, 
# MAGIC QE_ENTER_TI int, 
# MAGIC QE_LEAVE_DT int, 
# MAGIC QE_LEAVE_TI int, 
# MAGIC QE_ENTER_EVENT string, 
# MAGIC QE_LEAVE_EVENT string, 
# MAGIC QE_RSN int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_qe'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_rq (
# MAGIC RQ_ACC_CD string, 
# MAGIC RQ_ACC_NUM string, 
# MAGIC RQ_EMP_NUM_1 int, 
# MAGIC RQ_EMP_NUM_2 int, 
# MAGIC RQ_EMP_NUM_3 int, 
# MAGIC RQ_LOC_1 string, 
# MAGIC RQ_LOC_2 string, 
# MAGIC RQ_LOC_3 string, 
# MAGIC RQ_COPIES int, 
# MAGIC RQ_CTRL_ID int,
# MAGIC RQ_RQST_EMP_NUM int, 
# MAGIC RQ_RQST_EMP_LOC string, 
# MAGIC RQ_MARK_STAT string, 
# MAGIC RQ_NUM_QC_RECS int, 
# MAGIC RQ_STAT string, 
# MAGIC RQ_TRAN_CD int, 
# MAGIC RQ_BEG_DT int, 
# MAGIC RQ_CMPLT_DT int, 
# MAGIC RQ_END_DT int, 
# MAGIC RQ_RQST_DT int, 
# MAGIC RQ_RSN int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_rq'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_wt (
# MAGIC WT_PROJECT_CD string, 
# MAGIC WT_EMPE_NUM int, 
# MAGIC WT_TYPE string, 
# MAGIC WT_HRS_REG int, 
# MAGIC WT_HRS_OT int, 
# MAGIC WT_FY_PP int, 
# MAGIC WT_LAST_UPDT_DT int,
# MAGIC WT_LAST_UPDT_TI int, 
# MAGIC WT_RSN int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_wt'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.annotation_comment (
# MAGIC FK_REVIEW_ANNOTATION_ID int, 
# MAGIC CFK_EMPLOYEE_NO string, 
# MAGIC COMMENT_DT timestamp, 
# MAGIC COMMENT_SOURCE_CT string, 
# MAGIC COMMENT_TX string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS TIMESTAMP, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS TIMESTAMP, 
# MAGIC LAST_MOD_USER_ID string, 
# MAGIC ANNOTATION_COMMENT_ID int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/annotation_comment'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.custom_alert (
# MAGIC CUSTOM_ALERT_ID int, 
# MAGIC TITLE_TX string, 
# MAGIC TRIGGER_TYPE_CT string, 
# MAGIC USER_CONTROL_LEVEL_CT string, 
# MAGIC CFK_DOMAIN_MESSAGE_ID int, 
# MAGIC TRIGGER_SCHEDULE_DT TIMESTAMP, 
# MAGIC CFK_RECIPIENT_EMPLOYEE_NO string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS TIMESTAMP, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS TIMESTAMP, 
# MAGIC LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/custom_alert'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.ib_transaction (
# MAGIC FK_WORK_ITEM_GID string, 
# MAGIC ORIGIN_CT string, 
# MAGIC MPU_SENT_TS TIMESTAMP, 
# MAGIC MPU_SENT_STATUS_CT string, 
# MAGIC IB_RECEIPT_TS TIMESTAMP, 
# MAGIC IB_RECEIPT_STATUS_CT string, 
# MAGIC DATA_TYPE_CT string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/ib_transaction'
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
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.review_annotation (
# MAGIC REVIEW_ANNOTATION_ID int, 
# MAGIC FK_OFFICE_ACTIVITY_REVIEW_ID int, 
# MAGIC FK_DOCUMENT_COMPONENT_ID int, 
# MAGIC TEXT_SEGMENT_LOCATOR_TX string, 
# MAGIC TEXT_SEGMENT_TX string, 
# MAGIC ANNOTATION_CT string, 
# MAGIC FK_ANNOTATION_STATUS_CD string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS TIMESTAMP, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS TIMESTAMP, 
# MAGIC LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/review_annotation'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.section_2f_prior_reg (
# MAGIC FK_TRADEMARK_GID string, 
# MAGIC FK_PRIOR_REG_TRADEMARK_GID string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS TIMESTAMP, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS TIMESTAMP, 
# MAGIC LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/section_2f_prior_reg'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.section_2f_prior_reg_h (
# MAGIC FK_TRADEMARK_GID string, 
# MAGIC FK_PRIOR_REG_TRADEMARK_GID string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS TIMESTAMP, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS TIMESTAMP, 
# MAGIC LAST_MOD_USER_ID string, 
# MAGIC CFK_TRANSACTION_INSTANCE_GID string, 
# MAGIC BEGIN_EFFECTIVE_TS TIMESTAMP, 
# MAGIC END_EFFECTIVE_TS TIMESTAMP, 
# MAGIC ACTION_CT string  
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/section_2f_prior_reg_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.submission_elctrn_addr_h (
# MAGIC FK_SUBMISSION_GID string,
# MAGIC FK_ELECTRONIC_ADDRESS_GID string, 
# MAGIC PRIMARY_IN string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string, 
# MAGIC CFK_TRANSACTION_INSTANCE_GID string, 
# MAGIC BEGIN_EFFECTIVE_TS timestamp, 
# MAGIC END_EFFECTIVE_TS timestamp, 
# MAGIC ACTION_CT string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/submission_elctrn_addr_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

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
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_runtime (
# MAGIC DNAME string, 
# MAGIC DBROLE string, 
# MAGIC SNAME string, 
# MAGIC SHOST string, 
# MAGIC CSCHEMA string, 
# MAGIC ESCHEMA string, 
# MAGIC USCHEMA string, 
# MAGIC UOS string, 
# MAGIC UHOST string, 
# MAGIC USESSION string, 
# MAGIC TBSDATA string, 
# MAGIC TBSIDX string, 
# MAGIC TBSIDXLRG string, 
# MAGIC SOWNER string, 
# MAGIC DMLROLE string, 
# MAGIC PSTOP string, 
# MAGIC PERR string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_runtime'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)
