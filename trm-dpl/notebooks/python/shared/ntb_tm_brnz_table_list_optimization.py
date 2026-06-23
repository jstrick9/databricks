# Databricks notebook source
# MAGIC %md
# MAGIC #####Schema = ["TABLE_GROUP_NAME","TABLE_NAME","FULL_LOAD","DQ_FLTR","LARGE_TABLE_IND","numPartitions","fetchsize",'partitionColumn','lowerBound,'upperBound']

# COMMAND ----------

tmngpdb_metadata_group1 = [
    ('group1','TRADEMARK','Y','','L',2,50000,'LOCK_CONTROL_NO',0,1),
    ('group1','TM_LOCATIONS','Y','LAST_MOD_TS','',2,50000,'LOCK_CONTROL_NO',0,1),#11,976,361
    ('group1','TM_LITERAL','Y','LAST_MOD_TS','',2,10000,'LOCK_CONTROL_NO',0,1),#reset back 2,820,304
    ('group1','TM_EMPLOYEE_ASSIGNMENT','Y','LAST_MOD_TS','',2,50000,'LOCK_CONTROL_NO',0,1),#15,957,715
    ('group1','TM_RELATIONSHIP','Y','LAST_MOD_TS','','','','','',''),#486
    ('group1','TM_MILESTONE','Y','LAST_MOD_TS','',2,50000,'LOCK_CONTROL_NO',0,1),#reset back#39,288,633
    ('group1','TM_ITU','Y','LAST_MOD_TS','',2,10000,'LOCK_CONTROL_NO',0,1),#--4,121,401
    ('group1','TM_OFFICE_ACTIONS','Y','LAST_MOD_TS','',2,50000,'LOCK_CONTROL_NO',0,1),#reset back 11,673,112#only 0
    ('group1','TM_DIVISIONAL_CHILD','N','LAST_MOD_TS','','','','','',''),#78,982
    ('group1','STND_TM_MILESTONE','Y','LAST_MOD_TS','','','','','',''),#12
    ('group1','TM_PARTY_ROLE_OWNER','Y','LAST_MOD_TS','',2,50000,'LOCK_CONTROL_NO',0,1),#reset back 28,342,910
    ('group1','TM_CLASS_REFERENCE','Y','LAST_MOD_TS','',2,50000,'LOCK_CONTROL_NO',0,1),#52,956,672
    ('group1','USE_IN_ANOTHER_FORM','N','LAST_MOD_TS','','','','','',''),#65,123
    ('group1','ATTORNEY_HOLD','Y','LAST_MOD_TS','','','','','',''),#62,536
    #('group1','INTERNATIONAL_REG_TM','N','LAST_MOD_TS','','','','','',''),#391,571
    ('group1','BUSINESS_EVENT','Y','LAST_MOD_TS','',2,50000,'LOCK_CONTROL_NO',0,1),#307,338,416
    ('group1','STND_BUSINESS_EVENT_REASON','Y','LAST_MOD_TS','','','','','',''),#12,62
    ('group1','STND_CLASS','Y','LAST_MOD_TS','','','','','',''),#737
    ('group1','STND_FEE_PROCESS_TYPE','Y','LAST_MOD_TS','','','','','',''), #6
    ('group1','STND_MARK_DRAWING_TYPE','Y','LAST_MOD_TS','','','','','',''), #7
]

# COMMAND ----------

tmngpdb_metadata_group11 = [
    ('group11','TM_FILING_BASIS_H','Y','LAST_MOD_TS',''),
    ('group11','WORK_ITEM_OBJECT','Y','LAST_MOD_TS',''),
    ('group11','TM_PARTY_ROLE','N','LAST_MOD_TS',''),
    ('group11','TM_MAILING_ADDR','N','LAST_MOD_TS',''),
    ('group11','TM_ELECTRONIC_ADDR','Y','LAST_MOD_TS',''),#reset back
    ('group11','TM_CLASS','N','LAST_MOD_TS','L'),
    ('group11','INTERESTED_PARTY','Y','LAST_MOD_TS','L'),
    ('group11','MAILING_ADDRESS','Y','LAST_MOD_TS',''),
    ('group11','ELECTRONIC_ADDRESS','N','LAST_MOD_TS',''),
    ('group11','TM_FILING_BASIS','N','LAST_MOD_TS',''),
]

# COMMAND ----------

tmngpdb_metadata_group2 = [
    ('group2','ABANDONMENT_H','N','LAST_MOD_TS',''),
    ('group2','ABANDONMENT','N','LAST_MOD_TS',''),
    ('group2','ANNOTATION_COMMENT','Y','LAST_MOD_TS',''),
    #('group2','BASE_APPLICATION','N','LAST_MOD_TS',''),
    #('group2','BASE_APPLICATION_H','Y','LAST_MOD_TS',''),
    ('group2','CONCURRENT_USE','N','LAST_MOD_TS',''),
    ('group2','CUSTOM_ALERT','N','LAST_MOD_TS',''),
    ('group2','DOC_TMPLT_VER_FORM_PARA','Y','LAST_MOD_TS',''),
    ('group2','DOCKET_ITEM','Y','LAST_MOD_TS',''),
    ('group2','DOCKET_ITEM_EVENT','N','LAST_MOD_TS',''),
    ('group2','DOCKET_ITEM_EVENT_H','N','LAST_MOD_TS',''),
    ('group2','DOCKET_ITEM_H','Y','LAST_MOD_TS',''),
    #('group2','DOCUMENT_COMPONENT','N','LAST_MOD_TS','L'),
    ('group2','DOCUMENT_COMPONENT_RELTNSP','N','LAST_MOD_TS',''),
    ('group2','DOCUMENT_TEMPLATE_VERSION','N','LAST_MOD_TS',''),
    ('group2','DRAFT_DOC_VER_COMPNT_FPV','N','LAST_MOD_TS',''),
    ('group2','DRAFT_DOCUMENT','Y','LAST_MOD_TS',''),#reset back
    ('group2','DRAFT_DOCUMENT_VERSION','Y','LAST_MOD_TS',''),
    ('group2','DRAFT_DOCUMENT_VERSION_COMPNT','Y','LAST_MOD_TS',''),#reset back
    ('group2','ELECTRONIC_ADDRESS_H','Y','LAST_MOD_TS',''),
    ('group2','EMPLOYEE_AWARD_WITHDRAW','N','LAST_MOD_TS',''),
    ('group2','EMPLOYEE_CREDIT_TRANSACTION','N','LAST_MOD_TS',''),
    ('group2','EMPLOYEE_QUERY_APPEAL','N','LAST_MOD_TS',''),
    ('group2','EMPLOYEE_REVIEW_QUERY','N','LAST_MOD_TS',''),
    ('group2','EMPLOYEE_REVIEW_QUERY_STAT','N','LAST_MOD_TS',''),
    ('group2','EMPLOYEE_TM_CLASS_CREDIT','N','LAST_MOD_TS',''),
    ('group2','EVIDENCE_BIN_FOLDER','Y','LAST_MOD_TS',''),
    ('group2','EVIDENCE_BIN_FOLDER_H','Y','LAST_MOD_TS',''),
    ('group2','EVIDENCE_DOCUMENT','N','LAST_MOD_TS',''),
    ('group2','EVIDENCE_DOCUMENT_H','Y','LAST_MOD_TS',''),
    ('group2','FORM_PARAGRAPH_RULE','N','LAST_MOD_TS',''),
    ('group2','FSM_INSTANCE','N','LAST_MOD_TS',''),
    ('group2','FSM_INSTANCE_H','Y','LAST_MOD_TS',''),#reset back
    ('group2','GDS_SRVC_STMT_ANNOTATION','N','LAST_MOD_TS',''),
    #('group2','IB_TRANSACTION','N','',''),
    ('group2','INTERESTED_PARTY_ASSUMED_NM','N','LAST_MOD_TS',''),
    ('group2','INTERESTED_PARTY_ASSUMED_NM_H','Y','LAST_MOD_TS',''),
    ('group2','INTERNAL_NOTE','Y','LAST_MOD_TS',''),
    ('group2','TRAM_UPD','N','',''),
    ('group2','TM_DRAWING','N','LAST_MOD_TS',''),
    ('group2','TM_DRAWING_H','Y','LAST_MOD_TS',''),
]

# COMMAND ----------

tmngpdb_metadata_group3 = [ 
    #('group3','INTERNATIONAL_APPL_REG','N','LAST_MOD_TS',''),
    #('group3','INTERNATIONAL_APPL_REG_H','Y','LAST_MOD_TS',''),
    #('group3','INTERNATIONAL_APPLICATION','N','LAST_MOD_TS',''),
    #('group3','INTERNATIONAL_APPLICATION_H','Y','LAST_MOD_TS',''),
    #('group3','INTERNATIONAL_REG_TM_H','Y','LAST_MOD_TS',''),
    #('group3','INTERNATIONAL_REGISTRATION','N','LAST_MOD_TS',''),
    #('group3','INTERNATIONAL_REGISTRATION_H','Y','LAST_MOD_TS',''),
    #('group3','INTERNATIONAL_TM','N','LAST_MOD_TS',''),
    #('group3','INTERNATIONAL_TM_H','Y','LAST_MOD_TS',''),
    ('group3','INTRSTD_PARTY_RELATIONSHIP','N','LAST_MOD_TS',''),
    ('group3','INTRSTD_PARTY_RELATIONSHIP_H','N','LAST_MOD_TS',''),
    ('group3','IP_ELECTRONIC_ADDRESS','N','LAST_MOD_TS',''),
    ('group3','IP_MAILING_ADDRESS','N','LAST_MOD_TS',''),
    ('group3','IP_TELECOM_ADDRESS','N','LAST_MOD_TS',''),
    ('group3','IR_MAILING_ADDRESS','N','LAST_MOD_TS',''),
    ('group3','IR_MAILING_ADDRESS_GROUP','N','LAST_MOD_TS',''),
    ('group3','MAILING_ADDRESS_LINE','N','LAST_MOD_TS',''),
    ('group3','MAILING_ADDRESS_LINE_H','N','LAST_MOD_TS',''),
    ('group3','MV_MYUSPTO_TRAM_SEARCH','Y','',''),
    ('group3','MV_MYUSPTO_TRM_SEARCH','Y','',''),
    ('group3','MV_MYUSPTO_TRM_AT','Y','',''),
    ('group3','MV_MYUSPTO_TRM_MARK','Y','',''),
    ('group3','MV_MYUSPTO_TRM_OWNER','Y','',''),
    ('group3','MV_MYUSPTO_TRAM_AT','Y','',''),
    #('group3','MV_MYUSPTO_TRAM_MARK','Y','',''),
    ('group3','MV_MYUSPTO_TRAM_OWNER','Y','',''),
    #('group3','MV_MYUSPTO_TRAM_PH','Y','',''),
    #('group3','MV_MYUSPTO_TRM_PH','Y','',''),
    #('group3','MYUSPTO_TRAM_CHANGE_NTFCN','Y','',''),
    #('group3','MYUSPTO_TRAM_EVENT_TODAY','Y','',''),
    #('group3','MYUSPTO_TRAM_PH','Y','',''),
    #('group3','MYUSPTO_TRAM_STATUS_TODAY','Y','',''),
    ('group3','MYUSPTO_TRM_CHANGE_NTFCN','Y','',''),
    ('group3','MYUSPTO_TRM_EVENT_TODAY','Y','',''),
    ('group3','MYUSPTO_TRM_PH','Y','',''),
    ('group3','MYUSPTO_TRM_STATUS_TODAY','Y','',''),  
    ('group3','OBJECT_DISPATCH','N','LAST_MOD_TS',''),
    ('group3','OBJECT_DOCUMENT','Y','LAST_MOD_TS',''),
    ('group3','OBJECT_DOCUMENT_H','Y','LAST_MOD_TS',''),
    ('group3','OBJECT_FSM_INSTANCE','Y','LAST_MOD_TS',''),
    ('group3','OFFICE_ACTIVITY','Y','LAST_MOD_TS',''),
    ('group3','OFFICE_ACTIVITY_DRAFT_DOC_H','Y','LAST_MOD_TS',''),
    ('group3','OFFICE_ACTIVITY_DRAFT_DOCUMENT','Y','LAST_MOD_TS',''),#reset back
    ('group3','OFFICE_ACTIVITY_H','Y','LAST_MOD_TS',''),
    ('group3','OFFICE_ACTIVITY_REASON','N','LAST_MOD_TS',''),
    ('group3','OFFICE_ACTIVITY_REASON_H','Y','LAST_MOD_TS',''),
    ('group3','OFFICE_ACTIVITY_REVIEW','Y','LAST_MOD_TS',''),
    ('group3','TRANSACTION_INSTANCE','Y','LAST_MOD_TS','L'),
    ('group3','TM_DOCUMENT','Y','LAST_MOD_TS',''),
    ('group3','TM_DOCUMENT_REFERENCE','Y','LAST_MOD_TS',''),
]

# COMMAND ----------

tmngpdb_metadata_group4 = [ 
    
    ('group4','OG_TM_REVIEW','N','LAST_MOD_TS',''),
    ('group4','PRCDNG_EMPLOYEE_ASSIGNMENT','N','LAST_MOD_TS',''),
    ('group4','PRCDNG_EMPLOYEE_ASSIGNMENT_H','Y','LAST_MOD_TS',''),#reset back
    ('group4','PREDEFINED_PARAGRAPH','N','LAST_MOD_TS',''),
    ('group4','PREDEFINED_PARAGRAPH_VER','N','LAST_MOD_TS',''),
    ('group4','QUERY_APPEAL','N','LAST_MOD_TS',''),
    ('group4','QUERY_APPEAL_NOTE','N','LAST_MOD_TS',''),
    ('group4','QUERY_APPEAL_STATUS','N','LAST_MOD_TS',''),
    ('group4','QUERY_GROUND','N','LAST_MOD_TS',''),
    ('group4','RELATED_WORKER','N','LAST_MOD_TS',''),
    ('group4','REVIEW_ANNOTATION','Y','LAST_MOD_TS',''),
    ('group4','REVIEW_ISSUE','N','LAST_MOD_TS',''),
    ('group4','REVIEW_QUERY','N','LAST_MOD_TS',''),
    ('group4','REVIEW_QUERY_APPEAL','N','LAST_MOD_TS',''),
    ('group4','REVIEW_QUERY_CLASS','N','LAST_MOD_TS',''),
    ('group4','REVIEW_QUERY_NOTE','N','LAST_MOD_TS',''),
    #('group4','SEARCH_STRATEGY','N','LAST_MOD_TS',''),
    #('group4','SECTION_2F_PRIOR_REG','N','LAST_MOD_TS',''),
    #('group4','SECTION_2F_PRIOR_REG_H','N','LAST_MOD_TS',''),
    ('group4','SECTION_2F_STATEMENT','N','LAST_MOD_TS',''),
    ('group4','STND_ANNOTATION_STATUS','N','LAST_MOD_TS',''),
    ('group4','STND_APPEAL_RESULT','N','LAST_MOD_TS',''),
    ('group4','STND_APPEAL_STATUS','N','LAST_MOD_TS',''),
    ('group4','STND_ASSUMED_NAME_TYPE','N','LAST_MOD_TS',''),
    ('group4','STND_AVERMENT','N','LAST_MOD_TS',''),
    ('group4','STND_BUSINESS_EVENT_RSN_CT','N','LAST_MOD_TS',''),
    ('group4','STND_CATEGORY_DOC_TYPE','N','LAST_MOD_TS',''),
    ('group4','STND_CLASS_SCHEDULE','N','LAST_MOD_TS',''),
    ('group4','STND_CLASS_STATEMENT_TYPE','N','LAST_MOD_TS',''),
    ('group4','STND_COORDINATED_CLASS','N','LAST_MOD_TS',''),
    ('group4','STND_CREDIT_TRAN_RSN_TYPE','N','LAST_MOD_TS',''),
    ('group4','STND_DESIGN_SEARCH_CODE_ITEM','N','LAST_MOD_TS',''),
    ('group4','STND_DESIGN_SEARCH_GROUP','N','LAST_MOD_TS',''),
    ('group4','STND_DESIGN_SEARCH_GROUP_TYPE','N','LAST_MOD_TS',''),
    ('group4','STND_DOC_TYPE_CT','N','LAST_MOD_TS',''),
    ('group4','STND_DOCKET','N','LAST_MOD_TS',''),
    ('group4','STND_DOCKET_FSM_TYPE_STATE','N','LAST_MOD_TS',''),
    ('group4','STND_DOCKET_ITEM_EVENT_TYPE','N','LAST_MOD_TS',''),
    ('group4','STND_DOCUMENT_COMPONENT_TYPE','N','LAST_MOD_TS',''),
    ('group4','STND_DOCUMENT_TEMPLATE','N','LAST_MOD_TS',''),
    ('group4','STND_DOCUMENT_TYPE','N','LAST_MOD_TS',''),
    ('group4','STND_ELECTRONIC_ADDR_TYPE','N','LAST_MOD_TS',''),
    ('group4','STND_EVIDENCE_BIN','N','LAST_MOD_TS',''),
    ('group4','TM_ADDL_STMNT_PRIOR_REG','N','LAST_MOD_TS',''),
    ('group4','TM_ADDL_STMNT_PRIOR_REG_H','Y','LAST_MOD_TS',''),
    ('group4','TM_AMENDMENT','Y','LAST_MOD_TS',''),
    ('group4','TM_CLASS_FILING_BASIS','N','LAST_MOD_TS',''),
    ('group4','TM_CLASS_GDS_SRVC_TERM','N','LAST_MOD_TS',''),
    ('group4','TM_CLASS_GDS_SRVC_TERM_H','N','LAST_MOD_TS',''),   
    ('group4','TM_EMPLOYEE_ASSIGNMENT_H','Y','LAST_MOD_TS',''), 
    ('group4','TRAM_PQR','Y','',''),
    ('group4','TRAM_PRN','Y','',''),
    ('group4','TRAM_PQRE','Y','',''),   
    ('group4','TRAM_PRNA','Y','',''),                         
]                               

# COMMAND ----------

tmngpdb_metadata_group5 = [ 
    ('group5','STND_EVIDENCE_SOURCE_CATEGORY','N','LAST_MOD_TS',''), 
    ('group5','STND_FILING_BASIS','N','LAST_MOD_TS',''), 
    ('group5','STND_FSM_CATEGORY','N','LAST_MOD_TS',''), 
    ('group5','STND_FSM_STATE_LEGACY_STATE','N','LAST_MOD_TS',''), 
    ('group5','STND_FSM_TYPE','N','LAST_MOD_TS',''), 
    ('group5','STND_FSM_TYPE_EVENT','N','LAST_MOD_TS',''), 
    ('group5','STND_FSM_TYPE_STATE','N','LAST_MOD_TS',''), 
    ('group5','STND_FSM_TYPE_STATE_RULE','Y','LAST_MOD_TS',''), 
    ('group5','STND_GDS_SRVC_ANNOTN_STAT','N','LAST_MOD_TS',''), 
    ('group5','STND_GDS_SRVC_MATCH_STAT','N','LAST_MOD_TS',''), 
    ('group5','STND_GDS_SRVC_STATUS','N','LAST_MOD_TS',''), 
    ('group5','STND_GROUND','N','LAST_MOD_TS',''), 
    ('group5','STND_GROUND_TYPE','N','LAST_MOD_TS',''), 
    ('group5','STND_INTRSTD_PARTY_RLTNSP_TYPE','N','LAST_MOD_TS',''), 
    ('group5','STND_LEGACY_STATUS','N','LAST_MOD_TS',''), 
    ('group5','STND_LEGACY_TRANSACTION','N','LAST_MOD_TS',''), 
    ('group5','STND_LEGAL_ENTITY_TYPE','N','LAST_MOD_TS',''), 
    ('group5','STND_MAD_BIRTH_REC_CT_TYPE','N','LAST_MOD_TS',''), 
    ('group5','STND_MAD_TRANSACTION_TYPE','N','LAST_MOD_TS',''), 
    ('group5','STND_MARK_TYPE','N','LAST_MOD_TS',''), 
    ('group5','STND_MYUSPTO_EVENT','N','',''), 
    ('group5','STND_NOTE_TYPE','N','LAST_MOD_TS',''), 
    ('group5','STND_OBJECT_DISPATCH_TYPE','N','LAST_MOD_TS',''), 
    ('group5','STND_OBJECT_TYPE','N','LAST_MOD_TS',''), 
    ('group5','STND_OFFICE_ACTION_CATEGORY','N','LAST_MOD_TS',''), 
    ('group5','STND_OFFICE_ACTION_CT_STATE','Y','LAST_MOD_TS',''), 
    ('group5','STND_OFFICE_ACTION_RULE','N','LAST_MOD_TS',''), 
    ('group5','STND_OFFICE_ACTIVITY_REASON','N','LAST_MOD_TS',''), 
    ('group5','STND_OFFICE_ACTN_RULE_ITM','N','LAST_MOD_TS',''), 
    ('group5','STND_OFFICE_ACTVTY_RSN_CT','N','LAST_MOD_TS',''), 
    ('group5','STND_OWNER_TYPE','N','LAST_MOD_TS',''), 
    ('group5','STND_PAY_PERIOD','N','LAST_MOD_TS',''), 
    ('group5','STND_PRCDNG_EMPE_ASGMT_ROLE','N','LAST_MOD_TS',''), 
    ('group5','STND_PUBLICATION_CATEGORY','N','LAST_MOD_TS',''), 
    ('group5','STND_PUBLICATION_SUBCATEGORY','N','LAST_MOD_TS',''), 
    ('group5','STND_QUERY_REVIEW_STATUS','N','LAST_MOD_TS',''), 
    ('group5','STND_REG_STMNT_TYPE','N','LAST_MOD_TS',''), 
    ('group5','STND_RELATIONSHIP_TYPE','N','LAST_MOD_TS',''), 
    ('group5','STND_RESPONSE_ISSUE','N','LAST_MOD_TS',''), 
    ('group5','STND_REVIEW_ISSUE','N','LAST_MOD_TS',''), 
    ('group5','STND_REVIEW_RATING','N','LAST_MOD_TS',''), 
    ('group5','STND_STATEMENT_TYPE','N','LAST_MOD_TS',''), 
    ('group5','STND_SUBMISSION_METHOD','N','LAST_MOD_TS',''), 
    ('group5','STND_TELECOM_FORMAT','N','LAST_MOD_TS',''), 
    ('group5','STND_TELECOM_TYPE','N','LAST_MOD_TS',''), 
    ('group5','STND_TEMPLATE_PARA_TYPE','N','LAST_MOD_TS',''), 
    ('group5','STND_TM_AMENDMENT_REASON','N','LAST_MOD_TS',''), 
    ('group5','STND_TM_CLASS_STATUS','N','LAST_MOD_TS',''), 
    ('group5','TRAM_CL','N','',''),
    ('group5','TRAM_CM','N','',''),
    ('group5','TRAM_MC','N','',''),
]

# COMMAND ----------

tmngpdb_metadata_group6 = [
    ('group6','STND_TM_DIVISIONAL_STATUS','N','LAST_MOD_TS',''),
    ('group6','STND_TM_EMPLOYEE_ASGMT_ROLE','N','LAST_MOD_TS',''),
    ('group6','STND_TM_GROUP_TYPE','N','LAST_MOD_TS',''),
    ('group6','STND_TM_INTRSTD_PARTY_ROLE','N','LAST_MOD_TS',''),
    ('group6','STND_TM_PARTY_ROLE','N','LAST_MOD_TS',''),
    ('group6','STND_TM_REVIEW_STATUS','N','LAST_MOD_TS',''),
    ('group6','STND_US_INTL_CLS_MAPPING','N','LAST_MOD_TS',''),
    ('group6','STND_WORK_ITEM_RELTNSP_TYPE','N','LAST_MOD_TS',''),
    ('group6','STND_WORK_ITEM_REQUEST','N','LAST_MOD_TS',''),
    ('group6','STND_WORK_ITEM_TYPE','Y','LAST_MOD_TS',''),
    ('group6','STND_WORK_ITEM_TYPE_DOC_TMPLT','N','LAST_MOD_TS',''),
    ('group6','STND_WORK_ITEM_TYPE_RULE','N','LAST_MOD_TS',''),
    ('group6','STND_WORKER_RELTNSP_TYPE','N','LAST_MOD_TS',''),
    ('group6','STND_WRITING_RVW_ADDL_ACTN','N','LAST_MOD_TS',''),
    ('group6','SUBMISSION','Y','LAST_MOD_TS',''),
    ('group6','SUBMISSION_AVERMENT','N','LAST_MOD_TS',''),
    ('group6','SUBMISSION_AVERMENT_H','N','LAST_MOD_TS',''),
    ('group6','SUBMISSION_ELCTRN_ADDR','N','LAST_MOD_TS',''),
    ('group6','SUBMISSION_ELCTRN_ADDR_H','N','LAST_MOD_TS',''),
    ('group6','SUBMISSION_H','Y','LAST_MOD_TS',''),
    ('group6','SUBMISSION_ITEM','Y','LAST_MOD_TS',''),
    ('group6','SUBMISSION_ITEM_H','Y','LAST_MOD_TS',''),
    ('group6','SUBMISSION_SIGNATURE','N','LAST_MOD_TS',''),
    #('group6','SYNC_AUTHUSER','Y','LASTUPDATED',''),
    ('group6','SYNC_CASELOCK','Y','',''),
    ('group6','SYNC_CASESTATUS','N','CS_TIMESTAMP',''),
    #('group6','SYNC_CHECKPOINT','N','END_TS',''),
    #('group6','SYNC_EXCEPTION_TYPE','Y','',''),
    ('group6','SYNC_EXCEPTIONS','Y','INSERT_DT',''),
    #('group6','SYNC_LOG','Y','CREATEDATE',''),
    #('group6','SYNC_MIGRATION_RULES','Y','',''),
    #('group6','SYNC_MIGRATION_SCRIPT','N','',''),
    ('group6','SYNC_RUNTIME','Y','',''),
    ('group6','SYNC_STND_AM_STAT','N','',''),
    ('group6','SYNC_TM_COM_EXCEPTION','Y','RESOLVED_TS',''),
    #('group6','SYNC_TRAM_TRM_OBJ_ID_MAPPING','Y','',''),
    #('group6','SYNC_TRANLOG','Y','',''),
    ('group6','SYNC_TRANSLATE_ASSUMED_NAME','Y','',''),
    ('group6','SYNC_TRANSLATE_EMP_LO','Y','',''),
    ('group6','SYNC_TRANSLATE_EP','Y','',''),
    ('group6','SYNC_TRANSLATE_GEO','N','',''),
    ('group6','SYNC_TRANSLATE_LOCATION','Y','',''),
    ('group6','SYNC_TRANSLATE_OG_CATG','N','',''),
    ('group6','SYNC_TRANSLATE_PARTY_TYPE','N','',''),
    ('group6','SYNC_TRANSLATE_PETITION_DOCKT','N','',''),
    ('group6','SYNC_TRANSLATE_WORK_ITEM_CMS','Y','',''),
    #('group6','SYNC_TRM_TO_TRAM_CONTROL','Y','LAST_MOD_TS',''),
    ('group6','TELECOM_ADDRESS','Y','LAST_MOD_TS',''),
    ('group6','TELECOM_ADDRESS_H','Y','LAST_MOD_TS',''),
    ('group6','TM_ADDITIONAL_STATEMENT','N','LAST_MOD_TS',''),
    ('group6','WORK_ITEM_OBJECT_H','Y','LAST_MOD_TS',''),
    ('group6','TRAM_VT','N','',''),
]

# COMMAND ----------

tmngpdb_metadata_group7 = [
    ('group7','TM_ADDITIONAL_STATEMENT_H','Y','LAST_MOD_TS',''),
    ('group7','OG_PUBLICATION','N','LAST_MOD_TS',''),
    ('group7','OG_PUBLICATION_H','N','LAST_MOD_TS',''),
    ('group7','OG_PUBLICATION_TM','Y','LAST_MOD_TS',''),
    ('group7','OG_PUBLICATION_TM_H','Y','LAST_MOD_TS',''),
    ('group7','SECTION_2F_STATEMENT_H','Y','LAST_MOD_TS',''),
    ('group7','TM_DESIGN_ELEMENT','N','LAST_MOD_TS',''),  
    ('group7','TM_REGISTRATION_STATEMENT_H','Y','LAST_MOD_TS',''),
    ('group7','TM_RELATIONSHIP_H','N','LAST_MOD_TS',''),
    ('group7','TRADEMARK_H','Y','LAST_MOD_TS','L'),
    ('group7','MAILING_ADDRESS_H','Y','LAST_MOD_TS',''),
    ('group7','INTERESTED_PARTY_H','Y','LAST_MOD_TS','L'),
    ('group7','CONCURRENT_USE_H','Y','LAST_MOD_TS',''),
    ('group7','USE_IN_ANOTHER_FORM_H','Y','LAST_MOD_TS',''),
    ('group7','TM_PSEUDO_MARK_H','Y','LAST_MOD_TS',''),
    ('group7','TM_PUBLICATION','Y','LAST_MOD_TS',''),
    ('group7','TM_PUBLICATION_H','Y','LAST_MOD_TS',''),
    ('group7','TM_PUBLICATION_SUBCT','N','LAST_MOD_TS',''),
    ('group7','TM_PUBLICATION_SUBCT_H','Y','LAST_MOD_TS',''),
    ('group7','TM_CLASS_H','Y','LAST_MOD_TS','L'),
    ('group7','TM_FOREIGN_BASIS_H','Y','LAST_MOD_TS',''),
    ('group7','TM_PARTY_ROLE_H','Y','LAST_MOD_TS',''),
    ('group7','TM_PRIOR_REGISTRATION_H','Y','LAST_MOD_TS',''),
    ('group7','TM_MILESTONE_H','Y','LAST_MOD_TS',''),
]

# COMMAND ----------

tmngpdb_metadata_group8 = [
    ('group8','TM_DIVISIONAL','N','LAST_MOD_TS',''),
    ('group8','TM_DIVISIONAL_CHILD_H','N','LAST_MOD_TS',''),
    ('group8','TM_DIVISIONAL_H','N','LAST_MOD_TS',''),
    ('group8','TM_REGISTRATION_STATEMENT','N','LAST_MOD_TS',''),    
    ('group8','TM_RENEWAL','N','LAST_MOD_TS',''),
    ('group8','TM_RENEWAL_H','Y','LAST_MOD_TS',''),
    ('group8','TM_TELECOM_ADDR','N','LAST_MOD_TS',''),
    ('group8','TM_TELECOM_ADDR_H','Y','LAST_MOD_TS',''),
    ('group8','TRAM_AM','N','',''),
    ('group8','TRAM_AMQ','N','',''),
    ('group8','TRAM_AMQE','N','',''),
    ('group8','TRAM_ATH','N','',''),
    ('group8','TRAM_CAC','N','',''),
    ('group8','TRAM_CAD','N','',''),
    ('group8','TRAM_CB','N','',''),
    ('group8','TRAM_CDH','N','',''),
    #('group8','TRAM_CL','N','',''),
    #('group8','TRAM_CM','N','',''),
    ('group8','TRAM_COP','N','',''),
    ('group8','TRAM_CRQ','N','',''),
    ('group8','TRAM_CT','N','',''),
    ('group8','TRAM_DES','N','',''),
    ('group8','TRAM_DSC','N','',''),
    ('group8','TRAM_DV','N','',''),
    ('group8','TRAM_DVC','N','',''),
    ('group8','TRAM_ECR','Y','',''),
    ('group8','TRAM_EE','N','',''),
    ('group8','TRAM_EM','N','',''),
    ('group8','TRAM_EMA','N','',''),
    ('group8','TRAM_EME','N','',''),
    ('group8','TRAM_EML','N','',''),
    ('group8','TRAM_EP','N','',''),
    ('group8','TRAM_FN','N','',''),
    ('group8','TRAM_FPR','N','',''),
    ('group8','TRAM_FT','N','',''),
    ('group8','TRAM_GS','N','',''),
    ('group8','TRAM_IU','N','',''),
    ('group8','TRAM_IX','N','',''),
    ('group8','TRAM_JN','N','',''),
    ('group8','TRAM_MAD','N','',''),
    ('group8','TRAM_MAS','N','',''),
    #('group8','TRAM_MC','N','',''),
    ('group8','TRAM_MD','N','',''),
    ('group8','TRAM_MHI','N','',''),
    ('group8','TRAM_MIF','N','',''),
    ('group8','TRAM_MN','N','',''),
    ('group8','TRAM_NI','N','',''),
    ('group8','TRAM_OG','N','',''),
    ('group8','TRAM_OGH','N','',''),
    ('group8','TRAM_OI','N','',''),
]

# COMMAND ----------

tmngpdb_metadata_group9 = [
    ('group9','TRAM_OT','N','',''),
    ('group9','TRAM_PAS','N','',''),
    ('group9','TRAM_PCM','N','',''),
    ('group9','TRAM_PD','N','',''),
    ('group9','TRAM_PI','N','',''),
    ('group9','TRAM_PLH','N','',''),
    ('group9','TRAM_PQ','N','',''),
    ('group9','TRAM_PQC','N','',''),
    ('group9','TRAM_PR','N','',''),
    ('group9','TRAM_PSC','N','',''),
    ('group9','TRAM_PSL','N','',''),
    ('group9','TRAM_PX','N','',''),
    ('group9','TRAM_PXC','N','',''),
    ('group9','TRAM_PXQ','N','',''),
    ('group9','TRAM_PY','N','',''),
    ('group9','TRAM_QE','N','',''),
    ('group9','TRAM_RI','N','',''),
    ('group9','TRAM_RQ','N','',''),
    ('group9','TRAM_RT','N','',''),
    ('group9','TRAM_SC','N','',''),
    ('group9','TRAM_SSR','N','',''),
    ('group9','TRAM_STC','N','',''),
    ('group9','TRAM_TE','N','',''),
    ('group9','TRAM_TG','N','',''),
    ('group9','TRAM_TH','N','',''),
    ('group9','TRAM_TM','N','',''),
    ('group9','TRAM_TP','N','',''),
    ('group9','TRAM_TQR','N','',''),
    ('group9','TRAM_TRM','N','',''),
    ('group9','TRAM_TS','N','',''),
    ('group9','TRAM_TT','N','',''),
    ('group9','TRAM_TT1','N','',''),
    ('group9','TRAM_TY','N','',''),
    #('group9','TRAM_UPD','N','',''),
    ('group9','TRAM_VH','N','',''),
    #('group9','TRAM_VT','N','',''),
    ('group9','TRAM_WP','N','',''),
    ('group9','TRAM_WT','N','',''),
    ('group9','TRIGGER_EXCEPTIONS','Y','INSERT_TS',''),
    ('group9','USER_PARA_FORM_PARA_VER','N','LAST_MOD_TS',''),
    ('group9','USER_SESSION','N','LAST_MOD_TS',''),
    ('group9','WORK_ITEM','Y','LAST_MOD_TS',''),
    ('group9','WORK_ITEM_H','Y','LAST_MOD_TS',''),
    #('group9','WORK_ITEM_OBJECT_H','Y','LAST_MOD_TS',''),
    ('group9','WORK_ITEM_RELATIONSHIP','N','LAST_MOD_TS',''),
    ('group9','WORK_ITEM_RELATIONSHIP_H','Y','LAST_MOD_TS','L'),#reset back
    ('group9','WORK_ITEM_REQUEST','N','LAST_MOD_TS',''),
    ('group9','WORK_ITEM_REQUEST_EMPLOYEE','N','LAST_MOD_TS',''),
    ('group9','WORKER','N','LAST_MOD_TS',''),
    ('group9','WORKER_FOLDER','N','LAST_MOD_TS',''),
    ('group9','WORKER_FOLDER_ITEM','Y','LAST_MOD_TS',''),
    ('group9','WORKER_H','N','LAST_MOD_TS',''),
    ('group9','WRITING_REVIEW','N','LAST_MOD_TS',''),
    #('group9','TRANSACTION_INSTANCE','Y','LAST_MOD_TS','L'),
    ('group9','TM_STATES','N','LAST_MOD_TS',''),
    ('group9','TM_APPEALS','N','LAST_MOD_TS',''),
    ('group9','TM_ELECTRONIC_ADDR_H','Y','LAST_MOD_TS',''),
]

# COMMAND ----------

tmngpdb_metadata_group10 = [
    ('group10','TM_FILINGS','N','LAST_MOD_TS',''),
    ('group10','TM_FILING_BASES','N','LAST_MOD_TS',''),
    ('group10','TM_ITU_EXTENSION','Y','LAST_MOD_TS',''),
    ('group10','TM_ITU_EXTENSION_H','Y','LAST_MOD_TS',''),
    ('group10','TM_ITU_H','Y','LAST_MOD_TS',''),
    ('group10','TM_OG_PUBLICATIONS','N','LAST_MOD_TS',''),
    ('group10','TM_OG_PUBLICATIONS_H','Y','LAST_MOD_TS',''),
    ('group10','TM_CLASS_REFERENCE_H','Y','LAST_MOD_TS',''),
    ('group10','TM_GDS_SRVC_TERM_FILG_BASIS','N','LAST_MOD_TS',''),
    ('group10','TM_GDS_SRVC_TERM_FILG_BASIS_H','N','LAST_MOD_TS',''),
    ('group10','TM_GROUP','N','LAST_MOD_TS',''),
    ('group10','TM_GROUP_ITEM','Y','LAST_MOD_TS',''),
    ('group10','TM_LITERAL_H','Y','LAST_MOD_TS',''),
    ('group10','TM_LOCATIONS_H','Y','LAST_MOD_TS',''),#reset back
    ('group10','TM_MAILING_ADDR_H','Y','LAST_MOD_TS',''),
    ('group10','TM_MARK_TYPE','N','LAST_MOD_TS',''),
    ('group10','TM_MARK_TYPE_H','Y','LAST_MOD_TS',''),
    ('group10','TM_NOTIFICATION_MESSAGE','N','LAST_MOD_TS',''),
    ('group10','TM_ORGANIZATION_LOCATION','Y','LAST_MOD_TS',''),
    ('group10','TM_PHYSICAL_LOCATION','N','LAST_MOD_TS',''),
    ('group10','TM_POST_REGISTRATION','N','LAST_MOD_TS',''),
    ('group10','TM_PROCEEDING','Y','LAST_MOD_TS',''),
    ('group10','TM_PROCEEDING_H','Y','LAST_MOD_TS',''),
    ('group10','TM_PSEUDO_CLASS','N','LAST_MOD_TS',''),
    ('group10','TM_PSEUDO_CLASS_H','Y','LAST_MOD_TS',''),
    ('group10','TM_PSEUDO_MARK','N','LAST_MOD_TS',''),
    ('group10','TM_DESIGN_ELEMENT_H','Y','LAST_MOD_TS',''),
    ('group10','TM_FOREIGN_BASIS','N','LAST_MOD_TS',''),
    ('group10','TM_PRIOR_REGISTRATION','N','LAST_MOD_TS',''),
]

# COMMAND ----------

tmngpdb_metadata_group12 = [
    ('group12','SEARCH_STRATEGY','Y','LAST_MOD_TS',''),
]

# COMMAND ----------

tmbuscalendar_metadata = [
    ('cdc_load_tables','BUSINESS_CALENDAR_RANGE','N','LAST_MOD_TS'),
    ('cdc_load_tables','BUSINESS_CALENDAR_DAY','N','LAST_MOD_TS'),
    ('cdc_load_tables','BUS_CALENDAR_DAY_PROPERTY','N','LAST_MOD_TS')
]

# COMMAND ----------

tmintltm_metadata = [
    ('cdc_load_tables','INTERNATIONAL_APPL_EVENT','Y','LAST_MOD_TS'),
    ('cdc_load_tables','BASE_APPLICATION','N','LAST_MOD_TS'),
    ('cdc_load_tables','BASE_APPLICATION_H','N','LAST_MOD_TS'),
    ('cdc_load_tables','INTERNATIONAL_APPLICATION','N','LAST_MOD_TS'),
    ('cdc_load_tables','INTERNATIONAL_APPLICATION_H','N','LAST_MOD_TS'),
    ('cdc_load_tables','INTERNATIONAL_REGISTRATION','N','LAST_MOD_TS'),
    ('cdc_load_tables','INTERNATIONAL_REGISTRATION_H','N','LAST_MOD_TS'),
    ('cdc_load_tables','INTERNATIONAL_REG_TM','N','LAST_MOD_TS'),
    ('cdc_load_tables','INTERNATIONAL_REG_TM_H','N','LAST_MOD_TS'),
    ('cdc_load_tables','INTERNATIONAL_TM','N','LAST_MOD_TS'),
    ('cdc_load_tables','INTERNATIONAL_TM_H','N','LAST_MOD_TS'),
    ('cdc_load_tables','INTERNATIONAL_APPL_EVNT_RSN','Y','LAST_MOD_TS'),
    ('cdc_load_tables','BASE_APPL_INTL_REG','Y','LAST_MOD_TS'),
]

# COMMAND ----------

tmngfpepp_metadata = [
('full_load_tables','DATABASECHANGELOG','Y',''),
('full_load_tables','FORM_PARAGRAPH_REASON','Y',''),
('cdc_load_tables','DATABASECHANGELOGLOCK','N',''),
('cdc_load_tables','QRTZ_BLOB_TRIGGERS','N',''),
('cdc_load_tables','QRTZ_CALENDARS','N',''),
('cdc_load_tables','QRTZ_CRON_TRIGGERS','N',''),
('cdc_load_tables','QRTZ_FIRED_TRIGGERS','N',''),
('cdc_load_tables','QRTZ_JOB_DETAILS','N',''),
('cdc_load_tables','QRTZ_LOCKS','Y',''),
('cdc_load_tables','QRTZ_PAUSED_TRIGGER_GRPS','Y',''),
('cdc_load_tables','QRTZ_SCHEDULER_STATE','N',''),
('cdc_load_tables','QRTZ_SIMPLE_TRIGGERS','N',''),
('cdc_load_tables','QRTZ_SIMPROP_TRIGGERS','N',''),
('cdc_load_tables','QRTZ_TRIGGERS','N',''),
('cdc_load_tables','FORM_PARAGRAPH','Y','LAST_MOD_TS'),
('cdc_load_tables','FORM_PARAGRAPH_ACTION','N','LAST_MOD_TS'),
('cdc_load_tables','FORM_PARAGRAPH_VERSION','Y','LAST_MOD_TS'),
('cdc_load_tables','FPV_SCHEDULED_JOB','N','LAST_MOD_TS'),
('cdc_load_tables','STND_CHAPTER_SECTION','N','LAST_MOD_TS'),
('cdc_load_tables','STND_FORM_PARAGRAPH_ACTION','N','LAST_MOD_TS'),
('cdc_load_tables','STND_FORM_PARAGRAPH_CATEGORY','N','LAST_MOD_TS'),
('cdc_load_tables','STND_FORM_PARAGRAPH_GROUP','N','LAST_MOD_TS'),
('cdc_load_tables','STND_FORM_PARAGRAPH_REASON','N','LAST_MOD_TS'),
]

# COMMAND ----------

databridge_metadata = [
('cdc_load_tables','mhi','N',''),
('cdc_load_tables','em','N',''),
('cdc_load_tables','mas','N',''),
('cdc_load_tables','mif','N',''),
('cdc_load_tables','tt','N',''),
('cdc_load_tables','cm','N',''),
]

# COMMAND ----------

eogadmin_metadata = [
('cdc_load_tables','fsm_instance','N',''),
('cdc_load_tables','fsm_instance_h','N',''),
('cdc_load_tables','fsm_interlock','N',''),
('cdc_load_tables','og_appeal_fsm_instance','N',''),
('cdc_load_tables','og_review_fsm_instance','N',''),
('cdc_load_tables','og_review_query_fsm_instance','N',''),
('cdc_load_tables','qrtz_blob_triggers','N',''),
('cdc_load_tables','qrtz_calendars','N',''),
('cdc_load_tables','qrtz_cron_triggers','N',''),
('cdc_load_tables','qrtz_fired_triggers','N',''),
('cdc_load_tables','qrtz_job_details','N',''),
('cdc_load_tables','qrtz_locks','Y',''),
('cdc_load_tables','qrtz_paused_trigger_grps','Y',''),
('cdc_load_tables','qrtz_scheduler_state','N',''),
('cdc_load_tables','qrtz_simple_triggers','N',''),
('cdc_load_tables','qrtz_simprop_triggers','N',''),
('cdc_load_tables','qrtz_triggers','N',''),
('cdc_load_tables','stnd_domain','N',''),
('cdc_load_tables','stnd_fsm_category','N',''),
('cdc_load_tables','stnd_fsm_interlock','Y',''),
('cdc_load_tables','stnd_fsm_interlock_type','N',''),
('cdc_load_tables','stnd_fsm_type','N',''),
('cdc_load_tables','stnd_fsm_type_event','N',''),
('cdc_load_tables','stnd_fsm_type_state','N',''),
('cdc_load_tables','stnd_fsm_type_state_rule','N',''),
('cdc_load_tables','stnd_interlock_type','Y',''),
('cdc_load_tables','user_profile','N',''),
('cdc_load_tables','user_profile_preference','N',''),
]

# COMMAND ----------

jbteasps_metadata = [
    ('cdc_load_tables','stnd_source_system','N',''),
    ('cdc_load_tables','audit_log','N',''),
    ('cdc_load_tables','stnd_transaction_type','N',''),
]

# COMMAND ----------

proceeding_metadata = [
    ('cdc_load_tables','petition','N',''),
    ('cdc_load_tables','petition_h','N',''),
    ('cdc_load_tables','petition_response','N',''),
    ('cdc_load_tables','petition_response_document','N',''),
    ('cdc_load_tables','petition_response_document_h','N',''),
    ('cdc_load_tables','petition_response_h','N',''),
    ('full_load_tables','prcdng_trigger_exceptions','Y',''),
    ('cdc_load_tables','proceeding','N',''),
    ('cdc_load_tables','proceeding_class','N',''),
    ('cdc_load_tables','proceeding_class_h','N',''),
    ('cdc_load_tables','proceeding_document','N',''),
    ('cdc_load_tables','proceeding_document_h','N',''),
    ('cdc_load_tables','proceeding_event','N',''),
    ('cdc_load_tables','proceeding_event_reason','N',''),
    ('cdc_load_tables','proceeding_fee','N',''),
    ('cdc_load_tables','proceeding_fee_h','N',''),
    ('cdc_load_tables','proceeding_h','N',''),
    ('cdc_load_tables','proceeding_mark','N',''),
    ('cdc_load_tables','proceeding_mark_h','N',''),
    ('cdc_load_tables','proceeding_participant','N',''),
    ('cdc_load_tables','proceeding_participant_h','N',''),
    ('cdc_load_tables','proceeding_statement','N',''),
    ('cdc_load_tables','proceeding_statement_h','N',''),
    ('cdc_load_tables','proceeding_tran_instance', 'N',''),
    ('cdc_load_tables','sync_tm_com_exception','N',''),
    #('cdc_load_tables','tram_pi','N',''),
    ('cdc_load_tables','letter_of_protest','N',''),
    ('cdc_load_tables','letter_of_protest_h','N',''),
    ('cdc_load_tables','lop_legal_basis','N',''),
    ('cdc_load_tables','lop_legal_basis_h','N',''),
    ('cdc_load_tables','lop_legal_basis_trademark','N',''),
    ('cdc_load_tables','lop_legal_basis_trademark_h','N',''),
    ('cdc_load_tables','proceeding_intl_appl','N',''),
    ('cdc_load_tables','proceeding_intl_appl_h','N',''),
    #('cdc_load_tables','proceeding_statement_bkp','N',''),
    #('cdc_load_tables','proceeding_statement_h_bkp','N',''),
    ('cdc_load_tables','stnd_lop_legal_basis','N',''),
    ('cdc_load_tables','stnd_petition_to_director','N',''),
    #('cdc_load_tables','sync_tm_com_exception_old','N',''),
    #('cdc_load_tables','temp_lop_cntl','N',''),
]

# COMMAND ----------

tmprodvty_metadata = [
    ('cdc_load_tables','production_transaction','N',''),
    ('cdc_load_tables','productivity_action','N',''),
    ('cdc_load_tables','worker_time_entry','N',''),
    ('full_load_tables','PRODUCTION_TRANSACTION_ERRLOG','Y','')
]

# COMMAND ----------

tmreviews_metadata = [
    ('cdc_load_tables','PRE_EXAM_QUALITY_REVIEW','N',''),
    ('cdc_load_tables','PRE_EXAM_QUALITY_RVW_ERR','N',''),
    ('cdc_load_tables','POST_REG_QUALITY_REVIEW','N','LAST_MOD_TS'),
    ('full_load_tables','POST_REG_QUALITY_REVIEW_ERRLOG','Y','LAST_MOD_TS'),
    ('cdc_load_tables','POST_REG_QUALITY_REVIEW_H','N','LAST_MOD_TS'),
    ('cdc_load_tables','POST_REG_REVIEW_NOTICE','N','LAST_MOD_TS'),
    ('full_load_tables','POST_REG_REVIEW_NOTICE_ERRLOG','Y','LAST_MOD_TS'),
    ('cdc_load_tables','POST_REG_REVIEW_NOTICE_H','N','LAST_MOD_TS'), 
    ('cdc_load_tables','PREG_QUALITY_REVIEW_ELEMENT','N','LAST_MOD_TS'),
    ('full_load_tables','PREG_QUALITY_REVIEW_ELEMENT_ERRLOG','Y','LAST_MOD_TS'),
    ('cdc_load_tables','PREG_QUALITY_REVIEW_ELEMENT_H','N','LAST_MOD_TS'),
]

# COMMAND ----------

tmworker_metadata = [
    ('cdc_load_tables','TM_ORGANIZATION','N','LAST_MOD_TS'),
    ('cdc_load_tables','WORKER','N','LAST_MOD_TS'),
    ('cdc_load_tables','TM_ORGANIZATION_RLTNSHP','N','LAST_MOD_TS'),
    ('cdc_load_tables','TRANSACTION_INSTANCE','N','LAST_MOD_TS'),
    ('cdc_load_tables','USER_ROLE','N','LAST_MOD_TS'),
    ('cdc_load_tables','USER_ROLE_GROUP','N','LAST_MOD_TS'),
    ('cdc_load_tables','WORKER_H','N','LAST_MOD_TS'),
    ('cdc_load_tables','WORKER_ROLE_H','N','LAST_MOD_TS'),
    ('cdc_load_tables','WORKER_ROLE','N','LAST_MOD_TS'),
    ('full_load_tables','SYNC_TRANSLATE_LOCATION','Y','')
]

# COMMAND ----------

tmngidmp_metadata = [
    ('cdc_load_tables', 'audit_revision', 'N','LAST_MOD_TS'),
    ('full_load_tables','data_comp', 'Y', ''),
    ('full_load_tables','data_comp_parsed', 'Y', ''),
    ('full_load_tables','data_comp_result', 'Y', ''),
    ('full_load_tables','data_comp_sam', 'Y', ''),
    ('full_load_tables','data_comp_sam_result', 'Y', ''),
    ('full_load_tables','data_comp_test', 'Y', ''),
    ('full_load_tables','data_id', 'Y', ''),
    ('full_load_tables','data_id_case_level_result', 'Y', ''),
    ('full_load_tables','data_id_parsed', 'Y', ''),
    ('full_load_tables','data_id_parsed_standard', 'Y', ''),
    ('full_load_tables','data_teas_plus_clob', 'Y', ''),
    ('full_load_tables','data_teas_standard_clob', 'Y', ''),
    ('cdc_load_tables', 'goods_services_term', 'N', 'LAST_MOD_TS'),
    ('cdc_load_tables', 'goods_services_term_draft', 'N', 'LAST_MOD_TS'),
    ('cdc_load_tables', 'goods_services_term_note', 'Y', 'LAST_MOD_TS'),
    ('cdc_load_tables', 'goods_services_term_note_draft', 'N', 'LAST_MOD_TS'),
    ('cdc_load_tables', 'international_class_version', 'N', 'LAST_MOD_TS'),
    ('cdc_load_tables', 'international_clsfcn_edn', 'N', 'LAST_MOD_TS'),
    ('cdc_load_tables', 'intl_clsfcn_edn_ver', 'N', 'LAST_MOD_TS'),
    ('cdc_load_tables', 'intl_clsfcn_edn_ver_rel', 'N', 'LAST_MOD_TS'),
    ('cdc_load_tables', 'menu_item', 'N', 'LAST_MOD_TS'),
    ('cdc_load_tables', 'stnd_application_message', 'Y', 'LAST_MOD_TS'),
    ('cdc_load_tables', 'stnd_application_property', 'N', 'LAST_MOD_TS'),
    ('cdc_load_tables', 'stnd_class', 'N', 'LAST_MOD_TS'),
    ('cdc_load_tables', 'stnd_class_schedule', 'N', 'LAST_MOD_TS'),
    ('cdc_load_tables', 'stnd_coordinated_class', 'N', 'LAST_MOD_TS'),
    ('cdc_load_tables', 'stnd_goods_services_note', 'N', 'LAST_MOD_TS'),
    ('cdc_load_tables', 'stnd_synonym_group', 'Y', 'LAST_MOD_TS'),
    ('cdc_load_tables', 'stnd_term_status', 'N', 'LAST_MOD_TS'),
    ('cdc_load_tables', 'stnd_us_intl_cls_mapping', 'N', 'LAST_MOD_TS'),
    ('full_load_tables', 'sync_idm_update_log', 'Y', ''),
    ('cdc_load_tables', 'taxonomy_group', 'N', 'LAST_MOD_TS'),
    ('cdc_load_tables', 'tm5_file', 'N', 'LAST_MOD_TS'),
    ('cdc_load_tables', 'tm5_goods_services', 'N', 'LAST_MOD_TS')
]

# COMMAND ----------

efoiap_metadata = [
    ('cdc_load_tables','appeal_decision_issue','N',''),
    ('cdc_load_tables','document_type','N',''),
    ('cdc_load_tables','stnd_decision','N',''),
    ('cdc_load_tables','stnd_level_1_issue','N',''),
    ('cdc_load_tables','stnd_level_2_issue','N',''),
    ('cdc_load_tables','tm_appeal_decision','N',''),
    ('cdc_load_tables','tm_appeal_decision_h','N',''),
    ('cdc_load_tables','trademark_appeal_decision','N',''),
    ('cdc_load_tables','efoia_trigger_exceptions','Y',''),
    ('cdc_load_tables','prosecution_history_event','Y',''),
    ('cdc_load_tables','prosecution_history_event2','Y',''),
    ('cdc_load_tables','tmng_go_live','Y',''),
    ('cdc_load_tables','tm_appeal_decision_errlog','Y',''),
]


# COMMAND ----------

tmrefdata_metadata = [
    ('full_load_tables', 'CODE_TYPE', 'Y', ''),
    ('full_load_tables', 'CODE_TYPE_62623', 'Y', ''),
    ('full_load_tables', 'CODE_TYPE_DEPENDENCY', 'Y', ''),
    ('full_load_tables', 'CODE_TYPE_DEPENDENCY_62623', 'Y', ''),
    ('full_load_tables', 'CODE_TYPE_DOMAIN_SERVICE', 'Y', ''),
    ('full_load_tables', 'CODE_TYPE_PROPERTY_TYPE', 'Y', ''),
    ('full_load_tables', 'CODE_TYPE_PROPERTY_TYPE_62623', 'Y', ''),
    ('full_load_tables', 'CODE_VALUE', 'Y', ''),
    ('full_load_tables', 'CODE_VALUE_62623', 'Y', ''),
    ('full_load_tables', 'CODE_VALUE_BAK', 'Y', ''),
    ('full_load_tables', 'CODE_VALUE_DEPENDENCY', 'Y', ''),
    ('full_load_tables', 'CODE_VALUE_DEPENDENCY_62623', 'Y', ''),
    ('full_load_tables', 'CODE_VALUE_PROPERTY', 'Y', ''),
    ('full_load_tables', 'CODE_VALUE_PROPERTY_62623', 'Y', ''),
    ('full_load_tables', 'DOMAIN_SERVICE', 'Y', ''),
    ('full_load_tables', 'DOMAIN_SERVICE_COMPLETE_LIST', 'Y', '')
]
