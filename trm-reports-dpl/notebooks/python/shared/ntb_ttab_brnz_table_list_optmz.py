# Databricks notebook source
# MAGIC %md
# MAGIC Schema = ["TABLE_GROUP_NAME","TABLE_NAME","FULL_LOAD","DQ_FLTR","LARGE_TABLE_IND","ZORDER Columns","numPartitions","fetchsize",'partitionColumn','lowerBound,'upperBound']

# COMMAND ----------

# DBTITLE 1,ttab_metadata_group1 - Daily
ttabp_metadata_group1 = [
    ('daily_load', 'PROSECUTION_HISTORY_EVENT', 'N', 'LAST_UPDATE_TIMESTAMP', '', 'FK_PROCEEDINGNUMBER0, IDENTIFIER, FK_PROCEEDINGTYPE', 10, 10000, 'ENTRY_NUM', 1, 771)
]

# COMMAND ----------

# DBTITLE 1,ttab_metadata_group2 - Daily
ttabp_metadata_group2 = [
    ('daily_load', 'PROCEEDING_SCHEDULE', 'N', 'LAST_MOD_TS', '', 'PROCEEDING_SCHEDULE_ID', 10, 10000, 'SCHEDULE_SEQUENCE_NO', 1, 48),
    ('daily_load', 'PROPERTY', 'N', 'LAST_UPDATE_TIMESTAMP', '', 'IDENTIFIER', '', '', '', '', '')                    
]

# COMMAND ----------

# DBTITLE 1,ttab_metadata_group3 - Daily
ttabp_metadata_group3 = [
    ('daily_load', 'RPT_ACTIVITY', 'N', 'LAST_UPDATE_TIMESTAMP', '', 'FK_EMPLOYEE_PROIDENTIFIER, FK_RPT_EVENTSIDENTIFIER, ACTIVITY_DATE', 10, 10000, 'ACTIVITY_DATE', '2001-06-14 00:00:00', '2025-01-13 00:00:00'),
    ('daily_load', 'EVENT_LOG', 'N', 'EVENT_TS', '', 'EVENT_LOG_ID', '', '', '', '', ''),
]

# COMMAND ----------

# DBTITLE 1,ttab_metadata_group4 - Daily
ttabp_metadata_group4 = [
    ('daily_load', 'ADDRESS', 'N', 'LAST_UPDATE_TIMESTAMP', '', 'IDENTIFIER', '', '', '', '', ''),
    ('daily_load', 'CONTESTED_MOTION', 'N', 'LAST_MODIFIED_TS', '', 'CM_ID', '', '', '', '', ''),
    ('daily_load', 'EFILING_SESSION', 'N', 'LAST_MOD_TS', '', 'EFILING_SESSION_ID', '', '', '', '', ''),
    ('daily_load', 'EFILING_SESSION_EMAIL', 'N', 'LAST_MOD_TS', '', 'FK_EFILING_SESSION_ID, SEQUENCE_NO', '', '', '', '', ''),
    ('daily_load', 'PARTY', 'N', 'LAST_UPDATE_TIMESTAMP', '', 'IDENTIFIER, FK_PROCEEDINGNUMBER0, FK_PROCEEDINGTYPE', '', '', '', '', ''),
    ('daily_load', 'PROCEEDING', 'N', 'LAST_UPDATE_TIMESTAMP', '', 'NUMBER0, TYPE', '', '', '', '', ''),
    ('daily_load', 'PROCEEDING_STATUS_HIST', 'Y', '', '', 'PROCEEDING_STATUS_HIST', '', '', '', '', ''),
    ('daily_load', 'PROPERTY_FILING_TYPE', 'N', 'LAST_MOD_TS', '', 'PROPERTY_FILING_TYPE_ID', '', '', '', '', ''),
    ('daily_load', 'PROPERTY_GOOD_SERVICE', 'N', 'LAST_MOD_TS', '', 'PROPERTY_GOOD_SERVICE_ID', '', '', '', '', ''),
]

# COMMAND ----------

# DBTITLE 1,ttab_metadata_group5-Daily
ttabp_metadata_group5 = [
   
    ('daily_load', 'ADDRESS_HIST', 'N', 'LAST_UPDATE_TIMESTAMP', '', 'IDENTIFIER', '', '', '', '', ''),
    ('daily_load', 'EFILING_SESSION_ARCHV', 'Y', '', '', 'EFILING_SESSION_ARCHV_ID', '', '', '', '', ''),
    #changed to full load as doesn't have primary keys in dev
    ('daily_load', 'EFILING_SESSION_EMAIL_ARCHV', 'Y', '', '', 'FK_EFILING_SESSION_ARCHV_ID, SEQUENCE_NO', '', '', '', '', ''),
    ('daily_load', 'EMPLOYEE_HIST', 'N', 'PREV_UPDATE_DT', '', 'EMPLOYEE_HIST_ID', '', '', '', '', ''),
    ('daily_load', 'BOARD_DECISION', 'Y', '', '', 'CLASS_CD, TTAB_ACTION_CD', '', '', '', '', ''), 
    ('daily_load', 'EXTEND_TIME_GRND_PROC_HIST', 'N', 'LAST_MOD_TS', '', 'EXTEND_TIME_GRND_PROC_HIST_ID', '', '', '', '', ''),
    ('daily_load', 'PARTY_GRANTED_TO_DATE_HIST', 'N', 'PREV_UPDATE_DT', '', 'PARTY_GRANTED_TO_DATE_HIST_ID', '', '', '', '', ''), 
    ('daily_load', 'PARTY_HIST', 'N', 'LAST_UPDATE_TIMESTAMP', '', 'PARTY_HIST_ID', '', '', '', '', ''),
    ('daily_load', 'POINT_LOG', 'Y', '', '', 'POINT_LOG_ID', '', '', '', '', ''),
    ('daily_load', 'POINT_LOG_PRCDNG_PARTY', 'N', 'LAST_MOD_TS', '', 'POINT_LOG_PRCDNG_PARTY_ID', '', '', '', '', '') ,
    ('daily_load', 'POINT_LOG_PRCDNG_PROP', 'N', 'LAST_MOD_TS', '', 'POINT_LOG_PRCDNG_PROP_ID', '', '', '', '', ''),
    ('daily_load', 'POINT_LOG_PROCEEDING', 'N', 'LAST_MOD_TS', '', 'POINT_LOG_PROCEEDING_ID', '', '', '', '', ''),
    ('daily_load', 'PROPERTY_GROUND', 'N', 'LAST_MOD_TS', '', 'PROPERTY_GROUND_ID', '', '', '', '', ''),
    ('daily_load', 'QUALITY_REVIEW', 'N', 'LAST_MOD_TS', '', 'QUALITY_REVIEW_ID', '', '', '', '', ''),
    ('daily_load', 'REPORT_LOG', 'N', 'LAST_UPDATE_TS', '', 'RL_ID', '', '', '', '', ''),
    ('daily_load', 'REPORT_PARAMETER', 'Y', '', '', 'RP_ID', '', '', '', '', ''),
    ('daily_load', 'RPT_QA_RECEIPTS', 'N', 'LAST_UPDATE_TIMESTAMP', '', 'FK_RPT_EVENTSIDENTIFIER, RPT_EVENT_DATE', '', '', '', '', ''),
    ('daily_load', 'STND_RULE_HIST', 'N', 'PREV_LAST_MOD_TS', '', 'STND_RULE_HIST_ID', '', '', '', '', ''),
    ('daily_load', 'TTAB_APPLICATION_CASE_FILE', 'N', 'LAST_UPDATE_TIMESTAMP', '', 'SERIAL_NUMBER', '', '', '', '', ''),
    ('daily_load', 'TTAB_PANEL_INFO', 'N', 'LAST_MOD_TS', '', 'PK_PANEL_ID', '', '', '', '', '')
]

# COMMAND ----------

# DBTITLE 1,ttab_metadata_group6 - Reference Tables and weekly
ttabp_metadata_group6 = [
    ('weekly_load', 'APPLICATION_STATUS_REFERENCE', 'N', 'LAST_UPDATE_TIMESTAMP', '', 'APPLICATION_STATUS_CODE', '', '', '', '',''),
    ('weekly_load', 'BI_WEEk', 'N', 'LAST_MOD_TS', '', 'BI_WEEK_ID', '', '', '', '', ''),
    ('weekly_load', 'STND_CALENDAR', 'N', 'LAST_MOD_TS', '', 'CALENDAR_DT', '', '', '', '', ''),
    ('weekly_load', 'DOCUMENT_DETAIL', 'N', 'LAST_MODIFIED_TS', '', 'FK_OGC_DOCUMENT_ID, PAGE_NO', '', '', '', '', ''),
    ('weekly_load', 'EMPLOYEE', 'N', 'LAST_UPDATE_TIMESTAMP', '', 'NUMBER0', '', '', '', '', ''),
    ('weekly_load', 'EMPLOYEE_PROCEEDING', 'Y', '', '', 'IDENTIFIER', 10, 10000, 'FK_EMPLOYEENUMBER0', 0, 99294),
    ('weekly_load', 'EMPLOYEE_UNAVAILABILITY', 'N', 'LAST_UPDATE_TS', '', 'EMPLOYEE_UNAVAILABILITY_ID', '', '', '', '', ''),
    ('weekly_load', 'ENTRY_INFORMATION', 'N', 'LAST_UPDATE_TIMESTAMP', '', 'ENTRY_CODE', '', '', '', '', ''),
    ('weekly_load', 'GROUND_RULE_ASSOC', 'N', 'LAST_MOD_TS', '', 'GROUND_RULE_ID', '', '', '', '', ''),
    ('weekly_load', 'LOCATION_REFERENCE', 'Y', '', '', 'LOCATION_CD', '', '', '', '', ''),
    ('weekly_load', 'PARALEGAL_ASSIGNMENT_RULE', 'Y', '', '', 'PARALEGAL_ASSIGNMENT_RULE_ID', '', '', '', '', ''),
    ('weekly_load', 'PARTY_TYPE', 'N', 'LAST_UPDATE_TS', '', 'PT_ID', '', '', '', '', ''),
    ('weekly_load', 'PRCDNG_DSCVRY_CONF_AGRMT', 'Y', '', '', 'FK_PDC_ID, FK_PRCDNG_NUMBER0, FK_PRCDNG_TYPE, FK_SDCA_ID', '', '', '', '', ''),
    ('weekly_load', 'PRCDNG_DSCVRY_CONF', 'N', 'LAST_UPDATE_TS', '', 'PDC_ID, FK_PRCDNG_NUMBER0, FK_PRCDNG_TYPE', '', '', '', '', ''),
    ('weekly_load', 'PRCDNG_DSCVRY_CONF_CLAIM', 'Y', '', '', 'FK_PDC_ID, FK_PRCDNG_NUMBER0, FK_PRCDNG_TYPE, FK_SDCC_ID', '', '', '', '', ''),
    ('weekly_load', 'PROCEEDING_STATUS', 'Y', '', '', 'CODE', '', '', '', '', ''),
    ('weekly_load', 'PROCEEDING_TRIAL_EVENT', 'N', 'LAST_UPDATE_TS', '', 'PROCEEDING_TRIAL_EVENT_ID', '', '', '', '', ''),
    ('weekly_load', 'PROCEEDING_TRIAL_EVENT_HIST', 'Y', 'LAST_UPDATE_TS', '', 'PROCEEDING_TRIAL_EVENT_ID', '', '', '', '', ''),
    ('weekly_load', 'PROC_TYPE_GROUND_RULE', 'N', 'LAST_MOD_TS', '', 'PROC_TYPE_GROUND_RULE_ID', '', '', '', '', ''),
    ('weekly_load', 'QUALITY_REVIEW_FINDING_ROLE', 'N', 'LAST_MOD_TS', '', 'QUALITY_REVIEW_FINDING_ROLE_ID', '', '', '', '', ''),
    ('weekly_load', 'QUALITY_RVW_ATTN_SRC_ROLE', 'N', 'LAST_MOD_TS', '', 'QUALITY_RVW_ATTN_SRC_ROLE_ID', '', '', '', '', ''),
    ('weekly_load', 'ROLE_REPORT_GROUP', 'N', 'LAST_MODIFIED_TS', '', 'FK_SRG_ID, FK_SR_ROLE_CD', '', '', '', '', ''),
    ('weekly_load', 'RPT_EVENTS', 'N', 'LAST_UPDATE_TIMESTAMP', '', 'IDENTIFIER', '', '', '', '', ''),
    ('weekly_load', 'STND_BAR_JURISDICTION', 'N', 'LAST_MOD_TS', '', 'BAR_JURISDICTION_REGION_CD', '', '', '', '', ''),
    ('weekly_load', 'STND_CONSENT_MOTION_TYPE', 'N', 'LAST_MOD_TS', '', 'STND_CONSENT_MOTION_TYPE_ID', '', '', '', '', ''),
    ('weekly_load', 'STND_DECISION', 'N', 'LAST_MOD_TS', '', 'DECISION_CD', '', '', '', '', ''),
    ('weekly_load', 'STND_DOCUMENT_TYPE', 'N', 'LAST_MOD_TS', '', 'DOCUMENT_TYPE_CD', '', '','','',''),
    ('weekly_load', 'STND_DSCVRY_CONF_AGRMT', 'N', 'LAST_UPDATE_TS', '', 'SDCA_ID', '', '', '', '', ''),
    ('weekly_load', 'STND_DSCVRY_CONF_CLAIM', 'N', 'LAST_UPDATE_TS', '', 'SDCC_ID', '', '', '', '', ''),
    ('weekly_load', 'STND_EMPLOYEE_ACCESS_TYPE', 'N', 'LAST_MOD_TS', '', 'EMPLOYEE_ACCESS_TYPE_CD', '', '', '', '', ''),
    ('weekly_load', 'STND_ENTITY_TYPE', 'N', 'LAST_UPDATE_TIMESTAMP', '', 'STND_ENTITY_TYPE_ID', '', '', '', '', ''),
    ('weekly_load', 'STND_EXAM_ATT_ACTION_MAPPING', 'N', 'LAST_UPDATE_TIMESTAMP', '', 'STND_EXAM_ATT_ACTION_MAPPING_ID', '', '', '', '', ''),
    ('weekly_load', 'STND_EXT_OF_TIME_TYPE', 'N', 'LAST_MOD_TS', '', 'EXT_OF_TIME_TYPE_ID', '', '', '', '', ''),
    ('weekly_load', 'STND_GROUND', 'N', 'LAST_MOD_TS', '', 'GROUND_ID', '', '', '', '', ''),
    ('weekly_load', 'STND_GROUND_HIST', 'N', 'PREV_LAST_MOD_TS', '', 'GROUND_HIST_ID', '', '', '', '', ''),
    ('weekly_load', 'STND_MOTION', 'N', 'LAST_MODIFIED_TS', '', 'SM_ID', '', '', '', '', ''),
    ('weekly_load', 'STND_PNT_LOG_TYPE_PNT_TYPE', 'N', 'LAST_MOD_TS', '', 'PNT_LOG_TYPE_PNT_TYPE_ID', '', '', '', '', ''),
    ('weekly_load', 'STND_POINT_LOG_TYPE', 'N', 'LAST_MOD_TS', '', 'POINT_LOG_TYPE_ID', '', '', '', '', ''),
    ('weekly_load', 'STND_POINT_TYPE', 'N', 'LAST_MOD_TS', '', 'POINT_TYPE_ID', '', '', '', '', ''),
    ('weekly_load', 'STND_PRCDNG_DSCVRY_CONF_STAT', 'N', 'LAST_UPDATE_TS', '', 'SPDCS_ID', '', '', '', '', ''), 
    ('weekly_load', 'STND_PRCDNG_EXTENSION_GRND', 'N', 'LAST_MOD_TS', '', 'PRCDNG_EXTENSION_GRIND_ID', '', '', '', '', ''),
    ('weekly_load', 'STND_PROCEEDING_EVENT', 'N', 'LAST_MOD_TS', '', 'PROCEEDING_EVENT_ID', '', '', '', '', ''),
    ('weekly_load', 'STND_QUALITY_REVIEW_ATTN_SRC', 'N', 'LAST_MOD_TS', '', 'QUALITY_REVIEW_ATTN_SRC_ID', '', '', '', '', ''),
    ('weekly_load', 'STND_QUALITY_REVIEW_CATEGORY', 'N', 'LAST_MOD_TS', '', 'QUALITY_REVIEW_CATEGORY_ID', '', '', '', '', ''),
    ('weekly_load', 'STND_QUALITY_REVIEW_FINDING', 'N', 'LAST_MOD_TS', '', 'QUALITY_REVIEW_FINDING_ID', '', '', '', '', ''),
    ('weekly_load', 'STND_REPORT_FREQUENCY', 'Y', '', '', 'REPORT_FREQUENCY_CD', '', '', '', '', ''),
    ('weekly_load', 'STND_REPORT_GROUP', 'N', 'LAST_MODIFIED_TS', '', 'SRG_ID', '', '', '', '', ''),
    ('weekly_load', 'STND_ROLE', 'Y', '', '', 'ROLE_CD', '', '', '', '', ''),
    ('weekly_load', 'STND_RULE', 'N', 'LAST_MOD_TS', '', 'RULE_ID', '', '', '', '', ''),
    ('weekly_load', 'STND_SCHEDULE_TYPE', 'Y', '', '', 'SCHEDULE_TYPE_ID', '', '', '', '', ''),
    ('weekly_load', 'STND_VALID_APPLICATION_STATUS', 'N', 'LAST_MOD_TS', '', 'VALID_APPLICATION_STATUS_ID', '', '', '', '', ''),
    ('weekly_load', 'TTAB_EFOIA_UPLOAD_CONTROL', 'Y', '', '', 'TTAB_EFOIA_UPLOAD_CONTROL_ID', '', '', '', '', ''),
    ('weekly_load', 'TTAB_EFOIA_UPLOAD_CONTROL_ERR', 'Y', '', '', 'TTAB_EFOIA_UPLOAD_CONTROL_ID', '', '', '', '', ''),
    ('weekly_load', 'TRIAL_EVENT', 'N', 'LAST_MODIFIED_TS', '', 'TRIAL_EVENT_ID', '', '', '', '', ''),
    ('weekly_load', 'TTAB_STATUS_CODE_REFERENCE', 'N', 'LAST_UPDATE_TIMESTAMP', '', 'TTAB_STATUS_CODE', '', '', '', '', ''),
    ('weekly_load', 'TTAB_TM_MAPPING', 'N', 'LAST_UPDATE_TIMESTAMP', '', 'FK_ENTRY_CODE, PROCEEDING_TYPE_CD', '', '', '', '', ''),
    ('weekly_load', 'TTAB_USER', 'Y', '', '', 'USER_ID', '', '', '', '', '')
    ]

# COMMAND ----------

ttabp_metadata_group7 = [
    ('daily_load', 'ATTRIBUTES', 'Y', '', '', 'SERIALNUM', '', '', '', '', ''),
    ('daily_load', 'CODES', 'Y', '', '', 'CODE', '', '', '', '', ''),
    ('daily_load', 'FORM_STATUS', 'Y', '', '', 'ID', '', '', '', '', ''),
    ('daily_load', 'GRPMEMBERSHIP', 'Y', '', '', 'GROUP_NAME', '', '', '', '', ''),
    ('daily_load', 'LINK', 'Y', '', '', 'PARENT_ID', '', '', '', '', ''),
    ('daily_load', 'LOG', 'Y', '', '', 'TIME_STAMP', '', '', '', '', ''),
    ('daily_load', 'OBJECT', 'Y', '', '', 'ID', '', '', '', '', ''),
    ('daily_load', 'PRIVILEGE', 'Y', '', '', 'GROUP_NAME', '', '', '', '', ''),
    ('daily_load', 'PROFILE', 'Y', '', '', 'TIME_CREATED', '', '', '', '', ''),
    ('daily_load' , 'QUEUES', 'Y', '', '', 'ID', '', '', '', '', ''),
    ('daily_load' , 'QVARS', 'Y', '', '', 'VAR_ID', '', '', '', '', ''),
    ('daily_load' , 'RESOURCES', 'Y', '', '', 'LAST_LOGON', '', '', '', '', ''),
    ('daily_load' , 'USER_ACTIVITY_LOG', 'Y', '', '', 'LOGOFF_TIMESTAMP', '', '', '', '', '')
]

# COMMAND ----------

ttabp_metadata_group7 = [
    ('daily_load', 'ATTRIBUTES', 'Y', '', '', 'SERIALNUM', '', '', '', '', ''),
    ('daily_load', 'CODES', 'Y', '', '', 'CODE', '', '', '', '', ''),
    ('daily_load', 'FORM_STATUS', 'Y', '', '', 'ID', '', '', '', '', ''),
    ('daily_load', 'GRPMEMBERSHIP', 'Y', '', '', 'GROUP_NAME', '', '', '', '', ''),
    ('daily_load', 'LINK', 'Y', '', '', 'PARENT_ID', '', '', '', '', ''),
    ('daily_load', 'LOG', 'Y', '', '', 'TIME_STAMP', '', '', '', '', ''),
    ('daily_load', 'OBJECT', 'Y', '', '', 'ID', '', '', '', '', ''),
    ('daily_load', 'PRIVILEGE', 'Y', '', '', 'GROUP_NAME', '', '', '', '', ''),
    ('daily_load', 'PROFILE', 'Y', '', '', 'TIME_CREATED', '', '', '', '', ''),
    ('daily_load' , 'QUEUES', 'Y', '', '', 'ID', '', '', '', '', ''),
    ('daily_load' , 'QVARS', 'Y', '', '', 'VAR_ID', '', '', '', '', ''),
    ('daily_load' , 'RESOURCES', 'Y', '', '', 'LAST_LOGON', '', '', '', '', ''),
    ('daily_load' , 'NUM_DISTINCT', 'Y', '', '', 'LOGOFF_TIMESTAMP', '', '', '', '', '')
]
