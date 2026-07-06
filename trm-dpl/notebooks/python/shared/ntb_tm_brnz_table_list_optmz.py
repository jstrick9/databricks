# Databricks notebook source
# MAGIC %md
# MAGIC #####Schema = ["TABLE_GROUP_NAME","TABLE_NAME","FULL_LOAD","DQ_FLTR","LARGE_TABLE_IND","ZORDER Columns","numPartitions","fetchsize",'partitionColumn','lowerBound,'upperBound']

# COMMAND ----------

# DBTITLE 1,validate groups
import datetime

INVALID_GROUP_CONFIGURATION: list[tuple] = []

def is_valid_length(configuration: tuple) -> bool:
    """
    Helper to validate if a configuration is of the appropriate length.
    """
    if len(configuration) != 11:
        return False
    return True


def has_valid_timetstamp(configuration: tuple) -> bool:
    """
    Helper to validate if a configurations using LAST_MOD_TS as the partition column
    are the appropriate format.
    """
    print(configuration)
    if configuration[-3] == "LAST_MOD_TS":
        start_ts = configuration[-1]
        end_ts = configuration[-2]
        ts_format = "%Y-%m-%d %H:%M:%S"
        try:
            datetime.datetime.strptime(start_ts, ts_format)
            datetime.datetime.strptime(end_ts, ts_format)
        except Exception as e:
            print(e)
            return False
    return True


def validate_groups(grouping: list[tuple]) -> bool:
    """
    Wrapper for validating configurations
    """
    for configuration in grouping:
        if is_valid_length(configuration) and has_valid_timetstamp(configuration):
            continue
        else:
            INVALID_GROUP_CONFIGURATION.append(configuration)
    return False if len(INVALID_GROUP_CONFIGURATION) > 0 else True

# COMMAND ----------

# DBTITLE 1,tmngpdb_metadata_group1
tmngpdb_metadata_group1 = [
    ('daily_load', 'TRADEMARK', 'Y', '', 'L', 'TRADEMARK_GID', 2, 50000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'TM_LOCATIONS', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 2, 50000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'TM_LITERAL', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 2, 50000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'TM_EMPLOYEE_ASSIGNMENT', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 2, 50000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'TM_RELATIONSHIP', 'Y', 'LAST_MOD_TS', '', 'fk_parent_trademark_gid', '', '', '', '', ''),
    ('daily_load', 'TM_MILESTONE', 'Y', 'LAST_MOD_TS', '', 'FK_TM_MILESTONE_CD,fk_trademark_gid', 2, 50000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'TM_ITU', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 2, 50000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'TM_OFFICE_ACTIONS', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 2, 50000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'TM_DIVISIONAL_CHILD', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', '', '', '', '', ''),
    ('daily_load', 'TM_PARTY_ROLE_OWNER', 'Y', 'LAST_MOD_TS', '', 'fk_party_role_sequence_no,fk_trademark_gid', 2, 50000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'TM_CLASS_REFERENCE', 'Y', 'LAST_MOD_TS', '', 'fk_class_id,fk_trademark_gid', 2, 50000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'USE_IN_ANOTHER_FORM', 'N', 'LAST_MOD_TS', '', 'FK_CLASS_ID,fk_trademark_gid', '', '', '', '', ''),
    ('daily_load', 'ATTORNEY_HOLD', 'Y', 'LAST_MOD_TS', '', 'FK_WORK_ITEM_GID', '', '', '', '', ''),
    ('daily_load', 'INTERNATIONAL_REG_TM', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', '', '', '', '', ''),
    ('daily_load', 'BUSINESS_EVENT', 'Y', 'LAST_MOD_TS', '', 'fk_business_event_reason_id,CFK_OBJECT_GID', 2, 50000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'STND_BUSINESS_EVENT_REASON', 'Y', 'LAST_MOD_TS', '', 'business_event_reason_id', '', '', '', '', ''),
    ('daily_load', 'STND_CLASS', 'Y', 'LAST_MOD_TS', '', 'CLASS_ID', '', '', '', '', ''),
]
validate_groups(tmngpdb_metadata_group1)

# COMMAND ----------

# DBTITLE 1,tmngpdb_metadata_group2
tmngpdb_metadata_group2 = [
    ('daily_load', 'ABANDONMENT_H', 'N', 'LAST_MOD_TS', '', 'CFK_TRANSACTION_INSTANCE_GID', 10, 10000, 'LAST_MOD_TS', '2016-04-02 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'ABANDONMENT', 'N', 'LAST_MOD_TS', '', 'FK_WORK_ITEM_GID', 10, 10000, 'LAST_MOD_TS', '2016-04-02 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'ANNOTATION_COMMENT', 'Y', 'LAST_MOD_TS', '', 'ANNOTATION_COMMENT_ID', 10, 10000, 'FK_REVIEW_ANNOTATION_ID', 2, 7),
    ('daily_load', 'BASE_APPLICATION', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2016-08-28 00:00:00', '2024-04-27 00:00:00'),
    ('daily_load', 'BASE_APPLICATION_H', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2016-08-28 00:00:00', '2024-04-27 00:00:00'),
    ('daily_load', 'CONCURRENT_USE', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'STATEMENT_NO', 0, 121),
    ('daily_load', 'DOCKET_ITEM', 'Y', 'LAST_MOD_TS', '', 'DOCKET_ITEM_ID', 10, 10000, 'LAST_MOD_TS', '2016-03-20 00:00:00', '2024-06-05 00:00:00'),
    ('daily_load', 'DOCKET_ITEM_EVENT', 'N', 'LAST_MOD_TS', '', 'FK_DOCKET_ITEM_ID', 10, 10000, 'LAST_MOD_TS', '2017-07-16 00:00:00', '2024-06-05 00:00:00'),
    ('daily_load', 'DOCKET_ITEM_H', 'Y', 'LAST_MOD_TS', '', 'CFK_TRANSACTION_INSTANCE_GID, DOCKET_ITEM_ID', 10, 10000, 'LAST_MOD_TS', '2016-03-20 00:00:00', '2024-06-05 00:00:00'),
    ('daily_load', 'DRAFT_DOC_VER_COMPNT_FPV', 'N', 'LAST_MOD_TS', '', 'FK_DRAFT_DOCUMENT_ID,CFK_FORM_PARAGRAPH_VERSION_GID', 10, 10000, 'LAST_MOD_TS', '2016-02-01 00:00:00', '2024-03-25 00:00:00'),
    ('daily_load', 'DRAFT_DOCUMENT', 'Y', 'LAST_MOD_TS', '', 'DRAFT_DOCUMENT_ID', 10, 10000, 'LAST_MOD_TS', '2017-06-10 00:00:00', '2024-03-25 00:00:00'),
    ('daily_load', 'DRAFT_DOCUMENT_VERSION', 'Y', 'LAST_MOD_TS', '', 'DRAFT_DOCUMENT_MOD_NO, FK_DRAFT_DOCUMENT_ID', 10, 10000, 'LAST_MOD_TS', '2006-10-06 00:00:00', '2024-03-25 00:00:00'),
    ('daily_load', 'DRAFT_DOCUMENT_VERSION_COMPNT', 'Y', 'LAST_MOD_TS', '', 'FK_DOCUMENT_COMPONENT_ID', 10, 10000, 'LAST_MOD_TS', '2015-12-15 00:00:00', '2024-03-25 00:00:00'),
    ('daily_load', 'ELECTRONIC_ADDRESS_H', 'Y', 'LAST_MOD_TS', '', 'ELECTRONIC_ADDRESS_GID,ACTION_CT', 10, 10000, 'LAST_MOD_TS', '2011-10-12 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'EMPLOYEE_AWARD_WITHDRAW', 'N', 'LAST_MOD_TS', '', 'FK_AWARD_EMPE_CR_TRAN_ID, FK_WITHDRAW_EMPE_CR_TRAN_ID', 10, 10000, 'fk_withdraw_empe_cr_tran_id', 19611696, 22634863),
    ('daily_load', 'EMPLOYEE_CREDIT_TRANSACTION', 'N', 'LAST_MOD_TS', '', 'EMPLOYEE_CREDIT_TRAN_ID', 10, 10000, 'employee_credit_tran_id', 10126061, 22637576),
    ('daily_load', 'EMPLOYEE_REVIEW_QUERY', 'N', 'LAST_MOD_TS', '', 'EMPLOYEE_REVIEW_QUERY_ID', 10, 10000, 'employee_review_query_id', 1, 888608),
    ('daily_load', 'EMPLOYEE_REVIEW_QUERY_STAT', 'N', 'LAST_MOD_TS', '', 'FK_EMPLOYEE_REVIEW_QUERY_ID', 10, 10000, 'fk_employee_review_query_id', 1, 888608),
    ('daily_load', 'EMPLOYEE_TM_CLASS_CREDIT', 'N', 'LAST_MOD_TS', '', 'FK_CLASS_ID, FK_TRADEMARK_GID', 10, 10000, 'fk_employee_credit_tran_id', 19611321, 22637576),
    ('daily_load', 'EVIDENCE_BIN_FOLDER', 'Y', 'LAST_MOD_TS', '', 'EVIDENCE_BIN_FOLDER_ID', 10, 10000, 'evidence_bin_folder_id', 2, 5946016),
    ('daily_load', 'EVIDENCE_DOCUMENT', 'N', 'LAST_MOD_TS', '', 'EVIDENCE_DOCUMENT_ID', 10, 10000, 'evidence_document_id', 3, 4373743),
    ('daily_load', 'FSM_INSTANCE', 'N', 'LAST_MOD_TS', '', 'FSM_INSTANCE_GID', 10, 10000, 'LAST_MOD_TS', '2014-07-26 00:00:00', '2024-06-05 00:00:00'),
    ('daily_load', 'FSM_INSTANCE_H', 'Y', 'LAST_MOD_TS', '', 'fsm_instance_id', 10, 10000, 'LAST_MOD_TS', '2014-07-26 00:00:00', '2024-06-05 00:00:00'),
    ('daily_load', 'GDS_SRVC_STMT_ANNOTATION', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID,FK_CLASS_ID', 5, 10000, 'fk_class_id', 1, 925),
    ('daily_load', 'INTERESTED_PARTY_ASSUMED_NM', 'N', 'LAST_MOD_TS', '', 'INTRSTD_PARTY_ASSUMED_NAME_ID', 10, 10000, 'intrstd_party_assumed_name_id', 933713, 2516711),
    ('daily_load', 'INTERESTED_PARTY_ASSUMED_NM_H', 'Y', 'LAST_MOD_TS', '', 'INTRSTD_PARTY_ASSUMED_NAME_ID', 10, 10000, 'intrstd_party_assumed_name_id', 933713, 2516711),
    ('daily_load', 'INTERNAL_NOTE', 'Y', 'LAST_MOD_TS', '', 'INTERNAL_NOTE_ID', 10, 10000, 'internal_note_id', 311781, 1193236),
    ('daily_load', 'TM_DRAWING', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 2, 50000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'TM_DRAWING_H', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 2, 50000, 'LOCK_CONTROL_NO', 0, 1),
]
validate_groups(tmngpdb_metadata_group2)

# COMMAND ----------

# DBTITLE 1,tmngpdb_metadata_group3
tmngpdb_metadata_group3 = [
    ('daily_load', 'INTERNATIONAL_APPL_REG', 'N', 'LAST_MOD_TS', '', 'FK_INTERNATIONAL_APPL_GID', 10, 10000, 'LAST_MOD_TS', '2016-08-28 00:00:00', '2024-04-27 00:00:00'),
    ('daily_load', 'INTERNATIONAL_APPL_REG_H', 'Y', 'LAST_MOD_TS', '', 'FK_INTERNATIONAL_APPL_GID', 10, 10000, 'LAST_MOD_TS', '2016-08-28 00:00:00', '2024-04-27 00:00:00'),
    ('daily_load', 'INTERNATIONAL_APPLICATION', 'N', 'LAST_MOD_TS', '', 'INTERNATIONAL_APPLICATION_GID', 10, 10000, 'LAST_MOD_TS', '2016-08-28 00:00:00', '2024-04-27 00:00:00'),
    ('daily_load', 'INTERNATIONAL_APPLICATION_H', 'Y', 'LAST_MOD_TS', '', 'INTERNATIONAL_APPLICATION_GID', 10, 10000, 'LAST_MOD_TS', '2016-08-28 00:00:00', '2024-04-27 00:00:00'),
    ('daily_load', 'INTERNATIONAL_REG_TM_H', 'Y', 'LAST_MOD_TS', '', 'CFK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2016-08-28 00:00:00', '2024-04-26 00:00:00'),
    ('daily_load', 'INTERNATIONAL_REGISTRATION', 'N', 'LAST_MOD_TS', '', 'INTERNATIONAL_REG_GID', 10, 10000, 'LAST_MOD_TS', '2016-08-28 00:00:00', '2024-04-27 00:00:00'),
    ('daily_load', 'INTERNATIONAL_REGISTRATION_H', 'Y', 'LAST_MOD_TS', '', 'INTERNATIONAL_REG_GID', 10, 10000, 'LAST_MOD_TS', '2016-08-28 00:00:00', '2024-04-27 00:00:00'),
    ('daily_load', 'INTERNATIONAL_TM', 'N', 'LAST_MOD_TS', '', 'INTERNATIONAL_REG_NO', 10, 10000, 'international_reg_dt', '1947-06-23 00:00:00', '2024-03-28 00:00:00'),
    ('daily_load', 'INTERNATIONAL_TM_H', 'Y', 'LAST_MOD_TS', '', 'CFK_TRANSACTION_INSTANCE_GID', 10, 10000, 'international_reg_dt', '1947-06-23 00:00:00', '2024-03-28 00:00:00'),
    ('daily_load', 'IP_ELECTRONIC_ADDRESS', 'N', 'LAST_MOD_TS', '', 'FK_ELECTRONIC_ADDRESS_GID', 10, 10000, 'LAST_MOD_TS', '2021-12-24 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'IP_MAILING_ADDRESS', 'N', 'LAST_MOD_TS', '', 'FK_INTERESTED_PARTY_GID', 10, 10000, 'LAST_MOD_TS', '2021-12-21 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'IP_TELECOM_ADDRESS', 'N', 'LAST_MOD_TS', '', 'FK_TELECOM_ADDRESS_GID', 10, 10000, 'LAST_MOD_TS', '2021-12-21 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'MV_MYUSPTO_TRAM_SEARCH', 'Y', '', '', 'SERIAL_NUM', 10, 10000, 'registration_num', 1, 7423626),
    ('daily_load', 'MV_MYUSPTO_TRM_SEARCH', 'Y', '', '', 'SERIAL_NUM', 10, 10000, 'REGISTRATION_NUM', 0, 7423626),
    ('daily_load', 'MV_MYUSPTO_TRM_AT', 'Y', '', '', 'TRADEMARK_GID', '', '', '', '', ''),
    ('daily_load', 'MV_MYUSPTO_TRM_MARK', 'Y', '', '', 'SER_NUM', 10, 10000, 'reg_num', 1, 7403886),
    ('daily_load', 'MV_MYUSPTO_TRAM_MARK','Y','','','SER_NUM', 10, 10000, 'reg_num', 1, 7403886),
    ('daily_load', 'MV_MYUSPTO_TRM_OWNER', 'Y', '', '', 'TRADEMARK_GID', '', '', '', '', ''),
    ('daily_load', 'MV_MYUSPTO_TRAM_AT', 'Y', '', '', 'SER_NUM', 6, 10000, 'ser_num', 60001552, 98975050),
    ('daily_load', 'MV_MYUSPTO_TRAM_OWNER', 'Y', '', '', 'SER_NUM', 10, 10000, 'owner_id', 1, 29927275),
    ('daily_load', 'MYUSPTO_TRAM_CHANGE_NTFCN', 'Y', '', '', 'SERIAL_NUM', 10, 10000, 'event_dt', '2024-05-21 00:00:00', '2024-05-30 00:00:00'),
    ('daily_load', 'MYUSPTO_TRAM_EVENT_TODAY', 'Y', '', '', 'SERIAL_NUM', 10, 10000, 'serial_num', 0, 99999999),
    ('daily_load', 'MYUSPTO_TRAM_PH', 'Y', '', '', 'SERIAL_NUM', '', '', '', '', ''),
    ('daily_load', 'MYUSPTO_TRAM_STATUS_TODAY', 'Y', '', '', 'SERIAL_NUM', '', '', '', '', ''),
    ('daily_load', 'MYUSPTO_TRM_CHANGE_NTFCN', 'Y', '', '', 'SERIAL_NUM', '', '', '', '', ''),
    ('daily_load', 'MYUSPTO_TRM_EVENT_TODAY', 'Y', '', '', 'SERIAL_NUM', '', '', '', '', ''),
    ('daily_load', 'MYUSPTO_TRM_PH', 'Y', '', '', 'SERIAL_NUM', '', '', '', '', ''),
    ('daily_load', 'MYUSPTO_TRM_STATUS_TODAY', 'Y', '', '', 'SERIAL_NUM', '', '', '', '', ''),
    ('daily_load', 'OBJECT_DISPATCH', 'N', 'LAST_MOD_TS', '', 'CFK_OBJECT_GID', 8, 10000, 'action_current_dt', '2016-04-25 00:00:00', '2024-05-31 00:00:00'),
    ('daily_load', 'OBJECT_DOCUMENT', 'Y', 'LAST_MOD_TS', '', 'CFK_OBJECT_GID', 10, 10000, 'fk_tm_document_id', 441, 16932242),
    ('daily_load', 'OBJECT_DOCUMENT_H', 'Y', 'LAST_MOD_TS', '', 'CFK_OBJECT_GID', 10, 10000, 'fk_tm_document_id', 441, 16932242),
    ('daily_load', 'OBJECT_FSM_INSTANCE', 'Y', 'LAST_MOD_TS', '', 'CFK_OBJECT_GID', 10, 10000, 'LAST_MOD_TS', '1977-05-03 00:00:00', '2030-01-24 00:00:00'),
    ('daily_load', 'OFFICE_ACTIVITY', 'Y', 'LAST_MOD_TS', '', 'FK_WORK_ITEM_GID', 10, 10000, 'issue_dt', '2005-05-16 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'OFFICE_ACTIVITY_DRAFT_DOC_H', 'Y', 'LAST_MOD_TS', '', 'FK_WORK_ITEM_GID', 10, 10000, 'fk_draft_document_id', 42, 6397119),
    ('daily_load', 'OFFICE_ACTIVITY_DRAFT_DOCUMENT', 'Y', 'LAST_MOD_TS', '', 'FK_WORK_ITEM_GID', 10, 10000, 'LAST_MOD_TS', '2000-09-29 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'OFFICE_ACTIVITY_H', 'Y', 'LAST_MOD_TS', '', 'FK_WORK_ITEM_GID', 10, 10000, 'LAST_MOD_TS', '2000-09-29 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'OFFICE_ACTIVITY_REASON', 'N', 'LAST_MOD_TS', '', 'FK_WORK_ITEM_GID', 10, 10000, 'LAST_MOD_TS', '2016-02-01 00:00:00', '2024-05-25 00:00:00'),
    ('daily_load', 'OFFICE_ACTIVITY_REASON_H', 'Y', 'LAST_MOD_TS', '', 'FK_WORK_ITEM_GID', 10, 10000, 'LAST_MOD_TS', '2016-02-01 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'OFFICE_ACTIVITY_REVIEW', 'Y', 'LAST_MOD_TS', '', 'OFFICE_ACTIVITY_REVIEW_ID', 10, 10000, 'LAST_MOD_TS', '2016-05-24 00:00:00', '2024-05-29 00:00:00'),
    ('daily_load', 'TRANSACTION_INSTANCE', 'Y', 'LAST_MOD_TS', 'L', 'TRANSACTION_INSTANCE_GID', 10, 10000, 'LAST_MOD_TS', '2015-10-07 00:00:00', '2024-06-05 00:00:00'),
    ('daily_load', 'TM_DOCUMENT', 'Y', 'LAST_MOD_TS', '', 'TM_DOCUMENT_ID', 10, 10000, 'tm_document_id', 1, 16864343),
    ('daily_load', 'TM_DOCUMENT_REFERENCE', 'Y', 'LAST_MOD_TS', '', 'FK_TM_DOCUMENT_ID', 10, 10000, 'fk_tm_document_id', 1, 16932242),
]
validate_groups(tmngpdb_metadata_group3)

# COMMAND ----------

# DBTITLE 1,tmngpdb_metadata_group4
tmngpdb_metadata_group4 = [
    ('daily_load', 'OG_TM_REVIEW', 'N', 'LAST_MOD_TS', '', 'OG_TM_REVIEW_GID', 2, 10000, 'LOCK_CONTROL_NO', 0, 0),
    ('daily_load', 'PRCDNG_EMPLOYEE_ASSIGNMENT', 'N', 'LAST_MOD_TS', '', 'CFK_PROCEEDING_GID', 10, 10000, 'LAST_MOD_TS', '2022-01-12 00:00:00', '2024-05-31 00:00:00'),
    ('daily_load', 'PRCDNG_EMPLOYEE_ASSIGNMENT_H', 'Y', 'LAST_MOD_TS', '', 'CFK_PROCEEDING_GID', 10, 10000, 'LAST_MOD_TS', '2023-12-09 00:00:00', '2024-05-31 00:00:00'),
    ('daily_load', 'PREDEFINED_PARAGRAPH', 'N', 'LAST_MOD_TS', '', 'PREDEFINED_PARAGRAPH_ID', 10, 10000, 'LAST_MOD_TS', '2015-10-07 00:00:00', '2024-03-06 00:00:00'),
    ('daily_load', 'PREDEFINED_PARAGRAPH_VER', 'N', 'LAST_MOD_TS', '', 'FK_DOCUMENT_COMPONENT_ID', 10, 10000, 'LAST_MOD_TS', '2017-05-27 00:00:00', '2024-03-06 00:00:00'),
    ('daily_load', 'QUERY_APPEAL', 'N', 'LAST_MOD_TS', '', 'QUERY_APPEAL_GID', 10, 10000, 'LAST_MOD_TS', '2009-09-01 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'QUERY_APPEAL_STATUS', 'N', 'LAST_MOD_TS', '', 'FK_EMPLOYEE_QUERY_APPEAL_ID', 10, 10000, 'LAST_MOD_TS', '2014-07-26 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'QUERY_GROUND', 'N', 'LAST_MOD_TS', '', 'QUERY_GROUND_ID', 10, 10000, 'query_ground_id', 1, 643349),
    ('daily_load', 'RELATED_WORKER', 'N', 'LAST_MOD_TS', '', 'RELATED_WORKER_ID', 10, 10000, 'LAST_MOD_TS', '2016-12-15 00:00:00', '2023-06-02 00:00:00'),
    ('daily_load', 'REVIEW_ANNOTATION', 'Y', 'LAST_MOD_TS', '', 'REVIEW_ANNOTATION_ID', 10, 10000, 'LAST_MOD_TS', '2022-07-20 00:00:00', '2023-06-14 00:00:00'),
    ('daily_load', 'REVIEW_QUERY_APPEAL', 'N', 'LAST_MOD_TS', '', 'REVIEW_QUERY_APPEAL_ID', 10, 10000, 'LAST_MOD_TS', '2014-07-26 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'REVIEW_QUERY_CLASS', 'N', 'LAST_MOD_TS', '', 'FK_QUERY_GROUND_ID', 10, 10000, 'fk_class_id', 1, 923),
    ('daily_load', 'REVIEW_QUERY_NOTE', 'N', 'LAST_MOD_TS', '', 'FK_REVIEW_QUERY_GID', 10, 10000, 'LAST_MOD_TS', '2014-07-26 00:00:00', '2024-06-05 00:00:00'),
    ('daily_load', 'SECTION_2F_STATEMENT', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2018-08-26 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'STND_DESIGN_SEARCH_CODE_ITEM', 'N', 'LAST_MOD_TS', '', 'FK_DESIGN_SEARCH_GROUP_CD', 10, 10000, 'LAST_MOD_TS', '2007-01-08 00:00:00', '2023-12-14 00:00:00'),
    ('daily_load', 'STND_DESIGN_SEARCH_GROUP', 'N', 'LAST_MOD_TS', '', 'DESIGN_SEARCH_GROUP_CD', 10, 10000, 'LAST_MOD_TS', '2007-01-08 00:00:00', '2022-05-25 00:00:00'),
    ('daily_load', 'TM_ADDL_STMNT_PRIOR_REG', 'N', 'LAST_MOD_TS', '', 'FK_STATEMENT_TYPE_CD, FK_TRADEMARK_GID', 2, 10000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'TM_ADDL_STMNT_PRIOR_REG_H', 'Y', 'LAST_MOD_TS', '', 'FK_STATEMENT_TYPE_CD, FK_TRADEMARK_GID', 2, 10000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'TM_AMENDMENT', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2018-03-03 00:00:00', '2024-07-02 00:00:00'),
    ('daily_load', 'TM_CLASS_FILING_BASIS', 'N', 'LAST_MOD_TS', '', 'FK_FILING_BASIS_CD, FK_TRADEMARK_GID', 8, 10000, 'fk_class_id', 1, 926),
    ('daily_load', 'TM_EMPLOYEE_ASSIGNMENT_H', 'Y', 'LAST_MOD_TS', '', 'FK_TM_EMPLOYEE_ROLE_CD, FK_TRADEMARK_GID', 2, 10000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'TEMP_TRM_PQ','Y','','','','','','','',''),
    ('daily_load', 'TEMP_TRM_PQ_H','Y','','','','','','','',''),
    ('daily_load', 'TEMP_TRM_PQC','Y','','','','','','','',''),   
    ('daily_load', 'TEMP_TRM_PQC_H','Y','','','','','','','',''),
    ('daily_load', 'SUBMISSION', 'Y', 'LAST_MOD_TS', '', 'SUBMISSION_GID', '', '', '', '', ''),
    ('daily_load', 'SUBMISSION_H', 'Y', 'LAST_MOD_TS', '', 'SUBMISSION_GID', 10, 10000, 'LAST_MOD_TS', '1989-06-29 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'SUBMISSION_ITEM', 'Y', 'LAST_MOD_TS', '', 'SUBMISSION_ITEM_GID', 2, 10000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'SUBMISSION_ITEM_H', 'Y', 'LAST_MOD_TS', '', 'SUBMISSION_ITEM_GID', 2, 10000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'SYNC_CASESTATUS', 'N', 'CS_TIMESTAMP', '', 'CS_SERIAL_NUM', '', '', '', '', ''),
    ('daily_load', 'SYNC_EXCEPTIONS', 'Y', 'INSERT_DT', '', 'SYNC_EXCEPTIONS_ID', '', '', '', '', ''),
    ('daily_load', 'SYNC_LOG', 'Y', 'CREATEDATE', '', '', '', '', '', '', ''),
    ('daily_load', 'SYNC_TM_COM_EXCEPTION', 'Y', 'RESOLVED_TS', '', 'TM_COM_EXCEPTION_ID', '', '', '', '', ''),
    ('daily_load', 'SYNC_TRAM_TRM_OBJ_ID_MAPPING', 'Y', '', '', '', 10, 10000, 'serial_num', 60000001, 98975073),
    ('daily_load', 'SYNC_TRM_TO_TRAM_CONTROL', 'Y', 'LAST_MOD_TS', '', 'TRM_TRAM_SYNC_CONTROL_ID', 10, 10000, 'LAST_MOD_TS', '2023-10-11 00:00:00', '2024-05-30 00:00:00'),
    ('daily_load', 'TELECOM_ADDRESS', 'Y', 'LAST_MOD_TS', '', 'TELECOM_ADDRESS_GID', 2, 10000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'TELECOM_ADDRESS_H', 'Y', 'LAST_MOD_TS', '', 'TELECOM_ADDRESS_GID', 2, 10000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'TM_ADDITIONAL_STATEMENT', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 2, 10000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'WORK_ITEM_OBJECT_H', 'Y', 'LAST_MOD_TS', '', 'FK_WORK_ITEM_GID', 2, 10000, 'LOCK_CONTROL_NO', 0, 1),
]
validate_groups(tmngpdb_metadata_group4)

# COMMAND ----------

# DBTITLE 1,tmngpdb_metadata_group5
tmngpdb_metadata_group5 = [
    ('weekly_load','STND_TM_MILESTONE','N','LAST_MOD_TS', '', '', '', '', '', '', ''),
    ('weekly_load','STND_FEE_PROCESS_TYPE','N','LAST_MOD_TS', '', '', '', '', '', '', ''), 
    ('weekly_load','STND_MARK_DRAWING_TYPE','N','LAST_MOD_TS', '', '', '', '', '', '', ''), 
    ('weekly_load', 'CUSTOM_ALERT', 'N', 'LAST_MOD_TS', '', 'CUSTOM_ALERT_ID', '', '', '', '', ''),
    ('weekly_load', 'DOC_TMPLT_VER_FORM_PARA', 'Y', 'LAST_MOD_TS', '', 'DOC_TMPLT_VER_FORM_PARA_ID', 10, 10000, 'LAST_MOD_TS', '2015-10-21 00:00:00', '2024-03-27 00:00:00'),
    ('weekly_load', 'DOCKET_ITEM_EVENT_H', 'N', 'LAST_MOD_TS', '', 'CFK_TRANSACTION_INSTANCE_GID,FK_DOCKET_ITEM_ID', 10, 10000, 'LAST_MOD_TS', '2017-07-09 00:00:00', '2017-10-16 00:00:00'),
    ('weekly_load', 'DOCUMENT_COMPONENT_RELTNSP', 'N', 'LAST_MOD_TS', '', 'FK_PARENT_DOCUMENT_COMPNT_ID', 3, 10000, 'fk_child_document_component_id', 300704, 14387605),
    ('weekly_load', 'DOCUMENT_TEMPLATE_VERSION', 'N', 'LAST_MOD_TS', '', 'fk_document_template_cd', 10, 10000, 'LAST_MOD_TS', '2013-08-01 00:00:00', '2024-03-06 00:00:00'),
    ('weekly_load', 'EMPLOYEE_QUERY_APPEAL', 'N', 'LAST_MOD_TS', '', 'EMPLOYEE_QUERY_APPEAL_ID', 10, 10000, 'employee_query_appeal_id', 1, 14190),
    ('weekly_load', 'EVIDENCE_BIN_FOLDER_H', 'Y', 'LAST_MOD_TS', '', 'EVIDENCE_BIN_FOLDER_ID', 10, 10000, 'evidence_bin_folder_id', 1, 5823704),
    ('weekly_load', 'EVIDENCE_DOCUMENT_H', 'Y', 'LAST_MOD_TS', '', 'EVIDENCE_DOCUMENT_ID', 10, 10000, 'evidence_document_id', 1, 4018627),
    ('weekly_load', 'FORM_PARAGRAPH_RULE', 'N', 'LAST_MOD_TS', '', 'FORM_PARAGRAPH_RULE_ID', 10, 10000, 'form_paragraph_rule_id', 1, 1073),
    ('weekly_load', 'IB_TRANSACTION', 'N', '', '', 'FK_WORK_ITEM_GID', '', '', '', '', ''),
    ('weekly_load', 'INTRSTD_PARTY_RELATIONSHIP', 'N', 'LAST_MOD_TS', '', 'FK_INTERESTED_PARTY_GID', '', '', '', '', ''),
    ('weekly_load', 'INTRSTD_PARTY_RELATIONSHIP_H', 'N', 'LAST_MOD_TS', '', 'FK_INTERESTED_PARTY_GID', '', '', '', '', ''),
    ('weekly_load', 'IR_MAILING_ADDRESS', 'N', 'LAST_MOD_TS', '', 'FK_INTERNATIONAL_REG_GID', '', '', '', '', ''),
    ('weekly_load', 'IR_MAILING_ADDRESS_GROUP', 'N', 'LAST_MOD_TS', '', 'FK_INTERNATIONAL_REG_GID', '', '', '', '', ''),
    ('weekly_load', 'MAILING_ADDRESS_LINE', 'N', 'LAST_MOD_TS', '', 'FK_MAILING_ADDRESS_GID', '', '', '', '', ''),
    ('weekly_load', 'MAILING_ADDRESS_LINE_H', 'N', 'LAST_MOD_TS', '', 'FK_MAILING_ADDRESS_GID', 10, 10000, 'LAST_MOD_TS', '2023-09-19 00:00:00', '2023-10-12 00:00:00'),
    ('weekly_load', 'QUERY_APPEAL_NOTE', 'N', 'LAST_MOD_TS', '', 'FK_EMPLOYEE_QUERY_APPEAL_ID', 10, 10000, 'LAST_MOD_TS', '2014-09-12 00:00:00', '2023-12-06 00:00:00'),
    ('weekly_load', 'REVIEW_ISSUE', 'N', 'LAST_MOD_TS', '', 'FK_OFFICE_ACTIVITY_REVIEW_ID', 10, 10000, 'LAST_MOD_TS', '2016-05-24 00:00:00', '2016-09-15 00:00:00'),
    ('weekly_load', 'REVIEW_QUERY', 'N', 'LAST_MOD_TS', '', 'REVIEW_QUERY_GID', 10, 10000, 'og_page_no', 0, 99999),
    ('weekly_load', 'SECTION_2F_PRIOR_REG', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', '', '', '', '', ''),
    #('weekly_load', 'SECTION_2F_PRIOR_REG_H', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', '', '', '', '', ''),
    ('weekly_load', 'STND_ANNOTATION_STATUS', 'N', 'LAST_MOD_TS', '', 'ANNOTATION_STATUS_CD', 10, 10000, 'LAST_MOD_TS', '2014-07-11 00:00:00', '2014-07-11 00:00:00'),
    ('weekly_load', 'STND_APPEAL_RESULT', 'N', 'LAST_MOD_TS', '', 'APPEAL_RESULT_CD', 10, 10000, 'LAST_MOD_TS', '2014-07-25 00:00:00', '2014-07-25 00:00:00'),
    ('weekly_load', 'STND_APPEAL_STATUS', 'N', 'LAST_MOD_TS', '', 'APPEAL_STATUS_CD', 10, 10000, 'LAST_MOD_TS', '2014-07-25 00:00:00', '2014-07-25 00:00:00'),
    ('weekly_load', 'STND_ASSUMED_NAME_TYPE', 'N', 'LAST_MOD_TS', '', 'ASSUMED_NAME_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2013-05-02 00:00:00', '2013-05-02 00:00:00'),
    ('weekly_load', 'STND_AVERMENT', 'N', 'LAST_MOD_TS', '', 'AVERMENT_ID', 10, 10000, 'LAST_MOD_TS', '2015-06-11 00:00:00', '2015-06-11 00:00:00'),
    ('weekly_load', 'STND_BUSINESS_EVENT_RSN_CT', 'N', 'LAST_MOD_TS', '', 'BUSINESS_EVENT_RSN_CT_CD', 10, 10000, 'LAST_MOD_TS', '2015-10-07 00:00:00', '2015-10-07 00:00:00'),
    ('weekly_load', 'STND_CATEGORY_DOC_TYPE', 'N', 'LAST_MOD_TS', '', 'FK_DOCUMENT_TYPE_ID', 10, 10000, 'LAST_MOD_TS', '2015-07-22 00:00:00', '2024-05-12 00:00:00'),
    ('weekly_load', 'STND_CLASS_SCHEDULE', 'N', 'LAST_MOD_TS', '', 'CLASS_SCHEDULE_CD', 10, 10000, 'LAST_MOD_TS', '2013-05-02 00:00:00', '2013-05-02 00:00:00'),
    ('weekly_load', 'STND_CLASS_STATEMENT_TYPE', 'N', 'LAST_MOD_TS', '', 'CLASS_STATEMENT_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2013-05-29 00:00:00', '2013-05-29 00:00:00'),
    ('weekly_load', 'STND_COORDINATED_CLASS', 'N', 'LAST_MOD_TS', '', 'FK_CLASS_ID, FK_COORDINATED_CLASS_ID', 10, 10000, 'LAST_MOD_TS', '2013-05-02 00:00:00', '2023-10-31 00:00:00'),
    ('weekly_load', 'STND_CREDIT_TRAN_RSN_TYPE', 'N', 'LAST_MOD_TS', '', 'CREDIT_TRAN_RSN_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2014-12-09 00:00:00', '2015-06-19 00:00:00'),
    ('weekly_load', 'STND_DESIGN_SEARCH_GROUP_TYPE', 'N', 'LAST_MOD_TS', '', 'DESIGN_SEARCH_GROUP_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2013-05-02 00:00:00', '2013-05-02 00:00:00'),
    ('weekly_load', 'STND_DOC_TYPE_CT', 'N', 'LAST_MOD_TS', '', 'DOC_TYPE_CT_ID', 10, 10000, 'LAST_MOD_TS', '2015-07-22 00:00:00', '2022-03-04 00:00:00'),
    ('weekly_load', 'STND_DOCKET', 'N', 'LAST_MOD_TS', '', 'DOCKET_ID', 10, 10000, 'LAST_MOD_TS', '2015-06-25 00:00:00', '2024-01-26 00:00:00'),
    ('weekly_load', 'STND_DOCKET_FSM_TYPE_STATE', 'N', 'LAST_MOD_TS', '', 'CFK_FSM_TYPE_STATE_ID, FK_DOCKET_ID', 10, 10000, 'LAST_MOD_TS', '2015-01-12 00:00:00', '2022-07-29 00:00:00'),
    ('weekly_load', 'STND_DOCKET_ITEM_EVENT_TYPE', 'N', 'LAST_MOD_TS', '', 'DOCKET_ITEM_EVENT_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2017-07-09 00:00:00', '2024-01-26 00:00:00'),
    ('weekly_load', 'STND_DOCUMENT_COMPONENT_TYPE', 'N', 'LAST_MOD_TS', '', 'DOCUMENT_COMPONENT_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2013-05-02 00:00:00', '2015-10-07 00:00:00'),
    ('weekly_load', 'STND_DOCUMENT_TEMPLATE', 'N', 'LAST_MOD_TS', '', 'DOCUMENT_TEMPLATE_CD', 10, 10000, 'LAST_MOD_TS', '2013-07-02 00:00:00', '2024-04-04 00:00:00'),
    ('weekly_load', 'STND_DOCUMENT_TYPE', 'N', 'LAST_MOD_TS', '', 'DOCUMENT_TYPE_ID', 10, 10000, 'LAST_MOD_TS', '2015-06-11 00:00:00', '2024-04-19 00:00:00'),
    ('weekly_load', 'STND_ELECTRONIC_ADDR_TYPE', 'N', 'LAST_MOD_TS', '', 'ELECTRONIC_ADDR_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '1776-07-03 00:00:00', '2013-05-02 00:00:00'),
    ('weekly_load', 'STND_EVIDENCE_BIN', 'N', 'LAST_MOD_TS', '', 'EVIDENCE_BIN_CD', 10, 10000, 'LAST_MOD_TS', '2017-09-09 00:00:00', '2017-09-09 00:00:00'),
    ('weekly_load', 'TM_CLASS_GDS_SRVC_TERM', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID, SEQUENCE_NO', '', '', '', '', ''),
    ('weekly_load', 'TM_CLASS_GDS_SRVC_TERM_H', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID, SEQUENCE_NO', '', '', '', '', ''),
    ('weekly_load', 'STND_EVIDENCE_SOURCE_CATEGORY', 'N', 'LAST_MOD_TS', '', 'EVIDENCE_SOURCE_CATEGORY_CD', 10, 10000, 'LAST_MOD_TS', '2017-09-09 00:00:00', '2017-09-26 00:00:00'),
    ('weekly_load', 'STND_FILING_BASIS', 'N', 'LAST_MOD_TS', '', 'FILING_BASIS_CD', 10, 10000, 'LAST_MOD_TS', '2013-05-02 00:00:00', '2013-05-02 00:00:00'),
    ('weekly_load', 'STND_FSM_CATEGORY', 'N', 'LAST_MOD_TS', '', 'FSM_CATEGORY_CD', 10, 10000, 'LAST_MOD_TS', '1976-07-04 00:00:00', '2023-07-21 00:00:00'),
    ('weekly_load', 'STND_FSM_STATE_LEGACY_STATE', 'N', 'LAST_MOD_TS', '', 'CFK_FSM_TYPE_STATE_ID', 10, 10000, 'LAST_MOD_TS', '2015-02-23 00:00:00', '2024-01-26 00:00:00'),
    ('weekly_load', 'STND_FSM_TYPE', 'N', 'LAST_MOD_TS', '', 'FSM_TYPE_ID', 10, 10000, 'LAST_MOD_TS', '1976-07-04 00:00:00', '2024-03-14 00:00:00'),
    ('weekly_load', 'STND_GDS_SRVC_ANNOTN_STAT', 'N', 'LAST_MOD_TS', '', 'GDS_SRVC_ANNOTN_STATUS_CD', 10, 10000, 'LAST_MOD_TS', '2014-07-18 00:00:00', '2014-07-18 00:00:00'),
    ('weekly_load', 'STND_GDS_SRVC_MATCH_STAT', 'N', 'LAST_MOD_TS', '', 'GDS_SRVC_MATCH_STAT_CD', 10, 10000, 'LAST_MOD_TS', '2014-07-18 00:00:00', '2014-07-18 00:00:00'),
    ('weekly_load', 'STND_GDS_SRVC_STATUS', 'N', 'LAST_MOD_TS', '', 'GDS_SRVC_STATUS_CD', 10, 10000, 'LAST_MOD_TS', '2013-05-02 00:00:00', '2016-05-16 00:00:00'),
    ('weekly_load', 'STND_GROUND', 'N', 'LAST_MOD_TS', '', 'FK_GROUND_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2015-10-31 00:00:00', '2023-02-06 00:00:00'),
    ('weekly_load', 'STND_GROUND_TYPE', 'N', 'LAST_MOD_TS', '', 'GROUND_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2014-02-11 00:00:00', '2014-02-11 00:00:00'),
    ('weekly_load', 'STND_INTRSTD_PARTY_RLTNSP_TYPE', 'N', 'LAST_MOD_TS', '', 'INTRSTD_PARTY_RELTNSP_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2013-05-02 00:00:00', '2013-05-02 00:00:00'),
    ('weekly_load', 'STND_LEGACY_TRANSACTION', 'N', 'LAST_MOD_TS', '', 'LEGACY_TRANSACTION_CD', 10, 10000, 'LAST_MOD_TS', '2015-02-23 00:00:00', '2021-12-16 00:00:00'),
    ('weekly_load', 'STND_LEGAL_ENTITY_TYPE', 'N', 'LAST_MOD_TS', '', 'LEGAL_ENTITY_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2013-05-02 00:00:00', '2016-09-02 00:00:00'),
    ('weekly_load', 'STND_MAD_BIRTH_REC_CT_TYPE', 'N', 'LAST_MOD_TS', '', 'MAD_BIRTH_REC_CT_TYPE_CD', '', '', '', '', ''),
    ('weekly_load', 'STND_MAD_TRANSACTION_TYPE', 'N', 'LAST_MOD_TS', '', 'MAD_TRANSACTION_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2015-12-15 00:00:00', '2016-06-03 00:00:00'),
    ('weekly_load', 'STND_MARK_TYPE', 'N', 'LAST_MOD_TS', '', 'MARK_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2018-04-15 00:00:00', '2018-04-15 00:00:00'),
    ('weekly_load', 'STND_MYUSPTO_EVENT', 'N', '', '', 'EVENT_CD', '', '', '', '', ''),
    ('weekly_load', 'STND_NOTE_TYPE', 'N', 'LAST_MOD_TS', '', 'NOTE_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2014-07-25 00:00:00', '2014-07-25 00:00:00'),
    ('weekly_load', 'STND_OBJECT_DISPATCH_TYPE', 'N', 'LAST_MOD_TS', '', 'OBJECT_DISPATCH_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2015-03-20 00:00:00', '2015-03-20 00:00:00'),
    ('weekly_load', 'STND_OBJECT_TYPE', 'N', 'LAST_MOD_TS', '', 'OBJECT_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2015-03-20 00:00:00', '2021-12-17 00:00:00'),
    ('weekly_load', 'TM_GDS_SRVC_TERM_FILG_BASIS', 'N', 'LAST_MOD_TS', '', '', '', '', '', '', ''),
    ('weekly_load', 'TM_GDS_SRVC_TERM_FILG_BASIS_H', 'N', 'LAST_MOD_TS', '', '', '', '', '', '', ''),
]
validate_groups(tmngpdb_metadata_group5)

# COMMAND ----------

# DBTITLE 1,tmngpdb_metadata_group6
tmngpdb_metadata_group6 = [
    ('weekly_load', 'STND_OFFICE_ACTION_CATEGORY', 'N', 'LAST_MOD_TS', '', 'OFFICE_ACTION_CATEGORY_CD', 10, 10000, 'LAST_MOD_TS', '2017-02-21 00:00:00', '2023-12-08 00:00:00'),
    ('weekly_load', 'STND_OFFICE_ACTION_RULE', 'N', 'LAST_MOD_TS', '', 'FK_OFFICE_ACTION_CATEGORY_CD, FK_WORK_ITEM_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2017-02-28 00:00:00', '2024-05-16 00:00:00'),
    ('weekly_load', 'STND_OFFICE_ACTIVITY_REASON', 'N', 'LAST_MOD_TS', '', 'OFFICE_ACTIVITY_REASON_CD', 10, 10000, 'LAST_MOD_TS', '2014-10-27 00:00:00', '2024-01-26 00:00:00'),
    ('weekly_load', 'STND_OFFICE_ACTVTY_RSN_CT', 'N', 'LAST_MOD_TS', '', 'OFFICE_ACTVTY_RSN_CT_CD', 10, 10000, 'LAST_MOD_TS', '2015-06-25 00:00:00', '2015-08-03 00:00:00'),
    ('weekly_load', 'STND_OWNER_TYPE', 'N', 'LAST_MOD_TS', '', 'OWNER_TYPE_ID', 10, 10000, 'LAST_MOD_TS', '2013-11-04 00:00:00', '2013-11-04 00:00:00'),
    ('weekly_load', 'STND_PAY_PERIOD', 'N', 'LAST_MOD_TS', '', 'FISCAL_YEAR_NO,PERIOD_NO', 10, 10000, 'LAST_MOD_TS', '2015-06-10 00:00:00', '2015-06-10 00:00:00'),
    ('weekly_load', 'STND_PRCDNG_EMPE_ASGMT_ROLE', 'N', 'LAST_MOD_TS', '', 'PRCDNG_EMPLOYEE_ROLE_CD', 10, 10000, 'LAST_MOD_TS', '2021-12-16 00:00:00', '2021-12-16 00:00:00'),
    ('weekly_load', 'STND_PUBLICATION_CATEGORY', 'N', 'LAST_MOD_TS', '', 'PUBLICATION_CATEGORY_CD', 10, 10000, 'LAST_MOD_TS', '2016-04-01 00:00:00', '2016-04-01 00:00:00'),
    ('weekly_load', 'STND_PUBLICATION_SUBCATEGORY', 'N', 'LAST_MOD_TS', '', 'FK_PUBLICATION_CATEGORY_CD, PUBLICATION_SUBCATEGORY_CD', 10, 10000, 'LAST_MOD_TS', '2016-04-01 00:00:00', '2024-03-16 00:00:00'),
    ('weekly_load', 'STND_QUERY_REVIEW_STATUS', 'N', 'LAST_MOD_TS', '', 'QUERY_REVIEW_STATUS_CD', 10, 10000, 'LAST_MOD_TS', '2014-07-25 00:00:00', '2014-07-25 00:00:00'),
    ('weekly_load', 'STND_REG_STMNT_TYPE', 'N', 'LAST_MOD_TS', '', 'REG_STMNT_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2013-09-09 00:00:00', '2013-09-09 00:00:00'),
    ('weekly_load', 'STND_RELATIONSHIP_TYPE', 'N', 'LAST_MOD_TS', '', 'RELATIONSHIP_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2014-12-23 00:00:00', '2014-12-23 00:00:00'),
    ('weekly_load', 'STND_RESPONSE_ISSUE', 'N', 'LAST_MOD_TS', '', 'RESPONSE_ISSUE_CD', 10, 10000, 'LAST_MOD_TS', '2014-10-28 00:00:00', '2015-01-02 00:00:00'),
    ('weekly_load', 'STND_REVIEW_ISSUE', 'N', 'LAST_MOD_TS', '', 'REVIEW_ISSUE_CD', 10, 10000, 'LAST_MOD_TS', '2015-06-23 00:00:00', '2015-06-23 00:00:00'),
    ('weekly_load', 'STND_REVIEW_RATING', 'N', 'LAST_MOD_TS', '', 'REVIEW_RATING_CD', 10, 10000, 'LAST_MOD_TS', '2015-02-02 00:00:00', '2015-02-02 00:00:00'),
    ('weekly_load', 'STND_STATEMENT_TYPE', 'N', 'LAST_MOD_TS', '', 'STATEMENT_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2013-05-02 00:00:00', '2022-11-17 00:00:00'),
    ('weekly_load', 'STND_SUBMISSION_METHOD', 'N', 'LAST_MOD_TS', '', 'SUBMISSION_METHOD_CD', 10, 10000, 'LAST_MOD_TS', '2015-01-12 00:00:00', '2015-06-12 00:00:00'),
    ('weekly_load', 'STND_TELECOM_FORMAT', 'N', 'LAST_MOD_TS', '', 'TELECOM_FORMAT_CD', 10, 10000, 'LAST_MOD_TS', '2013-05-02 00:00:00', '2015-07-09 00:00:00'),
    ('weekly_load', 'STND_TELECOM_TYPE', 'N', 'LAST_MOD_TS', '', 'TELECOM_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2013-05-02 00:00:00', '2013-05-02 00:00:00'),
    ('weekly_load', 'STND_TEMPLATE_PARA_TYPE', 'N', 'LAST_MOD_TS', '', 'TEMPLATE_PARA_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2013-05-02 00:00:00', '2014-12-08 00:00:00'),
    ('weekly_load', 'STND_TM_AMENDMENT_REASON', 'N', 'LAST_MOD_TS', '', 'TM_AMENDMENT_REASON_CD', 10, 10000, 'LAST_MOD_TS', '2016-06-01 00:00:00', '2016-06-01 00:00:00'),
    ('weekly_load', 'STND_TM_CLASS_STATUS', 'N', 'LAST_MOD_TS', '', 'TM_CLASS_STATUS_CD', 10, 10000, 'LAST_MOD_TS', '2013-05-02 00:00:00', '2023-12-12 00:00:00'),
    ('weekly_load', 'STND_TM_DIVISIONAL_STATUS', 'N', 'LAST_MOD_TS', '', 'TM_DIVISIONAL_STATUS_CD', 10, 10000, 'LAST_MOD_TS', '2013-06-21 00:00:00', '2013-06-21 00:00:00'),
    ('weekly_load', 'STND_TM_EMPLOYEE_ASGMT_ROLE', 'N', 'LAST_MOD_TS', '', 'TM_EMPLOYEE_ROLE_CD', 10, 10000, 'LAST_MOD_TS', '2013-05-02 00:00:00', '2022-05-06 00:00:00'),
    ('weekly_load', 'STND_TM_GROUP_TYPE', 'N', 'LAST_MOD_TS', '', 'TM_GROUP_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2015-03-12 00:00:00', '2015-03-12 00:00:00'),
    ('weekly_load', 'STND_TM_INTRSTD_PARTY_ROLE', 'N', 'LAST_MOD_TS', '', 'TM_INTRSTD_PARTY_ROLE_CD', 10, 10000, 'LAST_MOD_TS', '2013-05-02 00:00:00', '2013-07-05 00:00:00'),
    ('weekly_load', 'STND_TM_PARTY_ROLE', 'N', 'LAST_MOD_TS', '', 'TM_PARTY_ROLE_CD', 10, 10000, 'LAST_MOD_TS', '2013-05-02 00:00:00', '2013-07-05 00:00:00'),
    ('weekly_load', 'STND_TM_REVIEW_STATUS', 'N', 'LAST_MOD_TS', '', 'TM_REVIEW_STATUS_CD', 10, 10000, 'LAST_MOD_TS', '2014-07-25 00:00:00', '2014-07-25 00:00:00'),
    ('weekly_load', 'STND_US_INTL_CLS_MAPPING', 'N', 'LAST_MOD_TS', '', 'FK_INTL_CLASS_ID, FK_US_CLASS_ID', 10, 10000, 'LAST_MOD_TS', '2013-05-02 00:00:00', '2023-10-31 00:00:00'),
    ('weekly_load', 'STND_WORK_ITEM_RELTNSP_TYPE', 'N', 'LAST_MOD_TS', '', 'WORK_ITEM_RELATIONSHIP_CD', 10, 10000, 'LAST_MOD_TS', '2014-06-02 00:00:00', '2019-04-13 00:00:00'),
    ('weekly_load', 'STND_WORK_ITEM_REQUEST', 'N', 'LAST_MOD_TS', '', 'WORK_ITEM_REQUEST_CD', 10, 10000, 'LAST_MOD_TS', '2014-04-22 00:00:00', '2024-05-24 00:00:00'),
    ('weekly_load', 'STND_WORK_ITEM_TYPE', 'Y', 'LAST_MOD_TS', '', 'WORK_ITEM_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2013-12-18 00:00:00', '2024-05-24 00:00:00'),
    ('weekly_load', 'STND_WORK_ITEM_TYPE_DOC_TMPLT', 'N', 'LAST_MOD_TS', '', 'FK_DOCUMENT_TEMPLATE_CD, FK_WORK_ITEM_TYPE_CD', 10, 10000, 'LAST_MOD_TS', '2015-06-29 00:00:00', '2018-08-26 00:00:00'),
    ('weekly_load', 'STND_WORK_ITEM_TYPE_RULE', 'N', 'LAST_MOD_TS', '', 'WORK_ITEM_TYPE_RULE_ID', 10, 10000, 'LAST_MOD_TS', '2015-02-27 00:00:00', '2024-01-26 00:00:00'),
    ('weekly_load', 'STND_WORKER_RELTNSP_TYPE', 'N', 'LAST_MOD_TS', '', 'WORKER_RELATIONSHIP_CD', 10, 10000, 'LAST_MOD_TS', '2016-07-07 00:00:00', '2022-06-15 00:00:00'),
    ('weekly_load', 'STND_WRITING_RVW_ADDL_ACTN', 'N', 'LAST_MOD_TS', '', 'WRITING_RVW_ADDL_ACTN_CD', 10, 10000, 'LAST_MOD_TS', '2015-06-12 00:00:00', '2015-10-07 00:00:00'),
    ('weekly_load', 'SUBMISSION_AVERMENT', 'N', 'LAST_MOD_TS', '', 'FK_SUBMISSION_GID, SEQUENCE_NO', '', '', '', '', ''),
    ('weekly_load', 'SUBMISSION_AVERMENT_H', 'N', 'LAST_MOD_TS', '', 'FK_SUBMISSION_GID, SEQUENCE_NO', '', '', '', '', ''),
    ('weekly_load', 'SUBMISSION_ELCTRN_ADDR', 'N', 'LAST_MOD_TS', '', 'FK_ELECTRONIC_ADDRESS_GID, FK_SUBMISSION_GID', '', '', '', '', ''),
    ('weekly_load', 'SUBMISSION_ELCTRN_ADDR_H', 'N', 'LAST_MOD_TS', '', 'FK_ELECTRONIC_ADDRESS_GID, FK_SUBMISSION_GID', '', '', '', '', ''),
    ('weekly_load', 'SUBMISSION_SIGNATURE', 'N', 'LAST_MOD_TS', '', 'FK_SUBMISSION_GID, SEQUENCE_NO', '', '', '', '', ''),
    ('weekly_load', 'SYNC_AUTHUSER', 'Y', 'LASTUPDATED', '', '', '', '', '', '', ''),
    ('weekly_load', 'SYNC_CASELOCK', 'Y', '', '', '', '', '', '', '', ''),
    ('weekly_load', 'SYNC_CHECKPOINT', 'N', 'END_TS', '', 'SCRIPT_NM, START_TS', '', '', '', '', ''),
    ('weekly_load', 'SYNC_EXCEPTION_TYPE', 'Y', '', '', '', '', '', '', '', ''),
    ('weekly_load', 'SYNC_MIGRATION_RULES', 'Y', '', '', '', '', '', '', '', ''),
    ('weekly_load', 'SYNC_MIGRATION_SCRIPT', 'N', '', 'SCRIPT_NUM', '', '', '', '', '', ''),
    ('weekly_load', 'SYNC_RUNTIME', 'Y', '', '', '', '', '', '', '', ''),
    ('weekly_load', 'SYNC_STND_AM_STAT', 'N', '', '', 'AM_STAT', '', '', '', '', ''),
    ('weekly_load', 'SYNC_TRANLOG', 'Y', '', '', '', '', '', '', '', ''),
    ('weekly_load', 'SYNC_TRANSLATE_ASSUMED_NAME', 'Y', '', '', '', '', '', '', '', ''),
    ('weekly_load', 'SYNC_TRANSLATE_EMP_LO', 'Y', '', '', '', '', '', '', '', ''),
    ('weekly_load', 'SYNC_TRANSLATE_EP', 'Y', '', '', '', '', '', '', '', ''),
    ('weekly_load', 'SYNC_TRANSLATE_GEO', 'N', '', '', 'LEGACY_CD', '', '', '', '', ''),
    ('weekly_load', 'SYNC_TRANSLATE_LOCATION', 'Y', '', '', '', '', '', '', '', ''),
    ('weekly_load', 'SYNC_TRANSLATE_OG_CATG', 'N', '', '', 'OG_CAT', '', '', '', '', ''),
    ('weekly_load', 'SYNC_TRANSLATE_PARTY_TYPE', 'N', '', '', 'LEGACY_PARTY_TYPE', '', '', '', '', ''),
    ('weekly_load', 'SYNC_TRANSLATE_PETITION_DOCKT', 'N', '', '', 'DOC_TYPE_CD', '', '', '', '', ''),
    ('weekly_load', 'SYNC_TRANSLATE_WORK_ITEM_CMS', 'Y', '', '', '', '', '', '', '', ''),
    ('weekly_load', 'WORKER', 'N', 'LAST_MOD_TS', '', 'WORKER_GID', 10, 10000, 'LAST_MOD_TS', '2016-12-15 00:00:00', '2023-05-11 00:00:00'),
    ('weekly_load', 'WORKER_H', 'N', 'LAST_MOD_TS', '', 'WORKER_GID', 10, 10000, 'LAST_MOD_TS', '2016-12-15 00:00:00', '2024-05-24 00:00:00'),
]
validate_groups(tmngpdb_metadata_group6)

# COMMAND ----------

# DBTITLE 1,tmngpdb_metadata_group7
tmngpdb_metadata_group7 = [
    ('daily_load', 'TM_ADDITIONAL_STATEMENT_H', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'last_mod_ts', '2017-08-13 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'OG_PUBLICATION', 'N', 'LAST_MOD_TS', '', 'OG_PUBLICATION_GID', 10, 10000, 'last_mod_ts', '2018-03-03 00:00:00', '2023-10-24 00:00:00'),
    ('daily_load', 'OG_PUBLICATION_H', 'N', 'LAST_MOD_TS', '', 'OG_PUBLICATION_GID', 10, 10000, 'LAST_MOD_TS', '2018-03-03 00:00:00', '2023-10-24 00:00:00'),
    ('daily_load', 'OG_PUBLICATION_TM', 'Y', 'LAST_MOD_TS', '', 'FK_OG_PUBLICATION_GID', 10, 10000, 'LAST_MOD_TS', '2018-03-03 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'OG_PUBLICATION_TM_H', 'Y', 'LAST_MOD_TS', '', 'FK_OG_PUBLICATION_GID', 10, 10000, 'LAST_MOD_TS', '2018-03-03 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'SECTION_2F_STATEMENT_H', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2018-08-26 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'TM_DESIGN_ELEMENT', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2016-03-18 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'TM_REGISTRATION_STATEMENT_H', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2016-03-18 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'TM_RELATIONSHIP_H', 'N', 'LAST_MOD_TS', '', 'FK_PARENT_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2016-03-17 00:00:00', '2024-05-30 00:00:00'),
    ('daily_load', 'TRADEMARK_H', 'Y', 'LAST_MOD_TS', 'L', 'TRADEMARK_GID', 10, 10000, 'last_mod_ts', '2016-03-16 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'MAILING_ADDRESS_H', 'Y', 'LAST_MOD_TS', '', 'MAILING_ADDRESS_GID', 10, 10000, 'last_mod_ts', '2002-03-08 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'INTERESTED_PARTY_H', 'Y', 'LAST_MOD_TS', 'L', 'INTERESTED_PARTY_GID', 10, 10000, 'last_mod_ts', '2008-09-04 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'CONCURRENT_USE_H', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2018-08-25 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'USE_IN_ANOTHER_FORM_H', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2017-10-15 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'TM_PSEUDO_MARK_H', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2016-03-17 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'TM_PUBLICATION', 'Y', 'LAST_MOD_TS', '', 'TM_PUBLICATION_GID', 10, 10000, 'LAST_MOD_TS', '2018-03-03 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'TM_PUBLICATION_H', 'Y', 'LAST_MOD_TS', '', 'TM_PUBLICATION_GID', 10, 10000, 'LAST_MOD_TS', '1984-09-28 00:00:00', '2031-07-12 00:00:00'),
    ('daily_load', 'TM_PUBLICATION_SUBCT', 'N', 'LAST_MOD_TS', '', 'FK_TM_PUBLICATION_GID', 10, 10000, 'LAST_MOD_TS', '2018-03-03 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'TM_PUBLICATION_SUBCT_H', 'Y', 'LAST_MOD_TS', '', 'FK_TM_PUBLICATION_GID', 10, 10000, 'LAST_MOD_TS', '1984-09-28 00:00:00', '2031-07-12 00:00:00'),
    ('daily_load', 'TM_CLASS_H', 'Y', 'LAST_MOD_TS', 'L', 'FK_TRADEMARK_GID', 8, 10000, 'fk_class_id', 0, 926),
    ('daily_load', 'TM_FOREIGN_BASIS_H', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2017-07-09 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'TM_PARTY_ROLE_H', 'Y', 'LAST_MOD_TS', '', 'TM_PARTY_ROLE_ID', 10, 10000, 'LAST_MOD_TS', '2019-01-19 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'TM_PRIOR_REGISTRATION_H', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2020-10-24 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'TM_MILESTONE_H', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2020-06-07 00:00:00', '2024-06-04 00:00:00'),
]
validate_groups(tmngpdb_metadata_group7)

# COMMAND ----------

# DBTITLE 1,tmngpdb_metadata_group8
tmngpdb_metadata_group8 = [
    ('daily_load', 'TM_DIVISIONAL', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 2, 10000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'TM_DIVISIONAL_CHILD_H', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2016-03-18 00:00:00', '2024-05-31 00:00:00'),
    ('daily_load', 'TM_DIVISIONAL_H', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2016-03-18 00:00:00', '2024-05-30 00:00:00'),
    ('daily_load', 'TM_REGISTRATION_STATEMENT', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2016-03-18 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'TM_RENEWAL', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 2, 10000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'TM_RENEWAL_H', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 2, 10000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'TM_TELECOM_ADDR', 'N', 'LAST_MOD_TS', '', 'FK_TELECOM_ADDRESS_GID', 10, 10000, 'fk_tm_party_role_id', 1, 56845407),
    ('daily_load', 'TM_TELECOM_ADDR_H', 'Y', 'LAST_MOD_TS', '', 'FK_TELECOM_ADDRESS_GID', 10, 10000, 'fk_tm_party_role_id', 1, 56845407),
    ('daily_load', 'tmcom_batch_ingest_control','Y','', '', '', '', '', '', '', ''),
]
validate_groups(tmngpdb_metadata_group8)

# COMMAND ----------

# DBTITLE 1,tmngpdb_metadata_group9
tmngpdb_metadata_group9 = [
    ('daily_load', 'TRIGGER_EXCEPTIONS', 'Y', 'INSERT_TS', '', '', '', '', '', '', ''),
    ('daily_load', 'USER_PARA_FORM_PARA_VER', 'N', 'LAST_MOD_TS', '', 'CFK_FORM_PARAGRAPH_VERSION_GID', 6, 10000, 'FK_DOCUMENT_COMPONENT_ID', 1056201, 122197922),
    ('daily_load', 'USER_SESSION', 'N', 'LAST_MOD_TS', '', 'USER_SESSION_GID', 10, 10000, 'LAST_MOD_TS', '2016-04-25 00:00:00', '2024-05-31 00:00:00'),
    ('daily_load', 'WORK_ITEM', 'Y', 'LAST_MOD_TS', '', 'WORK_ITEM_GID', 10, 10000, 'LAST_MOD_TS', '1989-06-29 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'WORK_ITEM_H', 'Y', 'LAST_MOD_TS', '', 'WORK_ITEM_GID', 10, 10000, 'LAST_MOD_TS', '1989-06-29 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'WORK_ITEM_RELATIONSHIP', 'N', 'LAST_MOD_TS', '', 'FK_PARENT_WORK_ITEM_GID', 10, 10000, 'LAST_MOD_TS', '2015-12-22 00:00:00', '2024-05-31 00:00:00'),
    ('daily_load', 'WORK_ITEM_RELATIONSHIP_H', 'Y', 'LAST_MOD_TS', 'L', 'FK_PARENT_WORK_ITEM_GID', 10, 10000, 'LAST_MOD_TS', '2015-12-22 00:00:00', '2024-05-31 00:00:00'),
    ('daily_load', 'WORK_ITEM_REQUEST', 'N', 'LAST_MOD_TS', '', 'FK_WORK_ITEM_GID', 10, 10000, 'LAST_MOD_TS', '2017-02-12 00:00:00', '2024-05-31 00:00:00'),
    ('daily_load', 'WORK_ITEM_REQUEST_EMPLOYEE', 'N', 'LAST_MOD_TS', '', 'FK_WORK_ITEM_GID', 10, 10000, 'LAST_MOD_TS', '2020-12-07 00:00:00', '2024-05-31 00:00:00'),
    ('daily_load', 'WORKER_FOLDER', 'N', 'LAST_MOD_TS', '', 'WORKER_FOLDER_ID', 5, 10000, 'worker_folder_id', 2, 108889),
    ('daily_load', 'WORKER_FOLDER_ITEM', 'Y', 'LAST_MOD_TS', '', 'FK_WORKER_FOLDER_ID', 10, 10000, 'cfk_item_object_id', 1921, 73176),
    ('daily_load', 'WRITING_REVIEW', 'N', 'LAST_MOD_TS', '', 'WRITING_REVIEW_ID', 5, 10000, 'writing_review_id', 1, 4502),
    ('daily_load', 'TM_STATES', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2022-08-20 00:00:00', '2024-06-02 00:00:00'),
    ('daily_load', 'TM_APPEALS', 'N', 'LAST_MOD_TS', '', 'CFK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2022-11-17 00:00:00', '2024-06-02 00:00:00'),
    ('daily_load', 'TM_ELECTRONIC_ADDR_H', 'Y', 'LAST_MOD_TS', '', 'FK_ELECTRONIC_ADDRESS_GID', 10, 10000, 'fk_tm_party_role_id', 1, 56845407),
]
validate_groups(tmngpdb_metadata_group9)

# COMMAND ----------

# DBTITLE 1,tmngpdb_metadata_group10
tmngpdb_metadata_group10 = [
    ('daily_load', 'STND_FSM_TYPE_EVENT', 'N', 'LAST_MOD_TS', '', 'FSM_TYPE_EVENT_ID', 10, 10000, 'LAST_MOD_TS', '1976-07-04 00:00:00', '2024-04-19 00:00:00'),
    ('daily_load', 'STND_FSM_TYPE_STATE', 'N', 'LAST_MOD_TS', '', 'FSM_TYPE_STATE_ID', 10, 10000, 'LAST_MOD_TS', '2013-05-23 00:00:00', '2024-05-24 00:00:00'),
    ('daily_load', 'STND_FSM_TYPE_STATE_RULE', 'Y', 'LAST_MOD_TS', '', 'STND_FSM_TYPE_STATE_RULE', 10, 10000, 'LAST_MOD_TS', '2013-05-23 00:00:00', '2024-05-30 00:00:00'),
    ('daily_load', 'STND_LEGACY_STATUS', 'N', 'LAST_MOD_TS', '', 'STATUS_NO', 10, 10000, 'LAST_MOD_TS', '2021-07-08 00:00:00', '2024-06-05 00:00:00'),
    ('daily_load', 'STND_OFFICE_ACTION_CT_STATE', 'Y', 'LAST_MOD_TS', '', 'CFK_FSM_TYPE_STATE_ID, FK_OFFICE_ACTION_CATEGORY_CD', 10, 10000, 'LAST_MOD_TS', '2017-03-01 00:00:00', '2024-05-22 00:00:00'),
    ('daily_load', 'STND_OFFICE_ACTN_RULE_ITM', 'N', 'LAST_MOD_TS', '', 'OFFICE_ACTN_RULE_ITM_ID', 10, 10000, 'LAST_MOD_TS', '2017-02-28 00:00:00', '2024-05-16 00:00:00'),
    ('daily_load', 'TM_FILINGS', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_APPLICANT_RESPONSE_DT', '1900-05-29 00:00:00', '2055-07-11 00:00:00'),
    ('daily_load', 'TM_FILING_BASES', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2023-08-31 00:00:00', '2024-06-02 00:00:00'),
    ('daily_load', 'TM_ITU_EXTENSION', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'EXPIRATION_DT', '1991-06-09 00:00:00', '2025-06-02 00:00:00'),
    ('daily_load', 'TM_ITU_EXTENSION_H', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'EXPIRATION_DT', '1991-06-09 00:00:00', '2025-12-02 00:00:00'),
    ('daily_load', 'TM_ITU_H', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2022-08-20 00:00:00', '2024-06-05 00:00:00'),
    ('daily_load', 'TM_OG_PUBLICATIONS', 'N', 'LAST_MOD_TS', '', 'CFK_TRADEMARK_GID', 10, 10000, 'OG_PUBD_FOR_OPSTN_DT', '1905-08-29 00:00:00', '2024-07-02 00:00:00'),
    ('daily_load', 'TM_OG_PUBLICATIONS_H', 'Y', 'LAST_MOD_TS', '', 'CFK_TRADEMARK_GID', 10, 10000, 'OG_PUBD_FOR_OPSTN_DT', '1905-08-29 00:00:00', '2024-07-02 00:00:00'),
    ('daily_load', 'TM_CLASS_REFERENCE_H', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2017-10-14 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'TM_GROUP', 'N', 'LAST_MOD_TS', '', 'TM_GROUP_ID', 10, 100000, 'tm_group_id', 1, 5668767),
    ('daily_load', 'TM_GROUP_ITEM', 'Y', 'LAST_MOD_TS', '', 'FK_TM_GROUP_ID, FK_TRADEMARK_GID', 10, 100000, 'fk_tm_group_id', 1, 5668767),
    ('daily_load', 'TM_LITERAL_H', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2016-03-16 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'TM_LOCATIONS_H', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2022-10-08 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'TM_MAILING_ADDR_H', 'Y', 'LAST_MOD_TS', '', 'FK_MAILING_ADDRESS_GID', 10, 10000, 'LAST_MOD_TS', '2002-03-08 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'TM_MARK_TYPE', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2018-04-15 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'TM_MARK_TYPE_H', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2018-04-15 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'TM_NOTIFICATION_MESSAGE', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'cfk_notification_message_id', 59705447, 80124404),
    ('daily_load', 'TM_ORGANIZATION_LOCATION', 'Y', 'LAST_MOD_TS', '', 'LOCATION_ID', 10, 10000, 'LAST_MOD_TS', '2022-06-04 00:00:00', '2023-02-16 00:00:00'),
    ('daily_load', 'TM_PHYSICAL_LOCATION', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 5, 10000, 'physical_location_dt', '2004-05-13 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'TM_POST_REGISTRATION', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2024-04-25 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'TM_PROCEEDING', 'Y', 'LAST_MOD_TS', '', 'TM_PROCEEDING_ID', 2, 10000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'TM_PROCEEDING_H', 'Y', 'LAST_MOD_TS', '', 'TM_PROCEEDING_ID', 10, 10000, 'LAST_MOD_TS', '1984-03-22 00:00:00', '2024-05-16 00:00:00'),
    ('daily_load', 'TM_PSEUDO_CLASS', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 5, 10000, 'fk_class_id', 1, 906),
    ('daily_load', 'TM_PSEUDO_CLASS_H', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 5, 10000, 'fk_class_id', 1, 906),
    ('daily_load', 'TM_PSEUDO_MARK', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 2, 10000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'TM_DESIGN_ELEMENT_H', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2016-03-18 00:00:00', '2024-06-01 00:00:00'),
    ('daily_load', 'TM_FOREIGN_BASIS', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 2, 10000, 'LOCK_CONTROL_NO', 0, 1),
    ('daily_load', 'TM_PRIOR_REGISTRATION', 'N', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2020-10-24 00:00:00', '2024-06-01 00:00:00'),
]
validate_groups(tmngpdb_metadata_group10)

# COMMAND ----------

# DBTITLE 1,tmngpdb_metadata_group11
tmngpdb_metadata_group11 = [
    ('daily_load', 'TM_FILING_BASIS_H', 'Y', 'LAST_MOD_TS', '', 'FK_TRADEMARK_GID', 10, 10000, 'LAST_MOD_TS', '2017-07-09 00:00:00', '2024-06-03 00:00:00'),
    ('daily_load', 'WORK_ITEM_OBJECT', 'Y', 'LAST_MOD_TS', '', 'FK_WORK_ITEM_GID', 10, 10000, 'LAST_MOD_TS', '1989-06-29 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'TM_PARTY_ROLE', 'N', 'LAST_MOD_TS', '', 'FK_TM_PARTY_ROLE_CD,TM_PARTY_ROLE_ID', 10, 10000, 'tm_party_role_id', 1, 56845407),
    ('daily_load', 'TM_MAILING_ADDR', 'N', 'LAST_MOD_TS', '', 'FK_TM_PARTY_ROLE_ID,FK_MAILING_ADDRESS_GID', 10, 10000, 'fk_tm_party_role_id', 1, 56845407),
    ('daily_load', 'TM_ELECTRONIC_ADDR', 'Y', 'LAST_MOD_TS', '', 'FK_ELECTRONIC_ADDRESS_GID', 10, 10000, 'LAST_MOD_TS', '2011-10-12 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'TM_CLASS', 'N', 'LAST_MOD_TS', 'L', '', 10, 10000, 'fk_class_id', 0, 926),
    ('daily_load', 'INTERESTED_PARTY', 'Y', 'LAST_MOD_TS', 'L', 'interested_party_gid', 10, 10000, 'LAST_MOD_TS', '2008-09-04 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'MAILING_ADDRESS', 'Y', 'LAST_MOD_TS', '', 'MAILING_ADDRESS_GID', 10, 10000, 'LAST_MOD_TS', '2002-03-08 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'ELECTRONIC_ADDRESS', 'N', 'LAST_MOD_TS', '', 'ELECTRONIC_ADDRESS_GID', 10, 10000, 'LAST_MOD_TS', '2011-10-12 00:00:00', '2024-06-04 00:00:00'),
    ('daily_load', 'TM_FILING_BASIS', 'N', 'LAST_MOD_TS', '', 'fk_filing_basis_cd,fk_trademark_gid', 2, 10000, 'LOCK_CONTROL_NO', 0, 1),
]
validate_groups(tmngpdb_metadata_group11)

# COMMAND ----------

# DBTITLE 1,tmngpdb_metadata_group12
tmngpdb_metadata_group12 = [
    ('daily_load', 'SEARCH_STRATEGY', 'Y', 'LAST_MOD_TS', '', 'SEARCH_STRATEGY_ID', 10, 10000, 'search_strategy_id', 161, 1903187)
]
validate_groups(tmngpdb_metadata_group12)

# COMMAND ----------

# DBTITLE 1,tmbuscalendar_metadata
tmbuscalendar_metadata = [
    ('weekly_load','BUSINESS_CALENDAR_RANGE','N','LAST_MOD_TS','','RANGE_NM','','','','',''),
    ('weekly_load','BUSINESS_CALENDAR_DAY','N','LAST_MOD_TS','','','','','','',''),
    ('weekly_load','BUS_CALENDAR_DAY_PROPERTY','N','LAST_MOD_TS','','CFK_PROPERTY_TYPE_CD','','','','','')
]
validate_groups(tmbuscalendar_metadata)

# COMMAND ----------

# DBTITLE 1,tmintltm_metadata
tmintltm_metadata = [
    ('daily_load','INTERNATIONAL_APPL_EVENT','Y','LAST_MOD_TS','','fk_international_appl_gid','8',50000,'INTERNATIONAL_APPL_EVENT_ID',3000000, 5500000),
    ('daily_load','BASE_APPLICATION','N','LAST_MOD_TS','','FK_INTERNATIONAL_APPL_GID','','','','',''),
    ('daily_load','BASE_APPLICATION_H','N','LAST_MOD_TS','','FK_INTERNATIONAL_APPL_GID','','','','',''),
    ('daily_load','INTERNATIONAL_APPLICATION','N','LAST_MOD_TS','','INTERNATIONAL_APPLICATION_GID','','','','',''),
    ('daily_load','INTERNATIONAL_APPLICATION_H','N','LAST_MOD_TS','','INTERNATIONAL_APPLICATION_GID','','','','',''),
    ('daily_load','INTERNATIONAL_REGISTRATION','N','LAST_MOD_TS','','INTERNATIONAL_REG_GID','','','','',''),
    ('daily_load','INTERNATIONAL_REGISTRATION_H','N','LAST_MOD_TS','','INTERNATIONAL_REG_GID','2',50000,'LOCK_CONTROL_NO',0,1),
    ('daily_load','INTERNATIONAL_REG_TM','N','LAST_MOD_TS','','CFK_TRADEMARK_GID','','','','',''),
    ('daily_load','INTERNATIONAL_REG_TM_H','N','LAST_MOD_TS','','CFK_TRADEMARK_GID','8',50000,'IB_PUBLICATION_DT','2010-01-01 00:00:00','2028-01-01 00:00:00'),
    ('daily_load','INTERNATIONAL_TM','N','LAST_MOD_TS','','INTERNATIONAL_REG_NO','','','','',''),
    ('daily_load','INTERNATIONAL_TM_H','N','LAST_MOD_TS','','CFK_TRANSACTION_INSTANCE_GID','8',50000,'INTERNATIONAL_REG_DT','2010-01-01 00:00:00','2028-01-01 00:00:00'),
    ('weekly_load','INTERNATIONAL_APPL_EVNT_RSN','Y','LAST_MOD_TS','','international_appl_evnt_rsn_id','','','','',''),
    ('daily_load','BASE_APPL_INTL_REG','Y','LAST_MOD_TS','','FK_INTERNATIONAL_APPL_GID','','','','',''),
    ('daily_load','INTERNATIONAL_REG_TM_NOTICE','N','LAST_MOD_TS','','','8',50000,'PROCESSED_NOTICE_DT','2010-01-01 00:00:00','2028-01-01 00:00:00'),
    ('daily_load','TM_BASE_APPLICATION_NOTICE','N','LAST_MOD_TS','','','','','','','')
]
validate_groups(tmintltm_metadata)

# COMMAND ----------

# DBTITLE 1,tmngfpepp_metadata
tmngfpepp_metadata = [
    ('weekly_load','DATABASECHANGELOG','Y','','','','','','','',''),
    ('daily_load','FORM_PARAGRAPH_REASON','Y','','','','','','','',''),
    ('daily_load','DATABASECHANGELOGLOCK','N','','','ID','','','','',''),
    ('weekly_load','QRTZ_BLOB_TRIGGERS','N','','','TRIGGER_NAME','','','','',''),
    ('weekly_load','QRTZ_CALENDARS','N','','','CALENDAR_NAME','','','','',''),
    ('daily_load','QRTZ_CRON_TRIGGERS','N','','','TRIGGER_NAME','','','','',''),
    ('weekly_load','QRTZ_FIRED_TRIGGERS','N','','','ENTRY_ID','','','','',''),
    ('weekly_load','QRTZ_JOB_DETAILS','N','','','SCHED_NAME','','','','',''),
    ('daily_load','QRTZ_LOCKS','Y','','','LOCK_NAME','','','','',''),
    ('weekly_load','QRTZ_PAUSED_TRIGGER_GRPS','Y','','','SCHED_NAME','','','','',''),
    ('daily_load','QRTZ_SCHEDULER_STATE','N','','','SCHED_NAME','','','','',''),
    ('weekly_load','QRTZ_SIMPLE_TRIGGERS','N','','','TRIGGER_NAME','','','','',''),
    ('weekly_load','QRTZ_SIMPROP_TRIGGERS','N','','','TRIGGER_NAME','','','','',''),
    ('daily_load','QRTZ_TRIGGERS','N','','','TRIGGER_NAME','','','','',''),
    ('daily_load','FORM_PARAGRAPH','Y','LAST_MOD_TS','','FORM_PARAGRAPH_GID','','','','',''),
    ('daily_load','FORM_PARAGRAPH_ACTION','N','LAST_MOD_TS','','FORM_PARAGRAPH_ACTION_GID','','','','',''),
    ('daily_load','FORM_PARAGRAPH_VERSION','Y','LAST_MOD_TS','','FORM_PARAGRAPH_VERSION_GID','','','','',''),
    ('weekly_load','FPV_SCHEDULED_JOB','N','LAST_MOD_TS','','FK_FORM_PARAGRAPH_VERSION_GID','','','','',''),
    ('daily_load','STND_CHAPTER_SECTION','N','LAST_MOD_TS','','CHAPTER_SECTION_ID','','','','',''),
    ('weekly_load','STND_FORM_PARAGRAPH_ACTION','N','LAST_MOD_TS','','FORM_PARAGRAPH_ACTION_CD','','','','',''),
    ('weekly_load','STND_FORM_PARAGRAPH_CATEGORY','N','LAST_MOD_TS','','FORM_PARAGRAPH_CATEGORY_ID','','','','',''),
    ('weekly_load','STND_FORM_PARAGRAPH_GROUP','N','LAST_MOD_TS','','FORM_PARAGRAPH_GROUP_ID','','','','',''),
    ('daily_load','STND_FORM_PARAGRAPH_REASON','N','LAST_MOD_TS','','FORM_PARAGRAPH_REASON_ID','','','','',''),
]
validate_groups(tmngfpepp_metadata)

# COMMAND ----------

# DBTITLE 1,eogadmin_metadata
eogadmin_metadata = [
    ('weekly_load','fsm_instance','N','last_mod_ts','','fsm_instance_id','','','','',''),
    ('weekly_load','fsm_instance_h','N','last_mod_ts','','fsm_instance_h_id','','','','',''),
    ('weekly_load','fsm_interlock','N','last_mod_ts','','fsm_interlock_id','','','','',''),
    ('weekly_load','og_appeal_fsm_instance','N','last_mod_ts','','cfk_root_fsm_instance_id','','','','',''),
    ('weekly_load','og_review_fsm_instance','N','last_mod_ts','','cfk_current_fsm_instance_id','','','','',''),
    ('weekly_load','og_review_query_fsm_instance','N','last_mod_ts','','cfk_current_fsm_instance_id','','','','',''),
    ('weekly_load','qrtz_blob_triggers','N','','','sched_name','','','','',''),
    ('weekly_load','qrtz_calendars','N','','','sched_name','','','','',''),
    ('weekly_load','qrtz_cron_triggers','N','','','sched_name','','','','',''),
    ('weekly_load','qrtz_fired_triggers','N','','','sched_name','','','','',''),
    ('weekly_load','qrtz_job_details','N','','','sched_name','','','','',''),
    ('weekly_load','qrtz_locks','Y','','','sched_name','','','','',''),
    ('weekly_load','qrtz_paused_trigger_grps','Y','','','sched_name','','','','',''),
    ('weekly_load','qrtz_scheduler_state','N','','','sched_name','','','','',''),
    ('weekly_load','qrtz_simple_triggers','N','','','sched_name','','','','',''),
    ('weekly_load','qrtz_simprop_triggers','N','','','sched_name','','','','',''),
    ('weekly_load','qrtz_triggers','N','','','sched_name','','','','',''),
    ('weekly_load','stnd_domain','N','last_mod_ts','','domain_cd','','','','',''),
    ('weekly_load','stnd_fsm_category','N','last_mod_ts','','fsm_category_cd','','','','',''),
    ('weekly_load','stnd_fsm_interlock','Y','last_mod_ts','','fsm_interlock_id','','','','',''),
    ('weekly_load','stnd_fsm_interlock_type','N','last_mod_ts','','fsm_interlock_type_cd','','','','',''),
    ('weekly_load','stnd_fsm_type','N','last_mod_ts','','fsm_type_id','','','','',''),
    ('weekly_load','stnd_fsm_type_event','N','last_mod_ts','','fsm_type_event_id','','','','',''),
    ('weekly_load','stnd_fsm_type_state','N','last_mod_ts','','fsm_type_state_id','','','','',''),
    ('weekly_load','stnd_fsm_type_state_rule','N','last_mod_ts','','fsm_type_state_rule_id','','','','',''),
    ('weekly_load','stnd_interlock_type','Y','last_mod_ts','','stnd_interlock_type_cd','','','','',''),
    ('weekly_load','user_profile','N','last_mod_ts','','user_profile_id','','','','',''),
    ('weekly_load','user_profile_preference','N','last_mod_ts','','fk_user_profile_id','','','','',''),
]
validate_groups(eogadmin_metadata)

# COMMAND ----------

# DBTITLE 1,jbteasps_metadata
jbteasps_metadata = [
    ('weekly_load','stnd_source_system','N','','','source_system_id','','','','',''),
    ('daily_load','audit_log','N','','','audit_log_id, serial_no','8',50000,'AUDIT_LOG_ID',0, 25000000),
    ('weekly_load','stnd_transaction_type','N','','','transaction_type_cd','','','','','')
]
validate_groups(jbteasps_metadata)


# COMMAND ----------

# DBTITLE 1,proceeding_metadata
proceeding_metadata = [
    ('daily_load','petition','N','','','FK_PROCEEDING_GID','','','','',''),
    ('weekly_load','petition_h','N','','','FK_PROCEEDING_GID','','','','',''),
    ('daily_load','petition_response','N','','','FK_PROCEEDING_GID','','','','',''),
    ('daily_load','petition_response_document','N','','','FK_PROCEEDING_GID','','','','',''),
    ('weekly_load','petition_response_document_h','N','','','FK_PROCEEDING_GID','','','','',''),
    ('daily_load','petition_response_h','N','','','FK_PROCEEDING_GID','','','','',''),
    ('weekly_load','prcdng_trigger_exceptions','Y','','','','','','','',''),
    ('daily_load','proceeding','N','','','PROCEEDING_GID','','','','',''),
    ('daily_load','proceeding_class','N','','','FK_PROCEEDING_GID','','','','',''),
    ('daily_load','proceeding_class_h','N','','','FK_PROCEEDING_GID','','','','',''),
    ('daily_load','proceeding_document','N','','','FK_PROCEEDING_GID','','','','',''),
    ('daily_load','proceeding_document_h','N','','','FK_PROCEEDING_GID','','','','',''),
    ('daily_load','proceeding_event','N','','','PROCEEDING_EVENT_ID','','','','',''),
    ('weekly_load','proceeding_event_reason','N','','','PROCEEDING_EVENT_REASON_ID','','','','',''),
    ('daily_load','proceeding_fee','N','','','FK_PROCEEDING_GID','','','','',''),
    ('daily_load','proceeding_fee_h','N','','','FK_PROCEEDING_GID','','','','',''),
    ('daily_load','proceeding_h','N','','','PROCEEDING_GID','','','','',''),
    ('daily_load','proceeding_mark','N','','','FK_PROCEEDING_GID','','','','',''),
    ('daily_load','proceeding_mark_h','N','','','FK_PROCEEDING_GID','','','','',''),
    ('daily_load','proceeding_participant','N','','','FK_PROCEEDING_GID','','','','',''),
    ('daily_load','proceeding_participant_h','N','','','FK_PROCEEDING_GID','','','','',''),
    ('daily_load','proceeding_statement','N','','','FK_PROCEEDING_GID','','','','',''),
    ('daily_load','proceeding_statement_h','N','','','FK_PROCEEDING_GID','','','','',''),
    ('daily_load','proceeding_tran_instance', 'N','','','PROCEEDING_TRAN_INSTNC_GID','','','','',''),
    ('daily_load','sync_tm_com_exception','N','','','TM_COM_EXCEPTION_ID','','','','',''),
    ('daily_load','letter_of_protest','N','','','FK_PROCEEDING_GID','','','','',''),
    ('daily_load','letter_of_protest_h','N','','','FK_PROCEEDING_GID','','','','',''),
    ('daily_load','lop_legal_basis','N','','','FK_PROCEEDING_GID','','','','',''),
    ('daily_load','lop_legal_basis_h','N','','','FK_PROCEEDING_GID','','','','',''),
    ('daily_load','lop_legal_basis_trademark','N','','','LOP_LEGAL_BASIS_TRADEMARK_ID','','','','',''),
    ('daily_load','lop_legal_basis_trademark_h','N','','','CFK_TRANSACTION_INSTANCE_GID','','','','',''),
    ('weekly_load','proceeding_intl_appl','N','','','FK_PROCEEDING_GID','','','','',''),
    ('weekly_load','proceeding_intl_appl_h','N','','','FK_PROCEEDING_GID','','','','',''),
    ('weekly_load','stnd_lop_legal_basis','N','','','LOP_LEGAL_BASIS_CD','','','','',''),
    ('weekly_load','stnd_petition_to_director','N','','','PETITION_TO_DIRECTOR_CD','','','','',''),
]
validate_groups(proceeding_metadata)

# COMMAND ----------

# DBTITLE 1,tmprodvty_metadata
tmprodvty_metadata = [
    ('daily_load','production_transaction','N','last_mod_ts','','cfk_object_gid','8',50000,'PRODUCTION_CREDIT_TRAN_ID',32000000,54000000),
    ('weekly_load','productivity_action','N','last_mod_ts','','productivity_action_id','','','','',''),
    ('weekly_load','worker_time_entry','N','last_mod_ts','','cfk_worker_gid','','','','',''),
    ('daily_load','PRODUCTION_TRANSACTION_ERRLOG','Y','LAST_MOD_TS','','CFK_OBJECT_GID','','','','','')
]
validate_groups(tmprodvty_metadata)

# COMMAND ----------

# DBTITLE 1,tmreviews_metadata
tmreviews_metadata = [
    ('daily_load','PRE_EXAM_QUALITY_REVIEW','N','LAST_MOD_TS','','CFK_TRADEMARK_GID','27',50000,'CREATE_USER_ID',1,60), 
    ('daily_load','PRE_EXAM_QUALITY_RVW_ERR','N','LAST_MOD_TS','','CFK_TRADEMARK_GID','','','','',''),
    ('daily_load','POST_REG_QUALITY_REVIEW','N','LAST_MOD_TS','','CFK_TRADEMARK_GID','','','','',''),
    ('weekly_load','POST_REG_QUALITY_REVIEW_ERRLOG','Y','LAST_MOD_TS','','CFK_TRADEMARK_GID','','','','',''),
    ('daily_load','POST_REG_QUALITY_REVIEW_H','N','LAST_MOD_TS','','CFK_TRADEMARK_GID','','','','',''),
    ('weekly_load','POST_REG_REVIEW_NOTICE','N','LAST_MOD_TS','','CFK_BCR_PAY_PERIOD_RANGE_NAME','','','','',''),
    ('daily_load','POST_REG_REVIEW_NOTICE_ERRLOG','Y','LAST_MOD_TS','','ORA_ERR_MESG,CFK_TRADEMARK_GID','','','','',''),
    ('weekly_load','POST_REG_REVIEW_NOTICE_H','N','LAST_MOD_TS','','CFK_BCR_PAY_PERIOD_RANGE_NAME,DN_SERIAL_NUM_TX','','','','',''), 
    ('daily_load','PREG_QUALITY_REVIEW_ELEMENT','N','LAST_MOD_TS','','CFK_TRADEMARK_GID,FK_PRQR_CREATED_DT','','','','',''),
    ('weekly_load','PREG_QUALITY_REVIEW_ELEMENT_ERRLOG','Y','LAST_MOD_TS','','ORA_ERR_MESG,CFK_TRADEMARK_GID','','','','',''),
    ('daily_load','PREG_QUALITY_REVIEW_ELEMENT_H','N','LAST_MOD_TS','','CFK_TRADEMARK_GID','','','','',''),
]
validate_groups(tmreviews_metadata)

# COMMAND ----------

# DBTITLE 1,tmworker_metadata
tmworker_metadata = [
    ('daily_load','TM_ORGANIZATION','N','LAST_MOD_TS','','TM_ORGANIZATION_GID','','','','',''),
    ('daily_load','WORKER','N','LAST_MOD_TS','','WORKER_GID','','','','',''),
    ('daily_load','TM_ORGANIZATION_RLTNSHP','N','LAST_MOD_TS','','FK_CHILD_TM_ORGANIZATION_GID, FK_PARENT_TM_ORGANIZATION_GID','','','','',''),
    ('daily_load','TRANSACTION_INSTANCE','N','LAST_MOD_TS','','TRANSACTION_INSTANCE_GID','','','','',''),
    ('weekly_load','USER_ROLE','N','LAST_MOD_TS','','USER_ROLE_ID','','','','',''),
    ('weekly_load','USER_ROLE_GROUP','N','LAST_MOD_TS','','USER_ROLE_GROUP_CD','','','','',''),
    ('weekly_load','WORKER_H','N','LAST_MOD_TS','','WORKER_GID','','','','',''),
    ('daily_load','WORKER_ROLE_H','N','LAST_MOD_TS','','FK_USER_ROLE_ID','','','','',''),
    ('weekly_load','WORKER_ROLE','N','LAST_MOD_TS','','FK_USER_ROLE_ID','','','','',''), 
    ('daily_load','SYNC_TRANSLATE_LOCATION','Y','','','','','','','','') 
]
validate_groups(tmworker_metadata)

# COMMAND ----------

# DBTITLE 1,tmngidmp_metadata
tmngidmp_metadata = [
    ('daily_load', 'audit_revision', 'N','LAST_MOD_TS','','AUDIT_REVISION_ID','','','','',''), 
    ('daily_load', 'goods_services_term', 'N', 'LAST_MOD_TS','','GOODS_SERVICES_TERM_ID','','','','',''),
    ('daily_load', 'goods_services_term_draft', 'N', 'LAST_MOD_TS','','GOODS_SERVICES_TERM_ID,GOODS_SERVICES_TERM_ID_TX','','','','',''), 
    ('daily_load', 'goods_services_term_note', 'Y', 'LAST_MOD_TS','','FK_GOODS_SERVICES_TERM_ID','','','','',''), 
    ('daily_load', 'goods_services_term_note_draft', 'N', 'LAST_MOD_TS','','FK_GOODS_SERVICES_TERM_ID','','','','',''), 
    ('daily_load', 'stnd_application_property', 'N', 'LAST_MOD_TS','','APPLICATION_PROPERTY_CD','','','','',''), 
    ('weekly_load','data_comp', 'Y', '','','SN','','','','',''), 
    ('weekly_load','data_comp_parsed', 'Y', '','','SN,ORG_TXT','','','','',''), 
    ('weekly_load','data_comp_result', 'Y', '','','SERIAL_NUMBER','','','','',''), 
    ('weekly_load','data_comp_sam', 'Y', '','','SN,TXT','','','','',''), 
    ('weekly_load','data_comp_sam_result', 'Y', '','','SERIAL_NUMBER,CLASS','','','','',''), 
    ('weekly_load','data_comp_test', 'Y', '','','SN,TXT','','','','',''), 
    ('weekly_load','data_id', 'Y', '','','CLS','','','','',''),
    ('weekly_load','data_id_case_level_result', 'Y', '','','SERIAL_NUMBER','','','','',''), 
    ('weekly_load','data_id_parsed', 'Y', '','','CLS','','','','',''),
    ('weekly_load','data_id_parsed_standard', 'Y', '','','CLS','','','','',''), 
    ('weekly_load','data_teas_plus_clob', 'Y', '','','SERIALNUMBER,CLASS','','','','',''), 
    ('weekly_load','data_teas_standard_clob', 'Y', '','','SERIALNUMBER,SUBMISSIONID','','','','',''), 
    ('weekly_load', 'international_class_version', 'N', 'LAST_MOD_TS','','FK_CLASS_ID','','','','',''), 
    ('weekly_load', 'international_clsfcn_edn', 'N', 'LAST_MOD_TS','','EDITION_NO','','','','',''), 
    ('weekly_load', 'intl_clsfcn_edn_ver', 'N', 'LAST_MOD_TS','','FK_EDITION_NO,VERSION_YEAR_NO','','','','',''), 
    ('weekly_load', 'intl_clsfcn_edn_ver_rel', 'N', 'LAST_MOD_TS','','FK_EDITION_NO','','','','',''), 
    ('weekly_load', 'menu_item', 'N', 'LAST_MOD_TS','','MENU_ITEM_ID','','','','',''), 
    ('weekly_load', 'stnd_application_message', 'Y', 'LAST_MOD_TS','','APPLICATION_MESSAGE_ID','','','','',''), 
    ('weekly_load', 'stnd_class', 'N', 'LAST_MOD_TS','','CLASS_NO,CLASS_ID','','','','',''), 
    ('weekly_load', 'stnd_class_schedule', 'N', 'LAST_MOD_TS','','CLASS_SCHEDULE_CD','','','','',''), 
    ('weekly_load', 'stnd_coordinated_class', 'N', 'LAST_MOD_TS','','FK_CLASS_ID','','','','',''), 
    ('weekly_load', 'stnd_goods_services_note', 'N', 'LAST_MOD_TS','','GOODS_SERVICES_NOTE_CD','','','','',''), 
    ('weekly_load', 'stnd_synonym_group', 'Y', 'LAST_MOD_TS','','SYNONYM_GROUP_ID','','','','',''), 
    ('weekly_load', 'stnd_term_status', 'N', 'LAST_MOD_TS','','TERM_STATUS_CD','','','','',''), 
    ('weekly_load', 'stnd_us_intl_cls_mapping', 'N', 'LAST_MOD_TS','','FK_US_CLASS_ID','','','','',''),
    ('weekly_load', 'sync_idm_update_log', 'Y', '','','INSERT_TS','','','','',''),
    ('weekly_load', 'taxonomy_group', 'N', 'LAST_MOD_TS','','TAXONOMY_GROUP_ID','','','','',''),
    ('weekly_load', 'tm5_file', 'N', 'LAST_MOD_TS','','TM5_FILE_ID','','','','',''), 
    ('weekly_load', 'tm5_goods_services', 'N', 'LAST_MOD_TS','','FK_TM5_FILE_ID','','','','','') 
]
validate_groups(tmngidmp_metadata)

# COMMAND ----------

# DBTITLE 1,efoiap_metadata
efoiap_metadata = [
    ('weekly_load','appeal_decision_issue','N','','','FK_SEQUENCE_NO, FK_TRADEMARK_PROCEEDING_NO, LEVEL_1_ISSUE_CD','','','','',''),
    ('weekly_load','document_type','N','','','','','','','',''),
    ('weekly_load','stnd_decision','N','','','','','','','',''),
    ('weekly_load','stnd_level_1_issue','N','','','','','','','',''),
    ('weekly_load','stnd_level_2_issue','N','','','FK_LEVEL_1_ISSUE_CD, LEVEL_2_ISSUE_CD','','','','',''),
    ('daily_load','tm_appeal_decision','N','','','TM_APPEAL_DECISION_ID','','','','',''),
    ('daily_load','tm_appeal_decision_h','N','','','TM_APPEAL_DECISION_H_ID','','','','',''),
    ('daily_load','trademark_appeal_decision','N','','','SEQUENCE_NO, TRADEMARK_PROCEEDING_NO','','','','',''),
    ('weekly_load','efoia_trigger_exceptions','Y','','','','','','','',''),
    ('weekly_load','prosecution_history_event','Y','','','fk_proceedingnumber0','8',50000,'ENTRY_DATE','1990-01-01 00:00:00','2028-01-01 00:00:00'),
    ('daily_load','prosecution_history_event2','Y','','','fk_proceedingnumber0','8',50000,'ENTRY_DATE','1990-01-01 00:00:00','2028-01-01 00:00:00'),
    ('weekly_load','tmng_go_live','Y','','','','','','','',''),
]
validate_groups(efoiap_metadata)

# COMMAND ----------

# DBTITLE 1,tmrefdata_metadata
tmrefdata_metadata = [
    ('daily_load', 'CODE_TYPE', 'Y', '','','CODE_TYPE_ID','','','','',''),
    ('daily_load', 'CODE_TYPE_62623', 'Y', '','','','','','','',''),
    ('daily_load', 'CODE_TYPE_DEPENDENCY', 'Y', '','','FK_DEPENDENT_CODE_TYPE_ID, FK_ENABLING_CODE_TYPE_ID','','','','',''),
    ('daily_load', 'CODE_TYPE_DEPENDENCY_62623', 'Y', '','','','','','','',''),
    ('daily_load', 'CODE_TYPE_DOMAIN_SERVICE', 'Y', '','','FK_CODE_TYPE_ID, FK_DOMAIN_SERVICE_ID','','','','',''),
    ('daily_load', 'CODE_TYPE_PROPERTY_TYPE', 'Y', '','','PROPERTY_TYPE_ID','','','','',''),
    ('daily_load', 'CODE_TYPE_PROPERTY_TYPE_62623', 'Y', '','','','','','','',''),
    ('daily_load', 'CODE_VALUE', 'Y', '','','CODE_VALUE_ID','','','','',''),
    ('daily_load', 'CODE_VALUE_62623', 'Y', '','','','','','','',''),
    ('daily_load', 'CODE_VALUE_BAK', 'Y', '','','','','','','',''),
    ('daily_load', 'CODE_VALUE_DEPENDENCY', 'Y', '','','FK_CHILD_CODE_VALUE_ID, FK_PARENT_CODE_VALUE_ID','','','','',''),
    ('daily_load', 'CODE_VALUE_DEPENDENCY_62623', 'Y', '','','','','','','',''),
    ('daily_load', 'CODE_VALUE_PROPERTY', 'Y', '','','FK_CODE_VALUE_ID, FK_PROPERTY_TYPE_ID','','','','',''),
    ('daily_load', 'CODE_VALUE_PROPERTY_62623', 'Y', '','','','','','','',''),
    ('daily_load', 'DOMAIN_SERVICE', 'Y', '','','DOMAIN_SERVICE_ID','','','','',''),
    ('daily_load', 'DOMAIN_SERVICE_COMPLETE_LIST', 'Y', '','','','','','','','')
]
validate_groups(tmrefdata_metadata)
