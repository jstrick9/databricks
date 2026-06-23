# Databricks notebook source
dbutils.widgets.text("dbx_env", "dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "tmngpdb-conf.yaml"
config_file = (
    "../../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name
)
if dbx_env == "qa":
    dbx_env = "test"
print(f"{config_file=},{dbx_env=}")

# COMMAND ----------

# MAGIC %run  ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# schema variables
common_configs = read_yaml(config_file)
tmngpdb_catalog = common_configs["schema"]["trgt_catalog"]
data_quality_catalog = common_configs["schema"]["data_quality_catalog"]
print(f"{tmngpdb_catalog=}, {data_quality_catalog=}")


# spark.conf.set('config.data_quality_catalog', data_quality_catalog.lower())
# spark.conf.set('conf.catalog', tmngpdb_catalog.lower())
# spark.conf.set('dbx_env', dbx_env)

# COMMAND ----------

database = "bronze"
control_table = "cdc_batch_job_control"
job_history_table = "cdc_batch_job_history"

spark.conf.set("conf.catalog", tmngpdb_catalog)
spark.conf.set("conf.database", database)
spark.conf.set("conf.control_table", control_table)
spark.conf.set("conf.job_history_table", job_history_table)
spark.conf.set("conf.dbx_env", dbx_env)

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${conf.catalog};
# MAGIC create schema if not exists  ${conf.database};
# MAGIC use ${conf.database};

# COMMAND ----------

tables_to_comment = {'query_ground': 'The query_ground table stores information related to queries and their associated grounds. It contains data such as the query ground ID, review query ID, ground code, ground type code, lock control number, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is significant to the business as it allows for the tracking and management of queries and their corresponding grounds. The data in this table represents the relationship between queries and grounds, providing valuable insights for analysis and decision-making.', 'mv_myuspto_trm_ph': 'The mv_myuspto_trm_ph table contains data related to events in the USPTO trademark system. It includes information such as the serial number of the event, the event code, and the date and time of the event. This table is significant to the business as it allows for tracking and analysis of trademark events, providing insights into the progress and status of trademark applications and registrations. The data in this table can be used to monitor timelines, identify bottlenecks, and improve efficiency in the trademark process.', 'tm_amendment': 'The tm_amendment table contains data related to trademark amendments. It stores information about the reasons for amendments, the sequence number of the amendment, the target element being amended, the lock control number, and the status of the amendment. The table also includes timestamps for when the amendment was created and last modified, as well as the user IDs associated with those actions. The target element code is also included in the table, representing the specific element being amended. This table is significant to the business as it allows for tracking and managing trademark amendments, providing a historical record of changes made to trademarks.', 'tm_divisional': 'The tm_divisional table in the bronze schema of the trm_tmngpdb catalog stores data related to divisional trademarks. It contains information about the divisional trademarks, including their unique identifiers, sequence numbers, lock control numbers, creation timestamps, and the IDs of the users who created and last modified the records. This table is essential for tracking and managing divisional trademarks within the business.', 'document_component_reltnsp': 'The document_component_reltnsp table represents the relationship between parent and child document components. It contains foreign key references to the parent and child document components, as well as a lock control number. The create_ts and last_mod_ts columns contain timestamps indicating when the relationship was created and last modified. The create_user_id and last_mod_user_id columns store the user IDs of the individuals who created and last modified the relationship. This table is important for tracking and managing the relationships between different document components within the business.', 'submission': 'The submission table contains data related to submissions made by users. It includes information such as the submission method, form type, received date, response, filing date, status, control number, creation and modification timestamps, and user IDs. This table is significant to the business as it tracks and stores all submissions made, allowing for easy retrieval and analysis of submission data. It provides valuable insights into user behavior, form types popularity, and submission response times.', 'stnd_response_issue': "The stnd_response_issue table in the bronze schema of the trm_tmngpdb catalog stores information about response issues. It contains data related to the code, title, and description of each response issue. Additionally, it includes timestamps for when the issues effectiveness begins and ends, as well as timestamps for when the issue was created and last modified. The user IDs of the individuals who created and last modified the issue are also recorded. This table is crucial for tracking and managing response issues within the business.", 'stnd_tm_employee_asgmt_role': 'The stnd_tm_employee_asgmt_role table contains information about the roles assigned to employees within the organization. It includes the role code, title, and description of each role. The begin_effective_dt and end_effective_dt columns indicate the period during which the role is valid. The create_ts and create_user_id columns capture the timestamp and user ID of when the role was created, while the last_mod_ts and last_mod_user_id columns capture the timestamp and user ID of when the role was last modified. This table is essential for managing and tracking employee assignments and their respective roles within the organization.', 'stnd_business_event_reason': 'The stnd_business_event_reason table contains information about the reasons for business events. It provides details such as the event reason code, milestone, title, description, legacy entity code, legacy entity type code, FSM type event ID, prosecution history indicator, alert trigger category, effective dates, creation and modification timestamps, and user IDs. This table is essential for understanding the reasons behind various business events and can be used for analysis, reporting, and decision-making purposes.', 'evidence_document': "The evidence_document table stores information about evidence documents related to a specific business process. It contains data such as the document ID, the folder ID it belongs to, the display order number, and the document alias name. The table also includes information about the documents source category code, creation and modification timestamps, and the user IDs of the creators and modifiers. This table is essential for tracking and managing evidence documents within the business process, allowing for efficient organization and retrieval of relevant information.", 'document_component': 'The document_component table stores information about the various components of a document. It includes details such as the component type, content, metadata, and timestamps for creation and modification. The table also contains a lock control number to manage concurrent access to the document components. This table is significant to the business as it allows for efficient organization and retrieval of document components, enabling seamless collaboration and version control. It serves as a central repository for managing document components across different processes and systems.', 'stnd_work_item_type_doc_tmplt': 'The stnd_work_item_type_doc_tmplt table stores the relationship between work item types and document templates. It contains information about the effective dates of the relationship, as well as the timestamps for when the relationship was created and last modified. This table is important for managing the document templates that are associated with different types of work items in the business. It allows for tracking changes to the relationships over time and ensures that the correct document templates are used for each work item type.', 'electronic_address': 'The electronic_address table stores information about electronic addresses associated with individuals or entities. It includes the type of electronic address, the address locator, and the control number for locking purposes. The table also tracks the creation and modification timestamps and user IDs. This data is important for managing and communicating with customers, suppliers, and other business partners.', 'employee_credit_transaction': 'The employee_credit_transaction table contains data related to credit transactions for employees. It includes information such as the work item, trademark, reason type, earner and approver employee numbers, transaction effective date, transaction value, transaction reason, transaction type, lock control number, creation and modification timestamps, user IDs, employee credit transaction ID, and active trademark class count. This table is significant to the business as it helps track and manage credit transactions for employees, providing valuable insights into their performance and contributions.', 'sync_runtime': 'The sync_runtime table contains information related to the synchronization runtime. It includes data such as the names and roles of different entities involved in the synchronization process, as well as details about schemas, hosts, and sessions. Additionally, it stores information about tablespaces used for data and index storage, ownership, DML roles, and error handling. This table provides valuable insights into the runtime aspects of synchronization and helps in monitoring and troubleshooting the synchronization process.', 'employee_query_appeal': "The employee_query_appeal table stores information related to employee query appeals. It contains data that represents the appeals made by employees regarding queries they have raised. The table includes details such as the appeal ID, the employees role, employee number, organization code, and timestamps for creation and modification. This table is significant to the business as it helps track and manage employee appeals, providing insights into the resolution process and allowing for analysis of query handling efficiency.", 'stnd_legacy_status': 'The stnd_legacy_status table represents the  status information for legacy records. It contains data related to the status number, description, tram state, common status code, common status descriptor, common status definition, live/dead count, effective dates, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is significant to the business as it provides a record of the various statuses that legacy records have gone through over time, allowing for historical analysis and tracking of changes made to these records.', 'stnd_tm_amendment_reason': 'The stnd_tm_amendment_reason table contains information about the reasons for amending a trademark. It includes the code for the amendment reason, the title and description of the reason, the effective dates of the reason, and the timestamps for when the reason was created and last modified. This table is important for tracking and categorizing the different reasons for amending trademarks, allowing for better analysis and understanding of the amendment process.', 'employee_award_withdraw': 'The employee_award_withdraw table stores information related to the withdrawal of awards given to employees. It tracks the transaction IDs for award and withdrawal events, as well as the control number for locking purposes. The table also includes timestamps for when the records were created and last modified, along with the corresponding user IDs. This data is important for tracking and managing the award withdrawal process within the business.', 'annotation_comment': 'The annotation_comment table contains information about comments made on review annotations. It includes the foreign key referencing the review annotation ID, the foreign key referencing the employee number, the date and time of the comment, the source of the comment, the text of the comment, the lock control number, the user ID of the user that generated the record, the date and time of the record creation, the user ID that last modified the record, and a unique identifier for the annotation comment. This table is significant for tracking and analyzing comments made on review annotations.', 'stnd_mad_birth_rec_ct_type': 'The stnd_mad_birth_rec_ct_type table contains information about the types of Madrid birth records in the system. It includes details such as the code for each type, the title of the type, and a description of what each type represents. The table also includes timestamps for when each record was created and last modified, as well as the user IDs associated with those actions. This table is important for tracking and managing different types of birth records within the business.', 'gds_srvc_stmt_annotation': 'The gds_srvc_stmt_annotation table contains data related to annotations made on service statements. It includes information such as the foreign key for the trademark, the foreign key for the class, the parse option count, the annotation count, the display order number, the text segment locator, the text segment, the foreign key for the match status code, the foreign key for the annotation status code, the lock control number, the creation timestamp, the ID of the user who created it, the last modification timestamp, and the ID of the user who last modified it. This table is significant for tracking and managing annotations on service statements for trademarks.', 'stnd_doc_type_ct': 'The stnd_doc_type_ct table stores information about different document types used in the business. It includes the document type code, title, and description. The table also contains details about the business unit associated with each document type, such as the business unit code and display order. Additionally, the table tracks the effective dates of each document type, as well as the timestamps for when the records were created and last modified. This table is essential for managing and categorizing various documents within the business.', 'stnd_filing_basis': 'The stnd_filing_basis table contains information about the different filing basis codes used in the business. It represents the various legal grounds or reasons for filing a trademark application. The table includes details such as the filing basis code, title, and description of each filing basis. It also includes the effective dates for when a filing basis becomes valid or expires. Additionally, the table tracks the timestamps for when each record was created or last modified, along with the corresponding user IDs. This table is essential for accurately categorizing and managing trademark applications based on their filing basis.', 'stnd_fsm_type_state': 'The stnd_fsm_type_state table represents the different states that a finite state machine (FSM) can be in. It provides information about the type of FSM, the root FSM type, and the title, description, human activity, automated activity, and start condition associated with each state. The table also includes timestamps for when each state was created and last modified, as well as the user IDs of the creators and modifiers. Additionally, the table includes information about the duration of each state and the conditions for transitioning into and out of each state. This table is essential for tracking and managing the various states of FSMs within the business.', 'international_reg_tm': 'The international_reg_tm table contains data related to international trademark registrations. It includes information such as the foreign key for the trademark, the foreign key for the international registration, the status code, status date, priority claimed date, auto protect date, notification date, cancellation date, first refusal information, lock control number, creation timestamp, creation user ID, last modification timestamp, last modification user ID, IB renewal date, and IB publication date. This table is significant to the business as it allows for tracking and managing international trademark registrations and their associated details.', 'stnd_statement_type': 'The stnd_statement_type table contains information about different types of statements used in the business. Each statement type has a title and description, which provide a brief summary of its purpose. The table also includes the effective dates for when a statement type is valid, allowing for tracking changes over time. Additionally, the table captures metadata such as the creation and modification timestamps, as well as the user IDs associated with those actions. This table is essential for managing and categorizing statements within the business.', 'interested_party': "The interested_party table contains information about individuals or entities who have expressed interest in a particular business or service. It includes details such as the type of legal entity, the statement provided by the entity, the name of the interested party, contact information, and other relevant details. This table is significant to the business as it helps in tracking and managing potential customers or partners. The data in this table represents the interested parties and their associated information, which can be used for lead generation, customer relationship management, and business development purposes.", 'query_appeal': 'The query_appeal table contains data related to appeals made by users. It represents the appeal process within the business. The table includes information such as the appeal result, the role of the person who approved the appeal, the date of the appeal result, the appeal proceeding number, the appeal decision, and the reason for the appeal. Additionally, it includes details about any emails sent to the director regarding the appeal. The table also tracks the creation and modification timestamps and user IDs for auditing purposes.', 'query_appeal_status': 'The query_appeal_status table contains information about the status of appeals made by employees. It includes timestamps for when the appeal status was updated, as well as the user IDs of the individuals who created and last modified the appeal status. The table also includes control numbers for locking purposes. This data is important for tracking the progress and history of appeals within the business.', 'ip_electronic_address': "The ip_electronic_address table stores information about electronic addresses associated with interested parties. This table is important for tracking and managing communication channels for the business. It contains data related to the interested partys global identifier, electronic address global identifier, lock control number, creation and modification timestamps, as well as user IDs for the creation and last modification. The table provides a centralized repository for electronic address data, enabling efficient management and retrieval of information related to interested parties contact details.", 'draft_document_version_compnt': 'The draft_document_version_compnt table represents the components of draft documents. It contains information about the draft document ID, draft document modification number, document component ID, rank order number, lock control number, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is significant to the business as it allows tracking and managing the different components of draft documents, enabling collaboration and version control during the drafting process.', 'mailing_address': 'The mailing_address table contains information about the mailing addresses associated with customers. It includes details such as the name lines, street lines, city, geographic region, postal code, country, and department. The table also includes information about the address type, lock control number, and timestamps for creation and last modification. This table is significant to the business as it allows for accurate and up-to-date customer address information, which is essential for effective communication, shipping, and location-based analysis.', 'tm_class_gds_srvc_term': 'The tm_class_gds_srvc_term table contains data related to the goods and services terms associated with trademarks. It includes information such as the trademark ID, class ID, sequence number, status codes, activity type code, first use dates, intent to use date, lock control number, creation and modification timestamps, user IDs, and the goods and services term text. This table is significant to the business as it helps in managing and tracking the usage of goods and services terms for trademarks.', 'ip_telecom_address': 'The ip_telecom_address table stores information about the telecom addresses associated with interested parties. It includes the foreign key references to the interested party and telecom address tables, as well as control numbers for locking purposes. The table also tracks the creation and modification timestamps, along with the corresponding user IDs. This data is crucial for managing and maintaining the telecom addresses of interested parties within the business system.', 'stnd_work_item_type': 'The stnd_work_item_type table contains information about different types of work items in the business. Each work item type is identified by a unique code and can have a parent work item type. The table also includes the title and description of each work item type, providing a brief overview of its purpose. Additionally, the table includes information about the work item group, work item category, and various flags indicating the status of office actions and activities related to the work item type. The table also tracks the effective dates, creation and modification timestamps, as well as the user IDs responsible for creating and modifying the records.', 'stnd_review_issue': 'The stnd_review_issue table contains data related to review issues in the business. It provides information about the parent review issue code, review issue code, title, description, type, hierarchy level, begin and end effective dates, creation and last modification timestamps, create and last modification user IDs, and review type. This table is important for tracking and managing review issues within the business, allowing for efficient identification and resolution of any issues that arise.', 'office_activity_reason': 'The office_activity_reason table contains data related to the reasons for office activities. It provides information on the reasons behind various work items and their associated office activities. The table includes details such as the unique identifier of the work item, the reason code for the office activity, the control number for locking purposes, and timestamps for creation and modification. This table is essential for tracking and analyzing the reasons behind office activities, enabling better decision-making and process improvement within the business.', 'telecom_address': 'The telecom_address table stores information about telecom addresses, including the telecom number, extension number, telecom type, and telecom format. It also includes details such as the lock control number, creation timestamp, and last modification timestamp. This table is significant to the business as it allows for the management and tracking of telecom addresses for various purposes. The submitted_telecom_no column represents the telecom number that has been submitted for processing.', 'stnd_work_item_reltnsp_type': 'The stnd_work_item_reltnsp_type table contains information about the relationship types between work items. It includes the code for each relationship type, as well as the title and description of the relationship. The table also includes timestamps for when the relationship type was created and last modified, along with the corresponding user IDs. This table is essential for managing and categorizing work items based on their relationships, allowing for efficient tracking and organization within the business.', 'stnd_annotation_status': 'The stnd_annotation_status table contains information about the status of annotations in the system. It includes details such as the title and description of the annotation, the dates when the annotation is effective, and the type of review associated with the annotation. This table is important for tracking the progress and status of annotations, allowing the business to manage and monitor the annotation process effectively.', 'stnd_intrstd_party_rltnsp_type': 'The stnd_intrstd_party_rltnsp_type table contains information about the different types of relationships between interested parties. It includes details such as the relationship type code, title, and description. Additionally, it provides information on whether the relationship is between individuals, organizations, or a combination of both. The table also includes timestamps for when the relationship begins and ends, as well as timestamps for when the records were created or last modified. This table is essential for understanding and managing the various relationships between interested parties in the business.', 'stnd_design_search_group_type': 'The stnd_design_search_group_type table contains information about the different types of design search groups. Each record represents a specific design search group type, including its title and description. The begin_effective_dt and end_effective_dt columns indicate the time period during which the design search group type is valid. The create_ts and create_user_id columns capture the timestamp and user ID of when the record was created, while the last_mod_ts and last_mod_user_id columns capture the timestamp and user ID of when the record was last modified. This table is essential for managing and categorizing design search groups within the business.', 'stnd_electronic_addr_type': 'The stnd_electronic_addr_type table contains information about the types of electronic addresses used in the business. This table provides a standardized list of electronic address types, such as email, phone number, or social media handle. It includes details such as the title and description of each address type, as well as the effective dates for when the address type is valid. The table also tracks the creation and modification timestamps and user IDs for auditing purposes.', 'stnd_worker_reltnsp_type': 'The stnd_worker_reltnsp_type table contains information about the different types of worker relationships in the business. It includes the title and description of each relationship, as well as the effective dates during which the relationship is valid. The table also includes control numbers for locking purposes, timestamps for creation and modification of records, and user IDs for tracking the individuals responsible for these actions. This table is essential for managing and understanding the various worker relationships within the organization.', 'stnd_gds_srvc_status': 'The stnd_gds_srvc_status table contains information about the status of various services provided by the business. It includes details such as the service title, description, and the dates when the status becomes effective and ends. The table also tracks the creation and modification timestamps, as well as the user IDs responsible for those actions. This data is crucial for monitoring and managing the availability and changes in service statuses, enabling the business to make informed decisions and ensure smooth service operations.', 'stnd_query_review_status': 'The stnd_query_review_status table contains information about the status of query reviews. It includes the status code, title, and description of each status. The table also includes the effective dates for each status, indicating when it becomes active and when it expires. Additionally, the table tracks the creation and modification timestamps, as well as the user IDs of the individuals who created and last modified the status records. This table is essential for tracking and managing the different stages of query reviews within the business.', 'sync_casestatus': 'The sync_casestatus table contains data related to the status of cases. It includes information such as the serial number of the case, the date and time when the case was last updated, the current status of the case, whether it is locked or not, and the timestamp of when the record was created. This table is important for tracking and managing the progress and status of cases within the business.', 'stnd_appeal_result': 'The stnd_appeal_result table contains data related to appeal results. It includes information such as the appeal result code, title, description, effective dates, creation and modification timestamps, and user IDs. This table is significant to the business as it allows for tracking and analyzing appeal outcomes. The data in this table represents the various appeal results that can occur within the business processes, providing valuable insights for decision-making and process improvement.', 'sync_translate_petition_dockt': 'The sync_translate_petition_dockt table contains data related to translated petition dockets. It includes information such as the document type code, description, role code, docket ID, docket text, and event code. This table is significant to the business as it allows for the synchronization and translation of petition dockets, enabling efficient processing and analysis of legal documents. The data in this table represents the various attributes and details associated with translated petition dockets, facilitating accurate and streamlined legal operations.', 'stnd_tm_party_role': 'The stnd_tm_party_role table contains information about the various roles that parties can have within the business. Each record represents a specific party role and includes details such as the title and description of the role. The table also includes information on the cardinality of the role, indicating whether it is a one-to-one or one-to-many relationship. The begin and end effective dates indicate the period during which the role is valid. The table also tracks the creation and modification timestamps, as well as the user IDs of the individuals who made the changes.', 'concurrent_use': 'The concurrent_use table contains data related to the concurrent use of trademarks. It includes information such as the unique identifier of a trademark, sequential number assigned to each statement, concurrent use year, month, and day numbers, category indicating the basis and status of concurrent use, number used for lock control purposes, timestamps for record creation and modification, and the statement text. This table is significant for tracking and managing concurrent use cases for trademarks in the business.', 'stnd_evidence_source_category': 'The stnd_evidence_source_category table contains information about the different categories of evidence sources. Each category is identified by a unique code and has a title and description. The table also includes timestamps for when the category was created and last modified, as well as the user IDs of the individuals who performed these actions. The begin_effective_dt and end_effective_dt columns indicate the period during which the category is valid. This table is important for categorizing and managing evidence sources within the business.', 'object_dispatch': 'The object_dispatch table stores information about the dispatch of objects within the business. It contains data related to the user session, object type, dispatch type, object ID, organization code, action start date, current action date, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is significant as it helps track and manage the dispatch of objects, allowing the business to monitor and analyze the movement of objects within the organization.', 'office_activity_draft_document': 'The office_activity_draft_document table stores information about draft documents associated with work items in the office activity. It contains data such as the unique identifier of the work item, the identifier of the draft document, the lock control number, timestamps for creation and last modification, and the user IDs of the creators and modifiers. This table is essential for tracking and managing draft documents within the office activity, allowing users to collaborate and make changes to documents before they are finalized.', 'search_strategy': 'The search_strategy table contains information about the different search strategies used by the business. It includes the unique identifier for each search strategy, the name of the search strategy, whether it is public or not, the employee number associated with the search strategy, the timestamps for when the search strategy was created and last modified, the user IDs of the individuals who created and last modified the search strategy, and a description of the search strategy. This table is important for tracking and managing the various search strategies employed by the business.', 'prcdng_employee_assignment': 'The prcdng_employee_assignment table contains information about the assignments of employees to proceedings. It tracks the unique identifier of the proceeding, the role code of the employee in the proceeding, and the employee number. Additionally, it includes timestamps for the effective date of the assignment, the creation and last modification of the record, as well as the user IDs associated with those actions. The lock control number is used for concurrency control. This table is essential for managing and tracking employee assignments to proceedings within the business.', 'review_query_note': 'The review_query_note table contains data related to notes added to review queries in the system. Each note is associated with a specific review query and can be of different types. The table also includes information about the employee who added the note, their role, and the organization they belong to. The note text provides additional details or comments related to the review query. The table also tracks the creation and modification timestamps, as well as the user IDs of the individuals who made those changes.', 'stnd_relationship_type': 'The stnd_relationship_type table stores information about different types of relationships. It includes the relationship type code, title, and description. The begin_effective_dt and end_effective_dt columns indicate the period during which the relationship type is valid. The create_ts, create_user_id, last_mod_ts, and last_mod_user_id columns track the creation and modification details of the records in the table. This table is essential for categorizing and managing relationships within the business system.', 'submission_averment': 'The submission_averment table  contains data related to averments made in submissions. Each row represents a specific averment made in a submission. The table includes information such as the foreign key to the submission, the sequence number of the averment, the non-standard averment text, and the lock control number. It also includes timestamps for when the averment was created and last modified, as well as the user IDs of the creators and modifiers. This table is significant to the business as it allows for tracking and management of averments made in submissions.', 'myuspto_trm_change_ntfcn': 'The myuspto_trm_change_ntfcn table contains data related to changes in trademark status. It includes a unique serial number for each change, as well as the dates and times when the status change event occurred. This table is significant to the business as it provides a historical record of when and how trademark statuses have changed over time. The data in this table can be used for analysis and reporting purposes, such as identifying trends in trademark status changes or tracking the progress of trademark applications.', 'stnd_business_event_rsn_ct': 'The stnd_business_event_rsn_ct table contains information about the reasons for various business events. It includes the title and description of each reason, as well as the effective dates for when the reason is valid. The table also tracks the creation and modification timestamps, as well as the user IDs of the individuals who made the changes. This data is important for understanding the context and history of business events within the organization.', 'predefined_paragraph': 'The predefined_paragraph table stores predefined paragraphs that can be used in various business processes. It represents a collection of standardized text content that can be easily referenced and inserted into documents or communications. The table includes information such as the paragraph ID, the content of the paragraph, and details about its creation and modification. This table is essential for streamlining and maintaining consistency in written communications within the business.', 'sync_translate_location': "The sync_translate_location table contains data related to the translation of location information. This table is significant to the business as it helps in synchronizing and translating location data across different systems or applications. The table includes columns such as law_office_cd, palm_short_cd, and tt_text, which store relevant information for the translation process. The data in this table represents the mappings and translations of location codes and descriptions, enabling accurate and consistent location data across the organizations systems.", 'stnd_fsm_type_event': 'The stnd_fsm_type_event table stores information about different types of events in the FSM system. It includes the title and description of each event, as well as the timestamps for when the event was created and last modified. This table is important for tracking and managing the various events in the system, providing a centralized repository for event information. The fsm_type_event_id column serves as the primary key for this table, uniquely identifying each event.', 'docket_item': 'The docket_item table contains information about individual items within a docket. It includes foreign keys referencing the unique identifiers of work items, assignees, assigning persons, objects, and organizations. The table also includes timestamps for the effective date, creation, and last modification of each record. Additionally, it includes control numbers for locking purposes and user IDs for the creators and modifiers of the records. The docket_item_id column serves as a unique identifier for each docket item.', 'tm_document_reference': 'The tm_document_reference table stores information about documents in the system. It includes data such as the document ID, document type, page count, and timestamps for creation and modification. This table is significant to the business as it allows for tracking and management of documents within the system. The data in this table represents the basic information and metadata associated with each document, enabling efficient document retrieval and organization.', 'abandonment': 'The abandonment table contains data related to the abandonment of work items. It includes information such as the unique identifier of the work item, the abandonment date, the time taken to receive a response for the item, the code for the response issue, a text description of the response issue, and an indicator if the response was received on time or not. It also includes details about any overrides for the abandonment date, response received time, and response on-time indicator. Additionally, it includes information about the locking control number, the user who created the record, and the user who last modified the record.', 'og_publication': "The og_publication table contains data related to publications. It includes information such as the publications unique identifier, publication date, lock control number, creation timestamp, ID of the user who created it, timestamp of the last modification, and ID of the user who made the last modification. This table is significant to the business as it allows for tracking and managing publications, including their creation and modification history.", 'stnd_tm_milestone': "The stnd_tm_milestone table in the bronze schema of the trm_tmngpdb catalog contains data related to milestones in a project or task management system. It includes information such as the milestone code, title, description, start and end dates, as well as details about when the milestone was created and last modified. This table is significant to the business as it allows for tracking and monitoring of project progress, enabling stakeholders to have a clear overview of important milestones and their associated details.", 'predefined_paragraph_ver': 'The predefined_paragraph_ver table contains information about predefined paragraphs used in documents. It represents the version history of these paragraphs, including their titles, creation and modification timestamps, and effective dates. The table also includes foreign keys to related components and instructions. The status_ct column indicates the current status of each paragraph version. This table is essential for tracking changes and managing the content of documents within the business.', 'stnd_prcdng_empe_asgmt_role': 'The stnd_prcdng_empe_asgmt_role table contains information about the roles assigned to employees in the proceedings. It includes the role code, title, and description of each role. The table also includes the effective dates for each role assignment, indicating when the role became active and when it ended. Additionally, the table tracks the timestamps and user IDs for the creation and last modification of each record. This table is essential for managing and tracking the roles of employees involved in proceedings within the business.', 'international_tm': 'The international_tm table contains data related to international trademarks. It includes information such as the international registration number, registration date, source country, and lock control number. The table also tracks the timestamps and user IDs for when the records were created and last modified. This table is significant to the business as it allows for the management and tracking of international trademark registrations, providing valuable data for legal and intellectual property purposes.', 'sync_translate_work_item_cms': 'The sync_translate_work_item_cms table contains data related to the translation of work item types and CMS document types. It provides information about the different types of work items and their corresponding CMS document types, along with a description of each document type. This table is significant to the business as it helps in managing and tracking the translation process for work items and ensures accurate mapping between work item types and CMS document types.', 'evidence_bin_folder': 'The evidence_bin_folder table stores information about folders used for organizing evidence in the system. Each row in the table represents a folder and its associated metadata. The table contains data such as the folder name, display order, creation and modification timestamps, and the ID of the parent folder if applicable. It also includes information about the work item and object type associated with the folder. This table is essential for managing and organizing evidence within the system.', 'sync_exceptions': 'The sync_exceptions table stores information about exceptions that occur during data synchronization. It captures details such as the timestamp of when the exception was inserted, the script number, and the source table and field that caused the exception. The table also includes information about the target table and field, the error number, and the rule that triggered the exception. Additionally, it records the error message, whether the exception has been cleared, the type of exception, the timestamp when it was resolved, the severity code, and the sync_exceptions_id. This table is essential for tracking and resolving data synchronization issues in the business process.', 'stnd_design_search_group': 'The stnd_design_search_group table contains data related to design search groups in the business. It includes information such as the design search group code, type, parent group code, search code, group number, title, description, effective dates, creation and modification timestamps, and user IDs. This table is important for managing and organizing design search groups within the business, allowing for easy categorization and retrieval of relevant design search information.', 'employee_review_query': "The employee_review_query table contains data related to employee reviews. It represents the queries made by employees regarding their performance reviews. The table includes information such as the employees unique identification, organization code, role code, and the date when the review assignment was made. It also includes timestamps for when the record was created and last modified, as well as the corresponding user IDs. This table is essential for tracking and managing employee review queries within the organization.", 'office_activity': 'The office_activity table contains data related to various activities performed in the office. It includes information about work items, such as their unique identifiers and issue dates, as well as details about the employees involved in the activities. The table also tracks the number of examinations and actions taken for each work item. Additionally, it captures information about partial refusals, full refusal overrides, response receipts, and on-time responses. The table further records timestamps for the creation and modification of records, along with the corresponding user IDs. Lastly, it includes flags for partial abandonments and overrides. Overall, this table provides a comprehensive view of the office activities and helps in analyzing the efficiency and productivity of the office operations.', 'tm_drawing': 'The tm_drawing table in the bronze schema of the trm_tmngpdb catalog stores information related to trademark drawings. It contains data such as the foreign key for the trademark, color information, three-dimensional representation, color claim text, lock control number, timestamps for creation and modification, and special forms filed for 3D and color drawings. This table is significant to the business as it allows for the storage and retrieval of trademark drawing data, which is essential for trademark registration and protection processes.', 'stnd_review_rating': 'The stnd_review_rating table contains data related to review ratings. It includes information such as the rating code, title, description, effective dates, and timestamps for creation and modification. This table is significant to the business as it helps in categorizing and organizing review ratings for various purposes. The data in this table represents the different rating codes, their corresponding titles and descriptions, and the time period during which they are effective. It also tracks the users who created and last modified the ratings. Overall, this table provides a comprehensive view of the review rating system used by the business.', 'section_2f_statement': "The section_2f_statement table contains data related to trademark statements filed under section 2(f) of the Trademark Act. This table represents the legal basis for claiming that a trademark has acquired distinctiveness through continuous and exclusive use in commerce. It includes information such as the trademarks global identifier, the section 2(f) claim, the basis for the claim, any limitations on the claim, and details about restrictions and lock control. The table also tracks the creation and modification timestamps and user IDs for auditing purposes.", 'tm_addl_stmnt_prior_reg': 'The tm_addl_stmnt_prior_reg table in the bronze schema of the trm_tmngpdb catalog contains data related to additional statements and prior registered trademarks. It serves as a reference for trademark information and provides details such as the foreign key for the trademark, statement type code, order number, foreign key for prior registered trademarks, lock control number, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is essential for tracking and managing trademark data within the business.', 'og_tm_review': "The og_tm_review table contains data related to reviews of publications. It serves as a record of the review process for each publication, including the reviewers employee number, organization code, and role code. The table also includes information about the publication, such as its publication date and serial number. Additionally, it tracks the status of the review and any previous bounce or lock control numbers. The table captures the creation and modification timestamps, along with the corresponding user IDs. Overall, this table provides valuable insights into the review history and status of publications within the business.", 'stnd_tm_intrstd_party_role': "The stnd_tm_intrstd_party_role table contains information about the roles of interested parties in the business. It includes data such as the role code, title, and description of the role. The table also includes timestamps for when the roles effectiveness begins and ends, as well as timestamps for when the role was created and last modified. This table is essential for tracking and managing the various roles of interested parties within the business.", 'internal_note': 'The internal_note table stores information about internal notes related to trademarks. It contains data such as the note type, subject, and location. This table is significant to the business as it allows for the tracking and management of internal notes for trademarks. The table also includes timestamps for when the notes were created and last modified, as well as information about the employees who completed the notes. Overall, this table provides valuable insights and documentation for internal communication and decision-making processes within the trademark management system.', 'stnd_ground': "The stnd_ground table represents the standard grounds for a business. It contains information about the ground code, title, description, sort order, grouping number, effective dates, ground type code, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is significant as it provides a standardized reference for different grounds used within the business operations. The data in this table helps in categorizing and organizing various grounds based on their types and attributes.", 'mv_myuspto_trm_owner': "The mv_myuspto_trm_owner table contains data related to trademark owners. It provides information about the unique identifier of the trademark, the owners ID, and the owners name. This table is significant to the business as it allows for tracking and managing trademark ownership. The data in this table represents the relationship between trademarks and their respective owners, enabling the business to identify and communicate with the owners of specific trademarks.", 'review_annotation': 'The review_annotation table contains data related to annotations made on office activity reviews. It provides information about the annotation ID, the associated review ID, the document component ID, the text segment locator, the text segment itself, the annotation count, the annotation status code, the lock control number, the creation and last modification timestamps, and the user IDs of the creator and last modifier. This table is significant to the business as it allows for tracking and analyzing annotations made on office activity reviews, providing insights into user engagement and feedback on specific review components.', 'stnd_object_dispatch_type': 'The stnd_object_dispatch_type table contains information about the different types of object dispatches. It includes details such as the dispatch type code, category, title, and description. The table also includes timestamps for when the dispatch type was created and last modified. This data is important for tracking and categorizing object dispatches within the business. The table allows for efficient management and organization of dispatch types, ensuring accurate and up-to-date information for business operations.', 'business_event': 'The business_event table contains data related to various business events within the organization. It represents the different types of events that occur in the business processes. The table captures information such as the event ID, domain code, object type code, object global ID, order number, effective timestamp, event reason ID, transaction instance global ID, FSM instance ID, proceeding number, document ID, paper in status, lock control number, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is essential for tracking and analyzing the different business events and their associated details.', 'form_paragraph_rule': 'The form_paragraph_rule table contains data related to the rules that govern the generation of paragraphs in forms. It includes information such as the rule name, type, condition, and the associated work item and document template. The table also stores details about the creation and modification timestamps, as well as the user IDs of the individuals who made the changes. Additionally, it includes a reference to the domain message ID and the call number of the form paragraph. The paragraph source type is also captured in this table. Overall, this table provides essential information for managing and customizing form paragraphs within the business application.', 'submission_signature': 'The submission_signature table stores information about the signatures associated with each submission. It contains data related to the method of signature, the signature text, the date and time of the signature, the signature image, the name and position of the signatory, and their contact number. Additionally, it includes information about the lock control number, creation timestamp, user ID of the creator, and the last modification timestamp and user ID. This table is crucial for tracking and managing signatures within the submission process.', 'sync_translate_geo': 'The sync_translate_geo table contains data related to geographic units and their corresponding codes. It provides information about legacy codes, geographic unit codes, geographic unit names, country codes, country names, and geographic type codes. This table is significant to the business as it allows for the translation and synchronization of geographic data across different systems and applications. The data in this table represents the relationships and mappings between various geographic units and their corresponding codes, enabling accurate and consistent identification of locations within the business operations.', 'stnd_assumed_name_type': "The stnd_assumed_name_type table contains data related to assumed name types. It includes information such as the type code, title, and description of each assumed name type. Additionally, it stores the effective dates for each type, indicating when they become valid or expire. The table also tracks the creation and modification timestamps, as well as the corresponding user IDs. This data is crucial for managing and categorizing assumed names within the business system.", 'stnd_fsm_type_state_rule': 'The stnd_fsm_type_state_rule table represents the rules that govern the state transitions for different types of finite state machines (FSMs) in the business. It contains information about the FSM type, the current state, the next state, and the event that triggers the transition. The table also includes descriptions and preconditions for each rule, as well as the actions to be taken when the transition occurs. The create and last modification timestamps and user IDs are also recorded for auditing purposes.', 'stnd_class_schedule': 'The stnd_class_schedule table contains information about class schedules. It includes the class schedule code, title, description, and other relevant details. The table also includes timestamps for when the schedule was created and last modified, as well as the corresponding user IDs. This table is significant to the business as it provides a centralized repository for managing and tracking class schedules, allowing for efficient scheduling and organization of classes.', 'stnd_document_component_type': 'The stnd_document_component_type table contains information about the different types of document components used in the business. Each record represents a specific document component type, including its title and description. The table also includes information about the effective dates for each document component type, indicating when it was first created and when it was last modified. Additionally, the table tracks the user IDs of the individuals who created and last modified each document component type. This table is essential for managing and categorizing document components within the business.', 'tm_divisional_child': "The tm_divisional_child table contains data related to the divisional status of trademarks. It stores information such as the parent trademarks global identifier, the sequence number, the child trademarks global identifier, the divisional status code, and the divisional status date. Additionally, it includes timestamps for when the trademark was received in the mailroom and when it was received by the unit. The table also includes control numbers for locking purposes, as well as timestamps for when the record was created and last modified, along with the corresponding user IDs. This table is essential for tracking and managing divisional trademarks within the business.", 'attorney_hold': 'The attorney_hold table stores information about work items that have been placed on hold. It includes details such as the foreign key referencing the work item ID, the date and time the item was placed on hold, the worker and user role IDs associated with the hold, the status code indicating the hold status, the organization ID associated with the hold, the category code indicating the hold category, the docket number associated with the hold, the worker and user role IDs associated with the last action on the item, the organization ID associated with the last action on the item, the lock control number, the timestamp of record creation, and the user ID of the record creator and modifier.', 'mv_myuspto_trm_search': 'The mv_myuspto_trm_search table contains data related to trademark registrations. It includes information such as the serial number, registration number, filing date, registration date, mark description, owner ID and name, attorney ID and name, whether the mark is dead or not, mark drawing code, search mark text, search owner name, search attorney name, and proceeding number list. This table is significant to the business as it provides a comprehensive view of trademark registrations and associated details, allowing for analysis and decision-making related to trademark management and protection.', 'stnd_fsm_state_legacy_state': 'The stnd_fsm_state_legacy_state table contains data related to the mapping of standardized FSM states to legacy states. It provides information on the relationship between different state types and their corresponding state numbers. The table also includes details on the office activity reason code associated with each state. Additionally, it includes timestamps for the creation and modification of records. This table is essential for understanding the historical progression and mapping of FSM states within the business system.', 'cdc_batch_job_control': 'The cdc_batch_job_control table in the bronze schema of the trm_tmngpdb catalog stores information about the control and status of Change Data Capture (CDC) batch jobs. It contains details such as the source folder, catalog name, database name, group name, and table name associated with the job. Additionally, it includes information about the source database and table names, primary keys, and whether a full load is required. The table also tracks the status of the initial load, indicating whether it has been finished or not through a boolean flag. This table is crucial for monitoring and managing CDC batch jobs in the business process.', 'stnd_class': "The stnd_class table contains information about different classes. Each row in the table represents a unique class and includes details such as the class ID, class schedule code, class number, modification number, title, description, international class short title, explanatory note, inclusions, exclusions, begin and end effective dates, creation timestamp, creation user ID, last modification timestamp, last modification user ID, and goods/services category. This table is significant to the business as it provides a comprehensive overview of all classes and their associated details.", 'ib_transaction': 'The ib_transaction table contains data related to the transaction process for incoming messages. It includes information such as the foreign key for the associated work item, the origin of the message, the timestamp when the message was sent, the status of the message when it was sent, the timestamp when the message was received, the status of the message when it was received, and the data type of the message. This table is significant for tracking and analyzing the flow and status of incoming messages, allowing for monitoring and troubleshooting of the transaction process.', 'object_document': 'The object_document table in the bronze schema of the trm_tmngpdb catalog stores information about documents associated with objects. It contains data related to the type of object, document ID, object global ID, lock control number, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is significant to the business as it allows for tracking and managing documents linked to objects, providing a comprehensive view of document-object relationships and enabling efficient document management processes.', 'doc_tmplt_ver_form_para': 'The doc_tmplt_ver_form_para table contains information about the form paragraphs associated with document templates. Each record in the table represents a form paragraph and includes details such as the foreign keys referencing the document template code and version number, the rank order number of the form paragraph, whether the paragraph is editable, the user ID that created and last modified the record, the call number associated with the form paragraph, the type of the form paragraph, and the ID of the document template version. This table is essential for managing and organizing form paragraphs within document templates.', 'stnd_office_activity_reason': 'The stnd_office_activity_reason table contains information about the reasons for office activities. It provides a standardized list of codes and descriptions for different types of office activity reasons. The table includes details such as the code, title, and description of each reason, as well as the effective dates for when the reason is valid. Additionally, it tracks the timestamps and user IDs for when the records were created or last modified. This table is essential for categorizing and understanding the various reasons behind office activities within the business.', 'stnd_publication_category': 'The stnd_publication_category table contains information about different categories of publications. It includes the category code, description, and the dates when the category is effective. The table also tracks the creation and modification timestamps, as well as the user IDs associated with those actions. This table is important for categorizing and managing publications within the business, allowing for efficient organization and retrieval of information.', 'stnd_category_doc_type': 'The stnd_category_doc_type table stores information about the relationship between document types and category types. It contains data on the foreign keys for document type and category type, as well as the effective dates for when the relationship is valid. The table also includes timestamps for when the records were created and last modified, along with the corresponding user IDs. This table is essential for managing and organizing documents based on their types and categories within the business.', 'sync_translate_og_catg': "The sync_translate_og_catg table contains data related to the translation of original categories. It provides information about the original category, publication category code, publication category description, publication sub-category code, publication sub-category description, level 1 category, and level 2 category. This table is significant to the business as it helps in mapping and translating different categories used in publications. The data in this table enables efficient categorization and organization of publications based on their original and translated categories.", 'stnd_fsm_type': "The stnd_fsm_type table contains information about different types of finite state machines (FSMs) used in the business. Each row represents a unique FSM type and includes details such as the types ID, its precedent FSM type ID, initial FSM type state ID, root FSM type ID, domain code, title, description, effective dates, creation and modification timestamps, and the category code it belongs to. This table is essential for managing and categorizing FSMs within the business.", 'sync_caselock': 'The sync_caselock table contains data related to the lock status of cases. The SERIAL_NUM column represents the unique identifier for each case. The LOCK_STATUS column indicates whether a case is locked or unlocked. The LOCK_REASON column provides information on the reason for the lock, if applicable. This table is important for tracking and managing the status of cases within the business, ensuring that only authorized users can access and modify locked cases.', 'stnd_class_statement_type': 'The stnd_class_statement_type table contains information about different types of class statements. Class statements are pre-formatted statements used in the education industry to communicate important information to students and parents. This table includes the type of statement, the pre-formatted statement text, a description of the statement, the effective dates for when the statement is valid, and timestamps for when the statement was created and last modified. This table is essential for managing and organizing class statements in the system.', 'stnd_us_intl_cls_mapping': 'The stnd_us_intl_cls_mapping table is used to store the mapping between US class IDs and international class IDs. It contains information about the effective dates of the mappings, as well as the timestamps for when the records were created and last modified. This table is important for tracking the relationship between US and international class IDs, which is crucial for trademark management and classification purposes.', 'stnd_tm_divisional_status': 'The stnd_tm_divisional_status table contains information about the divisional status of a company. It includes the divisional status code, title, description, effective dates, and details about the creation and modification of the data. This table is important for tracking and managing the divisional status of companies within the business. The data in this table provides insights into the current and historical divisional statuses of companies, allowing for analysis and decision-making based on this information.', 'stnd_fsm_category': "The stnd_fsm_category table stores information about different categories in the business field service management system. It includes the title and description of each category, as well as the dates when the category becomes effective and when it expires. The table also tracks the creation and modification timestamps, along with the corresponding user IDs. Additionally, the table includes a code that represents the categorys classification in the field service management system. This table is essential for managing and organizing the various categories within the system, allowing for efficient categorization and retrieval of relevant data.", 'sync_migration_rules': 'The sync_migration_rules table contains information about the migration rules for synchronizing data. It includes details such as the full name of the tram, the dataset it belongs to, the Cobol field name, mapping and transformation rules, data type cleansing, target table and column names, as well as the updated date. Additionally, it stores the rule number, approval/rejection status, approval/rejection date, and any comments related to the approval or rejection process. This table is crucial for managing and tracking the migration of data between systems.', 'section_2f_prior_reg': "The section_2f_prior_reg table contains information about prior registered trademarks. It includes data related to the foreign key of the trademark, the foreign key of the prior registered trademark, a lock control number, timestamps for creation and last modification, and user IDs for creation and last modification. This table is significant to the business as it helps track and manage prior registered trademarks, allowing for efficient management of intellectual property rights and legal compliance.", 'stnd_office_actvty_rsn_ct': "The stnd_office_actvty_rsn_ct table contains data related to the reasons for office activities. It includes information such as the reason code, title, and description of each activity. The table also includes timestamps for when the activitys effectiveness begins and ends, as well as timestamps for when the record was created and last modified. This table is significant to the business as it provides a standardized list of office activity reasons, allowing for consistent tracking and reporting of activities across the organization.", 'stnd_credit_tran_rsn_type': 'The stnd_credit_tran_rsn_type table contains information about the different types of credit transaction reasons. It provides a reference for the codes and descriptions associated with each reason type. The table includes details such as the title of the reason type, a description of the reason, and the effective dates for when the reason type is valid. This table is important for tracking and categorizing credit transaction reasons, allowing the business to analyze and understand the various reasons behind credit transactions.', 'sync_checkpoint': 'The sync_checkpoint table contains data related to script synchronization. It tracks the name of the script, the start timestamp, the number of commits made, the number of records committed, the timestamp of the last commit, the frequency of commits, and the end timestamp. This table provides valuable insights into the synchronization process, allowing the business to monitor script performance and track synchronization progress.', 'stnd_myuspto_event': 'The stnd_myuspto_event table contains data related to events in the US Patent and Trademark Office (USPTO). The table includes information about the event code and event description. This data is significant to the business as it provides a record of various events that occur within the USPTO, such as patent filings, trademark registrations, and legal actions. The table allows for tracking and analysis of these events, enabling the business to make informed decisions and monitor the progress of patent and trademark applications.', 'tm_additional_statement': 'The tm_additional_statement table contains data related to additional statements for trademarks. It includes information such as the foreign key for the trademark, the type of statement, the order number, and the lock control number. The table also includes timestamps for when the data was created and last modified, as well as the user IDs associated with those actions. The statement text and information about any active prior registrations are also included in this table.', 'tm_filings': 'The tm_filings table contains data related to trademark filings. It provides information on the incoming correspondence, paper correspondence received, and the last applicant response date. Additionally, it includes details on the latest submission received dates for different types of submissions. The table also includes information on the lock control number, creation and modification timestamps, and user IDs associated with the creation and modification of the records. Overall, this table is essential for tracking and managing trademark filings within the business.', 'tm_foreign_basis': 'The tm_foreign_basis table contains information about foreign trademarks and their registration details. It includes data such as the foreign trademark registration number, application number, filing date, country code, country name, registration date, expiration date, renewal effective date, renewal number, renewal expiration date, priority claimed information, lock control number, creation timestamp, creation user ID, last modification timestamp, last modification user ID, class ID, and geographic region code. This table is significant to the business as it provides a comprehensive view of foreign trademark registrations and their associated details.', 'stnd_design_search_code_item': 'The stnd_design_search_code_item table contains data related to design search code items. It includes information such as the design search group code, item number, description, effective dates, and user details for creation and modification. This table is significant to the business as it helps in organizing and categorizing design search code items, allowing for efficient searching and retrieval of relevant information. The data in this table represents the various design search code items used in the business, providing a comprehensive overview of the available options for design searches.', 'international_application': 'The international_application table contains data related to international patent applications. It includes information such as the application ID, status, filing date, payment details, and user details. This table is significant to the business as it allows tracking and managing the progress of international patent applications. The data in this table represents the various stages and details of each application, providing valuable insights for decision-making and ensuring efficient processing of international patent applications.', 'stnd_publication_subcategory': 'The stnd_publication_subcategory table contains information about the subcategories of publications. It includes the category code, subcategory code, description, and reasons for publication at different levels. The table also includes timestamps for when the data was created and last modified, as well as the user IDs associated with those actions. This table is important for categorizing and organizing publications, allowing for efficient retrieval and analysis of data related to specific subcategories.', 'stnd_pay_period': 'The stnd_pay_period table contains information about pay periods, including the period number, calendar year number, fiscal year number, fiscal quarter number, period start date, period end date, begin effective date, end effective date, create timestamp, create user ID, last modification timestamp, and last modification user ID. This table is significant to the business as it allows for tracking and managing pay periods, which is essential for payroll processing and financial reporting. The data in this table represents the various attributes and dates associated with each pay period.', 'base_application': 'The base_application table contains information about the relationship between trademarks and international applications. It serves as a reference for the unique identifiers of trademarks and international applications. Additionally, it includes details such as the lock control number, creation timestamp, user ID of the creator, last modification timestamp, and user ID of the last modifier. The creation and modification timestamps may contain personally identifiable information (PII). This table is essential for tracking and managing trademark and international application data within the business.', 'stnd_office_action_ct_state': 'The stnd_office_action_ct_state table contains data related to the state of office action categories in the business. It represents the different states that an office action category can be in. The table includes information such as the foreign key for the office action category code, the foreign key for the FSM type state ID, the timestamps for when the data was created and last modified, and the user IDs of the users who created and last modified the data. This table is important for tracking and managing the different states of office action categories in the business.', 'review_query': 'The review_query table contains data related to review queries in the business system. It represents the queries raised by users regarding specific reviews. The table includes information such as the query text, page number, and error details. It also tracks the creation and modification timestamps and user IDs. The table is essential for managing and resolving review queries efficiently, ensuring a smooth review process for the business.', 'ip_mailing_address': 'The ip_mailing_address table contains information about the mailing addresses associated with interested parties. It serves as a reference for the mailing addresses used by the business. The table includes details such as the unique identifiers for the interested party and the mailing address, as well as timestamps for when the records were created and last modified. This table is essential for maintaining accurate and up-to-date mailing address information for interested parties.', 'mailing_address_line': "The mailing_address_line table contains data related to mailing addresses. It represents the various lines of an address, such as street name, building number, and apartment number. The table includes information about the sequence of address lines, the lock control number, and the timestamps for creation and last modification. This table is significant to the business as it stores the details necessary for accurately identifying and contacting customers through their mailing addresses.", 'review_issue': "The review_issue table contains data related to review issues in the office activity. It represents the comments, issue codes, and lock control numbers associated with each review issue. The table also includes timestamps for when the review issue was created and last modified, as well as the user IDs of the individuals who performed these actions. The table provides valuable insights into the review process and helps track and manage office activity reviews efficiently.", 'stnd_gds_srvc_annotn_stat': "The stnd_gds_srvc_annotn_stat table contains information about the status of service annotations in the system. It includes details such as the status code, title, description, effective dates, and information about when the record was created or last modified. This table is important for tracking and managing the status of service annotations, allowing the business to ensure that annotations are accurate and up-to-date. The data in this table is crucial for maintaining the integrity of service annotations and ensuring that they provide accurate information to users.", 'stnd_mark_drawing_type': 'The stnd_mark_drawing_type table contains information about different types of mark drawings. It includes the type code, title, and description of each drawing type. The begin_effective_dt and end_effective_dt columns indicate the period during which a drawing type is valid. The create_ts, create_user_id, last_mod_ts, and last_mod_user_id columns track the creation and modification details of each record. This table is essential for managing and categorizing mark drawings in the business.', 'sync_migration_script': 'The sync_migration_script table contains information about migration scripts used for synchronizing data between source and target tables. It includes details such as the script number, sequence, name, source table, target table, default create and last user IDs, print only flag, script description, and commit count. This table is significant to the business as it helps track and manage the migration process, ensuring data consistency and integrity during synchronization. The commit count column indicates the number of times the script has been committed, providing insights into the execution status of each migration script.', 'draft_document': 'The draft_document table contains information about draft documents in the system. It includes the ID and name of the draft document, as well as the status of the draft document. Additionally, it stores the control number for locking purposes. The create and last modification timestamps are also recorded, although these columns contain personally identifiable information (PII). Lastly, the table captures the user IDs of the individuals who created and last modified the draft document.', 'tm_appeals': "The tm_appeals table contains data related to trademark appeals. It provides information on various stages and proceedings of trademark appeals, such as ex parte appeal decisions, concurrent use, cancellation pending TTAB proceedings, interference published, opposition pending TTAB proceedings, refusal appealed to TTAB, misplaced application requests, and oral hearing requests. The table also includes control information like the lock control number, creation timestamp, user ID of the creator, last modification timestamp, and user ID of the last modifier. This data is crucial for tracking and managing trademark appeal cases within the business.", 'draft_doc_ver_compnt_fpv': 'The draft_doc_ver_compnt_fpv table contains information about the versions and components of draft documents. It is used to track the modifications made to each document component and the associated form paragraph version. The table also includes timestamps for when the document version was created and last modified, as well as the user IDs of the individuals who made the changes. This data is important for maintaining a history of document revisions and tracking user activity in the document drafting process.', 'interested_party_assumed_nm': 'The interested_party_assumed_nm table contains data related to assumed names of interested parties. It represents the various assumed names used by interested parties in the business. The table includes information such as the ID of the assumed name, the ID of the interested party, the assumed name itself, the type of assumed name, and timestamps for creation and modification. This data is important for tracking and managing the different names used by interested parties in the business.', 'stnd_appeal_status': "The stnd_appeal_status table contains data related to the status of appeals. It includes information such as the appeal status code, title, description, effective dates, creation and modification timestamps, and user IDs. This table is significant to the business as it helps track and manage the different stages and progress of appeals. The data in this table provides insights into the current and historical appeal statuses, allowing for analysis and decision-making related to appeals management.", 'docket_item_event': 'The docket_item_event table contains information about events related to docket items. It includes the foreign key referencing the docket item ID, the foreign key referencing the event type code of the docket item, the foreign key referencing the employee number of the assignee, event dates and deadlines, a control number for locking purposes, timestamps for record creation and modification, and user IDs of the users who created and last modified the records. This table is significant for tracking and managing the progress and history of docket items within the business.', 'stnd_fee_process_type': 'The stnd_fee_process_type table contains information about different types of fee processes. It includes the fee process type code, title, and description. The table also includes the effective dates for when the fee process type is valid, as well as timestamps for when the record was created and last modified. The create_user_id and last_mod_user_id columns store the user IDs of the individuals who created and last modified the record, respectively. This table is important for managing and categorizing fee processes within the business.', 'stnd_office_actn_rule_itm': "The stnd_office_actn_rule_itm table contains data related to office action rule items in the business. It provides information on the rule name, conditions, and other attributes associated with each rule item. The table also includes timestamps for when the rule item was created and last modified. This data is essential for tracking and managing office action rules within the business processes. The table allows for effective management and customization of office action rules based on specific criteria and requirements.", 'ir_mailing_address': 'COMMENT REQUIRED', 'ir_mailing_address_group': 'The ir_mailing_address_group table stores information about international mailing address groups. It contains data related to the foreign key for the international region, address type, sequence number, lock control number, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is significant to the business as it allows for the organization and management of international mailing addresses, providing a centralized location for storing and retrieving relevant data. The table enables efficient tracking and updating of international mailing address groups, ensuring accurate and up-to-date information for business operations.', 'og_publication_tm': 'The og_publication_tm table represents the relationship between OG publications and TM publications. It contains information such as the foreign keys for OG and TM publications, record number, OG registration number, publication notice date, lock control number, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is significant to the business as it allows for tracking and managing the publications of OG and TM entities. The data in this table helps in understanding the relationship between OG and TM publications and their associated details.', 'stnd_mark_type': 'The stnd_mark_type table contains information about different types of marks. Each record represents a specific mark type and includes details such as the display order, title, and description of the mark. The begin and end effective dates indicate the period during which the mark type is valid. The create and last modification timestamps track when the record was created or last updated, along with the corresponding user IDs. This table is essential for categorizing and managing marks within the business system.', 'custom_alert': "The custom_alert table stores information about custom alerts in the business system. It contains data related to the alerts ID, title, trigger type, user control level, domain message ID, trigger schedule, recipient employee number, lock control number, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is important for tracking and managing custom alerts within the business system.", 'sync_translate_ep': 'The sync_translate_ep table contains data related to the translation of synchronization events. It represents the productivity code, productivity indicator, exam number, reason text, work item code, credit transaction reason type code, and reason category. This table is significant to the business as it provides information on the reasons for synchronization events and helps in analyzing productivity and performance. The data in this table is used for tracking and reporting purposes, enabling the business to make informed decisions and improve efficiency.', 'review_query_appeal': 'The review_query_appeal table stores data related to query appeals in the business. It represents the appeals made by users regarding certain queries. The table contains information such as the appeal ID, the ground ID of the query being appealed, the approval status of the appeal, and the control number for locking purposes. It also includes timestamps for creation and modification, as well as user IDs for tracking purposes. This table is essential for tracking and managing query appeals within the business.', 'stnd_telecom_format': 'The stnd_telecom_format table contains information about the standard telecom formats used by the business. It includes details such as the format code, title, description, country code, country name, and effective dates. This table is important for maintaining consistency in telecom formats across different countries and ensuring accurate communication. The create and last modification timestamps and user IDs provide a record of when and by whom the format information was created or modified.', 'stnd_document_type': 'The stnd_document_type table contains information about the different types of documents used in the business. It includes details such as the document type ID, the source of the document definition, the legacy document type code, and the legacy description and title of the document. The table also includes timestamps for when the document type was created and last modified, as well as the user IDs associated with those actions. This table is essential for managing and categorizing documents within the business.', 'stnd_submission_method': "The stnd_submission_method table contains information about the different methods used for submitting data. It includes details such as the submission method code, title, and description. The table also provides the effective dates for when each submission method is valid. Additionally, it tracks the creation and modification timestamps, as well as the user IDs associated with those actions. This table is essential for managing and tracking the various submission methods used within the business.", 'mv_myuspto_trm_mark': "The mv_myuspto_trm_mark table contains data related to trademarks registered with the US Patent and Trademark Office (USPTO). It includes information such as the trademarks unique identifier, serial number, registration number, filing date, registration date, textual description of the trademark, indication of whether the trademark is no longer active, drawing code associated with the trademarks visual representation, and a list of product names associated with the trademark. This table is essential for tracking and managing trademarks within the business, ensuring legal compliance, and protecting intellectual property rights.", 'stnd_office_action_category': 'The stnd_office_action_category table contains data related to the categories of office actions in the business. It provides information on the category code, title, and description of each office action category. The table also includes timestamps for when each category was created and last modified, as well as the user IDs of the individuals who performed these actions. This data is crucial for tracking and managing office actions, allowing the business to effectively categorize and analyze the different types of actions taken.', 'myuspto_trm_ph': 'The myuspto_trm_ph table contains data related to prosecution history events in the US Patent and Trademark Office (USPTO). The table includes a unique serial number for each event, the date and time of the event, and a code representing the type of event. This table is significant to the business as it provides a historical record of events that have occurred in the USPTO, allowing for analysis and tracking of patent and trademark activities. The data in this table can be used to identify trends, monitor application processing times, and assess the overall efficiency of the USPTO.', 'tm_class': 'The tm_class table contains data related to trademark classes. It includes information such as the class ID, trademark ID, class status, lock control number, creation and modification timestamps, user IDs, goods and services statement, annotated goods and services statement, first use in commerce and anywhere dates, intent to use date, and status date. This table is significant to the business as it helps in managing and organizing trademark classes, tracking the history of modifications, and providing important dates related to the trademark classes.', 'stnd_tm_group_type': 'The stnd_tm_group_type table contains information about different types of groups in the business. It includes the title and description of each group type, as well as the dates when they become effective and when they end. The table also tracks the creation and modification timestamps, along with the corresponding user IDs. This data is essential for managing and categorizing groups within the business, allowing for efficient organization and tracking of group types over time.', 'tm_filing_bases': 'The tm_filing_bases table contains information about the filing bases for trademarks. It represents the different reasons or grounds on which a trademark application is filed. This table is significant to the business as it helps in tracking and managing the various filing bases used by trademark applicants. The data in this table represents the filing bases information such as whether the trademark is filed with foreign parties, filed with foreign registration certificates, filed with use dates, foreign data entered, foreign priority claimed, and filed with specimens. The table also includes timestamps for record creation and modification, as well as user IDs for tracking purposes.', 'stnd_tm_review_status': 'The stnd_tm_review_status table contains data related to the status of reviews in the business. It includes information such as the review status code, title, description, effective dates, and timestamps for creation and modification. This table is significant for tracking the progress and history of reviews, allowing the business to monitor and manage the review process efficiently.', 'review_query_class': 'The review_query_class table stores information about the relationship between query classes and query grounds. It contains foreign key references to the class and ground tables, as well as a lock control number for concurrency control. The table also includes timestamps for when the records were created and last modified, along with the corresponding user IDs. This table is essential for tracking and managing query classes and their associated query grounds within the system.', 'stnd_writing_rvw_addl_actn': 'The stnd_writing_rvw_addl_actn table contains data related to additional actions taken during the writing review process. It includes information such as the action code, title, description, effective dates, and timestamps for creation and modification. This table is significant to the business as it provides a record of all additional actions taken, allowing for analysis and tracking of the review process. The data in this table represents the various types of additional actions that can be performed during the writing review, providing insights into the overall review workflow and any modifications made over time.', 'tm_document': 'The tm_document table stores information about documents in the system. It includes details such as the document ID, lock control number, creation timestamp, user ID of the creator, last modification timestamp, and user ID of the last modifier. This table is essential for tracking and managing documents within the business. It provides a historical record of document creation and modification, allowing for accountability and traceability. The data in this table is crucial for various business processes, such as auditing, version control, and user activity tracking.', 'sync_translate_party_type': 'The sync_translate_party_type table contains data related to party types. It provides a translation of legacy party types, milestone codes, owner type codes, and their corresponding IDs and sequence numbers. This table is significant for the business as it allows for the synchronization and translation of party type data across different systems or applications. The data in this table helps in identifying and categorizing parties based on their type, milestone, and ownership, enabling efficient management and analysis of party-related processes.', 'tm_filing_basis': 'The tm_filing_basis table in the bronze schema of the trm_tmngpdb catalog contains data related to trademark filing basis. It represents the different legal grounds or reasons for filing a trademark application. The table includes information such as the foreign key for the trademark, the filing basis code, and indicators for current, amended, and filed-in status. It also includes timestamps for record creation and modification, as well as user IDs associated with those actions. The table serves as a reference for understanding the filing basis of trademarks in the business.', 'employee_tm_class_credit': "The employee_tm_class_credit table stores information about the credits earned by employees for trademark classes. It contains data related to the employees identification, the trademark class ID, and the credit transaction ID. Additionally, it includes timestamps for when the records were created and last modified, as well as the user IDs associated with those actions. This table is essential for tracking and managing employee credits for trademark classes within the business.", 'sync_tm_com_exception': 'The sync_tm_com_exception table stores information about exceptions that occur during the synchronization process of communication services. It captures details such as the ID of the exception, the timestamp when it was inserted, the source IP address, the name of the communication service, the endpoint URL, the type of endpoint, the body of the endpoint, the HTTP error code and message, a flag indicating if a retry is needed, the timestamp when the exception was resolved, and a reference number. This table is essential for tracking and resolving any issues that arise during the synchronization process.', 'stnd_averment': 'The stnd_averment table contains information about averments, which are statements made under oath. This table includes details such as the averment ID, averment category, title, description, effective dates, creation and modification timestamps, and user IDs. The data in this table is significant to the business as it provides a record of all averments made, allowing for easy tracking and management. It is important for legal and compliance purposes, as well as for historical reference and analysis.', 'draft_document_version': 'The draft_document_version table stores information about the different versions of draft documents. It includes data such as the modification number of the draft document, the control number for locking the document, and the timestamps for when the document was created and last modified. This table is important for tracking changes made to draft documents and ensuring version control. It also includes foreign keys referencing the draft document ID, document template code, and version number for proper data relationships.', 'stnd_work_item_type_rule': 'The stnd_work_item_type_rule table contains information about the rules associated with different types of work items. It includes the rule name, type, condition, and effective dates. This table is important for managing and enforcing business rules for work items. It also tracks the creation and modification timestamps and user IDs for auditing purposes.', 'stnd_reg_stmnt_type': 'The stnd_reg_stmnt_type table contains information about the different types of regulatory statements used in the business. It includes the type code, title, and description of each statement. The table also includes the effective dates for when each statement becomes valid and when it expires. Additionally, it tracks the timestamps and user IDs for when the records are created or last modified. This table is essential for managing and categorizing regulatory statements within the business.', 'office_activity_review': 'The office_activity_review table contains data related to the reviews of office activities. It provides information on the review type, the work item associated with the review, and the timestamps for when the review was created and last modified. The table also includes user IDs for the users who created and last modified the review. The lock_control_no column represents a control number for locking purposes. This table is significant to the business as it allows for tracking and managing office activity reviews, ensuring compliance and accountability.', 'sync_translate_assumed_name': 'The sync_translate_assumed_name table  contains data related to translations. The table stores information about translated data and the corresponding conversion codes. This data is important for ensuring accurate and consistent translations across different systems or platforms. The table helps in maintaining a standardized approach to translation and enables efficient communication between different language versions of the same content. The sync_translate_assumed_name table plays a crucial role in supporting multilingual functionality and enhancing user experience.', 'employee_review_query_stat': 'The employee_review_query_stat table stores information about the status of employee review queries. It contains data related to the timestamp of the status, the ID of the employee review query, the code representing the review status, the reason for the status, the control number for locking purposes, the timestamp of creation and modification, and the user IDs of the creator and last modifier. This table is significant to the business as it allows tracking and monitoring of the progress and status of employee review queries.', 'fsm_instance': "The fsm_instance table stores information about instances of finite state machines (FSMs) in the business. Each row represents a unique FSM instance and contains data such as the parent and root FSM instance IDs, the type and current state of the FSM, the number of times it has been suspended, the depth of the FSM instance, and timestamps for creation and last modification. Additionally, there are columns for user IDs associated with the creation and last modification of the FSM instance, as well as a column indicating if the FSM instance has been terminated. This table is essential for tracking and managing FSM instances within the business.", 'stnd_legacy_transaction': 'The stnd_legacy_transaction table contains information about legacy transactions in the business. It includes details such as the transaction code, title, description, effective dates, and timestamps for creation and modification. This table is important for tracking historical transactions and their associated metadata. It provides a record of past transactions and allows for analysis and reporting on transaction history. The table is part of the bronze schema in the trm_tmngpdb catalog.', 'tm_class_filing_basis': 'The tm_class_filing_basis table in the bronze schema of the trm_tmngpdb catalog contains data related to trademark filing basis for different trademark classes. It provides information about the foreign key for the trademark, the foreign key for the class, and the filing basis code. Additionally, it includes details about the lock control number, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is significant for tracking and managing trademark filings and their associated classes and filing basis codes.', 'stnd_ground_type': 'The stnd_ground_type table stores information about different types of ground. It includes the title and description of each ground type, as well as the dates when they become effective and when they cease to be effective. The table also tracks the creation and modification timestamps, as well as the user IDs of the individuals who created and last modified the records. This table is important for categorizing and managing different types of ground in the business.', 'object_fsm_instance': 'The object_fsm_instance table contains information about the current state and activity of various objects in the business. It tracks the reasons for the current state, the type of object, and the unique identifiers for each object. Additionally, it records the current and last actions performed on the objects, as well as any active exparte appeals. The table also includes information about the number of examinations, extensions, and renewals associated with each object. Lastly, it keeps track of the lock control number and the timestamps and user IDs for when the records were created or last modified.', 'sync_authuser': 'The sync_authuser table stores user authentication information. It contains data related to user IDs, passwords, roles, creation dates, and last update timestamps. This table is crucial for managing user access and permissions within the business system. It enables secure authentication and authorization processes, ensuring that only authorized users can access the system and perform their designated roles. The creation date and last update timestamps provide valuable information for auditing and tracking user activity. Overall, the sync_authuser table plays a vital role in maintaining the security and integrity of the business system.', 'submission_item': 'The submission_item table contains data related to individual items within a submission. It represents the various items that are part of a submission process. The table includes information such as the unique identifier for each item, the unique identifier for the associated work item and submission, a control number for locking purposes, timestamps for creation and last modification, and user IDs for the creation and last modification. This table is essential for tracking and managing the different items within a submission and their associated details.', 'tm_employee_assignment': "The tm_employee_assignment table stores information about the assignments of employees to trademarks. It contains data related to the employees role, employee number, and the trademark they are assigned to. The table also includes timestamps for when the assignment was created and last modified, as well as the user IDs associated with those actions. Additionally, there is an effective date column that indicates when the assignment becomes active. This table is crucial for tracking and managing employee assignments within the business.", 'sync_log': 'The sync_log table stores information about synchronization actions performed by users. It contains records of when the synchronization action was created, the type of action performed, the user ID associated with the action, and any additional notes related to the synchronization. This table is crucial for tracking and auditing synchronization activities within the business system.', 'stnd_work_item_request': 'The stnd_work_item_request table contains data related to work item requests in the business. It includes information such as the request code, title, description, business unit code, effective dates, creation and modification timestamps, and user IDs. This table is significant as it stores the details of work item requests, allowing for tracking and management of these requests within the organization. The data in this table represents the various work item requests made by users and provides a historical record of their creation and modification.', 'stnd_docket_fsm_type_state': 'The stnd_docket_fsm_type_state table contains information about the different states of a docket in the business process. It tracks the start and end effective dates of each state, as well as the timestamps of when the records were created and last modified. The table also includes the IDs of the docket and the FSM type associated with each state. This data is important for analyzing the progression of dockets through different states and for tracking any changes made to the states over time.', 'submission_elctrn_addr': 'The submission_elctrn_addr table stores information about electronic addresses associated with submissions. It represents the relationship between a submission and an electronic address. The table contains data related to the foreign keys of the submission and electronic address, as well as information about the primary electronic address and lock control number. The timestamps and user IDs indicate when the record was created and last modified. This table is essential for tracking and managing electronic addresses for submissions in the business.', 'stnd_evidence_bin': "The stnd_evidence_bin table contains information about evidence bins. Each row represents a specific evidence bin and includes details such as the evidence bin code, title, description, effective dates, creation and modification timestamps, and user IDs. This table is significant to the business as it allows for the management and tracking of evidence bins, providing a centralized repository for storing and retrieving information related to evidence bins. The data in this table is crucial for various business processes, including evidence management, analysis, and reporting.", 'stnd_office_action_rule': "The stnd_office_action_rule table contains data related to office action rules. It represents the various types of work item and office action categories, along with their corresponding typical categories. The table also includes information about the creation and modification timestamps, as well as the user IDs of the individuals who performed those actions. This data is crucial for tracking and analyzing the office action process, allowing the business to identify patterns, optimize workflows, and improve efficiency.", 'tm_design_element': 'The tm_design_element table in the bronze schema of the trm_tmngpdb catalog contains data related to design elements for trademarks. It represents the relationship between trademark GIDs, design search group codes, lock control numbers, creation timestamps, creation user IDs, last modification timestamps, and last modification user IDs. This table is significant to the business as it allows for the tracking and management of design elements associated with trademarks. The data in this table provides valuable insights into the design aspects of trademarks and helps in maintaining the integrity and accuracy of trademark records.', 'stnd_docket': "The stnd_docket table contains information about dockets. Each row represents a docket and includes details such as the user role code, docket ID, docket code, title, description, begin effective date, end effective date, creation timestamp, creating user ID, last modification timestamp, and last modifying user ID. This table is significant to the business as it allows for the management and tracking of dockets, providing a centralized repository for relevant information. The data in this table can be used for various purposes such as analyzing docket trends, monitoring changes, and ensuring accurate and up-to-date information is available for users.", 'stnd_coordinated_class': 'The stnd_coordinated_class table contains information about coordinated classes. It includes the IDs of the class and the coordinated class, as well as the effective dates for when the coordination begins and ends. The table also includes timestamps for when the records were created and last modified, along with the corresponding user IDs. This table is important for tracking and managing coordinated classes within the business.', 'international_registration': 'The international_registration table stores information related to international registrations. It contains data that represents the unique identifiers, control numbers, and timestamps associated with each registration. This table is significant to the business as it allows for tracking and management of international registrations. The data in this table is used to monitor the creation and modification of registrations, providing valuable insights for decision-making and compliance purposes.', 'sync_stnd_am_stat': 'The sync_stnd_am_stat table represents the standardized status of assets. It contains information about the status of various assets, such as their current state and control numbers. This table is crucial for tracking and managing assets within the business, providing a centralized view of their statuses. The am_stat column represents the numerical code for the asset status, while the description column provides a brief explanation of the status. The control_num column stores the control number associated with each asset, and the tram_state column indicates the state of the asset in the TRAM system. Overall, this table plays a vital role in asset management and facilitates efficient decision-making processes.', 'mv_myuspto_trm_at': 'The mv_myuspto_trm_at table contains data related to trademarks. It provides information about trademark global IDs, as well as associated IDs and names. This table is significant to the business as it allows for tracking and managing trademarks, providing a centralized repository of trademark data. The data in this table represents the various trademarks owned or managed by the business, enabling efficient trademark management and analysis.', 'stnd_telecom_type': 'The stnd_telecom_type table contains information about different types of telecommunication services offered by the business. It includes details such as the title and description of each telecom type, as well as the effective dates for when the telecom type is valid. The table also tracks the creation and modification timestamps, along with the corresponding user IDs. This data is essential for managing and categorizing telecom services, enabling the business to effectively track and update their offerings over time.', 'stnd_legal_entity_type': 'The stnd_legal_entity_type table contains information about different types of legal entities. It includes the legal entity type code, title, and description. The table also includes the legal entity category, which represents the category to which the legal entity belongs. The begin_effective_dt and end_effective_dt columns indicate the period during which the legal entity type is valid. The create_ts, create_user_id, last_mod_ts, and last_mod_user_id columns track the creation and modification timestamps and user IDs respectively.', 'sync_tranlog': 'The sync_tranlog table contains data related to transaction logs for synchronization. It includes information such as the date, timer, serial number, state, and timestamp of each transaction. This table is significant to the business as it helps track and monitor the synchronization process, ensuring data consistency and integrity across systems. The data in this table is crucial for troubleshooting synchronization issues and analyzing the overall performance of the synchronization process.', 'intrstd_party_relationship': 'The intrstd_party_relationship table represents the relationships between interested parties and members. It contains information about the type of relationship, as well as timestamps for when the relationship was created and last modified. The table also includes a lock control number for data integrity purposes. This table is significant to the business as it allows for tracking and managing the relationships between interested parties and members, providing valuable insights for decision-making and relationship management.', 'stnd_owner_type': 'The stnd_owner_type table contains information about different types of owners in the business. It includes details such as the owner type ID, owner type code, title, and description. The table also includes timestamps for when the owner type was created and last modified, as well as the corresponding user IDs. This table is important for categorizing and managing different types of owners within the business system.', 'related_worker': "The related_worker table stores information about the relationships between workers in the business. It represents the connections between workers, such as supervisors and subordinates. The table contains data that identifies the related workers, their relationship type, and the effective time period of the relationship. This information is crucial for understanding the hierarchy and reporting structure within the organization, as well as for tracking changes in worker relationships over time.", 'stnd_tm_class_status': 'The stnd_tm_class_status table contains information about the status of trademark classes. It includes the title and description of each class status, as well as the effective dates for when the status is valid. The table also tracks the creation and modification timestamps, as well as the user IDs of the individuals who made the changes. This data is important for managing and tracking the different statuses assigned to trademark classes, providing a historical record of changes made over time.', 'tm_electronic_addr': 'The tm_electronic_addr table stores information about electronic addresses associated with party roles. It includes data such as the foreign key to the party role, the authorized email address, and whether the address is marked as primary. The table also includes timestamps for when the record was created and last modified, as well as the user IDs of the individuals who made those changes. The table serves as a central repository for managing electronic addresses for party roles within the business.', 'stnd_docket_item_event_type': 'The stnd_docket_item_event_type table contains information about different types of events related to docket items. Each row represents a specific event type, including its code, title, and description. The table also includes the effective dates for when each event type begins and ends, as well as timestamps for when the records were created and last modified. This table is essential for categorizing and tracking various events that occur within the docket items, providing valuable insights into the lifecycle and history of each item.', 'query_appeal_note': 'The query_appeal_note table stores notes related to appeals made by employees. Each note is associated with a specific appeal and contains information about the note sequence number, the content of the note, and the control number for locking purposes. The table also includes timestamps for when the note was created and last modified, as well as the user IDs of the individuals who performed these actions. This table is essential for tracking and managing the communication and updates related to employee appeals.', 'international_appl_reg': "The international_appl_reg table contains data related to international application registrations. It stores information about the unique identifiers for international registrations, their status, lock control number, creation and modification timestamps, as well as important dates such as renewal and publication dates. This table is significant to the business as it allows tracking and management of international application registrations, providing a centralized repository for important details related to these registrations.", 'tm_class_reference': 'The tm_class_reference table stores information about the relationship between trademarks and classes. It represents the mapping of trademarks to their corresponding classes and referenced classes. The table also includes timestamps for when the records were created and last modified, as well as user IDs for the users who performed these actions. The lock_control_no column is used for locking purposes. This table is essential for managing and tracking the classification of trademarks within the business.', 'stnd_gds_srvc_match_stat': 'The stnd_gds_srvc_match_stat table contains information about the status of service matches in the system. It includes details such as the status code, title, description, effective dates, and user information for creation and modification. This table is important for tracking and managing the status of service matches in the business, allowing for efficient monitoring and updating of match statuses.', 'stnd_object_type': 'The stnd_object_type table stores information about different types of objects in the business. It includes details such as the object type code, title, description, and the dates when the object type is effective. The table also tracks the creation and modification timestamps, as well as the user IDs associated with those actions. Additionally, it stores a global identifier prefix and an object type ID. The table name indicates that it is a standard object type table, suggesting that it serves as a reference for categorizing various objects within the business.', 'stnd_note_type': 'The stnd_note_type table contains information about different types of notes. Each note type is identified by a unique code and has a title and description associated with it. The table also includes the effective dates for when each note type is valid, as well as timestamps for when each record was created and last modified. This table is important for categorizing and organizing various types of notes within the business system.', 'stnd_mad_transaction_type': 'The stnd_mad_transaction_type table contains information about different types of transactions in the business. It includes the transaction type code, title, and description. The table also includes the effective dates for when the transaction type is valid, as well as timestamps for when the records were created and last modified. The create_user_id and last_mod_user_id columns store the user IDs of the individuals who created and last modified the records respectively. This table is important for categorizing and managing various transactions within the business.', 'cdc_batch_job_history': 'The cdc_batch_job_history table contains information about the Change Data Capture (CDC) files used in batch jobs. It includes the file path of the CDC file, the timestamp of when the metadata was sourced, the date of the CDC file, and the processing time. The processing time column contains personally identifiable information (PII) and provides details about the entities, scores, sample size, and hit rate related to the processing time. This table is essential for tracking and analyzing the history of CDC files used in batch jobs.', 'sync_translate_emp_lo': 'The sync_translate_emp_lo table contains data related to employee numbers and their corresponding employee locations. This table is used for translation purposes, allowing the system to map employee numbers to their respective locations. The empe_num column represents the unique identifier for each employee, while the empe_lo column represents the location of the employee. The data in this table is crucial for various business processes that require accurate mapping of employee numbers to their locations.', 'myuspto_trm_event_today': "The myuspto_trm_event_today table a contains data related to todays trademark events. It includes a unique identifier for each event represented by the serial_num column. This table is significant to the business as it provides real-time information about trademark events happening on the current day. The data in this table can be used for monitoring and tracking trademark activities, analyzing trends, and making informed business decisions based on the latest events in the trademark industry.", 'myuspto_trm_status_today': 'The myuspto_trm_status_today table contains data related to the current status of trademark applications. The table includes a column for the serial number of each application. This table is significant to the business as it provides up-to-date information on the progress and status of trademark applications, allowing for efficient tracking and management of the application process.', 'sync_exception_type': 'The sync_exception_type table stores information about different types of errors that occur during data synchronization. It provides insights into the nature and frequency of errors encountered, helping the business identify and address any issues in the synchronization process. The table includes columns for error text and error type, which provide further details about the specific errors encountered. This data is crucial for monitoring and improving the data synchronization process, ensuring data integrity and accuracy across systems.', 'tm_og_publications': 'The tm_og_publications table contains data related to trademark publications. It includes information about the publication dates, registration status, amendments, cancellations, certificates, orders, extracts, renewals, and republishing of trademarks. The table also includes details about the trademark descriptions and registration numbers. The LOCK_CONTROL_NO column is used for record locking purposes. The CREATE_TS, CREATE_USER_ID, and LAST_MOD_TS columns store information about the creation and modification timestamps and user IDs. This table is essential for tracking the publication and registration status of trademarks in the business.', 'tm_organization_location': 'The tm_organization_location table contains information about the physical locations associated with an organization. It includes details such as the location ID, location code, location description, and whether it is a physical or allocated location. The table also includes information about the lock control number and timestamps for when the location was created or last modified. This table is important for tracking and managing the various locations of an organization.', 'use_in_another_form': "The use_in_another_form table contains data related to the usage of trademarks in different forms. It captures information such as the unique identifier of the trademark, the class ID associated with the trademark, the type of statement made for the class, preformatted text, the month, day, and year of first use, a control number for locking purposes, timestamps for creation and modification, and the user IDs responsible for creating and modifying the data. The table also includes a statement text field that provides additional details about the usage of the trademark in another form.", 'tm_itu': 'The tm_itu table contains data related to trademark applications and their various stages and actions. It provides information on the filing of amendments to use, application marks, final and first action refusals, availability for statement of use, extensions not allowed, hold on first action refusal, informal responses received, informal letters mailed, ITU case publication for opposition, freeze period, latest ITU filing received date, denial letters mailed for statement of use extension, preparation of denial letters for last extension transaction, last possible extension date, filing of statement of use extension, completion of use affidavit processing, and issuance of notice of allowance.', 'trademark_perf': "The trademark_perf table contains data related to trademarks, including information such as the trademarks unique identifier, filing date, registration number, and mark description. It also includes details about the preferred contact method for the trademark owner, the effective filing date, and the status of the trademark. This table is significant to the business as it provides a comprehensive record of trademarks and their associated information, allowing for efficient management and analysis of trademark data.", 'tm_mailing_addr': 'The tm_mailing_addr table stores information related to mailing addresses for party roles. It contains data such as the foreign key to the party role, the primary indicator for the address, a lock control number, and timestamps for creation and last modification. This table is significant to the business as it allows for the management and tracking of mailing addresses associated with party roles.', 'tm_states': "The tm_states table contains information about the various states and statuses of trademark applications and registrations. It provides a historical record of the different stages that a trademark case can go through, such as amendments, assignments, concurrent use proceedings, opposition periods, and registration amendments. This table is essential for tracking the progress and status of trademark cases, as well as for generating reports and analytics on the overall trademark portfolio. The data in this table is used by the legal and intellectual property teams to manage and protect the companys trademarks.", 'work_item_request': 'The work_item_request table contains data related to work item requests made by employees. It includes information such as the request date, statement, description, status, and the business unit associated with the request. This table also tracks the notification status and lock control number for each request. The create and last modification timestamps and user IDs are recorded for auditing purposes. The sequence number column is used to maintain the order of the requests. Overall, this table provides a comprehensive view of work item requests and their associated details for effective management and tracking.', 'tm_party_role_owner': 'The tm_party_role_owner table contains data related to the ownership of trademarks. It provides information about the parties involved in the ownership, such as their roles and sequence numbers. Additionally, it includes details about the type of owner and any joint owners. Other relevant data in this table includes the reel number, frame number, assignment date, lock control number, and legacy assignment information. This table is essential for tracking and managing trademark ownership within the business.', 'tm_notification_message': 'The tm_notification_message table stores information about notification messages related to trademarks. It contains data that represents the communication between the system and users regarding trademark notifications. The table includes details such as the unique identifier of the trademark, the ID of the notification message, and timestamps for creation and modification. This table is essential for tracking and managing trademark notifications within the business.', 'tm_mark_type': 'The tm_mark_type table contains information about the different types of trademarks. It is used to categorize trademarks based on their type. This table is important for the business as it helps in organizing and classifying trademarks, making it easier to search and retrieve specific types of trademarks. The table includes columns for the unique identifier of the trademark, the type code of the trademark, and timestamps for creation and modification of the records.', 'trademark': "The trademark table contains data related to trademarks registered by the business. It includes information such as the trademarks unique identifier, the type of drawing associated with the trademark, the process type for fees, the serial number, the registration number, the filing date, the registry country, the standard characters used in the trademark, a description of the mark, the preferred contact method, the effective filing date, whether the trademark is collective or not, the legacy status code, the lock control number, timestamps for creation and modification, user IDs for creation and modification, and timestamps for status and last action.", 'tm_locations': 'The tm_locations table contains information about the locations of trademarks. It includes data such as the assigned examination law office, the date the case was reported lost, the charge location code, the worker number associated with the charge, and the current and physical location codes. Additionally, it includes information about the lock control number, creation and last modification timestamps, and the status of an official search in progress. This table is essential for tracking and managing the locations of trademarks within the business.', 'tm_milestone': 'The tm_milestone table contains data related to milestones for trademarks. It represents significant events or achievements in the lifecycle of a trademark. The table includes information such as the unique identifier of the trademark, the milestone code, the date of the milestone, and details about the user who created or last modified the milestone. This table is essential for tracking and managing the progress and history of trademarks within the business.', 'tm_publication_subct': 'The tm_publication_subct table contains data related to the subcategories of publications. It provides information about the subcategories of publications, such as their unique identifiers, category codes, legacy description codes, lock control numbers, creation and modification timestamps, and user IDs. This table is significant to the business as it allows for the categorization and organization of publications into specific subcategories, enabling efficient retrieval and analysis of publication data.', 'tm_group': "The tm_group table contains information about different groups within the business. It includes details such as the group ID, group type, owner employee number, group name, and description. The table also includes information about the lock control number, creation and modification timestamps, and user IDs. This table is important for managing and organizing groups within the business, allowing for efficient collaboration and communication among employees.", 'writing_review': "The writing_review table contains data related to the reviews of written content. It includes information such as the reviewers employee number, the rating of the review, the number of performance procedure errors, and the number of substantive errors. The table also includes details about any corrections made, comments provided by the reviewer, and the completion date of the review. Additionally, it tracks the creation and modification timestamps and user IDs for auditing purposes. This table is essential for monitoring and improving the quality of written content within the business.", 'tm_post_registration': 'The tm_post_registration table contains data related to post-registration activities for trademarks. It includes information such as the date of the latest correspondence received, whether a post-registration principal or supplemental registration has been filed, the status of section 8 filings and acceptances, section 15 filings and acknowledgments, section 71 filings and acceptances, the lock control number, and details about the cancellation reason code, renewal filings, and post-registration audits. This table is essential for tracking and managing post-registration activities for trademarks within the business.', 'tm_physical_location': 'The tm_physical_location table stores information about the physical locations associated with trademarks. It contains data such as the unique identifier for the trademark, the date and time of the physical location, a code representing the type of physical location, a control number for locking purposes, and timestamps for record creation and modification. This table is important for tracking and managing the physical locations of trademarks within the business.', 'tm_prior_registration': 'The tm_prior_registration table stores information about prior trademark registrations. It is used to track the relationships between different trademarks and their corresponding prior registrations. The table includes data such as the unique identifiers for the trademarks and prior registrations, control numbers for locking records, timestamps for record creation and modification, and user IDs for the users who created and last modified the records. This table is essential for managing and analyzing the history and relationships of trademark registrations within the business.', 'work_item_object': 'The work_item_object table stores information about the relationship between work items and objects in the system. It represents the connection between a work item and an object type. The table contains data related to the unique identifiers of the work item and object, as well as timestamps for when the record was created and last modified. This table is important for tracking the associations between work items and objects, allowing for efficient retrieval and management of these relationships within the business system.', 'tm_itu_extension': 'The tm_itu_extension table stores information related to trademark extensions. It contains data such as the foreign key to the trademark, the extension number, the expiration date of the extension, the lock control number, and timestamps for creation and last modification. This table is significant to the business as it allows tracking and managing trademark extensions, ensuring compliance with expiration dates and providing a history of modifications made to the extensions.', 'tm_registration_statement': 'The tm_registration_statement table contains data related to trademark registration statements. It includes information such as the foreign key for the trademark, the type of registration statement, the sequence number, and the date and time of creation and modification. The table also includes a lock control number for data integrity purposes. The statement text provides additional details about the registration statement. This table is significant to the business as it allows for tracking and managing trademark registration statements for legal and administrative purposes.', 'worker_folder': 'The worker_folder table stores information about folders that are associated with workers. Each folder is identified by a unique ID and is linked to a specific worker through a foreign key. The table also tracks the parent folder of each folder, allowing for hierarchical organization. Other attributes include the name of the folder, a lock control number for concurrency control, timestamps for creation and modification, user IDs for the creators and modifiers, and a display order number for sorting purposes. This table is essential for managing and organizing worker-related data within the business.', 'tm_pseudo_class': 'The tm_pseudo_class table contains data related to pseudo classes in the trademark management system. Pseudo classes are used to categorize trademarks based on their characteristics or attributes. This table stores information such as the pseudo class ID, the trademark global ID, the class ID, the service phrase, the lock control number, and timestamps for creation and last modification. The data in this table is crucial for managing and organizing trademarks based on their pseudo classes, allowing for efficient search and retrieval of relevant trademarks.', 'tm_renewal': 'The tm_renewal table contains data related to the renewal of trademarks. It includes information such as the unique identifier of the trademark, the sequence number, the date the renewal was filed, the effective dates of the renewal, the lock control number, and details about when the record was created and last modified. This table is significant to the business as it allows for tracking and managing the renewal process of trademarks, ensuring that they are properly maintained and protected.', 'transaction_instance': 'The transaction_instance table contains information about individual transaction instances. It represents the history of transactions conducted by employees. Each transaction instance is identified by a unique transaction instance ID. The table includes details such as the legacy transaction code, employee number, effective timestamp, details of the transaction, termination status, origin location, creation timestamp, and last modification timestamp. This table is essential for tracking and analyzing employee transactions, ensuring data integrity, and facilitating auditing and reporting processes.', 'user_para_form_para_ver': "The user_para_form_para_ver table stores information related to form paragraph versions. It contains data that represents the relationships between document components, form paragraph versions, and lock control numbers. The table also includes timestamps for when the records were created and last modified, as well as the user IDs associated with those actions. This data is significant to the business as it allows for tracking and managing the different versions of form paragraphs used in documents.", 'tm_publication': "The tm_publication table contains information about trademark publications. It includes data such as the trademarks unique identifier, the publications unique identifier, the date of the action taken on the publication, the legacy status code of the publication, a control number for locking purposes, timestamps for creation and modification, and the user IDs of the individuals who created and last modified the publication. Additionally, it includes a field for the description of the printed mark. This table is significant to the business as it allows for tracking and managing trademark publications throughout their lifecycle.", 'tmcom_batch_ingest_control': 'The tmcom_batch_ingest_control table is used to track the ingestion process of batches in the system. It contains information such as the serial number of the batch, the name of the batch, the target endpoint where the data is being ingested, the type of endpoint, any error codes or messages related to the ingestion, the timestamp when the ingestion process was completed, the status count of the batch, the user ID who created the batch, the timestamp when the batch was created, the user ID who last modified the batch, the timestamp of the last modification, and the batch date number. This table is essential for monitoring and managing the batch ingestion process in the business.', 'tm_office_actions': 'The tm_office_actions table contains data related to office actions in the trademark application process. It provides information on various actions taken by examiners and paralegals, such as first action mailed, first action publication, final refusal, and examining attorney display count. The table also includes timestamps for important dates, such as the first examiner action counted date and the last examiner action date. Additionally, it includes counts of paralegal and examiner actions, as well as a lock control number for data integrity. This table is essential for tracking the progress and status of trademark applications.', 'work_item_request_employee': "The work_item_request_employee table in the bronze schema of the trm_tmngpdb catalog contains data related to employee requests for work items. It stores information such as the unique identifier of the work item, the sequence number, the employee number of the receiver, and the email address of the receiver. Additionally, it includes details about the lock control number, the timestamps of creation and last modification, and the user IDs of the creator and last modifier. This table is significant to the business as it allows tracking and management of work item requests made by employees.", 'work_item_relationship': 'The work_item_relationship table stores the relationships between parent and child work items. It represents the hierarchical structure of work items within the business. The table contains information about the parent work item, child work item, and the type of relationship between them. It also includes timestamps for when the relationship was created and last modified, as well as the user IDs of the individuals who made those changes. This table is essential for tracking and managing the dependencies and relationships between work items in the business processes.', 'worker': "The worker table contains information about the workers in the business. It includes details such as the workers unique identifier, worker number, grade code, signatory authority count, BRS user ID, lock control number, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is significant as it provides a comprehensive record of all workers and their associated details, allowing for efficient management and tracking of worker information within the business.", 'tmapplser': 'The tmapplser table contains information about the actions associated with loads. It includes the action code, serial number of the load, date the row was pulled from source tables, table name from which the row was pulled, timestamp of when the record was created in the database, user identifier of the user who initiated the insert, timestamp of the last modification to the record, user identifier of the user who initiated the last modification, and a control number used for optimistic locking. This table is important for tracking and managing load actions in the business.', 'worker_folder_item': 'The worker_folder_item table contains information about the items associated with worker folders. It stores the object ID of the item, the folder ID it belongs to, the type of object it represents, its name, a lock control number, timestamps for creation and last modification, user IDs for creation and last modification, and a display order number. This table is crucial for tracking and managing the items within worker folders, allowing for efficient organization and retrieval of information.', 'tm_gds_srvc_term_filg_basis': 'The tm_gds_srvc_term_filg_basis table contains data related to the filing basis for goods and services terms in trademarks. It provides information on the foreign key for the trademark, the class ID, the sequence number for the goods and services term, the lock control number, the timestamp of creation and last modification, and the user IDs for creation and last modification. Additionally, it includes the foreign key for the filing basis code. This table is significant to the business as it allows for tracking and managing the filing basis for goods and services terms in trademarks.', 'tm_proceeding': "The tm_proceeding table contains data related to trademark proceedings. It represents the various legal proceedings associated with trademarks. The table includes information such as the proceeding ID, trademark global ID, proceeding number, lock control number, creation and modification timestamps, and user IDs. This data is crucial for tracking and managing trademark proceedings within the business.", 'user_session': "The user_session table contains data related to user sessions in the business application. It represents the activity and status of each user session, including the employee number associated with the session, the serssions unique identifier, and the current status. The table also includes timestamps for when the session was created and last modified, as well as the corresponding user IDs. This data is important for tracking user activity, session management, and auditing purposes.", 'tm_relationship': 'The tm_relationship table stores information about the relationships between trademarks. It includes the parent trademark, the related trademark, and the type of relationship between them. This table also tracks the creation and modification timestamps, as well as the user IDs of the individuals who made those changes. The lock_control_no column is used for concurrency control. This table is essential for understanding the connections and associations between different trademarks in the business.', 'tm_literal': 'The tm_literal table contains data related to trademark literals. It stores information such as the foreign key of the trademark, sequence number, lock control number, literal element, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is significant to the business as it allows for the management and tracking of trademark literals associated with trademarks. The data in this table represents the various literal elements used in trademarks and their corresponding details.', 'tm_party_role': 'The tm_party_role table contains information about the roles that parties play in relation to trademarks. It includes data such as the party role ID, the sequence number of the role, and the membership details of the party in the bar association. This table is significant to the business as it helps in managing and tracking the roles and memberships of parties involved in trademark-related activities. The table also stores information about the creation and modification timestamps and user IDs for auditing purposes.', 'work_item': 'The work_item table contains information about various work items in the business. It represents the different types of work items and their associated details. The table includes data such as the unique identifier for each work item, the type of work item, a control number for locking purposes, timestamps for creation and last modification, and the user IDs of the individuals who created and last modified the work items. This table is essential for tracking and managing work items within the business.', 'tm_group_item': 'The tm_group_item table represents the relationship between trademark groups and individual trademarks. It stores the foreign key references to the trademark group and trademark tables, as well as the lock control number for concurrency control. The create and last modification timestamps and user IDs are also recorded for auditing purposes. This table is essential for managing and organizing trademarks within groups, allowing for efficient retrieval and tracking of trademark data.', 'tm_telecom_addr': 'The tm_telecom_addr table stores information about telecom addresses associated with party roles. It contains data related to the telecom address ID, the party role ID, and whether the telecom address is primary or not. The table also includes information about the creation and modification timestamps, as well as the user IDs responsible for those actions. This table is important for tracking and managing telecom addresses for different party roles within the business.', 'tm_pseudo_mark': 'The tm_pseudo_mark table in the bronze schema of the trm_tmngpdb catalog contains data related to pseudo trademarks. Pseudo trademarks are alternative marks that may be used in place of actual trademarks. This table stores information such as the foreign key to the trademark, the sequence number, the pseudo mark text, the lock control number, and timestamps for creation and modification. The table is important for tracking and managing pseudo trademarks within the business.', 'trigger_exceptions': "The trigger_exceptions table stores information about errors that occur during trigger execution. It captures the timestamp of when the error occurred, the error number, the error message, the backtrace, and the callstack. This table is essential for troubleshooting and identifying issues with triggers in the system. It provides valuable insights into the nature of errors and helps in improving the overall reliability and performance of the system.", 'evidence_bin_folder_h': '[HISTORICAL] The evidence_bin_folder table stores information about folders used for organizing evidence in the system. Each row in the table represents a folder and its associated metadata. The table contains data such as the folder name, display order, creation and modification timestamps, and the ID of the parent folder if applicable. It also includes information about the work item and object type associated with the folder. This table is essential for managing and organizing evidence within the system.', 'tm_filing_basis_h': '[HISTORICAL] The tm_filing_basis table in the bronze schema of the trm_tmngpdb catalog contains data related to trademark filing basis. It represents the different legal grounds or reasons for filing a trademark application. The table includes information such as the foreign key for the trademark, the filing basis code, and indicators for current, amended, and filed-in status. It also includes timestamps for record creation and modification, as well as user IDs associated with those actions. The table serves as a reference for understanding the filing basis of trademarks in the business.', 'international_application_h': '[HISTORICAL] The international_application table contains data related to international patent applications. It includes information such as the application ID, status, filing date, payment details, and user details. This table is significant to the business as it allows tracking and managing the progress of international patent applications. The data in this table represents the various stages and details of each application, providing valuable insights for decision-making and ensuring efficient processing of international patent applications.', 'mailing_address_line_h': "[HISTORICAL] The mailing_address_line table contains data related to mailing addresses. It represents the various lines of an address, such as street name, building number, and apartment number. The table includes information about the sequence of address lines, the lock control number, and the timestamps for creation and last modification. This table is significant to the business as it stores the details necessary for accurately identifying and contacting customers through their mailing addresses.", 'prcdng_employee_assignment_h': '[HISTORICAL] The prcdng_employee_assignment table contains information about the assignments of employees to proceedings. It tracks the unique identifier of the proceeding, the role code of the employee in the proceeding, and the employee number. Additionally, it includes timestamps for the effective date of the assignment, the creation and last modification of the record, as well as the user IDs associated with those actions. The lock control number is used for concurrency control. This table is essential for managing and tracking employee assignments to proceedings within the business.', 'tm_foreign_basis_h': '[HISTORICAL] The tm_foreign_basis table contains information about foreign trademarks and their registration details. It includes data such as the foreign trademark registration number, application number, filing date, country code, country name, registration date, expiration date, renewal effective date, renewal number, renewal expiration date, priority claimed information, lock control number, creation timestamp, creation user ID, last modification timestamp, last modification user ID, class ID, and geographic region code. This table is significant to the business as it provides a comprehensive view of foreign trademark registrations and their associated details.', 'office_activity_h': '[HISTORICAL] The office_activity table contains data related to various activities performed in the office. It includes information about work items, such as their unique identifiers and issue dates, as well as details about the employees involved in the activities. The table also tracks the number of examinations and actions taken for each work item. Additionally, it captures information about partial refusals, full refusal overrides, response receipts, and on-time responses. The table further records timestamps for the creation and modification of records, along with the corresponding user IDs. Lastly, it includes flags for partial abandonments and overrides. Overall, this table provides a comprehensive view of the office activities and helps in analyzing the efficiency and productivity of the office operations.', 'tm_design_element_h': '[HISTORICAL] The tm_design_element table in the bronze schema of the trm_tmngpdb catalog contains data related to design elements for trademarks. It represents the relationship between trademark GIDs, design search group codes, lock control numbers, creation timestamps, creation user IDs, last modification timestamps, and last modification user IDs. This table is significant to the business as it allows for the tracking and management of design elements associated with trademarks. The data in this table provides valuable insights into the design aspects of trademarks and helps in maintaining the integrity and accuracy of trademark records.', 'docket_item_h': '[HISTORICAL] The docket_item table contains information about individual items within a docket. It includes foreign keys referencing the unique identifiers of work items, assignees, assigning persons, objects, and organizations. The table also includes timestamps for the effective date, creation, and last modification of each record. Additionally, it includes control numbers for locking purposes and user IDs for the creators and modifiers of the records. The docket_item_id column serves as a unique identifier for each docket item.', 'submission_elctrn_addr_h': '[HISTORICAL] The submission_elctrn_addr table stores information about electronic addresses associated with submissions. It represents the relationship between a submission and an electronic address. The table contains data related to the foreign keys of the submission and electronic address, as well as information about the primary electronic address and lock control number. The timestamps and user IDs indicate when the record was created and last modified. This table is essential for tracking and managing electronic addresses for submissions in the business.', 'object_document_h': '[HISTORICAL] The object_document table in the bronze schema of the trm_tmngpdb catalog stores information about documents associated with objects. It contains data related to the type of object, document ID, object global ID, lock control number, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is significant to the business as it allows for tracking and managing documents linked to objects, providing a comprehensive view of document-object relationships and enabling efficient document management processes.', 'submission_item_h': '[HISTORICAL] The submission_item table contains data related to individual items within a submission. It represents the various items that are part of a submission process. The table includes information such as the unique identifier for each item, the unique identifier for the associated work item and submission, a control number for locking purposes, timestamps for creation and last modification, and user IDs for the creation and last modification. This table is essential for tracking and managing the different items within a submission and their associated details.', 'telecom_address_h': '[HISTORICAL] The telecom_address table stores information about telecom addresses, including the telecom number, extension number, telecom type, and telecom format. It also includes details such as the lock control number, creation timestamp, and last modification timestamp. This table is significant to the business as it allows for the management and tracking of telecom addresses for various purposes. The submitted_telecom_no column represents the telecom number that has been submitted for processing.', 'tm_divisional_child_h': "[HISTORICAL] The tm_divisional_child table contains data related to the divisional status of trademarks. It stores information such as the parent trademarks global identifier, the sequence number, the child trademarks global identifier, the divisional status code, and the divisional status date. Additionally, it includes timestamps for when the trademark was received in the mailroom and when it was received by the unit. The table also includes control numbers for locking purposes, as well as timestamps for when the record was created and last modified, along with the corresponding user IDs. This table is essential for tracking and managing divisional trademarks within the business.", 'international_reg_tm_h': '[HISTORICAL] The international_reg_tm table contains data related to international trademark registrations. It includes information such as the foreign key for the trademark, the foreign key for the international registration, the status code, status date, priority claimed date, auto protect date, notification date, cancellation date, first refusal information, lock control number, creation timestamp, creation user ID, last modification timestamp, last modification user ID, IB renewal date, and IB publication date. This table is significant to the business as it allows for tracking and managing international trademark registrations and their associated details.', 'og_publication_tm_h': '[HISTORICAL] The og_publication_tm table represents the relationship between OG publications and TM publications. It contains information such as the foreign keys for OG and TM publications, record number, OG registration number, publication notice date, lock control number, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is significant to the business as it allows for tracking and managing the publications of OG and TM entities. The data in this table helps in understanding the relationship between OG and TM publications and their associated details.', 'docket_item_event_h': '[HISTORICAL] The docket_item_event table contains information about events related to docket items. It includes the foreign key referencing the docket item ID, the foreign key referencing the event type code of the docket item, the foreign key referencing the employee number of the assignee, event dates and deadlines, a control number for locking purposes, timestamps for record creation and modification, and user IDs of the users who created and last modified the records. This table is significant for tracking and managing the progress and history of docket items within the business.', 'concurrent_use_h': '[HISTORICAL] The concurrent_use table contains data related to the concurrent use of trademarks. It includes information such as the unique identifier of a trademark, sequential number assigned to each statement, concurrent use year, month, and day numbers, category indicating the basis and status of concurrent use, number used for lock control purposes, timestamps for record creation and modification, and the statement text. This table is significant for tracking and managing concurrent use cases for trademarks in the business.', 'section_2f_prior_reg_h': "[HISTORICAL] The section_2f_prior_reg table contains information about prior registered trademarks. It includes data related to the foreign key of the trademark, the foreign key of the prior registered trademark, a lock control number, timestamps for creation and last modification, and user IDs for creation and last modification. This table is significant to the business as it helps track and manage prior registered trademarks, allowing for efficient management of intellectual property rights and legal compliance.", 'interested_party_h': "[HISTORICAL] The interested_party table contains information about individuals or entities who have expressed interest in a particular business or service. It includes details such as the type of legal entity, the statement provided by the entity, the name of the interested party, contact information, and other relevant details. This table is significant to the business as it helps in tracking and managing potential customers or partners. The data in this table represents the interested parties and their associated information, which can be used for lead generation, customer relationship management, and business development purposes.", 'evidence_document_h': "[HISTORICAL] The evidence_document table stores information about evidence documents related to a specific business process. It contains data such as the document ID, the folder ID it belongs to, the display order number, and the document alias name. The table also includes information about the documents source category code, creation and modification timestamps, and the user IDs of the creators and modifiers. This table is essential for tracking and managing evidence documents within the business process, allowing for efficient organization and retrieval of relevant information.", 'section_2f_statement_h': "[HISTORICAL] The section_2f_statement table contains data related to trademark statements filed under section 2(f) of the Trademark Act. This table represents the legal basis for claiming that a trademark has acquired distinctiveness through continuous and exclusive use in commerce. It includes information such as the trademarks global identifier, the section 2(f) claim, the basis for the claim, any limitations on the claim, and details about restrictions and lock control. The table also tracks the creation and modification timestamps and user IDs for auditing purposes.", 'international_appl_reg_h': "[HISTORICAL] The international_appl_reg table contains data related to international application registrations. It stores information about the unique identifiers for international registrations, their status, lock control number, creation and modification timestamps, as well as important dates such as renewal and publication dates. This table is significant to the business as it allows tracking and management of international application registrations, providing a centralized repository for important details related to these registrations.", 'tm_electronic_addr_h': '[HISTORICAL] The tm_electronic_addr table stores information about electronic addresses associated with party roles. It includes data such as the foreign key to the party role, the authorized email address, and whether the address is marked as primary. The table also includes timestamps for when the record was created and last modified, as well as the user IDs of the individuals who made those changes. The table serves as a central repository for managing electronic addresses for party roles within the business.', 'tm_employee_assignment_h': "[HISTORICAL] The tm_employee_assignment table stores information about the assignments of employees to trademarks. It contains data related to the employees role, employee number, and the trademark they are assigned to. The table also includes timestamps for when the assignment was created and last modified, as well as the user IDs associated with those actions. Additionally, there is an effective date column that indicates when the assignment becomes active. This table is crucial for tracking and managing employee assignments within the business.", 'tm_additional_statement_h': '[HISTORICAL] The tm_additional_statement table contains data related to additional statements for trademarks. It includes information such as the foreign key for the trademark, the type of statement, the order number, and the lock control number. The table also includes timestamps for when the data was created and last modified, as well as the user IDs associated with those actions. The statement text and information about any active prior registrations are also included in this table.', 'mailing_address_h': '[HISTORICAL] The mailing_address table contains information about the mailing addresses associated with customers. It includes details such as the name lines, street lines, city, geographic region, postal code, country, and department. The table also includes information about the address type, lock control number, and timestamps for creation and last modification. This table is significant to the business as it allows for accurate and up-to-date customer address information, which is essential for effective communication, shipping, and location-based analysis.', 'fsm_instance_h': "[HISTORICAL] The fsm_instance table stores information about instances of finite state machines (FSMs) in the business. Each row represents a unique FSM instance and contains data such as the parent and root FSM instance IDs, the type and current state of the FSM, the number of times it has been suspended, the depth of the FSM instance, and timestamps for creation and last modification. Additionally, there are columns for user IDs associated with the creation and last modification of the FSM instance, as well as a column indicating if the FSM instance has been terminated. This table is essential for tracking and managing FSM instances within the business.", 'tm_drawing_h': '[HISTORICAL] The tm_drawing table in the bronze schema of the trm_tmngpdb catalog stores information related to trademark drawings. It contains data such as the foreign key for the trademark, color information, three-dimensional representation, color claim text, lock control number, timestamps for creation and modification, and special forms filed for 3D and color drawings. This table is significant to the business as it allows for the storage and retrieval of trademark drawing data, which is essential for trademark registration and protection processes.', 'abandonment_h': '[HISTORICAL] The abandonment table contains data related to the abandonment of work items. It includes information such as the unique identifier of the work item, the abandonment date, the time taken to receive a response for the item, the code for the response issue, a text description of the response issue, and an indicator if the response was received on time or not. It also includes details about any overrides for the abandonment date, response received time, and response on-time indicator. Additionally, it includes information about the locking control number, the user who created the record, and the user who last modified the record.', 'submission_h': '[HISTORICAL] The submission table contains data related to submissions made by users. It includes information such as the submission method, form type, received date, response, filing date, status, control number, creation and modification timestamps, and user IDs. This table is significant to the business as it tracks and stores all submissions made, allowing for easy retrieval and analysis of submission data. It provides valuable insights into user behavior, form types popularity, and submission response times.', 'submission_averment_h': '[HISTORICAL] The submission_averment table  contains data related to averments made in submissions. Each row represents a specific averment made in a submission. The table includes information such as the foreign key to the submission, the sequence number of the averment, the non-standard averment text, and the lock control number. It also includes timestamps for when the averment was created and last modified, as well as the user IDs of the creators and modifiers. This table is significant to the business as it allows for tracking and management of averments made in submissions.', 'base_application_h': '[HISTORICAL] The base_application table contains information about the relationship between trademarks and international applications. It serves as a reference for the unique identifiers of trademarks and international applications. Additionally, it includes details such as the lock control number, creation timestamp, user ID of the creator, last modification timestamp, and user ID of the last modifier. The creation and modification timestamps may contain personally identifiable information (PII). This table is essential for tracking and managing trademark and international application data within the business.', 'electronic_address_h': '[HISTORICAL] The electronic_address table stores information about electronic addresses associated with individuals or entities. It includes the type of electronic address, the address locator, and the control number for locking purposes. The table also tracks the creation and modification timestamps and user IDs. This data is important for managing and communicating with customers, suppliers, and other business partners.', 'interested_party_assumed_nm_h': '[HISTORICAL] The interested_party_assumed_nm table contains data related to assumed names of interested parties. It represents the various assumed names used by interested parties in the business. The table includes information such as the ID of the assumed name, the ID of the interested party, the assumed name itself, the type of assumed name, and timestamps for creation and modification. This data is important for tracking and managing the different names used by interested parties in the business.', 'intrstd_party_relationship_h': '[HISTORICAL] The intrstd_party_relationship table represents the relationships between interested parties and members. It contains information about the type of relationship, as well as timestamps for when the relationship was created and last modified. The table also includes a lock control number for data integrity purposes. This table is significant to the business as it allows for tracking and managing the relationships between interested parties and members, providing valuable insights for decision-making and relationship management.', 'tm_addl_stmnt_prior_reg_h': '[HISTORICAL] The tm_addl_stmnt_prior_reg table in the bronze schema of the trm_tmngpdb catalog contains data related to additional statements and prior registered trademarks. It serves as a reference for trademark information and provides details such as the foreign key for the trademark, statement type code, order number, foreign key for prior registered trademarks, lock control number, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is essential for tracking and managing trademark data within the business.', 'tm_class_gds_srvc_term_h': '[HISTORICAL] The tm_class_gds_srvc_term table contains data related to the goods and services terms associated with trademarks. It includes information such as the trademark ID, class ID, sequence number, status codes, activity type code, first use dates, intent to use date, lock control number, creation and modification timestamps, user IDs, and the goods and services term text. This table is significant to the business as it helps in managing and tracking the usage of goods and services terms for trademarks.', 'international_tm_h': '[HISTORICAL] The international_tm table contains data related to international trademarks. It includes information such as the international registration number, registration date, source country, and lock control number. The table also tracks the timestamps and user IDs for when the records were created and last modified. This table is significant to the business as it allows for the management and tracking of international trademark registrations, providing valuable data for legal and intellectual property purposes.', 'tm_divisional_h': '[HISTORICAL] The tm_divisional table in the bronze schema of the trm_tmngpdb catalog stores data related to divisional trademarks. It contains information about the divisional trademarks, including their unique identifiers, sequence numbers, lock control numbers, creation timestamps, and the IDs of the users who created and last modified the records. This table is essential for tracking and managing divisional trademarks within the business.', 'tm_class_h': '[HISTORICAL] The tm_class table contains data related to trademark classes. It includes information such as the class ID, trademark ID, class status, lock control number, creation and modification timestamps, user IDs, goods and services statement, annotated goods and services statement, first use in commerce and anywhere dates, intent to use date, and status date. This table is significant to the business as it helps in managing and organizing trademark classes, tracking the history of modifications, and providing important dates related to the trademark classes.', 'international_registration_h': '[HISTORICAL] The international_registration table stores information related to international registrations. It contains data that represents the unique identifiers, control numbers, and timestamps associated with each registration. This table is significant to the business as it allows for tracking and management of international registrations. The data in this table is used to monitor the creation and modification of registrations, providing valuable insights for decision-making and compliance purposes.', 'og_publication_h': "[HISTORICAL] The og_publication table contains data related to publications. It includes information such as the publications unique identifier, publication date, lock control number, creation timestamp, ID of the user who created it, timestamp of the last modification, and ID of the user who made the last modification. This table is significant to the business as it allows for tracking and managing publications, including their creation and modification history.", 'tm_class_reference_h': '[HISTORICAL] The tm_class_reference table stores information about the relationship between trademarks and classes. It represents the mapping of trademarks to their corresponding classes and referenced classes. The table also includes timestamps for when the records were created and last modified, as well as user IDs for the users who performed these actions. The lock_control_no column is used for locking purposes. This table is essential for managing and tracking the classification of trademarks within the business.', 'office_activity_reason_h': '[HISTORICAL] The office_activity_reason table contains data related to the reasons for office activities. It provides information on the reasons behind various work items and their associated office activities. The table includes details such as the unique identifier of the work item, the reason code for the office activity, the control number for locking purposes, and timestamps for creation and modification. This table is essential for tracking and analyzing the reasons behind office activities, enabling better decision-making and process improvement within the business.', 'tm_telecom_addr_h': '[HISTORICAL] The tm_telecom_addr table stores information about telecom addresses associated with party roles. It contains data related to the telecom address ID, the party role ID, and whether the telecom address is primary or not. The table also includes information about the creation and modification timestamps, as well as the user IDs responsible for those actions. This table is important for tracking and managing telecom addresses for different party roles within the business.', 'work_item_relationship_h': '[HISTORICAL] The work_item_relationship table stores the relationships between parent and child work items. It represents the hierarchical structure of work items within the business. The table contains information about the parent work item, child work item, and the type of relationship between them. It also includes timestamps for when the relationship was created and last modified, as well as the user IDs of the individuals who made those changes. This table is essential for tracking and managing the dependencies and relationships between work items in the business processes.', 'tm_registration_statement_h': '[HISTORICAL] The tm_registration_statement table contains data related to trademark registration statements. It includes information such as the foreign key for the trademark, the type of registration statement, the sequence number, and the date and time of creation and modification. The table also includes a lock control number for data integrity purposes. The statement text provides additional details about the registration statement. This table is significant to the business as it allows for tracking and managing trademark registration statements for legal and administrative purposes.', 'use_in_another_form_h': "[HISTORICAL] The use_in_another_form table contains data related to the usage of trademarks in different forms. It captures information such as the unique identifier of the trademark, the class ID associated with the trademark, the type of statement made for the class, preformatted text, the month, day, and year of first use, a control number for locking purposes, timestamps for creation and modification, and the user IDs responsible for creating and modifying the data. The table also includes a statement text field that provides additional details about the usage of the trademark in another form.", 'tm_itu_h': '[HISTORICAL] The tm_itu table contains data related to trademark applications and their various stages and actions. It provides information on the filing of amendments to use, application marks, final and first action refusals, availability for statement of use, extensions not allowed, hold on first action refusal, informal responses received, informal letters mailed, ITU case publication for opposition, freeze period, latest ITU filing received date, denial letters mailed for statement of use extension, preparation of denial letters for last extension transaction, last possible extension date, filing of statement of use extension, completion of use affidavit processing, and issuance of notice of allowance.', 'tm_locations_h': '[HISTORICAL] The tm_locations table contains information about the locations of trademarks. It includes data such as the assigned examination law office, the date the case was reported lost, the charge location code, the worker number associated with the charge, and the current and physical location codes. Additionally, it includes information about the lock control number, creation and last modification timestamps, and the status of an official search in progress. This table is essential for tracking and managing the locations of trademarks within the business.', 'tm_renewal_h': '[HISTORICAL] The tm_renewal table contains data related to the renewal of trademarks. It includes information such as the unique identifier of the trademark, the sequence number, the date the renewal was filed, the effective dates of the renewal, the lock control number, and details about when the record was created and last modified. This table is significant to the business as it allows for tracking and managing the renewal process of trademarks, ensuring that they are properly maintained and protected.', 'tm_publication_subct_h': '[HISTORICAL] The tm_publication_subct table contains data related to the subcategories of publications. It provides information about the subcategories of publications, such as their unique identifiers, category codes, legacy description codes, lock control numbers, creation and modification timestamps, and user IDs. This table is significant to the business as it allows for the categorization and organization of publications into specific subcategories, enabling efficient retrieval and analysis of publication data.', 'work_item_object_h': '[HISTORICAL] The work_item_object table stores information about the relationship between work items and objects in the system. It represents the connection between a work item and an object type. The table contains data related to the unique identifiers of the work item and object, as well as timestamps for when the record was created and last modified. This table is important for tracking the associations between work items and objects, allowing for efficient retrieval and management of these relationships within the business system.', 'tm_mark_type_h': '[HISTORICAL] The tm_mark_type table contains information about the different types of trademarks. It is used to categorize trademarks based on their type. This table is important for the business as it helps in organizing and classifying trademarks, making it easier to search and retrieve specific types of trademarks. The table includes columns for the unique identifier of the trademark, the type code of the trademark, and timestamps for creation and modification of the records.', 'tm_milestone_h': '[HISTORICAL] The tm_milestone table contains data related to milestones for trademarks. It represents significant events or achievements in the lifecycle of a trademark. The table includes information such as the unique identifier of the trademark, the milestone code, the date of the milestone, and details about the user who created or last modified the milestone. This table is essential for tracking and managing the progress and history of trademarks within the business.', 'tm_relationship_h': '[HISTORICAL] The tm_relationship table stores information about the relationships between trademarks. It includes the parent trademark, the related trademark, and the type of relationship between them. This table also tracks the creation and modification timestamps, as well as the user IDs of the individuals who made those changes. The lock_control_no column is used for concurrency control. This table is essential for understanding the connections and associations between different trademarks in the business.', 'tm_party_role_h': '[HISTORICAL] The tm_party_role table contains information about the roles that parties play in relation to trademarks. It includes data such as the party role ID, the sequence number of the role, and the membership details of the party in the bar association. This table is significant to the business as it helps in managing and tracking the roles and memberships of parties involved in trademark-related activities. The table also stores information about the creation and modification timestamps and user IDs for auditing purposes.', 'trademark_h': "[HISTORICAL] The trademark table contains data related to trademarks registered by the business. It includes information such as the trademarks unique identifier, the type of drawing associated with the trademark, the process type for fees, the serial number, the registration number, the filing date, the registry country, the standard characters used in the trademark, a description of the mark, the preferred contact method, the effective filing date, whether the trademark is collective or not, the legacy status code, the lock control number, timestamps for creation and modification, user IDs for creation and modification, and timestamps for status and last action.", 'tm_gds_srvc_term_filg_basis_h': '[HISTORICAL] The tm_gds_srvc_term_filg_basis table contains data related to the filing basis for goods and services terms in trademarks. It provides information on the foreign key for the trademark, the class ID, the sequence number for the goods and services term, the lock control number, the timestamp of creation and last modification, and the user IDs for creation and last modification. Additionally, it includes the foreign key for the filing basis code. This table is significant to the business as it allows for tracking and managing the filing basis for goods and services terms in trademarks.', 'tm_literal_h': '[HISTORICAL] The tm_literal table contains data related to trademark literals. It stores information such as the foreign key of the trademark, sequence number, lock control number, literal element, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is significant to the business as it allows for the management and tracking of trademark literals associated with trademarks. The data in this table represents the various literal elements used in trademarks and their corresponding details.', 'tm_itu_extension_h': '[HISTORICAL] The tm_itu_extension table stores information related to trademark extensions. It contains data such as the foreign key to the trademark, the extension number, the expiration date of the extension, the lock control number, and timestamps for creation and last modification. This table is significant to the business as it allows tracking and managing trademark extensions, ensuring compliance with expiration dates and providing a history of modifications made to the extensions.', 'tm_og_publications_h': '[HISTORICAL] The tm_og_publications table contains data related to trademark publications. It includes information about the publication dates, registration status, amendments, cancellations, certificates, orders, extracts, renewals, and republishing of trademarks. The table also includes details about the trademark descriptions and registration numbers. The LOCK_CONTROL_NO column is used for record locking purposes. The CREATE_TS, CREATE_USER_ID, and LAST_MOD_TS columns store information about the creation and modification timestamps and user IDs. This table is essential for tracking the publication and registration status of trademarks in the business.', 'tm_mailing_addr_h': '[HISTORICAL] The tm_mailing_addr table stores information related to mailing addresses for party roles. It contains data such as the foreign key to the party role, the primary indicator for the address, a lock control number, and timestamps for creation and last modification. This table is significant to the business as it allows for the management and tracking of mailing addresses associated with party roles.', 'worker_h': "[HISTORICAL] The worker table contains information about the workers in the business. It includes details such as the workers unique identifier, worker number, grade code, signatory authority count, BRS user ID, lock control number, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is significant as it provides a comprehensive record of all workers and their associated details, allowing for efficient management and tracking of worker information within the business.", 'tm_prior_registration_h': '[HISTORICAL] The tm_prior_registration table stores information about prior trademark registrations. It is used to track the relationships between different trademarks and their corresponding prior registrations. The table includes data such as the unique identifiers for the trademarks and prior registrations, control numbers for locking records, timestamps for record creation and modification, and user IDs for the users who created and last modified the records. This table is essential for managing and analyzing the history and relationships of trademark registrations within the business.', 'tm_publication_h': "[HISTORICAL] The tm_publication table contains information about trademark publications. It includes data such as the trademarks unique identifier, the publications unique identifier, the date of the action taken on the publication, the legacy status code of the publication, a control number for locking purposes, timestamps for creation and modification, and the user IDs of the individuals who created and last modified the publication. Additionally, it includes a field for the description of the printed mark. This table is significant to the business as it allows for tracking and managing trademark publications throughout their lifecycle.", 'work_item_h': '[HISTORICAL] The work_item table contains information about various work items in the business. It represents the different types of work items and their associated details. The table includes data such as the unique identifier for each work item, the type of work item, a control number for locking purposes, timestamps for creation and last modification, and the user IDs of the individuals who created and last modified the work items. This table is essential for tracking and managing work items within the business.', 'tm_pseudo_mark_h': '[HISTORICAL] The tm_pseudo_mark table in the bronze schema of the trm_tmngpdb catalog contains data related to pseudo trademarks. Pseudo trademarks are alternative marks that may be used in place of actual trademarks. This table stores information such as the foreign key to the trademark, the sequence number, the pseudo mark text, the lock control number, and timestamps for creation and modification. The table is important for tracking and managing pseudo trademarks within the business.', 'tm_pseudo_class_h': '[HISTORICAL] The tm_pseudo_class table contains data related to pseudo classes in the trademark management system. Pseudo classes are used to categorize trademarks based on their characteristics or attributes. This table stores information such as the pseudo class ID, the trademark global ID, the class ID, the service phrase, the lock control number, and timestamps for creation and last modification. The data in this table is crucial for managing and organizing trademarks based on their pseudo classes, allowing for efficient search and retrieval of relevant trademarks.', 'tm_proceeding_h': "[HISTORICAL] The tm_proceeding table contains data related to trademark proceedings. It represents the various legal proceedings associated with trademarks. The table includes information such as the proceeding ID, trademark global ID, proceeding number, lock control number, creation and modification timestamps, and user IDs. This data is crucial for tracking and managing trademark proceedings within the business."}

for table_name, comment in tables_to_comment.items(): 
    alter_table_query = f"""
    ALTER TABLE {tmngpdb_catalog}.{database}.{table_name}
    SET TBLPROPERTIES ('comment' = '{comment}')
    """
    spark.sql(alter_table_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_ground_type_cd': 'Foreign key referencing the ground type code', 'lock_control_no': 'A number used for locking purposes', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'query_ground_id': 'Unique identifier for each query ground', 'fk_review_query_gid': 'Foreign key referencing the review query', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'fk_ground_cd': 'Foreign key referencing the ground code'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="query_ground",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'serial_num': 'The serial number of the event', 'event_dt': 'The date and time of the event', 'event_cd': 'The code representing the type of event'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="mv_myuspto_trm_ph",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'cfk_status_cd': 'Code indicating the status of the amendment', 'create_ts': 'The timestamp of when the record was created', 'target_element_cd': 'Code indicating the target element of the amendment', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_tm_amendment_reason_cd': 'Foreign key referencing the primary key of the amendment reason code table', 'target_element_tx': 'Text describing the target element of the amendment', 'sequence_no': 'Number indicating the order of the amendment within a group', 'last_mod_user_id': 'The user ID that last modified the record', 'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_amendment",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'sequence_no': 'Number indicating the order of the divisional trademark', 'lock_control_no': 'A number used for locking purposes', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_divisional",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'fk_parent_document_compnt_id': 'Foreign key referencing the parent document component', 'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_child_document_component_id': 'Foreign key referencing the child document component'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="document_component_reltnsp",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'received_dt': 'Timestamp indicating the date and time of submission receipt', 'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'submission_gid': 'Unique identifier for each submission', 'filing_dt': 'Timestamp indicating the date and time of submission filing', 'response_in': 'Indicates if a response has been received for the submission', 'fk_submission_form_type_id': 'Foreign key referencing the submission form type ID', 'last_mod_user_id': 'The user ID that last modified the record', 'status_ct': 'Current status of the submission', 'fk_submission_method_cd': 'Foreign key referencing the submission method code'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="submission",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'title_tx': 'Title of the response issue', 'response_issue_cd': 'Code representing the response issue', 'description_tx': 'Description of the response issue'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_response_issue",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'tm_employee_role_cd': 'Code representing the employee role', 'last_mod_user_id': 'The user ID that last modified the record', 'description_tx': 'Description of the employee role', 'create_ts': 'The timestamp of when the record was created', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'title_tx': 'Title of the employee role'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_tm_employee_asgmt_role",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_business_event_rsn_ct_cd': 'Foreign key referencing the business event reason category code', 'legacy_cm_ent_cd': 'Code representing the legacy CM entity', 'title_tx': 'Title of the business event reason', 'business_event_reason_id': 'Unique identifier for the business event reason', 'prosecution_history_in': 'Indicator for whether the event is related to prosecution history', 'alert_trigger_ct': 'Alert trigger category for the event', 'cfk_fsm_type_event_id': 'Foreign key referencing the FSM type event ID', 'business_event_reason_cd': 'Code representing the business event reason', 'description_tx': 'Description of the business event reason', 'create_ts': 'The timestamp of when the record was created', 'legacy_cm_ent_type_cd': 'Code representing the type of legacy CM entity', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'tm_milestone_in': 'Indicator for whether the event is a milestone in the timeline', 'last_mod_ts': 'The timestamp of when the record was last modified', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_business_event_reason",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_sequence_no': 'Foreign key referencing the sequence number of the evidence document', 'create_user_id': 'The user ID that created the record', 'fk_tm_document_id': 'Foreign key referencing the TM document associated with the evidence document', 'fk_evidence_source_category_cd': 'Foreign key referencing the category code of the evidence source', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'evidence_document_id': 'Unique identifier for each evidence document', 'evidence_document_alias_nm': 'Alternate name or alias for the evidence document', 'fk_evidence_bin_folder_id': 'Foreign key referencing the bin folder where the evidence document is stored', 'display_order_no': 'Number indicating the display order of the evidence document', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="evidence_document",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'document_component_tx': 'Text content of the document component', 'create_ts': 'The timestamp of when the record was created', 'document_component_metadata_tx': 'Metadata associated with the document component', 'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'document_component_id': 'Unique identifier for each document component', 'lock_control_no': 'A number used for locking purposes', 'document_component_ct': 'Content type of the document component', 'fk_document_component_type_cd': 'Foreign key referencing the document component type'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="document_component",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_work_item_type_cd': 'Foreign key referencing the work item type code', 'fk_document_template_cd': 'Foreign key referencing the document template code', 'create_user_id': 'The user ID that created the record', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_work_item_type_doc_tmplt",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'electronic_addr_locator_tx': 'The actual electronic address (e.g. email address, phone number)', 'lock_control_no': 'A number used for locking purposes', 'electronic_address_gid': 'Unique identifier for each electronic address', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'fk_electronic_addr_type_cd': 'Foreign key referencing the electronic address type', 'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="electronic_address",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'active_tm_class_count_no': 'Numeric value representing the count of active trademark classes', 'transaction_reason_tx': 'Text description of the reason for the transaction', 'create_ts': 'The timestamp of when the record was created', 'transaction_effective_dt': 'Date and time when the transaction takes effect', 'last_mod_ts': 'The timestamp of when the record was last modified', 'employee_credit_tran_id': 'Unique identifier for an employee credit transaction', 'fk_trademark_gid': 'Foreign key referencing the unique identifier of a trademark', 'fk_work_item_gid': 'Foreign key referencing the unique identifier of a work item', 'transaction_value_no': 'Numeric value associated with the transaction', 'transaction_type_ct': 'Code representing the type category of the transaction', 'transaction_reason_ct': 'Code representing the reason category of the transaction', 'cfk_approver_empe_no': 'Custom foreign key referencing the employee number of the approver', 'fk_credit_tran_rsn_type_cd': 'Foreign key referencing the code for the reason type of a credit transaction', 'lock_control_no': 'A number used for locking purposes', 'cfk_earner_empe_no': 'Custom foreign key referencing the employee number of the earner'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="employee_credit_transaction",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'PERR': 'Error flag', 'USESSION': 'User session', 'TBSIDXLRG': 'Tablespace for large indexes', 'SNAME': 'Name of the server', 'UHOST': 'Host of the user', 'DMLROLE': 'Role for data manipulation language', 'SHOST': 'Host of the server', 'USCHEMA': 'User schema', 'TBSIDX': 'Tablespace for indexes', 'DNAME': 'Name of the database', 'UOS': 'Operating system of the user', 'SOWNER': 'Owner of the server', 'DBROLE': 'Role of the database', 'TBSDATA': 'Tablespace for data', 'CSCHEMA': 'Current schema', 'PSTOP': 'Stop flag', 'ESCHEMA': 'External schema'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_runtime",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'employee_query_appeal_id': 'Unique identifier for each employee query appeal', 'cfk_employee_no': 'Unique identifier for each employee', 'lock_control_no': 'A number used for locking purposes', 'cfk_employee_role_cd': 'Code representing the role of the employee', 'create_ts': 'The timestamp of when the record was created', 'cfk_organization_cd': 'Code representing the organization of the employee', 'fk_query_appeal_gid': 'Foreign key referencing the query appeal table', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="employee_query_appeal",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'TM5_COMMON_STAT_DEFINITION_TX': 'TM5 common status definition text', 'CREATE_TS': 'The timestamp of when the record was created', 'TM5_COMMON_STATUS_CD': 'TM5 common status code', 'TM5_COMMON_STAT_DESCRIPTOR_TX': 'TM5 common status descriptor text', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'TM5_STAT_DESC': 'TM5 status description', 'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'TM5_LIVE_DEAD_CT': 'TM5 live or dead indicator', 'STATUS_NO': 'Status number', 'BEGIN_EFFECTIVE_DT': 'The timestamp of when the record began its effectiveness', 'END_EFFECTIVE_DT': 'The timestamp of when the record is no longer effective', 'DESCRIPTION_TX': 'Description text', 'TRAM_STATE': 'TRAM state'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_legacy_status",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'description_tx': 'Description of the amendment reason', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'title_tx': 'Title of the amendment reason', 'last_mod_user_id': 'The user ID that last modified the record', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'tm_amendment_reason_cd': 'Code for the amendment reason', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_tm_amendment_reason",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_withdraw_empe_cr_tran_id': 'Foreign key referencing the primary key of the employee credit transaction table for withdrawal', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_award_empe_cr_tran_id': 'Foreign key referencing the primary key of the employee credit transaction table', 'lock_control_no': 'A number used for locking purposes'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="employee_award_withdraw",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CREATE_TS': 'The timestamp of when the record was created', 'COMMENT_TX': 'Text of the comment', 'COMMENT_SOURCE_CT': 'Source of the comment', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'CFK_EMPLOYEE_NO': 'Foreign key referencing the employee number', 'ANNOTATION_COMMENT_ID': 'Unique identifier for the annotation comment', 'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'FK_REVIEW_ANNOTATION_ID': 'Foreign key referencing the review annotation ID', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'COMMENT_DT': 'Date and time of the comment'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="annotation_comment",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'TITLE_TX': 'Title of the birth record', 'END_EFFECTIVE_DT': 'The timestamp of when the record is no longer effective', 'DESCRIPTION_TX': 'Description of the birth record', 'CREATE_USER_ID': 'The user ID that created the record', 'MAD_BIRTH_REC_CT_TYPE_CD': 'Code for the type of birth record', 'BEGIN_EFFECTIVE_DT': 'The timestamp of when the record began its effectiveness', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'CREATE_TS': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_mad_birth_rec_ct_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'annotation_ct': 'String representing the annotation for the statement', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'display_order_no': 'Integer representing the display order of the statement annotation', 'create_ts': 'The timestamp of when the record was created', 'fk_trademark_gid': 'Foreign key referencing the unique identifier of a trademark', 'parse_option_ct': 'String representing the parse option chosen for the statement annotation', 'text_segment_locator_tx': 'String representing the locator of the text segment', 'fk_gds_srvc_annotn_status_cd': 'Foreign key referencing the annotation status code of the statement annotation', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_class_id': 'Foreign key referencing the unique identifier of a class', 'fk_gds_srvc_match_stat_cd': 'Foreign key referencing the match status code of the statement annotation', 'lock_control_no': 'A number used for locking purposes', 'text_segment_tx': 'String representing the text segment of the statement annotation'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="gds_srvc_stmt_annotation",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'business_unit_display_order_no': 'Display order number for the business unit', 'doc_type_ct_id': 'Unique identifier for the document type', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'description_tx': 'Description of the document type', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'doc_type_ct_cd': 'Code representing the document type', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_parent_doc_type_ct_id': 'Foreign key referencing the parent document type', 'create_ts': 'The timestamp of when the record was created', 'title_tx': 'Title of the document type', 'last_mod_ts': 'The timestamp of when the record was last modified', 'cfk_business_unit_cd': 'Code representing the business unit', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_doc_type_ct",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'filing_basis_cd': 'Code representing the filing basis', 'create_user_id': 'The user ID that created the record', 'title_tx': 'Title of the filing basis', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'description_tx': 'Description of the filing basis'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_filing_basis",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'state_start_in': 'Indicator for the state start information for the FSM type state', 'create_ts': 'The timestamp of when the record was created', 'description_tx': 'Description of the FSM type state', 'create_user_id': 'The user ID that created the record', 'start_condition_tx': 'Start condition for the FSM type state', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fsm_type_state_id': 'Unique identifier for each FSM type state', 'automated_activity_tx': 'Automated activity associated with the FSM type state', 'fk_fsm_type_id': 'Foreign key referencing the FSM type', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_root_fsm_type_id': 'Foreign key referencing the root FSM type', 'title_tx': 'Title of the FSM type state', 'human_activity_tx': 'Human activity associated with the FSM type state', 'state_end_in': 'Indicator for the state end information for the FSM type state'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_fsm_type_state",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'first_refusal_in': 'Indicator of whether a first refusal right is applicable', 'cancellation_dt': 'Timestamp indicating the date and time of cancellation', 'lock_control_no': 'A number used for locking purposes', 'notification_dt': 'Timestamp indicating the date and time of notification', 'status_cd': 'Code indicating the status of the international registration', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_international_reg_gid': 'Foreign key referencing the unique identifier of an international registration', 'create_user_id': 'The user ID that created the record', 'ib_renewal_dt': 'Timestamp indicating the date and time of renewal for an international registration', 'auto_protect_dt': 'Timestamp indicating the date and time when automatic protection was granted', 'status_dt': 'Timestamp indicating the date and time of the status update', 'ib_publication_dt': 'Timestamp indicating the date and time of publication for an international registration', 'fk_trademark_gid': 'Foreign key referencing the unique identifier of a trademark', 'priority_claimed_dt': 'Timestamp indicating the date and time when priority was claimed'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="international_reg_tm",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'statement_type_cd': 'Code representing the type of statement', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'last_mod_user_id': 'The user ID that last modified the record', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_ts': 'The timestamp of when the record was created', 'title_tx': 'Title of the statement', 'last_mod_ts': 'The timestamp of when the record was last modified', 'description_tx': 'Description of the statement'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_statement_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'individual_family_nm': 'Family name of the individual', 'country_cd': 'Country code associated with the interested party', 'party_composition_tx': 'Composition details of the interested party', 'lock_control_no': 'A number used for locking purposes', 'fk_primary_electronic_addr_gid': 'Foreign key referencing the primary electronic address of the interested party', 'fk_legal_entity_type_cd': 'Foreign key referencing the legal entity type of the interested party', 'individual_given_nm': 'Given name of the individual', 'interested_party_ct': 'Type of interested party (e.g. individual, organization)', 'interested_party_gid': 'Unique identifier for each interested party', 'individual_middle_nm': 'Middle name of the individual', 'geographic_region_nm': 'Name of the geographic region of the interested party', 'legal_entity_statement_tx': 'Statement provided by the legal entity', 'last_mod_ts': 'The timestamp of when the record was last modified', 'individual_minor_in': 'Indicates if the individual is a minor', 'individual_suffix_nm': "Suffix of the individuals name", 'country_role_ct': 'Role of the country in relation to the interested party', 'create_user_id': 'The user ID that created the record', 'geographic_region_cd': 'Code representing the geographic region of the interested party', 'country_nm': 'Name of the country associated with the interested party', 'preferred_contact_method_ct': 'Preferred method of contact for the interested party', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_primary_telecom_addr_gid': 'Foreign key referencing the primary telecom address of the interested party', 'individual_prefix_nm': "Prefix of the individuals name", 'create_ts': 'The timestamp of when the record was created', 'interested_party_nm': 'Name of the interested party'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="interested_party",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'cfk_approved_by_employee_no': 'Foreign key referencing the employee number of the approver', 'create_user_id': 'The user ID that created the record', 'director_email_sent_in': 'Indicator that an email was sent to the director regarding the appeal', 'appeal_proceeding_no': 'Number associated with the appeal proceeding', 'create_ts': 'The timestamp of when the record was created', 'fk_appeal_result_cd': 'Foreign key referencing the appeal result code', 'query_appeal_gid': 'Unique identifier for each query appeal', 'lock_control_no': 'A number used for locking purposes', 'appeal_reason_tx': 'Text describing the reason for the appeal', 'last_mod_ts': 'The timestamp of when the record was last modified', 'cfk_approval_role_cd': 'Foreign key referencing the approval role code', 'last_mod_user_id': 'The user ID that last modified the record', 'appeal_result_dt': 'Date and time of the appeal result', 'appeal_decision_tx': 'Text describing the decision made for the appeal'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="query_appeal",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'appeal_status_ts': 'Timestamp for when the appeal status was updated', 'last_mod_user_id': 'The user ID that last modified the record', 'lock_control_no': 'A number used for locking purposes', 'fk_appeal_status_cd': 'Foreign key referencing the appeal status code', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_employee_query_appeal_id': 'Foreign key referencing the employee query appeal ID'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="query_appeal_status",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_interested_party_gid': 'Foreign key referencing the unique identifier of the interested party', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'fk_electronic_address_gid': 'Foreign key referencing the unique identifier of the electronic address', 'lock_control_no': 'A number used for locking purposes', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="ip_electronic_address",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_draft_document_mod_no': 'Foreign key referencing the primary key of the draft document modification number table', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'fk_document_component_id': 'Foreign key referencing the primary key of the document component table', 'rank_order_no': 'Number indicating the rank order of the document component', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_draft_document_id': 'Foreign key referencing the primary key of the draft document table'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="draft_document_version_compnt",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'country_cd': 'Code representing the country', 'name_line_2_tx': 'Second line of the name', 'geographic_region_nm': 'Name of the geographic region', 'lock_control_no': 'A number used for locking purposes', 'geographic_region_cd': 'Code representing the geographic region', 'country_nm': 'Name of the country', 'address_type_ct': 'Type of the address', 'street_line_1_tx': 'First line of the street address', 'street_line_2_tx': 'Second line of the street address', 'last_mod_user_id': 'The user ID that last modified the record', 'mailing_address_gid': 'Unique identifier for the mailing address', 'name_line_1_tx': 'First line of the name', 'city_nm': 'Name of the city', 'last_mod_ts': 'The timestamp of when the record was last modified', 'postal_cd': 'Postal code or ZIP code', 'department_nm': 'Name of the department', 'create_ts': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="mailing_address",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'FIRST_USE_ANYWHERE_YEAR_NO': 'Number indicating the year of first use anywhere', 'FIRST_USE_IN_COMMERCE_DAY_NO': 'Number indicating the day of first use in commerce', 'FK_TRADEMARK_GID': 'Foreign key referencing the trademark global identifier', 'FIRST_USE_IN_COMMERCE_MONTH_NO': 'Number indicating the month of first use in commerce', 'INTENT_TO_USE_DT': 'Timestamp indicating the intent to use date', 'FK_STMNT_ACTVTY_TYPE_CD': 'Foreign key referencing the statement activity type code', 'FK_GDS_SRVC_STATUS_CD': 'Foreign key referencing the goods and services status code', 'SUGGESTED_GDS_SRVC_TERM_TX': 'Text describing the suggested goods and services term', 'CREATE_USER_ID': 'The user ID that created the record', 'FK_GDS_SRVC_STATUS_RSN_CD': 'Foreign key referencing the goods and services status reason code', 'FK_GOODS_SERVICES_TERM_ID': 'The foreign key referencing the ID of the goods and services term associated with the trademark.', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'SEQUENCE_NO': 'Number indicating the sequence of the record', 'GDS_SRVC_TERM_TX': 'Text describing the goods and services term', 'FIRST_USE_IN_COMMERCE_YEAR_NO': 'Number indicating the year of first use in commerce', 'CREATE_TS': 'The timestamp of when the record was created', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'FIRST_USE_ANYWHERE_MONTH_NO': 'Number indicating the month of first use anywhere', 'FIRST_USE_ANYWHERE_DAY_NO': 'Number indicating the day of first use anywhere', 'FK_CLASS_ID': 'Foreign key referencing the class identifier', 'LAST_MOD_USER_ID': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_class_gds_srvc_term",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CREATE_USER_ID': 'The user ID that created the record', 'FK_TELECOM_ADDRESS_GID': 'Foreign key referencing the telecom address table', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'FK_INTERESTED_PARTY_GID': 'Foreign key referencing the interested party table', 'CREATE_TS': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="ip_telecom_address",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'description_tx': 'Description of the work item', 'work_item_group_in': 'Group to which the work item belongs', 'office_action_frst_actn_in': 'Indicator for whether it is the first action in an office action', 'last_mod_ts': 'The timestamp of when the record was last modified', 'title_tx': 'Title of the work item', 'office_action_pst_fnl_actn_in': 'Indicator for whether it is a post-final action in an office action', 'office_activity_credit_cand_in': 'Indicator for whether it is a candidate for office activity credit', 'fk_parent_work_item_type_cd': 'Foreign key referencing the parent work item type', 'office_action_sort_order_no': 'Sort order number for office actions', 'work_item_ct': 'Category of the work item', 'office_action_during_appeal_in': 'Indicator for whether it is an office action during an appeal', 'work_item_type_cd': 'Code representing the type of work item', 'office_action_pst_frst_actn_in': 'Indicator for whether it is a post-first action in an office action', 'last_mod_user_id': 'The user ID that last modified the record', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_work_item_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'review_issue_cd': 'Code representing the review issue', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_parent_review_issue_cd': 'Foreign key referencing the parent review issue code', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'description_tx': 'Description of the review issue', 'title_tx': 'Title of the review issue', 'hierarchy_level_ct': 'Hierarchy level of the review issue', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'review_type_ct': 'Type of the review', 'type_ct': 'Type of the review issue'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_review_issue",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'fk_work_item_gid': 'Foreign key referencing the unique identifier of a work item', 'fk_office_activity_reason_cd': 'Foreign key referencing the code of an office activity reason', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="office_activity_reason",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'fk_telecom_type_cd': 'Foreign key referencing the type of telecom address', 'telecom_no': 'Telephone number for the telecom address', 'create_user_id': 'The user ID that created the record', 'telecom_address_gid': 'Unique identifier for each telecom address', 'fk_telecom_format_cd': 'Foreign key referencing the format of the telecom address', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'submitted_telecom_no': 'Telephone number submitted for the telecom address', 'lock_control_no': 'A number used for locking purposes', 'extension_no': 'Extension number for the telecom address'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="telecom_address",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'description_tx': 'Description of the work item relationship', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'title_tx': 'Title of the work item relationship', 'last_mod_user_id': 'The user ID that last modified the record', 'work_item_relationship_cd': 'Code representing the type of relationship between work items', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'create_ts': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_work_item_reltnsp_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'annotation_status_cd': 'Code representing the status of an annotation', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'review_type_ct': 'Code representing the type of review for the annotation', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'title_tx': 'Title of the annotation', 'description_tx': 'Description of the annotation'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_annotation_status",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'individual_to_individual_in': 'Indicator for whether the relationship is between two individuals', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'last_mod_user_id': 'The user ID that last modified the record', 'organization_to_org_in': 'Indicator for whether the relationship is between two organizations', 'description_tx': 'Description of the relationship', 'create_user_id': 'The user ID that created the record', 'title_tx': 'Title of the relationship', 'create_ts': 'The timestamp of when the record was created', 'intrstd_party_reltnsp_type_cd': 'Code representing the type of relationship between interested parties', 'organization_to_individual_in': 'Indicator for whether the relationship is between an organization and an individual'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_intrstd_party_rltnsp_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'title_tx': 'Title of the design search group', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'description_tx': 'Description of the design search group', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'create_ts': 'The timestamp of when the record was created', 'design_search_group_type_cd': 'Code representing the type of design search group', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_design_search_group_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'electronic_addr_type_cd': 'Code representing the type of electronic address', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'description_tx': 'Description of the electronic address', 'title_tx': 'Title of the electronic address', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_electronic_addr_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'lock_control_no': 'A number used for locking purposes', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'description_tx': 'Description of the worker', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'last_mod_ts': 'The timestamp of when the record was last modified', 'worker_relationship_cd': 'Code representing the relationship of a worker', 'title_tx': 'Title of the worker', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_worker_reltnsp_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'end_effective_dt': 'The timestamp of when the record is no longer effective', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'description_tx': 'Description of the good or service', 'title_tx': 'Title of the good or service', 'gds_srvc_status_cd': 'Code representing the status of a good or service', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_gds_srvc_status",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'title_tx': 'Title of the query review', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'description_tx': 'Description of the query review', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_user_id': 'The user ID that created the record', 'query_review_status_cd': 'Code representing the status of a query review', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_query_review_status",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'cs_status': 'The current status of the case', 'cs_uj_timer': 'The time when the case was last updated', 'cs_serial_num': 'The serial number of the case', 'cs_timestamp': 'The timestamp of when the record was created', 'cs_uj_date': 'The date when the case was last updated', 'cs_lock': 'Indicates whether the case is locked or not'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_casestatus",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'title_tx': 'Title of the appeal result', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'last_mod_user_id': 'The user ID that last modified the record', 'description_tx': 'Description of the appeal result', 'appeal_result_cd': 'Code representing the result of an appeal', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_appeal_result",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'docket_id': 'Unique identifier for the docket', 'description_tx': 'Text description of the document', 'event_cd': 'Code representing the event', 'role_cd': 'Code representing the role', 'doc_type_cd': 'Code representing the type of document', 'docket_tx': 'Text description of the docket'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_translate_petition_dockt",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'title_tx': 'Title of the party role', 'description_tx': 'Description of the party role', 'tm_cardinality_ct': 'Cardinality of the party role', 'tm_party_role_cd': 'Code representing the party role', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_tm_party_role",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'statement_tx': 'The descriptive statement text', 'concurrent_use_month_no': 'Month number indicating the concurrent use', 'lock_control_no': 'A number used for locking purposes', 'concurrent_use_day_no': 'Day number indicating the concurrent use', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'concurrent_use_basis_ct': 'Category indicating the basis of concurrent use', 'concurrent_use_year_no': 'Year number indicating the concurrent use', 'statement_no': 'Sequential number assigned to each statement', 'fk_trademark_gid': 'Foreign key referencing the unique identifier of a trademark', 'create_ts': 'The timestamp of when the record was created', 'concurrent_use_status_ct': 'Category indicating the status of concurrent use'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="concurrent_use",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'create_ts': 'The timestamp of when the record was created', 'description_tx': 'Description of the evidence source category', 'title_tx': 'Title of the evidence source category', 'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'evidence_source_category_cd': 'Code representing the category of evidence source', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_evidence_source_category",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_user_session_gid': 'Foreign key referencing the user session', 'fk_object_dispatch_type_cd': 'Foreign key referencing the object dispatch type code', 'action_current_dt': 'Timestamp indicating the current action', 'action_start_dt': 'Timestamp indicating the start of the action', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'cfk_object_gid': 'Foreign key referencing the object global ID', 'cfk_organization_cd': 'Foreign key referencing the organization code', 'fk_object_type_cd': 'Foreign key referencing the object type code', 'create_ts': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="object_dispatch",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_work_item_gid': 'Foreign key referencing the unique identifier of a work item', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_draft_document_id': 'Foreign key referencing the unique identifier of a draft document', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="office_activity_draft_document",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'search_strategy_id': 'Unique identifier for each search strategy', 'public_in': 'Indicator if the search strategy is public or not', 'last_mod_ts': 'The timestamp of when the record was last modified', 'description_tx': 'Textual description of the search strategy', 'search_strategy_nm': 'Name of the search strategy', 'cfk_employee_no': 'Employee number of the creator of the search strategy', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="search_strategy",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'cfk_employee_no': 'Foreign key referencing the employee table', 'effective_dt': 'The timestamp of when the record is effective', 'last_mod_user_id': 'The user ID that last modified the record', 'lock_control_no': 'A number used for locking purposes', 'create_ts': 'The timestamp of when the record was created', 'cfk_proceeding_gid': 'Foreign key referencing the proceeding table', 'fk_prcdng_employee_role_cd': 'Foreign key referencing the employee role code table', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="prcdng_employee_assignment",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'lock_control_no': 'A number used for locking purposes', 'fk_note_type_cd': 'Foreign key referencing the note type code', 'cfk_organization_cd': 'Foreign key referencing the organization code', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'note_sequence_no': 'Sequence number of the note', 'cfk_employee_no': 'Foreign key referencing the employee number', 'note_tx': 'Text content of the note', 'cfk_employee_role_cd': 'Foreign key referencing the employee role code', 'fk_review_query_gid': 'Foreign key referencing the review query', 'create_ts': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="review_query_note",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'relationship_type_cd': 'Code representing the type of relationship', 'title_tx': 'Title of the relationship', 'description_tx': 'Description of the relationship', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_relationship_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'NON_STANDARD_AVERMENT_TX': 'Text field for non-standard averments', 'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'SEQUENCE_NO': 'Sequential number indicating the order of the averments', 'CREATE_TS': 'The timestamp of when the record was created', 'FK_AVERMENT_ID': 'Foreign key referencing the averment table', 'FK_SUBMISSION_GID': 'Foreign key referencing the submission table', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'LAST_MOD_TS': 'The timestamp of when the record was last modified'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="submission_averment",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'event_dt': 'Timestamp indicating the date and time of the event', 'serial_num': 'Unique identifier for each record', 'status_dt': 'Timestamp indicating the date and time of the status change'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="myuspto_trm_change_ntfcn",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'last_mod_user_id': 'The user ID that last modified the record', 'title_tx': 'Title of the business event reason category', 'description_tx': 'Description of the business event reason category', 'business_event_rsn_ct_cd': 'Code for the business event reason category', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_business_event_rsn_ct",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'cfk_fp_id': 'Foreign key referencing the ID of the associated form paragraph', 'predefined_paragraph_ct': 'Content of the predefined paragraph', 'predefined_paragraph_id': 'Unique identifier for each predefined paragraph', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'cfk_employee_no': 'Foreign key referencing the employee number of the user who created the predefined paragraph', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="predefined_paragraph",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'tt_text': 'Text for translation', 'palm_short_cd': 'Short code for the palm', 'law_office_cd': 'Code representing the law office'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_translate_location",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'fk_fsm_type_id': 'Foreign key referencing the FSM type', 'fsm_type_event_id': 'Unique identifier for each FSM type event', 'create_ts': 'The timestamp of when the record was created', 'title_tx': 'Title of the FSM type event', 'last_mod_ts': 'The timestamp of when the record was last modified', 'description_tx': 'Description of the FSM type event'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_fsm_type_event",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'docket_item_id': 'Unique identifier for a docket item', 'cfk_assignee_employee_no': 'Foreign key referencing the employee number of the assignee', 'lock_control_no': 'A number used for locking purposes', 'cfk_object_gid': 'Foreign key referencing the unique identifier of an object', 'fk_docket_id': 'Foreign key referencing the unique identifier of a docket', 'cfk_organization_cd': 'Foreign key referencing the code of an organization', 'effective_dt': 'The timestamp of when the record is effective', 'fk_work_item_gid': 'Foreign key referencing the unique identifier of a work item', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'cfk_assigning_employee_no': 'Foreign key referencing the employee number of the assigning person'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="docket_item",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'dn_cms_page_count_no': 'Number indicating the total page count of the document', 'lock_control_no': 'A number used for locking purposes', 'fk_tm_document_id': 'Foreign key referencing the document ID in another table', 'create_user_id': 'The user ID that created the record', 'cfk_document_id': 'Unique identifier for the document', 'create_ts': 'The timestamp of when the record was created', 'dn_cms_document_type_tx': 'Textual representation of the document type', 'sequence_no': 'Number indicating the sequence of the document'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_document_reference",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'response_issue_tx': 'Text description of the response issue', 'fk_work_item_gid': 'Foreign key referencing the unique identifier of the work item', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'response_on_time_override_in': 'The response on-time override indicator', 'response_received_override_in': 'Override value for the response received time', 'response_on_time_in': 'Indicates if the response was received on time or not', 'lock_control_no': 'A number used for locking purposes', 'fk_response_issue_cd': 'Foreign key referencing the code for the response issue', 'abandonment_dt': 'The abandonment date', 'create_ts': 'The timestamp of when the record was created', 'abandonment_date_override_in': 'Override value for the abandonment date', 'response_received_in': 'Time taken to receive a response for the item'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="abandonment",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'publication_dt': 'Date and time of publication', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'og_publication_gid': 'Unique identifier for each publication', 'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="og_publication",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'title_tx': 'Title of the milestone', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'description_tx': 'Description of the milestone', 'tm_milestone_cd': 'Code representing a milestone', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_tm_milestone",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_instruction_doc_compnt_id': 'Foreign key referencing the instruction document component table', 'version_no': 'Version number of the predefined paragraph version', 'last_mod_ts': 'The timestamp of when the record was last modified', 'status_ct': 'Status code indicating the status of the predefined paragraph version', 'fk_predefined_paragraph_id': 'Foreign key referencing the predefined paragraph table', 'fk_original_doc_compnt_id': 'Foreign key referencing the original document component table', 'paragraph_nm': 'Name of the paragraph', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_user_id': 'The user ID that created the record', 'fk_document_component_id': 'Foreign key referencing the document component table', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'paragraph_title_tx': 'Title of the paragraph', 'dn_fp_last_modified_dt': 'Timestamp indicating the last modified date of the predefined paragraph version'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="predefined_paragraph_ver",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_user_id': 'The user ID that created the record', 'prcdng_employee_role_cd': 'Code representing the role of the preceding employee', 'last_mod_user_id': 'The user ID that last modified the record', 'description_tx': 'Description of the preceding employee', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'title_tx': 'Title of the preceding employee'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_prcdng_empe_asgmt_role",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'international_reg_dt': 'Date of international registration', 'source_ct': 'Source country', 'create_ts': 'The timestamp of when the record was created', 'international_reg_no': 'International registration number', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="international_tm",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'work_item_type_cd': 'Code representing the type of work item', 'doc_description': 'Description of the document', 'cms_doc_type': 'Code representing the type of CMS document'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_translate_work_item_cms",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'folder_nm': 'Name of the evidence bin folder', 'create_ts': 'The timestamp of when the record was created', 'fk_work_item_gid': 'Global identifier for the associated work item', 'evidence_bin_folder_id': 'Unique identifier for each evidence bin folder', 'create_user_id': 'The user ID that created the record', 'display_order_no': 'Number indicating the display order of the folder', 'cfk_object_gid': 'Global identifier for the associated object', 'last_mod_user_id': 'The user ID that last modified the record', 'dn_object_type_cd': 'Code indicating the type of the associated object', 'fk_evidence_bin_cd': 'Foreign key referencing the evidence bin code', 'fk_parent_evidence_bin_fldr_id': 'Foreign key referencing the parent evidence bin folder', 'last_mod_ts': 'The timestamp of when the record was last modified'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="evidence_bin_folder",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'source_value': 'Value of the data that was sourced', 'severity_cd': 'Code indicating the severity of the error', 'rule': 'Rule number associated with the error', 'source_table': 'Name of the table from which the data was sourced', 'error_msg': 'Error message describing the error', 'object_gid': 'Global ID of the object', 'sync_exceptions_id': 'Unique identifier for the sync exception', 'source_field': 'Name of the field/column from which the data was sourced', 'serial_num': 'Serial number associated with the record', 'script_num': 'Unique identifier for the script', 'target_field': 'Name of the field/column where the data was targeted', 'resolved_ts': 'Timestamp when the error was resolved', 'error_num': 'Error number associated with the record', 'target_table': 'Name of the table where the data was targeted', 'type_ct': 'Type of error or exception', 'insert_dt': 'Timestamp when the record was inserted', 'cleared_ind': 'Indicator to determine if the error has been cleared'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_exceptions",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'design_search_group_no': 'Number for the design search group', 'description_tx': 'Description of the design search group', 'last_mod_ts': 'The timestamp of when the record was last modified', 'design_search_group_cd': 'Code for the design search group', 'fk_design_search_group_type_cd': 'Foreign key for the design search group type', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'design_search_code_in': 'Code for the design search', 'last_mod_user_id': 'The user ID that last modified the record', 'title_tx': 'Title of the design search group', 'fk_parent_design_search_grp_cd': 'Foreign key for the parent design search group'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_design_search_group",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'lock_control_no': 'A number used for locking purposes', 'fk_query_ground_id': 'Foreign key referencing the query ground table', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'review_assignment_dt': 'Date and time when the review was assigned', 'last_mod_ts': 'The timestamp of when the record was last modified', 'cfk_employee_no': 'Foreign key referencing the employee table using the employee number', 'cfk_employee_role_cd': 'Foreign key referencing the employee role table using the employee role code', 'cfk_organization_cd': 'Foreign key referencing the organization table using the organization code', 'create_user_id': 'The user ID that created the record', 'employee_review_query_id': 'Unique identifier for each employee review query'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="employee_review_query",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'full_refusal_override_in': 'Indicator for full refusal override', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'partial_abandonment_ovrd_in': 'Indicator for partial abandonment override', 'partial_refusal_in': 'Indicator for partial refusal', 'response_received_in': 'Indicator for response received', 'issue_dt': 'Date and time when the issue occurred', 'fk_work_item_gid': 'Foreign key referencing the unique identifier of a work item', 'issue_empe_no': 'Employee number associated with the issue', 'create_user_id': 'The user ID that created the record', 'response_on_time_in': 'Indicator for response received on time', 'lock_control_no': 'A number used for locking purposes', 'examination_no': 'Number indicating the examination', 'partial_abandonment_in': 'Indicator for partial abandonment', 'action_no': 'Number indicating the action taken', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="office_activity",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'spcl_form_filed_3d_drawing_in': 'Indicates whether a special form was filed for a three-dimensional drawing', 'three_dimension_in': 'Indicates whether the drawing is three-dimensional or not', 'last_mod_user_id': 'The user ID that last modified the record', 'spcl_form_fild_color_dwg_in': 'Indicates whether a special form was filed for a color drawing', 'color_in': 'Indicates if color is in the drawing', 'create_user_id': 'The user ID that created the record', 'lock_control_no': 'A number used for locking purposes', 'color_claim_tx': 'Textual description of the claimed colors in the drawing', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_drawing",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'end_effective_dt': 'The timestamp of when the record is no longer effective', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'title_tx': 'The title of the review rating.', 'description_tx': 'The description of the review rating.', 'review_rating_cd': 'The code representing the review rating.', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_review_rating",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'create_ts': 'The timestamp of when the record was created', 'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'section_2f_basis_ct': 'Text field indicating the basis for claiming eligibility under Section 2(f)', 'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'section_2f_ct': 'Text field indicating whether the trademark is eligible for registration under Section 2(f)', 'restrict_tx': 'Text field indicating any restrictions on the use or registration of the trademark', 'limitation_tx': 'Text field indicating any limitations or restrictions on the trademark'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="section_2f_statement",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CREATE_USER_ID': 'The user ID that created the record', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'FK_ORDER_NO': 'Foreign key referencing the order number in another table', 'FK_STATEMENT_TYPE_CD': 'Foreign key referencing the code representing the type of statement in another table', 'CREATE_TS': 'The timestamp of when the record was created', 'FK_PRIOR_REG_TRADEMARK_GID': 'Foreign key referencing the unique identifier of a prior registered trademark in another table', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'FK_TRADEMARK_GID': 'Foreign key referencing the unique identifier of a trademark in another table', 'LAST_MOD_TS': 'The timestamp of when the record was last modified'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_addl_stmnt_prior_reg",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'dn_tm_serial_num_tx': 'Serial number of the TM publication', 'previous_og_bounce_no': 'The number of the OG publication has bounced previously', 'cfk_organization_cd': 'Custom foreign key referencing the code for the organization', 'og_tm_review_gid': 'Unique identifier for each review', 'fk_work_item_gid': 'Foreign key referencing the unique identifier of the work item', 'cfk_reviewer_employee_no': 'Custom foreign key referencing the employee number of the reviewer', 'publication_dt': 'Date and time of publication', 'fk_tm_review_status_cd': 'Foreign key referencing the code for the review status', 'create_ts': 'The timestamp of when the record was created', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_tm_publication_gid': 'Foreign key referencing the unique identifier of the TM publication', 'create_user_id': 'The user ID that created the record', 'fk_og_publication_gid': 'Foreign key referencing the unique identifier of the OG publication', 'lock_control_no': 'A number used for locking purposes', 'cfk_employee_role_cd': 'Custom foreign key referencing the code for the employee role', 'last_mod_ts': 'The timestamp of when the record was last modified'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="og_tm_review",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'end_effective_dt': 'The timestamp of when the record is no longer effective', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'description_tx': 'Description of the party role', 'title_tx': 'Title of the party role', 'create_ts': 'The timestamp of when the record was created', 'tm_intrstd_party_role_cd': 'Code representing the interested party role', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_tm_intrstd_party_role",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'subject_tx': 'The subject of the internal note', 'completed_ts': 'The timestamp when the internal note was completed', 'last_mod_ts': 'The timestamp of when the record was last modified', 'prevent_publication_aprvl_in': 'Indicates if the internal note prevents publication approval', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_document_component_id': 'The foreign key referencing the document component related to the internal note', 'fk_work_item_gid': 'The foreign key referencing the work item associated with the internal note', 'fk_business_event_id': 'The foreign key referencing the business event associated with the internal note', 'create_user_id': 'The user ID that created the record', 'cfk_cms_evidence_id': 'The custom foreign key referencing the CMS evidence related to the internal note', 'internal_note_id': 'The unique identifier for each internal note', 'note_location_ct': 'The location of the internal note', 'legacy_jn_ent_num': 'The legacy job number entity number associated with the internal note', 'create_ts': 'The timestamp of when the record was created', 'note_type_ct': 'The category/type of the internal note', 'sequence_no': 'The sequence number of the internal note', 'cfk_completed_employee_no': 'The custom foreign key referencing the employee who completed the internal note', 'prevent_registration_allwnc_in': 'Indicates if the internal note prevents registration allowance', 'allow_delete_in': 'Indicates if the internal note can be deleted', 'fk_trademark_gid': 'The foreign key referencing the trademark to which the internal note belongs', 'lock_control_no': 'A number used for locking purposes'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="internal_note",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'title_tx': 'Title of the ground', 'description_tx': 'Description of the ground', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'grouping_no': 'Number used to group similar grounds together', 'create_ts': 'The timestamp of when the record was created', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'sort_order_no': 'Number used to determine the sorting order of the grounds', 'fk_ground_type_cd': 'Foreign key referencing the ground type code', 'ground_cd': 'Code representing the ground', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'last_mod_ts': 'The timestamp of when the record was last modified'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_ground",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'owner_nm': 'Name of the trademark owner', 'trademark_gid': 'Unique identifier of the trademark', 'owner_id': 'ID of the trademark owner'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="mv_myuspto_trm_owner",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'FK_DOCUMENT_COMPONENT_ID': 'Foreign key referencing the document component ID', 'FK_OFFICE_ACTIVITY_REVIEW_ID': 'Foreign key referencing the review ID of the office activity', 'REVIEW_ANNOTATION_ID': 'Unique identifier for each review annotation', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'TEXT_SEGMENT_TX': 'Text segment', 'FK_ANNOTATION_STATUS_CD': 'Foreign key referencing the annotation status code', 'ANNOTATION_CT': 'The annotation category', 'CREATE_USER_ID': 'The user ID that created the record', 'CREATE_TS': 'The timestamp of when the record was created', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'TEXT_SEGMENT_LOCATOR_TX': 'Text segment locator'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="review_annotation",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'end_effective_dt': 'The timestamp of when the record is no longer effective', 'description_tx': 'Description of the object dispatch', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'object_dispatch_type_ct': 'Category of object dispatch type', 'create_ts': 'The timestamp of when the record was created', 'object_dispatch_type_cd': 'Code representing the type of object dispatch', 'last_mod_user_id': 'The user ID that last modified the record', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'title_tx': 'Title of the object dispatch'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_object_dispatch_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'effective_ts': 'The timestamp of when the record is effective', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'cfk_domain_cd': 'The code representing the domain of the business event', 'cfk_object_gid': 'The global identifier of the object associated with the business event', 'last_mod_user_id': 'The user ID that last modified the record', 'cfk_proceeding_no': 'The proceeding number associated with the business event', 'document_id': 'The identifier of the document associated with the business event', 'business_event_id': 'The unique identifier for each business event', 'fk_object_type_cd': 'The code representing the type of object associated with the business event', 'order_no': 'The order number of the business event', 'cfk_fsm_instance_h_id': 'The unique identifier for the FSM (Finite State Machine) instance associated with the business event', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'fk_business_event_reason_id': 'The unique identifier for the reason of the business event', 'create_user_id': 'The user ID that created the record', 'paper_in': 'The indicator for business event if it was by mail'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="business_event",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'fk_document_template_cd': 'Foreign key referencing the document template code', 'rule_nm': 'Name of the rule', 'cfk_fp_call_number_tx': 'Call number for the form paragraph', 'cfk_domain_message_id': 'Foreign key referencing the domain message ID', 'paragraph_source_ct': 'Source of the paragraph content', 'create_ts': 'The timestamp of when the record was created', 'form_paragraph_rule_id': 'Unique identifier for each form paragraph rule', 'fk_work_item_type_cd': 'Foreign key referencing the work item type code', 'rule_type_ct': 'Type of the rule', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'rule_condition_tx': 'Condition for the rule'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="form_paragraph_rule",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'SIGNATORY_TELECOM_NO': 'Telephone number of the signatory', 'SIGNATURE_IMAGE_OBJ': 'String representing the image of the signature', 'FK_SUBMISSION_GID': 'Foreign key referencing the submission table', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'CREATE_TS': 'The timestamp of when the record was created', 'SIGNATURE_DT': 'Timestamp indicating the date and time of the signature', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'SIGNATORY_NAME_TX': 'Text containing the name of the signatory', 'SIGNATURE_METHOD_CT': 'Text describing the method used for the signature', 'CREATE_USER_ID': 'The user ID that created the record', 'SIGNATURE_TX': 'Text containing the actual signature', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'SIGNATORY_POSITION_TX': 'Text describing the position or role of the signatory', 'SEQUENCE_NO': 'Number indicating the sequence of the signature'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="submission_signature",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'country_cd': 'Code representing the country', 'country_nm': 'Name of the country', 'geo_unit_nm': 'Name of the geographical unit', 'legacy_cd': 'Code representing the legacy system', 'geo_unit_cd': 'Code representing the geographical unit', 'geo_type_cd': 'Code representing the type of geographical unit'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_translate_geo",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'end_effective_dt': 'The timestamp of when the record is no longer effective', 'description_tx': 'Description of the assumed name', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'title_tx': 'Title of the assumed name', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_ts': 'The timestamp of when the record was created', 'last_mod_user_id': 'The user ID that last modified the record', 'assumed_name_type_cd': 'Code representing the type of assumed name'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_assumed_name_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_root_fsm_type_id': 'Foreign key referencing the root FSM type', 'create_user_id': 'The user ID that created the record', 'description_tx': 'Text description of the FSM type state rule', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_current_fsm_type_state_id': 'Foreign key referencing the current FSM type state', 'fsm_type_state_rule_id': 'Unique identifier for each FSM type state rule', 'precondition_tx': 'Precondition for the FSM type state rule', 'fk_fsm_type_event_id': 'Foreign key referencing the FSM type event', 'rule_action_tx': 'Action to be performed by the FSM type state rule', 'fk_fsm_type_id': 'Foreign key referencing the FSM type', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_next_fsm_type_state_id': 'Foreign key referencing the next FSM type state', 'create_ts': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_fsm_type_state_rule",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'class_schedule_cd': 'Code representing a class schedule', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'title_tx': 'Title of the class schedule', 'create_user_id': 'The user ID that created the record', 'description_tx': 'Description of the class schedule', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'create_ts': 'The timestamp of when the record was created', 'us_in': 'Indicator for US class'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_class_schedule",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'end_effective_dt': 'The timestamp of when the record is no longer effective', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'title_tx': 'Title of the document component', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'description_tx': 'Description of the document component', 'document_component_type_cd': 'Code representing the type of document component'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_document_component_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'lock_control_no': 'A number used for locking purposes', 'fk_tm_divisional_status_cd': 'Foreign key referencing the divisional status code of the trademark table', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_child_trademark_gid': 'Foreign key referencing the primary key of the child trademark table', 'mailroom_received_dt': 'Date and time when the trademark was received in the mailroom', 'tm_divisional_status_dt': 'Date and time when the divisional status of the trademark was recorded', 'unit_received_dt': 'Date and time when the trademark was received in the unit', 'fk_sequence_no': 'Foreign key referencing the sequence number of the trademark table', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_divisional_child",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CFK_LAST_ACTION_USER_ROLE_ID': 'COMMENT REQUIRED', 'CFK_HOLD_WORKER_GID': 'COMMENT REQUIRED', 'LAST_ACTION_DT': 'COMMENT REQUIRED', 'FK_WORK_ITEM_GID': 'COMMENT REQUIRED', 'CFK_LAST_ACTION_WORKER_GID': 'COMMENT REQUIRED', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'CFK_HOLD_CATEGORY_CD': 'COMMENT REQUIRED', 'CREATE_TS': 'The timestamp of when the record was created', 'DN_SERIAL_NUM_TX': 'COMMENT REQUIRED', 'DN_HOLD_WORKER_NO': 'COMMENT REQUIRED', 'CFK_HOLD_TM_ORGANIZATION_GID': 'COMMENT REQUIRED', 'DN_LAST_ACTION_WORKER_NO': 'COMMENT REQUIRED', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'CFK_HOLD_STATUS_CD': 'COMMENT REQUIRED', 'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'CFK_LAST_ACTION_TM_ORG_GID': 'COMMENT REQUIRED', 'CFK_HOLD_USER_ROLE_ID': 'COMMENT REQUIRED', 'HOLD_DOCKET_NO': 'COMMENT REQUIRED', 'PLACED_ON_HOLD_DT': 'COMMENT REQUIRED'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="attorney_hold",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'OWNER_NM': 'Name of the trademark owner', 'PROCEEDING_NUM_LIST': 'List of proceeding numbers associated with the trademark', 'SEARCH_ATTORNEY_NM': 'Name used for searching the attorney representing the trademark owner', 'FILING_DT': 'Date when the trademark application was filed', 'MARK_DRAWING_CD': 'Code representing the drawing of the trademark', 'REGISTRATION_DT': 'Date when the trademark was registered', 'OWNER_ID': 'ID of the trademark owner', 'SEARCH_OWNER_NM': 'Name used for searching the trademark owner', 'REGISTRATION_NUM': 'Registration number assigned to the trademark', 'ATTORNEY_NM': 'Name of the attorney representing the trademark owner', 'SEARCH_MARK_TX': 'Text used for searching the trademark', 'SERIAL_NUM': 'Serial number assigned to the trademark', 'ATTORNEY_ID': 'ID of the attorney representing the trademark owner', 'DEAD_MARK_IN': 'Indicator if the trademark is dead or not', 'MARK_DESCRIPTION_TX': 'Description of the trademark'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="mv_myuspto_trm_search",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'legacy_state_type_ct': 'Code representing the type of legacy state', 'legacy_state_no': 'Unique identifier for a legacy state', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_office_activity_reason_cd': 'Foreign key referencing the primary key of the office_activity_reason table', 'last_mod_ts': 'The timestamp of when the record was last modified', 'stnd_fsm_state_legacy_state_id': 'Unique identifier for a mapping between standard FSM state and legacy state', 'create_ts': 'The timestamp of when the record was created', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'cfk_fsm_type_state_id': 'Foreign key referencing the primary key of the fsm_type_state table', 'create_user_id': 'The user ID that created the record', 'examination_no': 'Unique identifier for an examination'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_fsm_state_legacy_state",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'full_load': 'Indicates if it is a full load or not', 'primary_keys': 'The primary keys of the table', 'database_name': 'The name of the database', 'catalog_name': 'The name of the catalog', 'src_folder': 'The folder where the source data is stored', 'table_name': 'The name of the table', 'group_name': 'The name of the group', 'initial_load_finished': 'Indicates if the initial load has finished', 'source_table_name': 'The name of the source table', 'source_db_name': 'The name of the source database'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="cdc_batch_job_control",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'description_tx': 'Description of the class', 'title_tx': 'Title of the class', 'intl_class_inclusions_tx': 'Inclusions for the international class', 'create_user_id': 'The user ID that created the record', 'intl_class_short_title_tx': 'Short title of the international class', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'goods_services_ct': 'Category of goods/services associated with the class', 'intl_class_exclusions_tx': 'Exclusions for the international class', 'fk_class_schedule_cd': 'Foreign key referencing the class schedule code', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'class_no': 'Unique identifier for each class number', 'create_ts': 'The timestamp of when the record was created', 'intl_class_explanatory_note_tx': 'Explanatory note for the international class', 'last_mod_user_id': 'The user ID that last modified the record', 'class_id': 'Unique identifier for each class', 'modification_no': 'Number indicating the modification of the class'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_class",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'ORIGIN_CT': 'The origin of the transaction', 'FK_WORK_ITEM_GID': 'Foreign key referencing the unique identifier of a work item', 'MPU_SENT_TS': 'Timestamp indicating when the transaction was sent to MPU', 'IB_RECEIPT_STATUS_CT': 'The status of the transaction when it was received by IB', 'IB_RECEIPT_TS': 'Timestamp indicating when the transaction was received by IB', 'MPU_SENT_STATUS_CT': 'The status of the transaction when it was sent to MPU', 'DATA_TYPE_CT': 'The type of data in the transaction'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="ib_transaction",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_tm_document_id': 'Foreign key referencing the document ID in the TM system', 'cfk_object_gid': 'Composite foreign key referencing the object global ID', 'last_mod_user_id': 'The user ID that last modified the record', 'lock_control_no': 'A number used for locking purposes', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'fk_object_type_cd': 'Foreign key referencing the object type code', 'last_mod_ts': 'The timestamp of when the record was last modified'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="object_document",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'doc_tmplt_ver_form_para_id': 'The ID of the document template version ', 'editable_in': 'Indicates if the form paragraph is editable', 'create_ts': 'The timestamp of when the record was created', 'fk_document_template_cd': 'Foreign key referencing the document template code', 'cfk_fp_call_number_tx': 'Call number associated with the form paragraph', 'fk_template_para_type_cd': 'Foreign key referencing the paragraph type code', 'create_user_id': 'The user ID that created the record', 'paragraph_type_ct': 'Type of the form paragraph', 'rank_order_no': 'Rank order number of the form paragraph', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_version_no': 'Foreign key referencing the version number'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="doc_tmplt_ver_form_para",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'office_activity_reason_cd': 'Code representing the reason for office activity', 'description_tx': 'Description of the office activity reason', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'fk_office_actvty_rsn_ct_cd': 'Foreign key to the office activity reason category code', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'last_mod_user_id': 'The user ID that last modified the record', 'title_tx': 'Title or name of the office activity reason'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_office_activity_reason",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'publication_category_cd': 'Code representing the category of publication', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'description_tx': 'Text description of the publication category', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_publication_category",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_doc_type_ct_id': 'Foreign key referencing the primary key of the category document type table', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_document_type_id': 'Foreign key referencing the primary key of the document type table', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_category_doc_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'pub_cat_cd': 'The published category code', 'pub_cat_des': 'The description of the published category', 'og_cat': 'The original category code', 'lvl1': 'The first level of categorization', 'lvl2': 'The second level of categorization', 'pub_sub_cd': 'The published sub-category code', 'pub_sub_des': 'The description of the published sub-category'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_translate_og_catg",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_root_fsm_type_id': 'Foreign key referencing the root FSM type', 'fk_fsm_category_cd': 'Foreign key referencing the FSM category code', 'fsm_type_id': 'Unique identifier for each FSM type', 'last_mod_user_id': 'The user ID that last modified the record', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'fk_precedent_fsm_type_id': 'Foreign key referencing the precedent FSM type', 'create_ts': 'The timestamp of when the record was created', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'fk_domain_cd': 'Foreign key referencing the domain code', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'description_tx': 'Description or details of the FSM type', 'title_tx': 'Title or name of the FSM type', 'fk_initial_fsm_type_state_id': 'Foreign key referencing the initial FSM type state'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_fsm_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'SERIAL_NUM': 'Unique identifier for each record in the table.', 'LOCK_STATUS': 'Indicates whether the record is locked or not.', 'LOCK_REASON': 'Reason for locking the record, if applicable.'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_caselock",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'description_tx': 'Description of the class statement', 'last_mod_ts': 'The timestamp of when the record was last modified', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'pre_formatted_statement_tx': 'Text of the class statement before formatting', 'last_mod_user_id': 'The user ID that last modified the record', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'class_statement_type_cd': 'Code representing the type of class statement'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_class_statement_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'fk_us_class_id': 'Foreign key referencing the US class ID', 'create_ts': 'The timestamp of when the record was created', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'fk_intl_class_id': 'Foreign key referencing the international class ID', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_us_intl_cls_mapping",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'end_effective_dt': 'The timestamp of when the record is no longer effective', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'description_tx': 'Description of the divisional status', 'title_tx': 'Title of the divisional status', 'tm_divisional_status_cd': 'Code representing the divisional status'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_tm_divisional_status",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'last_mod_user_id': 'The user ID that last modified the record', 'description_tx': 'The description of the FSM category', 'title_tx': 'The title of the FSM category', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_user_id': 'The user ID that created the record', 'fsm_category_cd': 'The code representing the FSM category'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_fsm_category",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'tmng_transformation_rule': 'Transformation rule for TMNG', 'approve_reject_date': 'Date of approval or rejection', 'rule_num': 'Rule number for migration', 'updated_date': 'Date when the record was last updated', 'tram_full_name': 'The full name in TRAM', 'approve_reject': 'Approval or rejection status', 'target_column_name': 'Name of the target column', 'tmng_mapping': 'Mapping information for TMNG', 'approval_rejection_comments': 'Comments for approval or rejection', 'target_table_name': 'Name of the target table', 'tmng_data_type_cleansing': 'Data type cleansing rule for TMNG', 'cobol_field_name': 'Name of the field in COBOL format', 'dataset': 'Name of the dataset'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_migration_rules",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'FK_TRADEMARK_GID': 'Foreign key referencing the primary key of the trademark table', 'FK_PRIOR_REG_TRADEMARK_GID': 'Foreign key referencing the primary key of the prior registered trademark table', 'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'CREATE_TS': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="section_2f_prior_reg",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'office_actvty_rsn_ct_cd': 'Code for the office activity reason category', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'title_tx': 'Title of the office activity reason', 'create_ts': 'The timestamp of when the record was created', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'description_tx': 'Description of the office activity reason'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_office_actvty_rsn_ct",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'end_effective_dt': 'The timestamp of when the record is no longer effective', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_user_id': 'The user ID that created the record', 'credit_tran_rsn_type_ct': 'Count of credit transaction reason types', 'description_tx': 'Description of the credit transaction reason type', 'create_ts': 'The timestamp of when the record was created', 'last_mod_user_id': 'The user ID that last modified the record', 'title_tx': 'Title of the credit transaction reason type', 'credit_tran_rsn_type_cd': 'Code for the credit transaction reason type', 'last_mod_ts': 'The timestamp of when the record was last modified'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_credit_tran_rsn_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'script_nm': 'The name of the script that is being executed', 'last_commit_ts': 'The timestamp of the last commit made during the script execution', 'end_ts': 'The timestamp when the script execution ended', 'records_commited': 'The total number of records that were committed during the script execution', 'start_ts': 'The timestamp when the script execution started', 'commit_frequency': 'The frequency at which commits are made during the script execution', 'commit_count': 'The total number of commits made during the script execution'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_checkpoint",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'event_cd': 'The event code associated with the event in the USPTO.', 'event_tx': 'The description of the event in the USPTO.'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_myuspto_event",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'actv_pr_other_prior_reg_in': 'Indicator for active prior registration in other countries', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'order_no': 'Number indicating the order of the statement', 'create_ts': 'The timestamp of when the record was created', 'statement_tx': 'The descriptive statement text', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'fk_statement_type_cd': 'Foreign key referencing the primary key of the statement type code table', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_additional_statement",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'LATEST_TQR_SUBMN_RECEIVED_DT': 'Date of the latest TQR (Trademark Query Report) submission received', 'LATEST_SUBMN_RECEIVED_DT': 'Date of the latest submission received', 'CREATE_USER_ID': 'The user ID that created the record', 'LATEST_LIE_SUBMN_RECEIVED_DT': 'Date of the latest LIE (Letter of Irregularity) submission received', 'INCOMING_CORRESPONDENCE_IN': 'Indicator for whether there is incoming correspondence', 'PAPER_CORRESPONDENCE_RCVD_IN': 'Indicator for whether there is paper correspondence received', 'LAST_APPLICANT_RESPONSE_DT': 'Date of the last applicant response', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'FK_TRADEMARK_GID': 'Foreign key referencing the trademark global ID', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'CREATE_TS': 'The timestamp of when the record was created', 'CFK_LAST_INCNG_CORR_EVENT_CD': 'Foreign key referencing the code for the last incoming correspondence event'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_filings",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'lock_control_no': 'A number used for locking purposes', 'foreign_tm_reg_num': 'Registration number of the foreign trademark', 'sequence_no': 'Number indicating the order of the foreign basis', 'foreign_tm_appl_num': 'Application number of the foreign trademark', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'priority_claimed_in': 'Indicator for the country where priority is claimed for the foreign trademark', 'foreign_registration_dt': 'Date when the foreign trademark was registered', 'country_cd': 'Code representing the country of the foreign trademark', 'country_nm': 'Name of the country of the foreign trademark', 'foreign_renewal_expiration_dt': 'Date when the renewal of the foreign trademark expires', 'last_mod_user_id': 'The user ID that last modified the record', 'foreign_filing_dt': 'Date when the foreign trademark was filed', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'cfk_geographic_region_cd': 'Code representing the geographic region of the foreign trademark', 'foreign_renewal_num': 'Renewal number of the foreign trademark', 'foreign_renewal_effective_dt': 'Date when the renewal of the foreign trademark becomes effective', 'dn_geographic_region_nm': 'Name of the geographic region of the foreign trademark', 'foreign_expiration_dt': 'Date when the foreign trademark expires', 'fk_class_id': 'Foreign key referencing the primary key of the class table'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_foreign_basis",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'description_tx': 'Text description of the item', 'last_mod_ts': 'The timestamp of when the record was last modified', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'fk_design_search_group_cd': 'Foreign key referencing the design search group code', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'item_no': 'Unique identifier for each item'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_design_search_code_item",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'payment_type_ct': 'COMMENT REQUIRED', 'status_cd': 'Status code for the application', 'create_ts': 'The timestamp of when the record was created', 'automatic_certification_in': 'Indicator for automatic certification', 'lock_control_no': 'A number used for locking purposes', 'original_filing_dt': 'Timestamp indicating the original filing date', 'payment_reference_no': 'Reference number for payment', 'last_mod_user_id': 'The user ID that last modified the record', 'reply_by_dt': 'Timestamp indicating the deadline for reply', 'international_application_gid': 'Unique identifier for international applications', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_electronic_address_gid': 'Foreign key referencing the electronic address table', 'status_dt': 'Timestamp indicating the status date', 'international_us_ref_no': 'Reference number for international applications in the US'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="international_application",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'reason_for_pub_lvl1': 'First level reason for publishing', 'create_user_id': 'The user ID that created the record', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'description_tx': 'Textual description of the publication subcategory', 'publication_subcategory_cd': 'Code representing the publication subcategory', 'fk_publication_category_cd': 'Foreign key referencing the publication category code', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_ts': 'The timestamp of when the record was created', 'reason_for_pub_lvl2': 'Second level reason for publishing', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_publication_subcategory",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'period_no': 'Period number', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'fiscal_year_no': 'Fiscal year number', 'period_end_dt': 'End date of the period', 'last_mod_user_id': 'The user ID that last modified the record', 'period_start_dt': 'Start date of the period', 'calendar_year_no': 'Calendar year number', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'fiscal_quarter_no': 'Fiscal quarter number', 'last_mod_ts': 'The timestamp of when the record was last modified'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_pay_period",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'fk_trademark_gid': 'Foreign key referencing the unique identifier of a trademark', 'fk_international_appl_gid': 'Foreign key referencing the unique identifier of an international application', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="base_application",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_office_action_category_cd': 'Foreign key referencing the office action category code', 'last_mod_user_id': 'The user ID that last modified the record', 'cfk_fsm_type_state_id': 'Foreign key referencing the FSM type state ID', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_office_action_ct_state",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_og_tm_review_gid': 'Foreign key referencing the unique identifier of the Official Gazette TM review', 'query_tx': 'Text of the review query', 'last_mod_ts': 'The timestamp of when the record was last modified', 'cfk_approval_role_cd': 'Code representing the approval role for the query', 'review_query_gid': 'Unique identifier for each review query', 'create_ts': 'The timestamp of when the record was created', 'print_error_in': 'Indicator that an error was  printed', 'last_mod_user_id': 'The user ID that last modified the record', 'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'og_page_no': 'Page number in the Official Gazette TM review'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="review_query",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_mailing_address_gid': 'Foreign key referencing the unique identifier of the mailing address', 'fk_interested_party_gid': 'Foreign key referencing the unique identifier of the interested party', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="ip_mailing_address",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_mailing_address_gid': 'Foreign key referencing the primary key of the mailing address table', 'lock_control_no': 'A number used for locking purposes', 'create_ts': 'The timestamp of when the record was created', 'sequence_no': 'Number indicating the order of the address lines for a specific mailing address', 'address_line_tx': 'Text value of the address line', 'address_line_ct': 'Code indicating the type of address line (e.g. street, city, state)', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="mailing_address_line",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'comment_tx': 'Text comment or description for the review issue', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'fk_review_issue_cd': 'Foreign key referencing the primary key of the review issue code table', 'fk_office_activity_review_id': 'Foreign key referencing the primary key of the office activity review table', 'last_mod_ts': 'The timestamp of when the record was last modified'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="review_issue",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'description_tx': 'Description of the service annotation', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'title_tx': 'Title of the service annotation', 'gds_srvc_annotn_status_cd': 'Code representing the status of the service annotation', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_gds_srvc_annotn_stat",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'last_mod_user_id': 'The user ID that last modified the record', 'description_tx': 'Description of the mark drawing', 'title_tx': 'Title of the mark drawing', 'create_user_id': 'The user ID that created the record', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'last_mod_ts': 'The timestamp of when the record was last modified', 'mark_drawing_type_cd': 'Code representing the type of mark drawing'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_mark_drawing_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'source_table': 'The table from which data is being migrated', 'script_name': 'The name of the migration script', 'target_table': 'The table to which data is being migrated', 'default_last_userid': 'The default user ID for the last modification of the migration script', 'script_seq': 'The sequential number of the migration script', 'default_create_userid': 'The default user ID for creating the migration script', 'script_description': 'The description or purpose of the migration script', 'print_only': 'Flag indicating if the migration script should only be printed without executing', 'commit_count': 'The number of commits made during the execution of the migration script', 'script_num': 'The unique identifier for each migration script'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_migration_script",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'draft_document_nm': 'Name of the draft document', 'draft_document_status_ct': 'Status of the draft document', 'lock_control_no': 'A number used for locking purposes', 'last_mod_user_id': 'The user ID that last modified the record', 'draft_document_id': 'Unique identifier for each draft document', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="draft_document",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'TTAB_ORAL_HEARING_REQUESTED_IN': 'Indicator of whether an oral hearing has been requested before the TTAB', 'CREATE_TS': 'The timestamp of when the record was created', 'CFK_TRADEMARK_GID': 'Unique identifier for the trademark', 'CONCURRENT_USE_IN': 'Indicator for concurrent use ', 'CREATE_USER_ID': 'The user ID that created the record', 'INTERFERENCE_PUBLISHED_IN': 'Indicator of whether interference has been published', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'INTF_PENDING_TTAB_PRCDNG_IN': 'Indicator of whether interference is pending before the TTAB', 'TTAB_MISPLACED_APPL_REQ_IN': 'Indicator of whether a request for misplaced application has been made to the TTAB', 'CNCR_USE_PEND_TTAB_PRCDNG_IN': 'Indicator of whether concurrent use is pending before the TTAB', 'OPPOSITION_PEND_TTAB_PRCDNG_IN': 'Indicator of whether opposition is pending before the TTAB', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'CNCL_PENDING_TTAB_PRCDNG_IN': 'Indicator of whether cancellation is pending before the Trademark Trial and Appeal Board (TTAB)', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'REFUSAL_APPEALED_TO_TTAB_IN': 'Indicator of whether a refusal has been appealed to the TTAB', 'EXPARTE_APPEAL_DECISION_IN': 'Indicator of whether an ex parte appeal decision has been made'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_appeals",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'FK_DRAFT_DOCUMENT_ID': 'Foreign key referencing the ID of the draft document', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'CFK_FORM_PARAGRAPH_VERSION_GID': 'Global ID of the form paragraph version', 'FK_DRAFT_DOCUMENT_MOD_NO': 'Foreign key referencing the modification number of the draft document', 'FK_DOCUMENT_COMPONENT_ID': 'Foreign key referencing the ID of the document component', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'CREATE_TS': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="draft_doc_ver_compnt_fpv",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'assumed_nm': 'The assumed name of the interested party', 'fk_interested_party_gid': 'Foreign key referencing the interested party global ID', 'intrstd_party_assumed_name_id': 'Unique identifier for the interested party assumed name', 'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_assumed_name_type_cd': 'Foreign key referencing the code for the type of assumed name', 'create_ts': 'The timestamp of when the record was created', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="interested_party_assumed_nm",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'description_tx': 'Description of the appeal status', 'appeal_status_cd': 'Code representing the status of an appeal', 'create_ts': 'The timestamp of when the record was created', 'last_mod_user_id': 'The user ID that last modified the record', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'title_tx': 'Title of the appeal status'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_appeal_status",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'event_deadline_dt': 'The event item deadline date', 'event_goal_dt': 'The date of the docket item goal', 'cfk_assignee_employee_no': 'Foreign key referencing the employee number of the assignee', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'fk_docket_item_event_type_cd': 'Foreign key referencing the event type code of the docket item', 'lock_control_no': 'A number used for locking purposes', 'event_dt': 'The date of the docket item event', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_docket_item_id': 'Foreign key referencing the docket item ID'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="docket_item_event",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'end_effective_dt': 'The timestamp of when the record is no longer effective', 'fee_process_type_cd': 'Code representing the type of fee process', 'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_ts': 'The timestamp of when the record was created', 'description_tx': 'Description of the fee process', 'create_user_id': 'The user ID that created the record', 'title_tx': 'Title of the fee process'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_fee_process_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'rule_condition_tx': 'Text describing the condition of the rule', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'fk_work_item_type_cd': 'Foreign key referencing the work item type code', 'fk_office_action_category_cd': 'Foreign key referencing the office action category code', 'editable_in': 'Indicates where the rule can be edited', 'office_actn_rule_itm_id': 'Unique identifier for each office action rule item', 'create_ts': 'The timestamp of when the record was created', 'ready_to_send_in': 'Indicates if the rule is ready to be sent', 'rule_nm': 'Name of the rule', 'item_no': 'Number indicating the order of the item within the office action rule'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_office_actn_rule_itm",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'FK_INTERNATIONAL_REG_GID': 'Foreign key for international registration', 'NAME_LINE_QT': 'Quantity of name lines', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'FK_FAX_TELECOM_ADDRESS_GID': 'Foreign key for fax telecom address', 'LEGAL_NATURE_TX': 'Legal nature text', 'ENTITLEMENT_TYPE_CT': 'Entitlement type', 'NATIONALITY_COUNTRY_CD': 'Nationality country code', 'INCORPORATION_LOCATION_TX': 'Incorporation location text', 'CREATE_TS': 'The timestamp of when the record was created', 'FK_MAILING_ADDRESS_GID': 'Foreign key for mailing address', 'FK_SEQUENCE_NO': 'Foreign key for sequence number', 'FK_INTERNATIONAL_APPL_GID': 'Foreign key for international application', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'FK_ENTLMNT_MAILING_ADDRESS_GID': 'Foreign key for entitlement mailing address', 'CREATE_USER_ID': 'The user ID that created the record', 'ADDRESS_LINE_QT': 'Quantity of address lines', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'ENTITLEMENT_ADDRESS_LINE_QT': 'Quantity of entitlement address lines', 'FK_ADDRESS_TYPE_CT': 'Foreign key for address type', 'FK_EMAIL_ELECTRONIC_ADDR_GID': 'Foreign key for email electronic address'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="ir_mailing_address",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'SEQUENCE_NO': 'Number indicating the sequence of the address within the group', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'ADDRESS_TYPE_CT': 'Code indicating the type of address', 'FK_INTERNATIONAL_REG_GID': 'Foreign key referencing the primary key of the international registration table', 'CREATE_TS': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="ir_mailing_address_group",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'og_registration_no': 'Registration number assigned to the OG publication', 'record_no': 'Sequential number assigned to each record', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'publication_notice_dt': 'Date and time when the publication notice was made', 'fk_og_publication_gid': 'Foreign key referencing the unique identifier of the OG publication', 'fk_tm_publication_gid': 'Foreign key referencing the unique identifier of the TM publication', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="og_publication_tm",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'display_order_no': 'Number indicating the display order of the mark', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'description_tx': 'Description of the mark', 'last_mod_ts': 'The timestamp of when the record was last modified', 'mark_type_cd': 'Code representing the type of mark', 'create_ts': 'The timestamp of when the record was created', 'title_tx': 'Title of the mark', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'end_effective_dt': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_mark_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'TRIGGER_SCHEDULE_DT': 'Date and time for the trigger schedule', 'CUSTOM_ALERT_ID': 'Unique identifier for each custom alert', 'TRIGGER_TYPE_CT': 'Type of trigger for the custom alert', 'TITLE_TX': 'Description of the custom alert', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'CFK_DOMAIN_MESSAGE_ID': 'Foreign key referencing the domain message ID', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'CREATE_USER_ID': 'The user ID that created the record', 'CREATE_TS': 'The timestamp of when the record was created', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'USER_CONTROL_LEVEL_CT': 'Control level for the custom alert', 'CFK_RECIPIENT_EMPLOYEE_NO': 'Foreign key referencing the recipient employee number'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="custom_alert",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'exam_no': 'Exam number', 'reason_tx': 'Reason text', 'prodvty_cd': 'Productivity code', 'fk_work_item_code': 'Foreign key for work item code', 'reason_ct': 'Reason category', 'fk_credit_tran_rsn_type_cd': 'Foreign key for credit transaction reason type code', 'prodvty_ind': 'Productivity indicator'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_translate_ep",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'approval_in': 'Indicator for approval status', 'create_user_id': 'The user ID that created the record', 'fk_query_ground_id': 'Foreign key referencing the query ground ID', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'review_query_appeal_id': 'Unique identifier for each review query appeal', 'fk_query_appeal_gid': 'Foreign key referencing the query appeal group ID', 'lock_control_no': 'A number used for locking purposes'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="review_query_appeal",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'title_tx': 'Title or name of the telecom format', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'country_cd': 'Code representing the country associated with the telecom format', 'description_tx': 'Description of the telecom format', 'country_nm': 'Name of the country associated with the telecom format', 'telecom_format_cd': 'Code representing the format of a telecommunication'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_telecom_format",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'definition_source_ct': 'Source of the document type definition', 'fk_work_item_type_cd': 'Foreign key referencing the work item type code', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'cfk_cms_document_type_cd': 'Code for the CMS document type', 'legacy_description_tx': 'Description of the legacy document type', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'legacy_document_type_cd': 'Code for the legacy document type', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'document_type_id': 'Unique identifier for the document type', 'legacy_title_tx': 'Title of the legacy document type'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_document_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'title_tx': 'Title of the submission method', 'description_tx': 'Description of the submission method', 'last_mod_ts': 'The timestamp of when the record was last modified', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'submission_method_cd': 'Code representing the method of submission', 'create_user_id': 'The user ID that created the record', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_submission_method",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fil_dt': 'Date of filing the trademark', 'dead_mark_in': 'Indicator if the trademark is dead or not', 'ser_num': 'Serial number of the trademark', 'reg_dt': 'Date of registration of the trademark', 'trademark_gid': 'Global unique identifier for the trademark', 'pn_list': 'List of prior numbers associated with the trademark', 'mark_drawing_cd': 'Code representing the drawing of the trademark', 'reg_num': 'Registration number of the trademark', 'mark_tx': 'Text description of the trademark'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="mv_myuspto_trm_mark",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'title_tx': 'Title of the office action', 'last_mod_user_id': 'The user ID that last modified the record', 'office_action_category_cd': 'Code representing the category of office action', 'create_user_id': 'The user ID that created the record', 'description_tx': 'Description of the office action', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_office_action_category",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'event_dt': 'Date and time of the prosecution history event', 'serial_num': 'Unique serial number for each prosecution history event', 'event_cd': 'Code representing the type of prosecution history event'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="myuspto_trm_ph",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'first_use_anywhere_year_no': 'Year number indicating the first use anywhere', 'last_mod_ts': 'The timestamp of when the record was last modified', 'first_use_anywhere_day_no': 'Day number indicating the first use anywhere', 'gds_srvcs_stmnt_annotated_tx': 'Text field for annotated goods and services statement', 'status_dt': 'Timestamp indicating the status date', 'fk_class_id': 'Foreign key referencing the class ID in another table', 'first_use_in_commerce_day_no': 'Day number indicating the first use in commerce', 'first_use_anywhere_month_no': 'Month number indicating the first use anywhere', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_tm_class_status_cd': 'Foreign key referencing the status code in another table', 'create_ts': 'The timestamp of when the record was created', 'intent_to_use_dt': 'Timestamp indicating the intent to use date', 'first_use_in_commerce_month_no': 'Month number indicating the first use in commerce', 'create_user_id': 'The user ID that created the record', 'lock_control_no': 'A number used for locking purposes', 'gds_srvcs_stmnt_tx': 'Text field for goods and services statement', 'fk_trademark_gid': 'Foreign key referencing the trademark GID in another table', 'first_use_in_commerce_year_no': 'Year number indicating the first use in commerce'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_class",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'description_tx': 'Description of the group', 'title_tx': 'Title of the group', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_ts': 'The timestamp of when the record was created', 'last_mod_user_id': 'The user ID that last modified the record', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'tm_group_type_cd': 'Code representing the type of group'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_tm_group_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CREATE_TS': 'The timestamp of when the record was created', 'FILED_WITH_USE_dateS_IN': 'Indicator for if dates when the trademark was filed with use exist', 'FOREIGN_DATA_ENTERED_IN': 'Indicates if foreign data was entered for the trademark', 'CREATE_USER_ID': 'The user ID that created the record', 'FK_TRADEMARK_GID': 'Foreign key referencing the primary key of the trademark table', 'FILED_WITH_SPECIMENS_IN': 'Indicates if the trademark was filed with specimens', 'FILED_WITH_FRGN_REG_CERT_IN': 'Indicates if the trademark was filed with a foreign registration certificate', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'FILED_WITH_FOREIGN_PRTY_DT_IN': 'Indicator for the date when the trademark was filed with a foreign party', 'FOREIGN_PRIORITY_CLAIMED_IN': 'Indicates if foreign priority was claimed for the trademark', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'LAST_MOD_TS': 'The timestamp of when the record was last modified'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_filing_bases",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'title_tx': 'Title of the review', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'description_tx': 'Description of the review', 'tm_review_status_cd': 'Code representing the status of a review', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_tm_review_status",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'fk_query_ground_id': 'Foreign key referencing the query ground table', 'create_ts': 'The timestamp of when the record was created', 'fk_class_id': 'Foreign key referencing the class table', 'create_user_id': 'The user ID that created the record', 'lock_control_no': 'A number used for locking purposes', 'last_mod_ts': 'The timestamp of when the record was last modified'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="review_query_class",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'end_effective_dt': 'The timestamp of when the record is no longer effective', 'description_tx': 'Description of the additional action', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'writing_rvw_addl_actn_cd': 'Code representing the additional action taken during the writing review process', 'last_mod_ts': 'The timestamp of when the record was last modified', 'title_tx': 'Title of the additional action'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_writing_rvw_addl_actn",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'lock_control_no': 'A number used for locking purposes', 'tm_document_id': 'Unique identifier for each document in the table', 'last_mod_ts': 'The timestamp of when the record was last modified'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_document",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_owner_type_id': 'Foreign key referencing the owner type', 'owner_type_sequence_no': 'Sequence number for the owner type', 'milestone_cd': 'Code representing a milestone', 'legacy_party_type': 'Type of party in the legacy system', 'owner_type_cd': 'Code representing the type of owner'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_translate_party_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'lock_control_no': 'A number used for locking purposes', 'current_in': 'Indicator of whether the filing basis is currently active or not', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'filed_in': 'Indicator of whether the filing basis has been filed or not', 'create_ts': 'The timestamp of when the record was created', 'amended_in': 'Indicator of whether the filing basis has been amended or not', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_filing_basis_cd': 'Foreign key referencing the primary key of the filing basis code table'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_filing_basis",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_class_id': 'Foreign key referencing the unique identifier of a class in another table', 'create_user_id': 'The user ID that created the record', 'fk_trademark_gid': 'Foreign key referencing the unique identifier of a trademark in another table', 'lock_control_no': 'A number used for locking purposes', 'create_ts': 'The timestamp of when the record was created', 'fk_employee_credit_tran_id': 'Foreign key referencing the unique identifier of an employee credit transaction in another table'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="employee_tm_class_credit",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'tm_com_service_nm': 'Name of the communication service related to the exception', 'insert_ts': 'Timestamp when the exception was inserted', 'resolved_ts': 'Timestamp when the exception was resolved', 'tm_com_exception_id': 'Unique identifier for each exception', 'endpoint_type_cd': 'Code indicating the type of endpoint', 'source_ip': 'IP address of the source that triggered the exception', 'retry_ind': 'Indicator whether the exception is eligible for retry', 'ref_no': 'Reference number associated with the exception', 'http_error_msg': 'Error message associated with the HTTP error', 'endpoint_body': 'Body of the request sent to the endpoint', 'http_error_cd': 'Code indicating the HTTP error status', 'endpoint_url': 'URL of the endpoint that caused the exception'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_tm_com_exception",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'title_tx': 'Title of the averment', 'averment_id': 'Unique identifier for each averment', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'create_ts': 'The timestamp of when the record was created', 'description_tx': 'Description of the averment', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'averment_ct': 'Category of the averment'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_averment",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_version_no': 'Foreign key referencing the version number', 'fk_draft_document_id': 'Foreign key referencing the draft document ID', 'create_user_id': 'The user ID that created the record', 'draft_document_mod_no': 'Modification number of the draft document', 'fk_document_template_cd': 'Foreign key referencing the document template code', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="draft_document_version",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'work_item_type_rule_id': 'Unique identifier for each work item type rule', 'rule_type_ct': 'Type of the rule', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'rule_nm': 'Name of the rule', 'rule_condition_tx': 'Condition of the rule', 'fk_work_item_type_cd': 'Foreign key referencing the work item type code'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_work_item_type_rule",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'title_tx': 'Title of the regulatory statement', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_ts': 'The timestamp of when the record was created', 'description_tx': 'Description of the regulatory statement', 'create_user_id': 'The user ID that created the record', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'last_mod_user_id': 'The user ID that last modified the record', 'reg_stmnt_type_cd': 'Code representing the type of regulatory statement'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_reg_stmnt_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'fk_work_item_gid': 'Foreign key referencing the work item', 'review_type_ct': 'Code indicating the type of review', 'office_activity_review_id': 'Unique identifier for each office activity review', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="office_activity_review",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'conv_cd': 'The conversion code', 'data_tx': 'The transaction data'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_translate_assumed_name",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'lock_control_no': 'A number used for locking purposes', 'fk_query_review_status_cd': 'Foreign key referencing the code for the query review status', 'create_user_id': 'The user ID that created the record', 'status_ts': 'Timestamp indicating the date and time of the status update', 'status_reason_tx': 'Textual description providing the reason for the status update', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_employee_review_query_id': 'Foreign key referencing the employee review query associated with this status', 'create_ts': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="employee_review_query_stat",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_parent_fsm_instance_gid': 'Foreign key referencing the parent FSM instance', 'fk_current_fsm_type_state_id': 'Foreign key referencing the current FSM type state', 'last_mod_user_id': 'The user ID that last modified the record', 'terminated_in': 'String indicating where the FSM instance was terminated', 'fk_fsm_type_id': 'Foreign key referencing the FSM type', 'create_user_id': 'The user ID that created the record', 'fsm_instance_gid': 'Unique identifier for each FSM instance', 'suspended_no': 'Number indicating if the FSM instance is suspended (1) or not (0)', 'fk_root_fsm_instance_gid': 'Foreign key referencing the root FSM instance', 'depth_no': 'Number indicating the depth of the FSM instance in the hierarchy', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="fsm_instance",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'end_effective_dt': 'The timestamp of when the record is no longer effective', 'last_mod_user_id': 'The user ID that last modified the record', 'legacy_transaction_cd': 'Legacy transaction code', 'last_mod_ts': 'The timestamp of when the record was last modified', 'description_tx': 'Description of the transaction', 'title_tx': 'Title of the transaction', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_legacy_transaction",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_class_id': 'Foreign key referencing the unique identifier of a class', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_trademark_gid': 'Foreign key referencing the unique identifier of a trademark', 'fk_filing_basis_cd': 'Foreign key referencing the code for the filing basis', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'lock_control_no': 'A number used for locking purposes', 'create_ts': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_class_filing_basis",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_user_id': 'The user ID that created the record', 'description_tx': 'Description of the ground type', 'last_mod_user_id': 'The user ID that last modified the record', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'create_ts': 'The timestamp of when the record was created', 'title_tx': 'Title of the ground type', 'ground_type_cd': 'Code representing the type of ground'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_ground_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'cfk_root_fsm_instance_gid': 'Foreign key referencing the root FSM instance global ID', 'lock_control_no': 'A number used for locking purposes', 'cfk_current_fsm_type_state_id': 'Foreign key referencing the current FSM type state ID', 'exparte_appeal_active_in': 'Indicator of whether exparte appeal is active', 'cfk_object_gid': 'Foreign key referencing the object global ID', 'last_action_no': 'Number indicating the last action', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'sou_last_extension_no': 'Number indicating the last extension for SOU', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_object_type_cd': 'Foreign key referencing the object type code', 'current_registration_rnwl_no': 'Number indicating the current registration renewal', 'create_ts': 'The timestamp of when the record was created', 'current_examination_no': 'Number indicating the current examination', 'fk_cur_ste_ofc_actvty_rsn_cd': 'Foreign key referencing the current state office activity reason code'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="object_fsm_instance",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'lastupdated': 'Date and time when the user account was last updated', 'id': 'Unique identifier for each user', 'userid': 'Username of the user', 'password': 'Password for the user account', 'createdate': 'Date and time when the user account was created', 'role': 'Role assigned to the user'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_authuser",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'fk_submission_gid': 'Foreign key referencing the submission associated with the submission item', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'submission_item_gid': 'Unique identifier for each submission item', 'create_ts': 'The timestamp of when the record was created', 'fk_work_item_gid': 'Foreign key referencing the work item associated with the submission item'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="submission_item",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_trademark_gid': 'Foreign key referencing the unique identifier of the trademark in the tm_employee_assignment table', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_tm_employee_role_cd': 'Foreign key referencing the code representing the role of the employee in the tm_employee_assignment table', 'create_ts': 'The timestamp of when the record was created', 'effective_dt': 'The timestamp of when the record is effective', 'cfk_employee_no': 'Foreign key referencing the unique identifier of the employee in the tm_employee_assignment table', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'lock_control_no': 'A number used for locking purposes'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_employee_assignment",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'note': 'Additional information or comments about the action', 'action': 'Type of action performed', 'createdate': 'Timestamp of when the log entry was created', 'userid': 'Identifier of the user who performed the action', 'id': 'Unique identifier for each log entry'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_log",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'work_item_request_cd': 'Code representing the work item request', 'title_tx': 'Title of the work item request', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'last_mod_ts': 'The timestamp of when the record was last modified', 'description_tx': 'Description of the work item request', 'last_mod_user_id': 'The user ID that last modified the record', 'cfk_business_unit_cd': 'Code representing the business unit associated with the work item request', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'end_effective_dt': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_work_item_request",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_docket_id': 'Foreign key referencing the primary key of the docket table', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'cfk_fsm_type_state_id': 'Foreign key referencing the primary key of the FSM type state table', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_docket_fsm_type_state",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'FK_ELECTRONIC_ADDRESS_GID': 'Foreign key referencing the electronic address table', 'FK_SUBMISSION_GID': 'Foreign key referencing the submission table', 'PRIMARY_IN': 'Indicator for primary electronic address', 'CREATE_USER_ID': 'The user ID that created the record', 'CREATE_TS': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="submission_elctrn_addr",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'description_tx': 'The description text associate with the evidence bin category', 'title_tx': 'The title text associate with the evidence bin category', 'evidence_bin_cd': 'The unique code associated with the evidence bin', 'create_user_id': 'The user ID that created the record', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'end_effective_dt': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_evidence_bin",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_office_action_category_cd': 'Foreign key referencing the office action category code', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'fk_work_item_type_cd': 'Foreign key referencing the work item type code', 'typical_ct': 'Indicates the typical category', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_office_action_rule",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_design_search_group_cd': 'Foreign key referencing the primary key of the design search group code table', 'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_design_element",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'title_tx': 'Title of the docket', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'cfk_user_role_cd': 'Code representing the user role', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'docket_id': 'Unique identifier for the docket', 'last_mod_ts': 'The timestamp of when the record was last modified', 'docket_cd': 'Code representing the docket', 'description_tx': 'Description of the docket'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_docket",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_ts': 'The timestamp of when the record was created', 'fk_class_id': 'Foreign key referencing the class table', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'fk_coordinated_class_id': 'Foreign key referencing the coordinated class table', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_coordinated_class",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'international_reg_gid': 'Unique identifier for international registration', 'lock_control_no': 'A number used for locking purposes', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'international_reg_seq_no': 'Sequential number for international registration', 'create_ts': 'The timestamp of when the record was created', 'fk_international_reg_no': 'Foreign key referencing international registration number'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="international_registration",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'description': 'Description of the status', 'am_stat': 'The status number', 'control_num': 'Control number of the status', 'tram_state': 'The state in TRAM'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_stnd_am_stat",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'trademark_gid': 'Global ID of the trademark', 'at_id': 'ID of the attorney', 'at_nm': 'Name of the attorney'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="mv_myuspto_trm_at",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'telecom_type_cd': 'Code representing the type of telecom', 'create_user_id': 'The user ID that created the record', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'description_tx': 'Description of the telecom', 'title_tx': 'Title of the telecom', 'last_mod_ts': 'The timestamp of when the record was last modified', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_telecom_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'description_tx': 'Description of the legal entity', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'legal_entity_type_cd': 'Code representing the type of legal entity', 'last_mod_ts': 'The timestamp of when the record was last modified', 'title_tx': 'Title of the legal entity', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_ts': 'The timestamp of when the record was created', 'legal_entity_ct': 'Category of the legal entity'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_legal_entity_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'TL_DATE': 'Date of the transaction log entry', 'TL_TIMER': 'Duration of the transaction log entry in seconds', 'TL_TIMESTAMP': 'Timestamp of the transaction log entry', 'TL_SER_NUM': 'Serial number of the transaction log entry', 'TL_STATE': 'State of the transaction log entry'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_tranlog",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CREATE_USER_ID': 'The user ID that created the record', 'CREATE_TS': 'The timestamp of when the record was created', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'FK_MEMBER_INTERESTED_PARTY_GID': "Foreign key referencing the individuals interested party global identifier", 'FK_INTERESTED_PARTY_GID': "Foreign key referencing the interested individuals global identifier", 'FK_IP_RELTNSP_TYPE_CD': 'Foreign key referencing the relationship type code for interested party relationships'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="intrstd_party_relationship",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'title_tx': 'Title of the owner type', 'owner_type_cd': 'Code representing the owner type', 'create_user_id': 'The user ID that created the record', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'last_mod_user_id': 'The user ID that last modified the record', 'owner_type_id': 'Unique identifier for the owner type', 'description_tx': 'Description of the owner type', 'create_ts': 'The timestamp of when the record was created', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_owner_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_worker_relationship_cd': 'Foreign key referencing the code representing the type of relationship between workers', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'create_user_id': 'The user ID that created the record', 'fk_related_worker_gid': "Foreign key referencing the related workers global identifier", 'related_worker_id': 'Unique identifier for each related worker', 'fk_base_worker_gid': "Foreign key referencing the base workers global identifier", 'lock_control_no': 'A number used for locking purposes'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="related_worker",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'tm_class_status_cd': 'Code representing the status of a trademark class', 'create_user_id': 'The user ID that created the record', 'description_tx': 'Description of the trademark class', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'title_tx': 'Title of the trademark class'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_tm_class_status",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_electronic_address_gid': 'Foreign key referencing the global ID of an electronic address in another table', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'lock_control_no': 'A number used for locking purposes', 'create_ts': 'The timestamp of when the record was created', 'fk_tm_party_role_id': 'Foreign key referencing the party role ID in another table', 'authorized_email_in': 'Indicates whether the email address is authorized or not', 'primary_in': 'Indicates whether the email address is the primary one for the party role'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_electronic_addr",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'description_tx': 'Description of the docket item event', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'title_tx': 'Title of the docket item event', 'docket_item_event_type_cd': 'Code representing the type of docket item event', 'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_ts': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_docket_item_event_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'appeal_note_tx': 'Text of the appeal note', 'lock_control_no': 'A number used for locking purposes', 'fk_employee_query_appeal_id': 'Foreign key referencing the query appeal ID', 'note_sequence_no': 'Sequence number of the note', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="query_appeal_note",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'status_cd': 'Code indicating the status of the international registration', 'lock_control_no': 'A number used for locking purposes', 'ib_publication_dt': 'Date and time when the international registration was published', 'create_user_id': 'The user ID that created the record', 'fk_international_appl_gid': 'Foreign key referencing the unique identifier of the international application', 'fk_international_reg_gid': 'Foreign key referencing the unique identifier of the international registration', 'status_dt': 'Date and time when the status of the international registration was last updated', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'ib_renewal_dt': 'Date and time when the international registration is scheduled for renewal'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="international_appl_reg",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_class_id': 'Foreign key referencing the primary key of the class table', 'fk_referenced_class_id': 'Foreign key referencing the primary key of the referenced class table', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_class_reference",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'gds_srvc_match_stat_cd': 'Code representing the status of the service match', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'title_tx': 'Title of the service match', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_ts': 'The timestamp of when the record was created', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'last_mod_ts': 'The timestamp of when the record was last modified', 'description_tx': 'Description of the service match'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_gds_srvc_match_stat",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'table_name_tx': 'Name of the table where the object type is stored', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'object_type_id_ct': 'Unique identifier for the object type', 'create_ts': 'The timestamp of when the record was created', 'title_tx': 'Title of the object', 'object_type_cd': 'Code representing the type of object', 'global_identifier_prefix_tx': 'Prefix used for generating global identifiers for the object', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'description_tx': 'Description of the object', 'end_effective_dt': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_object_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'description_tx': 'Description of the note', 'last_mod_ts': 'The timestamp of when the record was last modified', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'note_type_cd': 'Code representing the type of note', 'title_tx': 'Title of the note', 'create_ts': 'The timestamp of when the record was created', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_note_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'description_tx': 'Description of the transaction', 'mad_transaction_type_cd': 'Code representing the type of transaction', 'last_mod_user_id': 'The user ID that last modified the record', 'title_tx': 'Title of the transaction', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_user_id': 'The user ID that created the record', 'end_effective_dt': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="stnd_mad_transaction_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'meta_src_time': 'The timestamp of when the metadata was sourced', 'cdc_file_path': 'The file path of the CDC (Change Data Capture) file', 'cdc_file_date': 'The date of the CDC (Change Data Capture) file', 'processing_time': 'The timestamp of when the processing occurred'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="cdc_batch_job_history",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'empe_num': 'Unique identifier for each employee', 'empe_lo': 'Location of the employee'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_translate_emp_lo",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'serial_num': 'Unique identifier for each trademark event'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="myuspto_trm_event_today",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'serial_num': 'The serial number of each trademark application.'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="myuspto_trm_status_today",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'error_tx': 'Text description of the error message', 'error_type': 'Type of error that occurred'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="sync_exception_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'OG_CERTIFICATE_CORRECTION_IN': 'Indicator if there is a certificate correction for the trademark', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'OG_PUBD_FOR_OPSTN_SEC_12C_DT': 'Date when the trademark was published for opposition under Section 12C', 'CFK_TRADEMARK_GID': 'Unique identifier for the trademark', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'OG_REGISTRATION_IN': 'Indicator if the trademark has a registration', 'OG_ORDER_RESTRICTING_SCOPE_IN': 'Indicator if there is an order restricting the scope of the trademark', 'OG_CANCELLED_REGISTRATION_IN': 'Indicator if the trademark has a cancelled registration', 'OG_REGISTRATION_NUM_FOUND_IN': 'Registration number found for the trademark', 'PRINT_MARK_DESCRIPTION_IN': 'Description of the printed mark', 'OG_IN_PUBLICATION_IN': 'Indicator if the trademark is in publication', 'OG_EXTRACT_PUBLICATION_IN': 'Indicator if the trademark has an extract publication', 'REPUBLISH_SECTION_12_IN': 'Indicator if there is a republication under Section 12 for the trademark', 'OG_SEC_12C_REPUBLICATION_IN': 'Indicator if there is a republication under Section 12C for the trademark', 'OG_CERTIFICATE_OF_REG_IN': 'Indicator if there is a certificate of registration for the trademark', 'OG_PUBD_FOR_OPSTN_DT': 'Timestamp when the trademark was published for opposition', 'OG_AMENDED_REGISTRATION_IN': 'Indicator if the trademark has an amended registration', 'OG_RENEWAL_IN': 'Indicator if the trademark has a renewal', 'CREATE_TS': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_og_publications",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'cfk_tm_organization_gid': 'Foreign key referencing the organization table', 'physical_location_in': 'Indicator for whether the location is physical or not', 'location_cd': 'Code representing the location', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'locc_in': 'Indicator for whether the location is locked or not', 'location_id': 'Unique identifier for each location', 'lock_control_no': 'A number used for locking purposes', 'aloc_in': 'Indicator for whether the location is allocated or not', 'location_desc_tx': 'Description of the location'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_organization_location",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_trademark_gid': 'Foreign key referencing the unique identifier of a trademark', 'create_ts': 'The timestamp of when the record was created', 'fk_class_statement_type_cd': 'Foreign key referencing the code for the type of class statement', 'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'preformatted_text_in': 'Text input that has been preformatted', 'fk_class_id': 'Foreign key referencing the unique identifier of a class', 'statement_tx': 'The descriptive statement text', 'first_use_month_no': 'Numeric value representing the month of first use', 'first_use_day_no': 'Numeric value representing the day of first use', 'first_use_year_no': 'Numeric value representing the year of first use', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="use_in_another_form",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'SOU_RECEIVED_DT': 'The date when the statement of use was received', 'AVAILABLE_FOR_SOU_IN': 'Available for statement of use', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'LAST_EXT_TRAN_DNIL_LTR_PREP_IN': 'Last extension transaction denial letter prepared', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'CREATE_TS': 'The timestamp of when the record was created', 'SOU_EXT_DENIAL_LTR_MAILED_IN': 'Indicates a statement of use extension denial letter mailed', 'NOA_MAILED_IN': 'Indicates if a notice of allowance has been mailed', 'FIRST_ACTION_REFUSAL_ATU_IN': 'First action refusal for ATU', 'LAST_UA_TRAN_INFRML_LTR_ML_IN': 'Indicates last UA transaction informal letter mailed', 'POTENTIEL_ABANDONMENT_DT': 'The potential date of abandonment for the trademark application', 'CREATE_USER_ID': 'The user ID that created the record', 'LAST_EXT_TRAN_SOU_EXT_FILED_IN': 'Indicates a last extension transaction statement of use extension has been filed', 'APPLICATION_MARK_IN_2': 'Second application mark', 'LATEST_ITU_FILNG_RECEIVED_DT': 'Latest ITU filing received date', 'ITU_FREEZE_PERIOD_IN': 'Indicates an ITU freeze period', 'FINAL_ACTION_REFUSAL_ATU_IN': 'Final action refusal for ATU', 'EXTENSIONS_NOT_ALLOWED_IN': 'Indicator for whether extensions not allowed', 'NOA_ISSUED_IN': 'Indicator for notice of allowance issued', 'APPLICATION_MARK_IN_1': 'First application mark', 'AMENDMENT_TO_USE_FILED_IN': 'Date when amendment to use was filed', 'USE_AFFIDAVIT_PRCSG_COMPLT_IN': 'Indicator for use affidavit processing complete', 'ITU_CASE_PUBD_FOR_OPSTN_IN': 'Indicates an ITU case published for opposition', 'FK_TRADEMARK_GID': 'Foreign key for trademark', 'LAST_UA_TRAN_INFRML_RSP_RCV_IN': 'Indicates an last UA transaction informal response received', 'SOU_EXTENSION_REQ_FILED_IN': 'Indicates if a request for extension of the statement of use has been filed', 'LAST_POSSIBLE_EXTENSION_DT': 'Last possible extension date', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'HOLD_FIRST_ACTION_RFSL_ATU_IN': 'Indicator for hold first action refusal for ATU'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_itu",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'standard_character_tx': 'Text describing the standard characters used in the trademark', 'registry_ct': 'The registry category', 'status_dt': 'Date indicating the status of the trademark', 'fk_filed_fee_process_type_cd': 'COMMENT REQUIRED', 'collective_in': 'Indicator specifying if the trademark is a collective mark', 'registration_num': 'Registration number of the trademark', 'available_for_sou_in': 'COMMENT REQUIRED', 'last_action_dt': 'Date of the last action performed on the trademark', 'effective_filing_dt': 'Effective date of filing for the trademark', 'external_reference_tx': 'COMMENT REQUIRED', 'create_ts': 'The timestamp of when the record was created', 'fk_fee_process_type_cd': 'Foreign key referencing the fee process type of the trademark', 'lock_control_no': 'A number used for locking purposes', 'last_event_type_cd': 'COMMENT REQUIRED', 'last_mod_user_id': 'The user ID that last modified the record', 'filing_dt': 'Date when the trademark was filed', 'legacy_status_cd': 'Legacy status code of the trademark', 'create_user_id': 'The user ID that created the record', 'fk_mark_drawing_type_cd': 'Foreign key referencing the drawing type of the trademark', 'serial_num_tx': 'Serial number of the trademark', 'last_mod_ts': 'The timestamp of when the record was last modified', 'uspto_generated_image_in': 'COMMENT REQUIRED', 'mark_description_tx': 'Description of the trademark', 'preferred_contact_method_ct': 'Preferred contact method category for the trademark owner', 'trademark_gid': 'Unique identifier for each trademark'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="trademark_perf",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'last_mod_user_id': 'The user ID that last modified the record', 'primary_in': 'Indicator for whether the mailing address is the primary address', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_mailing_address_gid': 'Foreign key referencing the global ID of the mailing address', 'create_user_id': 'The user ID that created the record', 'lock_control_no': 'A number used for locking purposes', 'fk_tm_party_role_id': 'Foreign key referencing the party role ID in another table'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_mailing_addr",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'INTERNAL_NOTE_IN': 'Internal note related to the trademark', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'MISCELLANEOUS_1_IN': 'Miscellaneous field related to the trademark', 'CHILD_APPLICATION_IN': 'Indicates if there is a child application related to the trademark', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'TTAB_ORAL_HEARING_REQUESTED_IN': 'Indicates if an oral hearing has been requested for the Trademark Trial and Appeal Board (TTAB)', 'ACTIVE_PETITION_IN': 'Indicates if there is an active petition related to the trademark', 'CONCURRENT_USE_STATUS_CT': 'The status of concurrent use for the trademark', 'AMENDED_TM_APPLICATION_IN': 'Indicates if the trademark application has been amended', 'COMPLETE_CASE_IN_TICRS_IN': 'Indicates if the trademark case is complete in TICRS (Trademark Case Retrieval System)', 'NO_ACED_IN': 'Indicates if there is no action or decision recorded for the trademark', 'INTF_PENDING_TTAB_PRCDNG_IN': 'Indicates if there is a pending interference proceeding for the trademark', 'PARENT_APPLICATION_IN': 'Indicates if there is a parent application related to the trademark', 'REFUSAL_APPEALED_TO_TTAB_IN': 'Indicates if a refusal has been appealed to the TTAB', 'REGISTRATION_AMENDED_IN': 'Indicates if the registration has been amended for the trademark', 'SERIAL_NUMBER_VERIFIED_IN': 'Indicates if the serial number of the trademark has been verified', 'EXPARTE_APPEAL_DECISION_IN': 'Indicates if there has been a decision made on an ex parte appeal', 'LOP_RECEIVED_IN': 'Indicates if a Letter of Protest (LOP) has been received for the trademark', 'INACTIVE_IN': 'Indicates if the trademark is inactive', 'OPPOSITION_PERIOD_ENDED_DT': 'Date when the opposition period ended for the trademark', 'CREATE_TS': 'The timestamp of when the record was created', 'CREATE_USER_ID': 'The user ID that created the record', 'OPPOSITION_PEND_TTAB_PRCDNG_IN': 'Indicates if there is an ongoing opposition proceeding at the TTAB', 'CNCR_USE_PEND_TTAB_PRCDNG_IN': 'Indicates if there is a pending concurrent use proceeding for the trademark', 'REGISTER_AMENDED_PRINCIPAL_IN': 'Indicates if the registration has been amended for the principal part of the trademark', 'FK_TRADEMARK_GID': 'Foreign key referencing the unique identifier of a trademark', 'ASSIGNMENT_RECORDED_IN': 'Indicates if an assignment has been recorded for the trademark', 'INTERFERENCE_PUBLISHED_IN': 'Indicates if interference has been published for the trademark', 'NEW_TM_CASE_ADDED_IN': 'Indicates if a new trademark case has been added', 'IN_PUBLICATION_IN': 'Indicates if the trademark is currently in the publication stage', 'UNANSWERED_PETITION_IN': 'Indicates if there is an unanswered petition related to the trademark', 'NOT_ELECTRONIC_IN': 'Indicates if the trademark is not electronic', 'CONCURRENT_USE_PUBLISHED_IN': 'Indicates if concurrent use has been published for the trademark', 'REGISTER_AMENDED_SUPL_IN': 'Indicates if the registration has been amended for the supplemental part of the trademark', 'LATEST_SUSPENSION_CHECK_DT': 'The date of the latest suspension check for the trademark', 'CONCURRENT_USE_IN': 'Indicates if concurrent use is applicable to the trademark', 'LAST_MOD_TS': 'The timestamp of when the record was last modified'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_states",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'request_description_tx': 'Text containing the description of the request', 'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'fk_work_item_request_cd': 'Foreign key referencing the code of the work item request', 'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'request_statement_tx': 'Text containing the statement of the request', 'business_unit_addr_tx': 'Text containing the address of the business unit', 'request_status_ct': 'Code indicating the status of the request', 'fk_work_item_gid': 'Foreign key referencing the unique identifier of the work item', 'notify_status_complete_in': 'Code indicating the notification status for completion', 'request_dt': 'Date and time when the request was made', 'sequence_no': 'Number indicating the sequence of the record', 'cfk_business_unit_cd': 'Foreign key referencing the code of the business unit', 'cfk_sender_employee_no': 'Foreign key referencing the employee number of the sender'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="work_item_request",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'owner_type_sequence_no': 'Sequence number of an owner type', 'frame_num_tx': 'String value representing the frame number', 'fk_party_role_sequence_no': 'Foreign key referencing the sequence number of a party role', 'last_mod_ts': 'The timestamp of when the record was last modified', 'assignment_dt': 'Timestamp indicating the date of assignment', 'reel_num_tx': 'Numeric value representing the reel number', 'lock_control_no': 'A number used for locking purposes', 'create_ts': 'The timestamp of when the record was created', 'fk_owner_type_id': 'Foreign key referencing the unique identifier of an owner type', 'fk_tm_party_role_cd': 'Foreign key referencing the code of a party role associated with a trademark', 'create_user_id': 'The user ID that created the record', 'joint_owner_sequence_no': 'Sequence number of a joint owner', 'fk_trademark_gid': 'Foreign key referencing the unique identifier of a trademark', 'legacy_assignment_tx': 'String value representing a legacy assignment'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_party_role_owner",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'cfk_notification_message_id': 'Foreign key referencing the primary key of the notification message table', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_notification_message",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'fk_mark_type_cd': 'Foreign key referencing the primary key of the mark type code table', 'lock_control_no': 'A number used for locking purposes', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_mark_type",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'standard_character_tx': 'Text description of the standard characters used in the trademark', 'status_dt': 'Date when the status of the trademark was last updated', 'trademark_gid': 'Unique identifier for each trademark', 'last_mod_user_id': 'The user ID that last modified the record', 'legacy_status_cd': 'Legacy status code for the trademark', 'registration_num': 'Registration number of the trademark', 'external_reference_tx': 'COMMENT REQUIRED', 'fk_mark_drawing_type_cd': 'Foreign key referencing the drawing type of the trademark', 'fk_fee_process_type_cd': 'Foreign key referencing the fee process type of the trademark', 'lock_control_no': 'A number used for locking purposes', 'last_action_dt': 'Date of the last action performed on the trademark', 'last_event_type_cd': 'COMMENT REQUIRED', 'last_mod_ts': 'The timestamp of when the record was last modified', 'filing_dt': 'Date when the trademark was filed', 'collective_in': 'Indicator if the trademark is a collective mark', 'create_ts': 'The timestamp of when the record was created', 'available_for_sou_in': 'COMMENT REQUIRED', 'effective_filing_dt': 'Effective date of filing for the trademark', 'fk_filed_fee_process_type_cd': 'COMMENT REQUIRED', 'registry_ct': 'COMMENT REQUIRED', 'serial_num_tx': 'Serial number of the trademark', 'mark_description_tx': 'Description of the trademark', 'create_user_id': 'The user ID that created the record', 'preferred_contact_method_ct': 'Preferred contact method category for the trademark owner', 'uspto_generated_image_in': 'COMMENT REQUIRED'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="trademark",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_trademark_gid': 'Foreign key referencing the unique identifier of a trademark', 'current_location_dt': "Date when the trademarks current location was recorded", 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'fk_current_location_cd': 'Foreign key referencing the current location code of the trademark', 'cfk_asgnd_exam_law_ofc_org_cd': 'Code representing the assigned examination law office organization', 'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'fk_physical_location_cd': 'Foreign key referencing the physical location code of the trademark', 'physical_location_dt': "Date when the trademarks physical location was recorded", 'official_search_in_progress_in': 'Location where an official search is currently in progress', 'fk_charge_to_location_cd': 'Foreign key referencing the location code where the charge is assigned to', 'case_reported_lost_in': 'Indicator for if the case was reported lost', 'case_reported_lost_dt': 'Date when the case was reported lost', 'last_mod_user_id': 'The user ID that last modified the record', 'cfk_charge_to_worker_no': 'Code representing the worker number associated with the charge'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_locations",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'milestone_dt': 'Date and time when the milestone was achieved', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_tm_milestone_cd': 'Foreign key referencing the primary key of the milestone code table', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'lock_control_no': 'A number used for locking purposes', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_milestone",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_publication_subcategory_cd': 'Foreign key referencing the subcategory code of the publication', 'create_ts': 'The timestamp of when the record was created', 'fk_publication_category_cd': 'Foreign key referencing the category code of the publication', 'lock_control_no': 'A number used for locking purposes', 'fk_tm_publication_gid': 'Foreign key referencing the global ID of the publication', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'legacy_des_cd': 'Code representing the legacy description of the publication'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_publication_subct",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'tm_group_id': 'The unique identifier for each group within the business.', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'description_tx': 'The description or details about the group.', 'group_nm': 'The name of the group.', 'cfk_owner_employee_no': 'The foreign key referencing the employee number of the group owner.', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'fk_tm_group_type_cd': 'The foreign key referencing the group type code.', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_group",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_work_item_gid': 'Foreign key for work item global ID in writing review', 'review_complete_dt': 'Timestamp for when the review is completed in writing review', 'lock_control_no': 'A number used for locking purposes', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'performance_procedure_error_qt': 'Quantity of performance procedure errors in writing review', 'review_comment_tx': 'Text comment for the review in writing review', 'substantive_error_qt': 'Quantity of substantive errors in writing review', 'cfk_reviewer_employee_no': 'Custom foreign key for reviewer employee number in writing review', 'last_mod_ts': 'The timestamp of when the record was last modified', 'writing_review_id': 'Unique identifier for writing review', 'create_user_id': 'The user ID that created the record', 'fk_writing_rvw_addl_actn_cd': 'Foreign key for additional action code in writing review', 'correction_in': 'Indicator for correction in writing review', 'fk_review_rating_cd': 'Foreign key for review rating code in writing review', 'comprehensively_excellent_in': 'Indicator for comprehensively excellent in writing review'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="writing_review",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'section_8_partial_accepted_in': 'Indicator of whether Section 8 has been partially accepted for the trademark', 'create_user_id': 'The user ID that created the record', 'section_71_filed_in': 'Indicator of whether Section 71 has been filed for the trademark', 'fk_trademark_gid': 'Foreign key referencing the unique identifier of a trademark', 'section_71_accepted_in': 'Indicator of whether Section 71 has been accepted for the trademark', 'renewal_filed_in': 'Indicator of whether renewal has been filed for the trademark', 'post_reg_supplemental_rgstr_in': 'Indicator of whether the trademark is registered as a supplemental registration', 'last_mod_ts': 'The timestamp of when the record was last modified', 'post_registration_audit_in': 'Indicator of whether post-registration audit has been conducted for the trademark', 'section_15_filed_in': 'Indicator of whether Section 15 has been filed for the trademark', 'post_reg_audit_begin_dt': 'Timestamp indicating the start date and time of the post-registration audit', 'post_reg_principal_rgstr_in': 'Indicator of whether the trademark is registered as a principal registration', 'section_8_filed_in': 'Indicator of whether Section 8 has been filed for the trademark', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'republish_section_12_in': 'Indicator of whether Section 12 republishing has been done for the trademark', 'section_8_accepted_in': 'Indicator of whether Section 8 has been accepted for the trademark', 'section_15_ackd_in': 'Indicator of whether Section 15 has been acknowledged for the trademark', 'cfk_cancellation_reason_cd': 'Code indicating the reason for cancellation of the trademark', 'latest_correspondence_rcvd_dt': 'Timestamp indicating the date and time of the most recent correspondence received', 'registration_amended_in': 'Indicator of whether the trademark registration has been amended', 'last_mod_user_id': 'The user ID that last modified the record', 'section_71_partial_accepted_in': 'Indicator of whether Section 71 has been partially accepted for the trademark'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_post_registration",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'fk_physical_location_cd': 'Foreign key referencing the primary key of the physical location code table', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'physical_location_dt': 'Date and time of the physical location', 'create_ts': 'The timestamp of when the record was created', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_physical_location",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_prior_trademark_gid': 'Foreign key referencing the primary key of the prior trademark table', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'lock_control_no': 'A number used for locking purposes', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_prior_registration",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'fk_work_item_gid': 'Foreign key referencing the unique identifier of a work item', 'fk_object_type_cd': 'Foreign key referencing the code representing the type of object', 'cfk_object_gid': 'Foreign key referencing the unique identifier of an object'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="work_item_object",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'FK_TRADEMARK_GID': 'Foreign key referencing the primary key of the trademark table', 'CREATE_USER_ID': 'The user ID that created the record', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'EXPIRATION_DT': 'Date and time when the extension expires', 'CREATE_TS': 'The timestamp of when the record was created', 'ITU_EXTENSION_NO': 'Number representing the ITU extension'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_itu_extension",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'statement_month_no': 'Month of the registration statement', 'statement_day_no': 'Day of the registration statement', 'sequence_no': 'Sequential number assigned to each registration statement', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'statement_tx': 'The descriptive statement text', 'fk_trademark_gid': 'Foreign key referencing the unique identifier of a trademark', 'statement_year_no': 'Year of the registration statement', 'fk_reg_stmnt_type_cd': 'Foreign key referencing the code for the type of registration statement', 'last_mod_user_id': 'The user ID that last modified the record', 'lock_control_no': 'A number used for locking purposes'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_registration_statement",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'worker_folder_id': 'Unique identifier for each worker folder', 'fk_worker_gid': 'Foreign key referencing the worker group ID', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_parent_worker_folder_id': 'Foreign key referencing the parent worker folder ID', 'name_tx': 'Name or description of the worker folder', 'lock_control_no': 'A number used for locking purposes', 'display_order_no': 'Number used to determine the display order of the worker folder'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="worker_folder",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_class_id': 'Foreign key referencing the class ID', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'gds_srvc_phrase_tx': 'Text field for the GDS service phrase', 'fk_trademark_gid': 'Foreign key referencing the trademark GID', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_pseudo_class_id': 'Foreign key referencing the pseudo class ID', 'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_pseudo_class",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'renewal_begin_effective_dt': 'Date when the renewal begins to take effect', 'lock_control_no': 'A number used for locking purposes', 'renewal_end_effective_dt': 'Date when the renewal ends and is no longer effective', 'last_mod_ts': 'The timestamp of when the record was last modified', 'sequence_no': 'Sequential number indicating the order of renewal filings', 'renewal_filed_dt': 'Date when the renewal was filed', 'fk_trademark_gid': 'Foreign key referencing the unique identifier of a trademark', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_renewal",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'effective_ts': 'The timestamp of when the record is effective', 'origin_location_tx': 'Text field indicating the origin location of the transaction', 'fk_legacy_transaction_cd': 'Foreign key referencing the legacy transaction code', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'cfk_employee_no': 'Foreign key referencing the employee number', 'details_tx': 'Text field containing details of the transaction', 'terminated_in': 'Indicates where the transaction was terminated', 'transaction_instance_id': 'Identifier for the transaction instance', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'transaction_instance_gid': 'Global identifier for the transaction instance'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="transaction_instance",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'FK_DOCUMENT_COMPONENT_ID': 'Foreign key referencing the document component ID', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'CREATE_TS': 'The timestamp of when the record was created', 'CREATE_USER_ID': 'The user ID that created the record', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'CFK_FORM_PARAGRAPH_VERSION_GID': 'Global ID of the form paragraph version'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="user_para_form_para_ver",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'legacy_og_status_cd': 'Code indicating the status of the trademark publication in the legacy system', 'tm_publication_gid': 'Foreign key referencing the primary key of the publication table', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'og_action_dt': 'Date and time of the action taken on the trademark publication', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'print_mark_description_in': 'Indicator for the description of the mark printed'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_publication",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CREATE_USER_ID': 'The user ID that created the record', 'ENDPOINT_TYPE': 'Type of the endpoint', 'CREATE_TS': 'The timestamp of when the record was created', 'BATCH_NM': 'Name of the batch', 'TARGET_ENDPOINT': 'Endpoint where the data is being sent', 'STATUS_CT': 'Status category of the batch', 'COMPLETED_TS': 'Timestamp when the batch was completed', 'TARGET_ERROR_CODE': 'Error code for any errors encountered during data sending', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'TARGET_ERROR_MSG': 'Error message for any errors encountered during data sending', 'BATCH_DT_NO': 'Number representing the batch date', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'SERIAL_NUM': 'Unique identifier for each record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tmcom_batch_ingest_control",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'hld_exmg_atty_dspl_cnt_in': 'Indicator of whether a held examining attorney disposition is counted', 'second_ea_action_counted_in': 'Indicator of whether the second examining attorney action is counted', 'total_paralegal_actions_no': 'Total number of paralegal actions', 'FRST_PR_PRLGL_ACTN_CNTED_DT': 'Date when the first paralegal action is counted', 'examining_attorney_dspl_cnt_in': 'Indicator of whether an examining attorney disposition is counted', 'EXAMINING_ATTY_DSPL_CNT_IN': 'Indicator of whether an examining attorney disposition is counted', 'first_action_publication_in': 'Indicator of whether a first action has been published', 'frst_pr_paralegal_actn_cnted_dt': 'Date when the first primary paralegal action is counted', 'hld_frst_exmg_atty_actn_cnt_in': 'Indicator of whether the first examining attorney action is held', 'first_ea_action_counted_dt': 'Date when the first examining attorney action is counted', 'fk_trademark_gid': 'Foreign key referencing the unique identifier of a trademark', 'total_examiner_actions_no': 'Total number of examiner actions', 'last_mod_user_id': 'The user ID that last modified the record', 'first_ea_action_counted_in': 'Indicator of whether the first examining attorney action is counted', 'hld_frst_exmg_at_actn_cnt_in': 'Indicator of whether a held first examining attorney action is counted', 'capture_scnd_ea_actn_cntd_in': 'Indicator of whether a second examining attorney action is captured', 'final_refusal_in': 'Indicator of whether a final refusal has been issued', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'first_action_mailed_in': 'Indicator of whether a first action has been mailed', 'lock_control_no': 'A number used for locking purposes', 'last_examiner_action_dt': 'Date of the last examiner action', 'first_para_action_counted_in': 'Indicator of whether the first paralegal action is counted'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_office_actions",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'cfk_receiver_employee_no': 'Foreign key referencing the employee number of the receiver', 'fk_work_item_gid': 'Foreign key referencing the unique identifier of a work item', 'create_user_id': 'The user ID that created the record', 'fk_sequence_no': 'Sequence number indicating the order of the work item', 'create_ts': 'The timestamp of when the record was created', 'last_mod_user_id': 'The user ID that last modified the record', 'lock_control_no': 'A number used for locking purposes', 'receiver_email_addr_tx': 'Email address of the receiver'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="work_item_request_employee",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'lock_control_no': 'A number used for locking purposes', 'fk_parent_work_item_gid': 'Foreign key referencing the parent work item in the work item relationship', 'fk_work_item_relationship_cd': 'Foreign key referencing the code for the type of work item relationship', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_child_work_item_gid': 'Foreign key referencing the child work item in the work item relationship'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="work_item_relationship",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'brs_user_id': 'User ID associated with the worker in the BRS system', 'worker_gid': 'Global ID of the worker', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'worker_no': 'Unique identifier for the worker', 'grade_cd': 'Code representing the grade of the worker', 'signatory_authority_ct': 'Number of signatory authorities held by the worker', 'lock_control_no': 'A number used for locking purposes', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="worker",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'pulldt': 'The date row was pulled from source tables', 'sernum': 'The serial number of load', 'tabname': 'The table_name were row was pulled from', 'actcd': 'The action code associated with the load', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tmapplser",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_object_type_cd': 'The foreign key referencing the code representing the type of object', 'create_ts': 'The timestamp of when the record was created', 'display_order_no': 'The number indicating the display order of the item in the worker folder', 'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_worker_folder_id': 'The foreign key referencing the ID of the worker folder', 'name_tx': 'The name or title of the item in the worker folder', 'create_user_id': 'The user ID that created the record', 'lock_control_no': 'A number used for locking purposes', 'cfk_item_object_id': 'The foreign key referencing the object ID of the item in the worker folder'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="worker_folder_item",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'FK_GDS_SRVC_TERM_SEQUENCE_NO': 'Foreign key referencing the unique identifier of a goods/service term sequence', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'CREATE_USER_ID': 'The user ID that created the record', 'FK_CLASS_ID': 'Foreign key referencing the unique identifier of a class', 'CREATE_TS': 'The timestamp of when the record was created', 'FK_FILING_BASIS_CD': 'Foreign key referencing the filing basis code', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'FK_TRADEMARK_GID': 'Foreign key referencing the unique identifier of a trademark'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_gds_srvc_term_filg_basis",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'cfk_proceeding_no': 'Unique number assigned to each proceeding', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'fk_trademark_gid': 'Foreign key referencing the global identifier of the trademark', 'last_mod_user_id': 'The user ID that last modified the record', 'tm_proceeding_id': 'Unique identifier for each trademark proceeding'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_proceeding",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'status_ct': 'Status category of the user session', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'cfk_empe_no': 'Employee number of the user', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'user_session_gid': 'Global ID of the user session'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="user_session",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_parent_trademark_gid': "Foreign key referencing the parent trademarks global ID", 'create_user_id': 'The user ID that created the record', 'lock_control_no': 'A number used for locking purposes', 'fk_relationship_type_cd': 'Foreign key referencing the relationship type code', 'fk_related_trademark_gid': "Foreign key referencing the related trademarks global ID"}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_relationship",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'sequence_no': 'Number indicating the sequence of the literal element', 'literal_element_tx': 'Textual representation of the literal element', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_literal",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'bar_membership_year_no': 'Number representing the year of bar membership', 'fk_tm_party_role_cd': 'Foreign key referencing the party role code table', 'party_role_sequence_no': 'Sequence number for the party role', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'fk_interested_party_gid': 'Foreign key referencing the interested party table', 'tm_party_role_id': 'Unique identifier for a party role', 'bar_membership_state_nm': 'Name of the state of bar membership', 'fk_trademark_gid': 'Foreign key referencing the trademark table', 'bar_membership_month_no': 'Number representing the month of bar membership', 'bar_membership_state_cd': 'Code representing the state of bar membership', 'lock_control_no': 'A number used for locking purposes', 'cfk_patron_id': 'Foreign key referencing the patron table', 'bar_information_tx': 'Text field for bar information', 'last_mod_user_id': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_party_role",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'lock_control_no': 'A number used for locking purposes', 'work_item_gid': 'Unique identifier for each work item', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'fk_work_item_type_cd': 'Foreign key referencing the work item type'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="work_item",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_trademark_gid': 'Foreign key referencing the ID of the trademark', 'lock_control_no': 'A number used for locking purposes', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'fk_tm_group_id': 'Foreign key referencing the ID of the trademark group'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_group_item",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'fk_tm_party_role_id': 'Foreign key referencing the primary key of the party role table', 'fk_telecom_address_gid': 'Foreign key referencing the primary key of the telecom address table', 'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'primary_in': 'Indicator for whether the telecom address is the primary address for the party role', 'create_ts': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_telecom_addr",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'pseudo_mark_tx': 'Text representing the pseudo mark', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'create_ts': 'The timestamp of when the record was created', 'sequence_no': 'Number indicating the order of the pseudo mark'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_pseudo_mark",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'error_msg': 'Message describing the error that occurred', 'callstack': 'Stack trace information for debugging purposes', 'insert_ts': 'Timestamp when the record was inserted into the table', 'backtrace': 'Stack trace information for debugging purposes', 'error_num': 'Numeric code representing the type of error'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="trigger_exceptions",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'display_order_no': 'Number indicating the display order of the folder', 'fk_work_item_gid': 'Global identifier for the associated work item', 'dn_object_type_cd': 'Code indicating the type of the associated object', 'evidence_bin_folder_id': 'Unique identifier for each evidence bin folder', 'cfk_object_gid': 'Global identifier for the associated object', 'fk_parent_evidence_bin_fldr_id': 'Foreign key referencing the parent evidence bin folder', 'folder_nm': 'Name of the evidence bin folder', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'fk_evidence_bin_cd': 'Foreign key referencing the evidence bin code', 'last_mod_user_id': 'The user ID that last modified the record', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="evidence_bin_folder_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'current_in': 'Indicator of whether the filing basis is currently active or not', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_filing_basis_cd': 'Foreign key referencing the primary key of the filing basis code table', 'filed_in': 'Indicator of whether the filing basis has been filed or not', 'amended_in': 'Indicator of whether the filing basis has been amended or not', 'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_filing_basis_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'reply_by_dt': 'Timestamp indicating the deadline for reply', 'create_user_id': 'The user ID that created the record', 'payment_reference_no': 'Reference number for payment', 'status_cd': 'Status code for the application', 'status_dt': 'Timestamp indicating the status date', 'create_ts': 'The timestamp of when the record was created', 'international_application_gid': 'Unique identifier for international applications', 'international_us_ref_no': 'Reference number for international applications in the US', 'original_filing_dt': 'Timestamp indicating the original filing date', 'fk_electronic_address_gid': 'Foreign key referencing the electronic address table', 'payment_type_ct': 'COMMENT REQUIRED', 'automatic_certification_in': 'Indicator for automatic certification', 'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'last_mod_user_id': 'The user ID that last modified the record', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="international_application_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_mailing_address_gid': 'Foreign key referencing the primary key of the mailing address table', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'address_line_ct': 'Code indicating the type of address line (e.g. street, city, state)', 'address_line_tx': 'Text value of the address line', 'sequence_no': 'Number indicating the order of the address lines for a specific mailing address', 'create_user_id': 'The user ID that created the record', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'action_ct': 'The action category executed on the record', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="mailing_address_line_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'effective_dt': 'The timestamp of when the record is effective', 'cfk_proceeding_gid': 'Foreign key referencing the proceeding table', 'last_mod_ts': 'The timestamp of when the record was last modified', 'cfk_employee_no': 'Foreign key referencing the employee table', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'fk_prcdng_employee_role_cd': 'Foreign key referencing the employee role code table', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'action_ct': 'The action category executed on the record', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="prcdng_employee_assignment_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'country_cd': 'Code representing the country of the foreign trademark', 'create_user_id': 'The user ID that created the record', 'cfk_geographic_region_cd': 'Code representing the geographic region of the foreign trademark', 'create_ts': 'The timestamp of when the record was created', 'foreign_tm_appl_num': 'Application number of the foreign trademark', 'lock_control_no': 'A number used for locking purposes', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'fk_class_id': 'Foreign key referencing the primary key of the class table', 'dn_geographic_region_nm': 'Name of the geographic region of the foreign trademark', 'priority_claimed_in': 'Indicator for the country where priority is claimed for the foreign trademark', 'foreign_renewal_expiration_dt': 'Date when the renewal of the foreign trademark expires', 'foreign_registration_dt': 'Date when the foreign trademark was registered', 'foreign_filing_dt': 'Date when the foreign trademark was filed', 'country_nm': 'Name of the country of the foreign trademark', 'foreign_renewal_num': 'Renewal number of the foreign trademark', 'foreign_tm_reg_num': 'Registration number of the foreign trademark', 'foreign_expiration_dt': 'Date when the foreign trademark expires', 'last_mod_ts': 'The timestamp of when the record was last modified', 'foreign_renewal_effective_dt': 'Date when the renewal of the foreign trademark becomes effective', 'sequence_no': 'Number indicating the order of the foreign basis', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_foreign_basis_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'partial_refusal_in': 'Indicator for partial refusal', 'response_on_time_in': 'Indicator for response received on time', 'fk_work_item_gid': 'Foreign key referencing the unique identifier of a work item', 'response_received_in': 'Indicator for response received', 'action_no': 'Number indicating the action taken', 'issue_empe_no': 'Employee number associated with the issue', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'issue_dt': 'Date and time when the issue occurred', 'full_refusal_override_in': 'Indicator for full refusal override', 'last_mod_ts': 'The timestamp of when the record was last modified', 'partial_abandonment_ovrd_in': 'Indicator for partial abandonment override', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'examination_no': 'Number indicating the examination', 'partial_abandonment_in': 'Indicator for partial abandonment', 'action_ct': 'The action category executed on the record', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="office_activity_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'fk_design_search_group_cd': 'Foreign key referencing the primary key of the design search group code table', 'create_user_id': 'The user ID that created the record', 'lock_control_no': 'A number used for locking purposes', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'action_ct': 'The action category executed on the record', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_design_element_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'lock_control_no': 'A number used for locking purposes', 'cfk_object_gid': 'Foreign key referencing the unique identifier of an object', 'cfk_assigning_employee_no': 'Foreign key referencing the employee number of the assigning person', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'cfk_assignee_employee_no': 'Foreign key referencing the employee number of the assignee', 'last_mod_user_id': 'The user ID that last modified the record', 'effective_dt': 'The timestamp of when the record is effective', 'cfk_organization_cd': 'Foreign key referencing the code of an organization', 'docket_item_id': 'Unique identifier for a docket item', 'fk_work_item_gid': 'Foreign key referencing the unique identifier of a work item', 'fk_docket_id': 'Foreign key referencing the unique identifier of a docket', 'action_ct': 'The action category executed on the record', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="docket_item_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'FK_ELECTRONIC_ADDRESS_GID': 'Foreign key referencing the electronic address table', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'CREATE_USER_ID': 'The user ID that created the record', 'CREATE_TS': 'The timestamp of when the record was created', 'FK_SUBMISSION_GID': 'Foreign key referencing the submission table', 'PRIMARY_IN': 'Indicator for primary electronic address', 'BEGIN_EFFECTIVE_TS': 'The timestamp of when the record began its effectiveness', 'CFK_TRANSACTION_INSTANCE_GID': 'The foreign key referencing the unique identifier of the transaction instance', 'ACTION_CT': 'The action category executed on the record', 'END_EFFECTIVE_TS': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="submission_elctrn_addr_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'fk_object_type_cd': 'Foreign key referencing the object type code', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_tm_document_id': 'Foreign key referencing the document ID in the TM system', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'cfk_object_gid': 'Composite foreign key referencing the object global ID', 'action_ct': 'The action category executed on the record', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="object_document_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'lock_control_no': 'A number used for locking purposes', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_submission_gid': 'Foreign key referencing the submission associated with the submission item', 'submission_item_gid': 'Unique identifier for each submission item', 'fk_work_item_gid': 'Foreign key referencing the work item associated with the submission item', 'create_user_id': 'The user ID that created the record', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'action_ct': 'The action category executed on the record', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'end_effective_ts': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="submission_item_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'submitted_telecom_no': 'Telephone number submitted for the telecom address', 'telecom_no': 'Telephone number for the telecom address', 'lock_control_no': 'A number used for locking purposes', 'extension_no': 'Extension number for the telecom address', 'fk_telecom_type_cd': 'Foreign key referencing the type of telecom address', 'fk_telecom_format_cd': 'Foreign key referencing the format of the telecom address', 'telecom_address_gid': 'Unique identifier for each telecom address', 'last_mod_user_id': 'The user ID that last modified the record', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="telecom_address_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'mailroom_received_dt': 'Date and time when the trademark was received in the mailroom', 'fk_sequence_no': 'Foreign key referencing the sequence number of the trademark table', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_tm_divisional_status_cd': 'Foreign key referencing the divisional status code of the trademark table', 'unit_received_dt': 'Date and time when the trademark was received in the unit', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'fk_child_trademark_gid': 'Foreign key referencing the primary key of the child trademark table', 'lock_control_no': 'A number used for locking purposes', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'last_mod_user_id': 'The user ID that last modified the record', 'tm_divisional_status_dt': 'Date and time when the divisional status of the trademark was recorded', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'action_ct': 'The action category executed on the record', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'end_effective_ts': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_divisional_child_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'status_cd': 'Code indicating the status of the international registration', 'last_mod_user_id': 'The user ID that last modified the record', 'cancellation_dt': 'Timestamp indicating the date and time of cancellation', 'fk_international_reg_gid': 'Foreign key referencing the unique identifier of an international registration', 'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'ib_renewal_dt': 'Timestamp indicating the date and time of renewal for an international registration', 'priority_claimed_dt': 'Timestamp indicating the date and time when priority was claimed', 'ib_publication_dt': 'Timestamp indicating the date and time of publication for an international registration', 'status_dt': 'Timestamp indicating the date and time of the status update', 'notification_dt': 'Timestamp indicating the date and time of notification', 'auto_protect_dt': 'Timestamp indicating the date and time when automatic protection was granted', 'fk_trademark_gid': 'Foreign key referencing the unique identifier of a trademark', 'first_refusal_in': 'Indicator of whether a first refusal right is applicable', 'last_mod_ts': 'The timestamp of when the record was last modified', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="international_reg_tm_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'lock_control_no': 'A number used for locking purposes', 'fk_tm_publication_gid': 'Foreign key referencing the unique identifier of the TM publication', 'og_registration_no': 'Registration number assigned to the OG publication', 'create_ts': 'The timestamp of when the record was created', 'last_mod_user_id': 'The user ID that last modified the record', 'record_no': 'Sequential number assigned to each record', 'fk_og_publication_gid': 'Foreign key referencing the unique identifier of the OG publication', 'publication_notice_dt': 'Date and time when the publication notice was made', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="og_publication_tm_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'event_deadline_dt': 'The event item deadline date', 'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'cfk_assignee_employee_no': 'Foreign key referencing the employee number of the assignee', 'create_ts': 'The timestamp of when the record was created', 'fk_docket_item_event_type_cd': 'Foreign key referencing the event type code of the docket item', 'last_mod_user_id': 'The user ID that last modified the record', 'event_dt': 'The date of the docket item event', 'fk_docket_item_id': 'Foreign key referencing the docket item ID', 'event_goal_dt': 'The date of the docket item goal', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="docket_item_event_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'concurrent_use_month_no': 'Month number indicating the concurrent use', 'last_mod_user_id': 'The user ID that last modified the record', 'lock_control_no': 'A number used for locking purposes', 'statement_no': 'Sequential number assigned to each statement', 'concurrent_use_status_ct': 'Category indicating the status of concurrent use', 'concurrent_use_basis_ct': 'Category indicating the basis of concurrent use', 'create_ts': 'The timestamp of when the record was created', 'concurrent_use_year_no': 'Year number indicating the concurrent use', 'last_mod_ts': 'The timestamp of when the record was last modified', 'statement_tx': 'The descriptive statement text', 'concurrent_use_day_no': 'Day number indicating the concurrent use', 'create_user_id': 'The user ID that created the record', 'fk_trademark_gid': 'Foreign key referencing the unique identifier of a trademark', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'action_ct': 'The action category executed on the record', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="concurrent_use_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'FK_TRADEMARK_GID': 'Foreign key referencing the primary key of the trademark table', 'CREATE_TS': 'The timestamp of when the record was created', 'CREATE_USER_ID': 'The user ID that created the record', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'FK_PRIOR_REG_TRADEMARK_GID': 'Foreign key referencing the primary key of the prior registered trademark table', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'END_EFFECTIVE_TS': 'The timestamp of when the record is no longer effective', 'ACTION_CT': 'The action category executed on the record', 'BEGIN_EFFECTIVE_TS': 'The timestamp of when the record began its effectiveness', 'CFK_TRANSACTION_INSTANCE_GID': 'The foreign key referencing the unique identifier of the transaction instance'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="section_2f_prior_reg_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'individual_middle_nm': 'Middle name of the individual', 'country_nm': 'Name of the country associated with the interested party', 'country_cd': 'Country code associated with the interested party', 'individual_minor_in': 'Indicates if the individual is a minor', 'lock_control_no': 'A number used for locking purposes', 'country_role_ct': 'Role of the country in relation to the interested party', 'individual_family_nm': 'Family name of the individual', 'individual_prefix_nm': "Prefix of the individuals name", 'party_composition_tx': 'Composition details of the interested party', 'fk_primary_electronic_addr_gid': 'Foreign key referencing the primary electronic address of the interested party', 'geographic_region_nm': 'Name of the geographic region of the interested party', 'interested_party_ct': 'Type of interested party (e.g. individual, organization)', 'legal_entity_statement_tx': 'Statement provided by the legal entity', 'last_mod_ts': 'The timestamp of when the record was last modified', 'preferred_contact_method_ct': 'Preferred method of contact for the interested party', 'fk_legal_entity_type_cd': 'Foreign key referencing the legal entity type of the interested party', 'interested_party_gid': 'Unique identifier for each interested party', 'individual_suffix_nm': "Suffix of the individuals name", 'individual_given_nm': 'Given name of the individual', 'last_mod_user_id': 'The user ID that last modified the record', 'geographic_region_cd': 'Code representing the geographic region of the interested party', 'fk_primary_telecom_addr_gid': 'Foreign key referencing the primary telecom address of the interested party', 'create_user_id': 'The user ID that created the record', 'interested_party_nm': 'Name of the interested party', 'action_ct': 'The action category executed on the record', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="interested_party_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_tm_document_id': 'Foreign key referencing the TM document associated with the evidence document', 'evidence_document_alias_nm': 'Alternate name or alias for the evidence document', 'evidence_document_id': 'Unique identifier for each evidence document', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'fk_evidence_source_category_cd': 'Foreign key referencing the category code of the evidence source', 'fk_evidence_bin_folder_id': 'Foreign key referencing the bin folder where the evidence document is stored', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_sequence_no': 'Foreign key referencing the sequence number of the evidence document', 'display_order_no': 'Number indicating the display order of the evidence document', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'action_ct': 'The action category executed on the record', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="evidence_document_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'create_user_id': 'The user ID that created the record', 'section_2f_basis_ct': 'Text field indicating the basis for claiming eligibility under Section 2(f)', 'limitation_tx': 'Text field indicating any limitations or restrictions on the trademark', 'section_2f_ct': 'Text field indicating whether the trademark is eligible for registration under Section 2(f)', 'restrict_tx': 'Text field indicating any restrictions on the use or registration of the trademark', 'action_ct': 'The action category executed on the record', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="section_2f_statement_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'status_cd': 'Code indicating the status of the international registration', 'status_dt': 'Date and time when the status of the international registration was last updated', 'ib_publication_dt': 'Date and time when the international registration was published', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'fk_international_reg_gid': 'Foreign key referencing the unique identifier of the international registration', 'ib_renewal_dt': 'Date and time when the international registration is scheduled for renewal', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_international_appl_gid': 'Foreign key referencing the unique identifier of the international application', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="international_appl_reg_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_electronic_address_gid': 'Foreign key referencing the global ID of an electronic address in another table', 'lock_control_no': 'A number used for locking purposes', 'authorized_email_in': 'Indicates whether the email address is authorized or not', 'fk_tm_party_role_id': 'Foreign key referencing the party role ID in another table', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'primary_in': 'Indicates whether the email address is the primary one for the party role', 'last_mod_user_id': 'The user ID that last modified the record', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'action_ct': 'The action category executed on the record', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_electronic_addr_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_tm_employee_role_cd': 'Foreign key referencing the code representing the role of the employee in the tm_employee_assignment table', 'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'effective_dt': 'The timestamp of when the record is effective', 'cfk_employee_no': 'Foreign key referencing the unique identifier of the employee in the tm_employee_assignment table', 'fk_trademark_gid': 'Foreign key referencing the unique identifier of the trademark in the tm_employee_assignment table', 'create_user_id': 'The user ID that created the record', 'action_ct': 'The action category executed on the record', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_employee_assignment_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'order_no': 'Number indicating the order of the statement', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'statement_tx': 'The descriptive statement text', 'last_mod_user_id': 'The user ID that last modified the record', 'lock_control_no': 'A number used for locking purposes', 'actv_pr_other_prior_reg_in': 'Indicator for active prior registration in other countries', 'fk_statement_type_cd': 'Foreign key referencing the primary key of the statement type code table', 'last_mod_ts': 'The timestamp of when the record was last modified', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_additional_statement_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'mailing_address_gid': 'Unique identifier for the mailing address', 'country_nm': 'Name of the country', 'department_nm': 'Name of the department', 'lock_control_no': 'A number used for locking purposes', 'country_cd': 'Code representing the country', 'address_type_ct': 'Type of the address', 'name_line_1_tx': 'First line of the name', 'name_line_2_tx': 'Second line of the name', 'postal_cd': 'Postal code or ZIP code', 'create_ts': 'The timestamp of when the record was created', 'last_mod_user_id': 'The user ID that last modified the record', 'geographic_region_cd': 'Code representing the geographic region', 'geographic_region_nm': 'Name of the geographic region', 'create_user_id': 'The user ID that created the record', 'street_line_1_tx': 'First line of the street address', 'street_line_2_tx': 'Second line of the street address', 'city_nm': 'Name of the city', 'last_mod_ts': 'The timestamp of when the record was last modified', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="mailing_address_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'fsm_instance_gid': 'Unique identifier for each FSM instance', 'fk_root_fsm_instance_gid': 'Foreign key referencing the root FSM instance', 'depth_no': 'Number indicating the depth of the FSM instance in the hierarchy', 'terminated_in': 'String indicating where the FSM instance was terminated', 'fk_fsm_type_id': 'Foreign key referencing the FSM type', 'suspended_no': 'Number indicating if the FSM instance is suspended (1) or not (0)', 'fk_current_fsm_type_state_id': 'Foreign key referencing the current FSM type state', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_parent_fsm_instance_gid': 'Foreign key referencing the parent FSM instance', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="fsm_instance_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'three_dimension_in': 'Indicates whether the drawing is three-dimensional or not', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'last_mod_user_id': 'The user ID that last modified the record', 'color_in': 'Indicates if color is in the drawing', 'create_user_id': 'The user ID that created the record', 'spcl_form_filed_3d_drawing_in': 'Indicates whether a special form was filed for a three-dimensional drawing', 'spcl_form_fild_color_dwg_in': 'Indicates whether a special form was filed for a color drawing', 'color_claim_tx': 'Textual description of the claimed colors in the drawing', 'lock_control_no': 'A number used for locking purposes', 'create_ts': 'The timestamp of when the record was created', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'action_ct': 'The action category executed on the record', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_drawing_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'abandonment_date_override_in': 'Override value for the abandonment date', 'fk_response_issue_cd': 'Foreign key referencing the code for the response issue', 'response_received_override_in': 'Override value for the response received time', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_work_item_gid': 'Foreign key referencing the unique identifier of the work item', 'abandonment_dt': 'The abandonment date', 'lock_control_no': 'A number used for locking purposes', 'response_issue_tx': 'Text description of the response issue', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'response_received_in': 'Time taken to receive a response for the item', 'response_on_time_override_in': 'The response on-time override indicator', 'response_on_time_in': 'Indicates if the response was received on time or not', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'action_ct': 'The action category executed on the record', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="abandonment_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'filing_dt': 'Timestamp indicating the date and time of submission filing', 'create_user_id': 'The user ID that created the record', 'fk_submission_form_type_id': 'Foreign key referencing the submission form type ID', 'fk_submission_method_cd': 'Foreign key referencing the submission method code', 'status_ct': 'Current status of the submission', 'response_in': 'Indicates if a response has been received for the submission', 'last_mod_user_id': 'The user ID that last modified the record', 'submission_gid': 'Unique identifier for each submission', 'create_ts': 'The timestamp of when the record was created', 'received_dt': 'Timestamp indicating the date and time of submission receipt', 'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="submission_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CREATE_USER_ID': 'The user ID that created the record', 'FK_SUBMISSION_GID': 'Foreign key referencing the submission table', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'SEQUENCE_NO': 'Sequential number indicating the order of the averments', 'NON_STANDARD_AVERMENT_TX': 'Text field for non-standard averments', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'FK_AVERMENT_ID': 'Foreign key referencing the averment table', 'CREATE_TS': 'The timestamp of when the record was created', 'END_EFFECTIVE_TS': 'The timestamp of when the record is no longer effective', 'BEGIN_EFFECTIVE_TS': 'The timestamp of when the record began its effectiveness', 'CFK_TRANSACTION_INSTANCE_GID': 'The foreign key referencing the unique identifier of the transaction instance', 'ACTION_CT': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="submission_averment_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_trademark_gid': 'Foreign key referencing the unique identifier of a trademark', 'create_user_id': 'The user ID that created the record', 'fk_international_appl_gid': 'Foreign key referencing the unique identifier of an international application', 'action_ct': 'The action category executed on the record', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'end_effective_ts': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="base_application_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'fk_electronic_addr_type_cd': 'Foreign key referencing the electronic address type', 'electronic_address_gid': 'Unique identifier for each electronic address', 'electronic_addr_locator_tx': 'The actual electronic address (e.g. email address, phone number)', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'action_ct': 'The action category executed on the record', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'end_effective_ts': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="electronic_address_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'lock_control_no': 'A number used for locking purposes', 'fk_interested_party_gid': 'Foreign key referencing the interested party global ID', 'create_ts': 'The timestamp of when the record was created', 'fk_assumed_name_type_cd': 'Foreign key referencing the code for the type of assumed name', 'intrstd_party_assumed_name_id': 'Unique identifier for the interested party assumed name', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'assumed_nm': 'The assumed name of the interested party', 'last_mod_ts': 'The timestamp of when the record was last modified', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'action_ct': 'The action category executed on the record', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'end_effective_ts': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="interested_party_assumed_nm_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CREATE_USER_ID': 'The user ID that created the record', 'FK_MEMBER_INTERESTED_PARTY_GID': "Foreign key referencing the individuals interested party global identifier", 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'FK_IP_RELTNSP_TYPE_CD': 'Foreign key referencing the relationship type code for interested party relationships', 'CREATE_TS': 'The timestamp of when the record was created', 'FK_INTERESTED_PARTY_GID': "Foreign key referencing the interested individuals global identifier", 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'END_EFFECTIVE_TS': 'The timestamp of when the record is no longer effective', 'ACTION_CT': 'The action category executed on the record', 'BEGIN_EFFECTIVE_TS': 'The timestamp of when the record began its effectiveness', 'CFK_TRANSACTION_INSTANCE_GID': 'The foreign key referencing the unique identifier of the transaction instance'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="intrstd_party_relationship_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'CREATE_TS': 'The timestamp of when the record was created', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'FK_PRIOR_REG_TRADEMARK_GID': 'Foreign key referencing the unique identifier of a prior registered trademark in another table', 'FK_TRADEMARK_GID': 'Foreign key referencing the unique identifier of a trademark in another table', 'CREATE_USER_ID': 'The user ID that created the record', 'FK_STATEMENT_TYPE_CD': 'Foreign key referencing the code representing the type of statement in another table', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'FK_ORDER_NO': 'Foreign key referencing the order number in another table', 'BEGIN_EFFECTIVE_TS': 'The timestamp of when the record began its effectiveness', 'CFK_TRANSACTION_INSTANCE_GID': 'The foreign key referencing the unique identifier of the transaction instance', 'action_ct': 'The action category executed on the record', 'END_EFFECTIVE_TS': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_addl_stmnt_prior_reg_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'FK_GOODS_SERVICES_TERM_ID': 'The foreign key referencing the ID of the goods and services term associated with the trademark.', 'FIRST_USE_IN_COMMERCE_MONTH_NO': 'Number indicating the month of first use in commerce', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'GDS_SRVC_TERM_TX': 'Text describing the goods and services term', 'FK_GDS_SRVC_STATUS_RSN_CD': 'Foreign key referencing the goods and services status reason code', 'CREATE_USER_ID': 'The user ID that created the record', 'FK_CLASS_ID': 'Foreign key referencing the class identifier', 'SEQUENCE_NO': 'Number indicating the sequence of the record', 'CREATE_TS': 'The timestamp of when the record was created', 'INTENT_TO_USE_DT': 'Timestamp indicating the intent to use date', 'FIRST_USE_IN_COMMERCE_DAY_NO': 'Number indicating the day of first use in commerce', 'FIRST_USE_ANYWHERE_MONTH_NO': 'Number indicating the month of first use anywhere', 'FK_STMNT_ACTVTY_TYPE_CD': 'Foreign key referencing the statement activity type code', 'SUGGESTED_GDS_SRVC_TERM_TX': 'Text describing the suggested goods and services term', 'FK_TRADEMARK_GID': 'Foreign key referencing the trademark global identifier', 'FK_GDS_SRVC_STATUS_CD': 'Foreign key referencing the goods and services status code', 'FIRST_USE_ANYWHERE_DAY_NO': 'Number indicating the day of first use anywhere', 'FIRST_USE_IN_COMMERCE_YEAR_NO': 'Number indicating the year of first use in commerce', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'FIRST_USE_ANYWHERE_YEAR_NO': 'Number indicating the year of first use anywhere', 'ACTION_CT': 'The action category executed on the record', 'END_EFFECTIVE_TS': 'The timestamp of when the record is no longer effective', 'BEGIN_EFFECTIVE_TS': 'The timestamp of when the record began its effectiveness', 'CFK_TRANSACTION_INSTANCE_GID': 'The foreign key referencing the unique identifier of the transaction instance'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_class_gds_srvc_term_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'source_ct': 'Source country', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'international_reg_no': 'International registration number', 'lock_control_no': 'A number used for locking purposes', 'international_reg_dt': 'Date of international registration', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'action_ct': 'The action category executed on the record', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="international_tm_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'lock_control_no': 'A number used for locking purposes', 'sequence_no': 'Number indicating the order of the divisional trademark', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'create_ts': 'The timestamp of when the record was created', 'action_ct': 'The action category executed on the record', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'end_effective_ts': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_divisional_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'first_use_anywhere_day_no': 'Day number indicating the first use anywhere', 'first_use_in_commerce_day_no': 'Day number indicating the first use in commerce', 'first_use_in_commerce_month_no': 'Month number indicating the first use in commerce', 'first_use_anywhere_year_no': 'Year number indicating the first use anywhere', 'fk_trademark_gid': 'Foreign key referencing the trademark GID in another table', 'first_use_anywhere_month_no': 'Month number indicating the first use anywhere', 'gds_srvcs_stmnt_tx': 'Text field for goods and services statement', 'intent_to_use_dt': 'Timestamp indicating the intent to use date', 'lock_control_no': 'A number used for locking purposes', 'fk_tm_class_status_cd': 'Foreign key referencing the status code in another table', 'gds_srvcs_stmnt_annotated_tx': 'Text field for annotated goods and services statement', 'create_ts': 'The timestamp of when the record was created', 'fk_class_id': 'Foreign key referencing the class ID in another table', 'first_use_in_commerce_year_no': 'Year number indicating the first use in commerce', 'last_mod_user_id': 'The user ID that last modified the record', 'status_dt': 'Timestamp indicating the status date', 'last_mod_ts': 'The timestamp of when the record was last modified', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_class_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'lock_control_no': 'A number used for locking purposes', 'international_reg_gid': 'Unique identifier for international registration', 'fk_international_reg_no': 'Foreign key referencing international registration number', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'international_reg_seq_no': 'Sequential number for international registration', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'action_ct': 'The action category executed on the record', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="international_registration_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'lock_control_no': 'A number used for locking purposes', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'publication_dt': 'Date and time of publication', 'og_publication_gid': 'Unique identifier for each publication', 'last_mod_ts': 'The timestamp of when the record was last modified', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'action_ct': 'The action category executed on the record', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'end_effective_ts': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="og_publication_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_class_id': 'Foreign key referencing the primary key of the class table', 'last_mod_user_id': 'The user ID that last modified the record', 'lock_control_no': 'A number used for locking purposes', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'create_ts': 'The timestamp of when the record was created', 'fk_referenced_class_id': 'Foreign key referencing the primary key of the referenced class table', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'action_ct': 'The action category executed on the record', 'end_effective_ts': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_class_reference_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'fk_office_activity_reason_cd': 'Foreign key referencing the code of an office activity reason', 'lock_control_no': 'A number used for locking purposes', 'fk_work_item_gid': 'Foreign key referencing the unique identifier of a work item', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'action_ct': 'The action category executed on the record', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="office_activity_reason_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'primary_in': 'Indicator for whether the telecom address is the primary address for the party role', 'create_ts': 'The timestamp of when the record was created', 'fk_telecom_address_gid': 'Foreign key referencing the primary key of the telecom address table', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_tm_party_role_id': 'Foreign key referencing the primary key of the party role table', 'create_user_id': 'The user ID that created the record', 'lock_control_no': 'A number used for locking purposes', 'action_ct': 'The action category executed on the record', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_telecom_addr_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'fk_parent_work_item_gid': 'Foreign key referencing the parent work item in the work item relationship', 'fk_child_work_item_gid': 'Foreign key referencing the child work item in the work item relationship', 'fk_work_item_relationship_cd': 'Foreign key referencing the code for the type of work item relationship', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'action_ct': 'The action category executed on the record', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="work_item_relationship_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'sequence_no': 'Sequential number assigned to each registration statement', 'statement_day_no': 'Day of the registration statement', 'fk_reg_stmnt_type_cd': 'Foreign key referencing the code for the type of registration statement', 'statement_tx': 'The descriptive statement text', 'create_user_id': 'The user ID that created the record', 'statement_year_no': 'Year of the registration statement', 'fk_trademark_gid': 'Foreign key referencing the unique identifier of a trademark', 'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'create_ts': 'The timestamp of when the record was created', 'statement_month_no': 'Month of the registration statement', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'action_ct': 'The action category executed on the record', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_registration_statement_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'first_use_day_no': 'Numeric value representing the day of first use', 'last_mod_user_id': 'The user ID that last modified the record', 'statement_tx': 'The descriptive statement text', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_trademark_gid': 'Foreign key referencing the unique identifier of a trademark', 'preformatted_text_in': 'Text input that has been preformatted', 'lock_control_no': 'A number used for locking purposes', 'fk_class_statement_type_cd': 'Foreign key referencing the code for the type of class statement', 'fk_class_id': 'Foreign key referencing the unique identifier of a class', 'first_use_year_no': 'Numeric value representing the year of first use', 'first_use_month_no': 'Numeric value representing the month of first use', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="use_in_another_form_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'NOA_ISSUED_IN': 'Indicator for notice of allowance issued', 'EXTENSIONS_NOT_ALLOWED_IN': 'Indicator for whether extensions not allowed', 'APPLICATION_MARK_IN_1': 'First application mark', 'LAST_POSSIBLE_EXTENSION_DT': 'Last possible extension date', 'AMENDMENT_TO_USE_FILED_IN': 'Date when amendment to use was filed', 'FINAL_ACTION_REFUSAL_ATU_IN': 'Final action refusal for ATU', 'SOU_EXT_DENIAL_LTR_MAILED_IN': 'Indicates a statement of use extension denial letter mailed', 'LATEST_ITU_FILNG_RECEIVED_DT': 'Latest ITU filing received date', 'FK_TRADEMARK_GID': 'Foreign key for trademark', 'POTENTIEL_ABANDONMENT_DT': 'The potential date of abandonment for the trademark application', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'AVAILABLE_FOR_SOU_IN': 'Available for statement of use', 'ITU_CASE_PUBD_FOR_OPSTN_IN': 'Indicates an ITU case published for opposition', 'USE_AFFIDAVIT_PRCSG_COMPLT_IN': 'Indicator for use affidavit processing complete', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'LAST_UA_TRAN_INFRML_RSP_RCV_IN': 'Indicates an last UA transaction informal response received', 'LAST_EXT_TRAN_DNIL_LTR_PREP_IN': 'Last extension transaction denial letter prepared', 'SOU_EXTENSION_REQ_FILED_IN': 'Indicates if a request for extension of the statement of use has been filed', 'SOU_RECEIVED_DT': 'The date when the statement of use was received', 'ITU_FREEZE_PERIOD_IN': 'Indicates an ITU freeze period', 'LAST_UA_TRAN_INFRML_LTR_ML_IN': 'Indicates last UA transaction informal letter mailed', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'FIRST_ACTION_REFUSAL_ATU_IN': 'First action refusal for ATU', 'CREATE_USER_ID': 'The user ID that created the record', 'CREATE_TS': 'The timestamp of when the record was created', 'LAST_EXT_TRAN_SOU_EXT_FILED_IN': 'Indicates a last extension transaction statement of use extension has been filed', 'APPLICATION_MARK_IN_2': 'Second application mark', 'NOA_MAILED_IN': 'Indicates if a notice of allowance has been mailed', 'HOLD_FIRST_ACTION_RFSL_ATU_IN': 'Indicator for hold first action refusal for ATU', 'END_EFFECTIVE_TS': 'The timestamp of when the record is no longer effective', 'BEGIN_EFFECTIVE_TS': 'The timestamp of when the record began its effectiveness', 'CFK_TRANSACTION_INSTANCE_GID': 'The foreign key referencing the unique identifier of the transaction instance', 'ACTION_CT': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_itu_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'cfk_asgnd_exam_law_ofc_org_cd': 'Code representing the assigned examination law office organization', 'fk_current_location_cd': 'Foreign key referencing the current location code of the trademark', 'fk_physical_location_cd': 'Foreign key referencing the physical location code of the trademark', 'physical_location_dt': "Date when the trademarks physical location was recorded", 'lock_control_no': 'A number used for locking purposes', 'case_reported_lost_in': 'Indicator for if the case was reported lost', 'current_location_dt': "Date when the trademarks current location was recorded", 'fk_charge_to_location_cd': 'Foreign key referencing the location code where the charge is assigned to', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'cfk_charge_to_worker_no': 'Code representing the worker number associated with the charge', 'fk_trademark_gid': 'Foreign key referencing the unique identifier of a trademark', 'case_reported_lost_dt': 'Date when the case was reported lost', 'last_mod_ts': 'The timestamp of when the record was last modified', 'official_search_in_progress_in': 'Location where an official search is currently in progress', 'action_ct': 'The action category executed on the record', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'end_effective_ts': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_locations_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'sequence_no': 'Sequential number indicating the order of renewal filings', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_trademark_gid': 'Foreign key referencing the unique identifier of a trademark', 'lock_control_no': 'A number used for locking purposes', 'last_mod_ts': 'The timestamp of when the record was last modified', 'renewal_end_effective_dt': 'Date when the renewal ends and is no longer effective', 'create_ts': 'The timestamp of when the record was created', 'renewal_filed_dt': 'Date when the renewal was filed', 'renewal_begin_effective_dt': 'Date when the renewal begins to take effect', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'action_ct': 'The action category executed on the record', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_renewal_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'legacy_des_cd': 'Code representing the legacy description of the publication', 'fk_publication_subcategory_cd': 'Foreign key referencing the subcategory code of the publication', 'create_user_id': 'The user ID that created the record', 'fk_tm_publication_gid': 'Foreign key referencing the global ID of the publication', 'fk_publication_category_cd': 'Foreign key referencing the category code of the publication', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'last_mod_ts': 'The timestamp of when the record was last modified', 'action_ct': 'The action category executed on the record', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_publication_subct_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_work_item_gid': 'Foreign key referencing the unique identifier of a work item', 'lock_control_no': 'A number used for locking purposes', 'fk_object_type_cd': 'Foreign key referencing the code representing the type of object', 'create_user_id': 'The user ID that created the record', 'cfk_object_gid': 'Foreign key referencing the unique identifier of an object', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="work_item_object_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_mark_type_cd': 'Foreign key referencing the primary key of the mark type code table', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'action_ct': 'The action category executed on the record', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_mark_type_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'milestone_dt': 'Date and time when the milestone was achieved', 'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'create_ts': 'The timestamp of when the record was created', 'fk_tm_milestone_cd': 'Foreign key referencing the primary key of the milestone code table', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'action_ct': 'The action category executed on the record', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_milestone_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'lock_control_no': 'A number used for locking purposes', 'fk_relationship_type_cd': 'Foreign key referencing the relationship type code', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_related_trademark_gid': "Foreign key referencing the related trademarks global ID", 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'fk_parent_trademark_gid': "Foreign key referencing the parent trademarks global ID", 'action_ct': 'The action category executed on the record', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_relationship_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'bar_information_tx': 'Text field for bar information', 'bar_membership_month_no': 'Number representing the month of bar membership', 'lock_control_no': 'A number used for locking purposes', 'fk_trademark_gid': 'Foreign key referencing the trademark table', 'bar_membership_state_cd': 'Code representing the state of bar membership', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'fk_interested_party_gid': 'Foreign key referencing the interested party table', 'cfk_patron_id': 'Foreign key referencing the patron table', 'tm_party_role_id': 'Unique identifier for a party role', 'bar_membership_state_nm': 'Name of the state of bar membership', 'bar_membership_year_no': 'Number representing the year of bar membership', 'fk_tm_party_role_cd': 'Foreign key referencing the party role code table', 'party_role_sequence_no': 'Sequence number for the party role', 'last_mod_ts': 'The timestamp of when the record was last modified', 'action_ct': 'The action category executed on the record', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_party_role_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'registration_num': 'Registration number of the trademark', 'fk_fee_process_type_cd': 'Foreign key referencing the fee process type of the trademark', 'create_ts': 'The timestamp of when the record was created', 'effective_filing_dt': 'Effective date of filing for the trademark', 'trademark_gid': 'Unique identifier for each trademark', 'fk_filed_fee_process_type_cd': 'COMMENT REQUIRED', 'lock_control_no': 'A number used for locking purposes', 'uspto_generated_image_in': 'COMMENT REQUIRED', 'preferred_contact_method_ct': 'Preferred contact method category for the trademark owner', 'last_event_type_cd': 'COMMENT REQUIRED', 'create_user_id': 'The user ID that created the record', 'last_action_dt': 'Date of the last action performed on the trademark', 'fk_mark_drawing_type_cd': 'Foreign key referencing the drawing type of the trademark', 'legacy_status_cd': 'Legacy status code for the trademark', 'collective_in': 'Indicator if the trademark is a collective mark', 'external_reference_tx': 'COMMENT REQUIRED', 'available_for_sou_in': 'COMMENT REQUIRED', 'registry_ct': 'COMMENT REQUIRED', 'standard_character_tx': 'Text description of the standard characters used in the trademark', 'serial_num_tx': 'Serial number of the trademark', 'mark_description_tx': 'Description of the trademark', 'last_mod_ts': 'The timestamp of when the record was last modified', 'filing_dt': 'Date when the trademark was filed', 'last_mod_user_id': 'The user ID that last modified the record', 'status_dt': 'Date when the status of the trademark was last updated', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'action_ct': 'The action category executed on the record', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="trademark_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'LOCK_CONTROL_NO': 'A number used for locking purposes', 'FK_CLASS_ID': 'Foreign key referencing the unique identifier of a class', 'FK_TRADEMARK_GID': 'Foreign key referencing the unique identifier of a trademark', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'CREATE_TS': 'The timestamp of when the record was created', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'CREATE_USER_ID': 'The user ID that created the record', 'FK_GDS_SRVC_TERM_SEQUENCE_NO': 'Foreign key referencing the unique identifier of a goods/service term sequence', 'FK_FILING_BASIS_CD': 'Foreign key referencing the filing basis code', 'BEGIN_EFFECTIVE_TS': 'The timestamp of when the record began its effectiveness', 'ACTION_CT': 'The action category executed on the record', 'END_EFFECTIVE_TS': 'The timestamp of when the record is no longer effective', 'CFK_TRANSACTION_INSTANCE_GID': 'The foreign key referencing the unique identifier of the transaction instance'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_gds_srvc_term_filg_basis_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'literal_element_tx': 'Textual representation of the literal element', 'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'sequence_no': 'Number indicating the sequence of the literal element', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'action_ct': 'The action category executed on the record', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_literal_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'EXPIRATION_DT': 'Date and time when the extension expires', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'FK_TRADEMARK_GID': 'Foreign key referencing the primary key of the trademark table', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'CREATE_TS': 'The timestamp of when the record was created', 'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'ITU_EXTENSION_NO': 'Number representing the ITU extension', 'END_EFFECTIVE_TS': 'The timestamp of when the record is no longer effective', 'CFK_TRANSACTION_INSTANCE_GID': 'The foreign key referencing the unique identifier of the transaction instance', 'BEGIN_EFFECTIVE_TS': 'The timestamp of when the record began its effectiveness', 'ACTION_CT': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_itu_extension_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'OG_CERTIFICATE_OF_REG_IN': 'Indicator if there is a certificate of registration for the trademark', 'CFK_TRADEMARK_GID': 'Unique identifier for the trademark', 'OG_PUBD_FOR_OPSTN_SEC_12C_DT': 'Date when the trademark was published for opposition under Section 12C', 'OG_EXTRACT_PUBLICATION_IN': 'Indicator if the trademark has an extract publication', 'OG_PUBD_FOR_OPSTN_DT': 'Timestamp when the trademark was published for opposition', 'OG_IN_PUBLICATION_IN': 'Indicator if the trademark is in publication', 'OG_REGISTRATION_IN': 'Indicator if the trademark has a registration', 'PRINT_MARK_DESCRIPTION_IN': 'Description of the printed mark', 'OG_RENEWAL_IN': 'Indicator if the trademark has a renewal', 'OG_REGISTRATION_NUM_FOUND_IN': 'Registration number found for the trademark', 'OG_AMENDED_REGISTRATION_IN': 'Indicator if the trademark has an amended registration', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'OG_ORDER_RESTRICTING_SCOPE_IN': 'Indicator if there is an order restricting the scope of the trademark', 'OG_CERTIFICATE_CORRECTION_IN': 'Indicator if there is a certificate correction for the trademark', 'OG_CANCELLED_REGISTRATION_IN': 'Indicator if the trademark has a cancelled registration', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'REPUBLISH_SECTION_12_IN': 'Indicator if there is a republication under Section 12 for the trademark', 'CREATE_TS': 'The timestamp of when the record was created', 'OG_SEC_12C_REPUBLICATION_IN': 'Indicator if there is a republication under Section 12C for the trademark', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'CREATE_USER_ID': 'The user ID that created the record', 'ACTION_CT': 'The action category executed on the record', 'BEGIN_EFFECTIVE_TS': 'The timestamp of when the record began its effectiveness', 'CFK_TRANSACTION_INSTANCE_GID': 'The foreign key referencing the unique identifier of the transaction instance', 'END_EFFECTIVE_TS': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_og_publications_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'lock_control_no': 'A number used for locking purposes', 'fk_mailing_address_gid': 'Foreign key referencing the global ID of the mailing address', 'primary_in': 'Indicator for whether the mailing address is the primary address', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_tm_party_role_id': 'Foreign key referencing the party role ID in another table', 'create_ts': 'The timestamp of when the record was created', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'action_ct': 'The action category executed on the record', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_mailing_addr_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'brs_user_id': 'User ID associated with the worker in the BRS system', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'worker_gid': 'Global ID of the worker', 'grade_cd': 'Code representing the grade of the worker', 'last_mod_user_id': 'The user ID that last modified the record', 'worker_no': 'Unique identifier for the worker', 'create_user_id': 'The user ID that created the record', 'lock_control_no': 'A number used for locking purposes', 'signatory_authority_ct': 'Number of signatory authorities held by the worker', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="worker_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'fk_prior_trademark_gid': 'Foreign key referencing the primary key of the prior trademark table', 'lock_control_no': 'A number used for locking purposes', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'action_ct': 'The action category executed on the record', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'end_effective_ts': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_prior_registration_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'print_mark_description_in': 'Indicator for the description of the mark printed', 'last_mod_user_id': 'The user ID that last modified the record', 'tm_publication_gid': 'Foreign key referencing the primary key of the publication table', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'lock_control_no': 'A number used for locking purposes', 'og_action_dt': 'Date and time of the action taken on the trademark publication', 'legacy_og_status_cd': 'Code indicating the status of the trademark publication in the legacy system', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'last_mod_ts': 'The timestamp of when the record was last modified', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_publication_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'work_item_gid': 'Unique identifier for each work item', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'fk_work_item_type_cd': 'Foreign key referencing the work item type', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="work_item_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'pseudo_mark_tx': 'Text representing the pseudo mark', 'sequence_no': 'Number indicating the order of the pseudo mark', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_trademark_gid': 'Foreign key referencing the primary key of the trademark table', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_pseudo_mark_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'gds_srvc_phrase_tx': 'Text field for the GDS service phrase', 'fk_trademark_gid': 'Foreign key referencing the trademark GID', 'create_user_id': 'The user ID that created the record', 'fk_pseudo_class_id': 'Foreign key referencing the pseudo class ID', 'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'create_ts': 'The timestamp of when the record was created', 'fk_class_id': 'Foreign key referencing the class ID', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'action_ct': 'The action category executed on the record', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_pseudo_class_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'ACTION_CT': 'The action category executed on the record', 'END_EFFECTIVE_TS': 'The timestamp of when the record is no longer effective', 'CREATE_TS': 'The timestamp of when the record was created', 'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'BEGIN_EFFECTIVE_TS': 'The timestamp of when the record began its effectiveness', 'CFK_TRANSACTION_INSTANCE_GID': 'The foreign key referencing the unique identifier of the transaction instance', 'LOCK_CONTROL_NO': 'A number used for locking purposes'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tm_proceeding_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)
