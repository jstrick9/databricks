-- =============================================================================
-- TDET Search Results Detail Population
-- =============================================================================
-- Joins user-provided serial numbers with trademark data from silver layer.
-- 
-- Parameters (injected via str.format()):
--   {tdet_catalog}          - Target catalog (e.g., 'tdet_dev')
--   {tdet_schema}           - Target schema (e.g., 'gold')
--   {table_search_history}  - Search history table name
--   {table_search_detail}   - Search detail table name
--   {output_file_name}      - Generated filename for Excel export
--   {search_id}             - UUID linking to search session
--   {what_matched_logic}    - Dynamic SQL expression for what_matched column
--
-- Behavior:
--   - Each row gets a unique UUID via uuid() function
--   - Returns only current/active records (_is_record_active = true)
--   - Serial numbers not in silver table are omitted from results
--   - Results ordered by serial_number for consistency
-- =============================================================================

INSERT INTO {tdet_catalog}.{tdet_schema}.{table_search_detail} (
    id,
    search_id,
    output_file_name,
    serial_number,
    mark_tx,
    filing_date,
    filed_bases,
    current_bases,
    registration_number,
    registration_date,
    owner_name,
    owner_name_historical,
    owner_address,
    owner_country,
    owner_email,
    owner_email_historical,
    owner_phone,
    attorney_membership_number,
    attorney_name,
    attorney_name_historical,
    attorney_address,
    attorney_email,
    attorney_email_historical,
    attorney_phone,
    correspondent_name,
    correspondent_name_historical,
    correspondent_address,
    correspondent_email,
    correspondent_email_secondary,
    correspondent_email_historical,
    correspondent_phone,
    domestic_representative_name,
    domestic_representative_name_historical,
    domestic_representative_email,
    domestic_representative_email_historical,
    domestic_representative_phone,
    examiner_number,
    examiner_name,
    docket_number,
    firm_name,
    law_office,
    class_list,
    status,
    status_date,
    og_issue_date,
    og_status,
    og_category,
    international_registration_number,
    international_us_reference_number,
    specimen_url,
    what_matched,
    created_date,
    created_user_email,
    natural_key_hash,
    record_data_hash,
    _created_timestamp
)
SELECT 
    uuid() AS id,
    tash.search_id,
    '{output_file_name}' AS output_file_name,
    tas.serial_number,
    tas.mark_tx,
    tas.filing_date,
    tas.filed_bases,
    tas.current_bases,
    tas.registration_number,
    tas.registration_date,
    tas.owner_name,
    tas.owner_name_historical,
    tas.owner_address,
    tas.owner_country,
    tas.owner_email,
    tas.owner_email_historical,
    tas.owner_phone,
    tas.attorney_membership_number,
    tas.attorney_name,
    tas.attorney_name_historical,
    tas.attorney_address,
    tas.attorney_email,
    tas.attorney_email_historical,
    tas.attorney_phone,
    tas.correspondent_name,
    tas.correspondent_name_historical,
    tas.correspondent_address,
    tas.correspondent_email,
    tas.correspondent_email_secondary,
    tas.correspondent_email_historical,
    tas.correspondent_phone,
    tas.domestic_representative_name,
    tas.domestic_representative_name_historical,
    tas.domestic_representative_email,
    tas.domestic_representative_email_historical,
    tas.domestic_representative_phone,
    CAST(tas.examiner_number AS STRING) AS examiner_number,
    tas.examiner_name,
    tas.docket_number,
    tas.firm_name,
    tas.law_office,
    tas.class_list,
    tas.status,
    tas.status_date,
    tas.og_issue_date,
    tas.og_status,
    tas.og_category,
    tas.international_registration_number,
    tas.international_us_reference_number,
    tas.specimen_url,
    {what_matched_logic}    AS what_matched,
    tas._created_date       AS created_date,
    tash.created_user_email AS created_user_email,
    tas._natural_key_hash   AS natural_key_hash,
    tas._record_data_hash   AS record_data_hash,
    current_timestamp()     AS _created_timestamp
FROM {tdet_catalog}.silver.tdet_app_search tas
INNER JOIN {tdet_catalog}.{tdet_schema}.{table_search_history} tash 
    ON tash.serial_number = tas.serial_number
WHERE tash.search_id = '{search_id}'
  AND tas._is_record_active = true
ORDER BY tas.serial_number ASC;