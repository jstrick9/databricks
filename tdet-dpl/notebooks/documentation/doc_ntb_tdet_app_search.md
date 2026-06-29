# TDET ETL Notebook Documentation
---
**Created By:** Joshua Strickland  
**Created Date:** 2025-10-20     
**Last Updated By:** Joshua Strickland  
**Last Updated Date:** 2025-10-20
---
## Table of Contents

- Overview
- Data Sources
- Output Schema
- Configuration

---
## Overview

**Notebook Name:** `ntb_tdet_app_search`  
**Location:** `notebooks/python/silver/ntb_tdet_app_search`  
**Purpose:** Extract, transform, and load trademark data into the silver layer for consumption by the TDET Streamlit application  
**Output Table:** `{catalog}.silver.tdet_app_search`  
**Update Frequency:** Daily at 7:40 AM ET  
**Load Type:** Incremental (SCD Type 2)

---

## Data Sources

### Primary Sources

#### 1. Trademark Master Data (`trm_tmngpdb_{env}.bronze`)

**Tables Used:**

| Table | Columns Used | Purpose |
|-------|-------------|---------|
| `trademark` | serial_num_tx, registration_num, legacy_status_cd, status_dt, external_reference_tx, standard_character_tx | Base trademark information |
| `stnd_legacy_status` | status_no, description_tx | Status code lookup |
| `tm_party_role` | trademark_gid, party_role_cd, interested_party_gid, bar_information_tx | Current party relationships |
| `tm_party_role_h` | (same as above) | Historical party relationships |
| `interested_party` | interested_party_gid, interested_party_nm, country_cd | Party names (current) |
| `interested_party_h` | (same as above) | Party names (historical) |
| `tm_electronic_addr` | tm_party_role_id, electronic_address_gid, primary_in | Email mappings |
| `electronic_address` | electronic_address_gid, electronic_addr_locator_tx | Email addresses |
| `tm_mailing_addr` | tm_party_role_id, mailing_address_gid | Address mappings |
| `mailing_address` | street_line_1_tx, city_nm, postal_cd, country_cd, name_line_2_tx | Physical addresses |
| `tm_telecom_addr` | tm_party_role_id, telecom_address_gid | Phone mappings |
| `telecom_address` | telecom_no | Phone numbers |
| `tm_filing_basis` | trademark_gid, filing_basis_cd, current_in, filed_in | Filing basis codes |
| `tm_publication` | trademark_gid, legacy_og_status_cd | Publication info |
| `og_publication_tm` | tm_publication_gid, og_publication_gid | OG linkage |
| `og_publication` | og_publication_gid, publication_dt | OG dates |
| `tm_publication_subct` | tm_publication_gid, legacy_des_cd | OG categories |
| `tm_literal` | trademark_gid, literal_element_tx | Literal text fallback |

#### 2. Reporting Data (`trm_reporting_{env}.silver`)

| Table | Columns Used | Purpose | Volume |
|-------|-------------|---------|--------|
| `class` | ser_num, class | International classifications | 20-30M rows |
| `milestone` | ser_num, filing_dt, registration_dt | Key dates | 10-15M rows |
| `bibliography` | ser_num, exmr_eid, law_office | Examiner assignments | 10-15M rows |

#### 3. International Trademarks (`trm_tmintltm_{env}.bronze`)

| Table | Columns Used | Purpose |
|-------|-------------|---------|
| `base_appl_intl_reg` | cfk_trademark_gid, fk_international_reg_gid, fk_international_appl_gid | International registrations |
| `international_reg_tm` | cfk_trademark_gid, fk_international_reg_gid | International trademark mappings |

#### 4. Worker Data (`trm_tmworker_{env}.bronze`)

| Table | Columns Used | Purpose |
|-------|-------------|---------|
| `worker` | worker_no, worker_nm | Examiner names |

#### 5. Goods & Services (`tm.silver`)

| Table | Columns Used | Purpose |
|-------|-------------|---------|
| `goods_service` | serial_num_tx, specimen_website_address | Specimen URLs |

---

## Output Schema

### Table: `{catalog}.silver.tdet_app_search`

**Full Name by Environment:**
- Dev: `tdet_dev.silver.tdet_app_search`
- Test: `tdet_test.silver.tdet_app_search`
- Prod: `tdet.silver.tdet_app_search`

**Format:** Delta Lake  
**Partitioning:** `_created_date` (date partition)  
**Compression:** Snappy (default)  
**Expected Size:** 5-10 GB (active records only)

### Column Specifications

#### Business Columns (48 total)

| Column Name | Data Type | Nullable | Description | Source |
|-------------|-----------|----------|-------------|--------|
| `serial_number` | INT | No | Trademark serial number (8 digits) | trademark.serial_num_tx |
| `mark_tx` | STRING | Yes | Trademark text/literal | trademark.standard_character_tx or tm_literal |
| `filing_date` | DATE | Yes | Application filing date | milestone.filing_dt |
| `filed_bases` | STRING | Yes | Original filing bases (comma-separated) | tm_filing_basis (filed_in=Y) |
| `current_bases` | STRING | Yes | Current filing bases (comma-separated) | tm_filing_basis (current_in=Y) |
| `registration_number` | INT | Yes | Registration number (if registered) | trademark.registration_num |
| `registration_date` | DATE | Yes | Registration date | milestone.registration_dt |
| `owner_name` | STRING | Yes | Current owner name | interested_party via tm_party_role (OWNER) |
| `owner_name_historical` | STRING | Yes | Historical owner names (semicolon-separated) | interested_party_h via tm_party_role_h |
| `owner_address` | STRING | Yes | Current owner address | mailing_address via tm_party_role |
| `owner_country` | STRING | Yes | Owner country code | interested_party.country_cd |
| `owner_email` | STRING | Yes | Current owner email | electronic_address via tm_party_role |
| `owner_email_historical` | STRING | Yes | Historical owner emails (semicolon-separated) | electronic_address_h |
| `owner_phone` | STRING | Yes | Owner phone number | telecom_address via tm_party_role |
| `attorney_membership_number` | STRING | Yes | Attorney bar number | tm_party_role.bar_information_tx (AT) |
| `attorney_name` | STRING | Yes | Current attorney name | interested_party via tm_party_role (AT) |
| `attorney_name_historical` | STRING | Yes | Historical attorney names | interested_party_h |
| `attorney_address` | STRING | Yes | Attorney address | mailing_address |
| `attorney_email` | STRING | Yes | Attorney emails (semicolon-separated) | electronic_address |
| `attorney_email_historical` | STRING | Yes | Historical attorney emails | electronic_address_h |
| `attorney_phone` | STRING | Yes | Attorney phone | telecom_address |
| `correspondent_name` | STRING | Yes | Current correspondent name | interested_party via tm_party_role (COR) |
| `correspondent_name_historical` | STRING | Yes | Historical correspondent names | interested_party_h |
| `correspondent_address` | STRING | Yes | Correspondent address | mailing_address |
| `correspondent_email` | STRING | Yes | Primary correspondent emails | electronic_address (primary_in=Y) |
| `correspondent_email_secondary` | STRING | Yes | Secondary correspondent emails | electronic_address (primary_in!=Y) |
| `correspondent_email_historical` | STRING | Yes | Historical correspondent emails | electronic_address_h |
| `correspondent_phone` | STRING | Yes | Correspondent phone | telecom_address |
| `domestic_representative_name` | STRING | Yes | Domestic representative name | interested_party via tm_party_role (DR) |
| `domestic_representative_name_historical` | STRING | Yes | Historical DR names | interested_party_h |
| `domestic_representative_email` | STRING | Yes | DR emails | electronic_address |
| `domestic_representative_email_historical` | STRING | Yes | Historical DR emails | electronic_address_h |
| `domestic_representative_phone` | STRING | Yes | DR phone | telecom_address |
| `examiner_number` | INT | Yes | USPTO examiner employee ID | bibliography.exmr_eid |
| `examiner_name` | STRING | Yes | USPTO examiner name | worker.worker_nm |
| `docket_number` | STRING | Yes | Firm/client docket number | trademark.external_reference_tx |
| `firm_name` | STRING | Yes | Law firm name | mailing_address.name_line_2_tx |
| `law_office` | STRING | Yes | USPTO law office code | bibliography.law_office |
| `class_list` | STRING | Yes | International classes (semicolon-separated) | class table aggregated |
| `status` | STRING | Yes | Current status (code + description) | stnd_legacy_status joined to trademark |
| `status_date` | DATE | Yes | Status effective date | trademark.status_dt |
| `og_issue_date` | DATE | Yes | Official Gazette issue dates (semicolon-separated) | og_publication.publication_dt |
| `og_status` | STRING | Yes | OG status codes (semicolon-separated) | tm_publication.legacy_og_status_cd |
| `og_category` | STRING | Yes | OG categories (semicolon-separated) | tm_publication_subct.legacy_des_cd |
| `international_registration_number` | STRING | Yes | "Y" if 66(a) or starts with "79", else "N" | Derived from filing_basis and serial_num |
| `international_us_reference_number` | STRING | Yes | "Y" if has international application, else "N" | international_reg_tm table |
| `specimen_url` | STRING | Yes | Specimen URLs (semicolon-separated) | goods_service.specimen_website_address |

#### Metadata Columns (5 total)

| Column Name | Data Type | Nullable | Description | Purpose |
|-------------|-----------|----------|-------------|---------|
| `_created_date` | DATE | No | Date this version was created | Partitioning, data freshness tracking |
| `_created_timestamp` | TIMESTAMP | No | Exact timestamp of creation | Audit trail, debugging |
| `_updated_timestamp` | TIMESTAMP | No | Last update timestamp | Change tracking |
| `_is_record_active` | BOOLEAN | No | True if current version, False if superseded | **CRITICAL: Always filter = true** |
| `_natural_key_hash` | STRING | No | SHA-256 hash of serial_number | Deduplication key |
| `_record_data_hash` | STRING | No | SHA-256 hash of deterministic data columns | Change detection for SCD Type 2 |

### Constraints

**Primary Key (Logical):** `serial_number` WHERE `_is_record_active = true`  
**Unique Constraint:** One active record per serial_number  
**Data Quality Rule:** `COUNT(*) = COUNT(DISTINCT serial_number)` WHERE `_is_record_active = true`

### Indexes & Optimization

**Partition Pruning:** Queries filtering by `_created_date` benefit from partition pruning  
**Z-Ordering:** Table uses Z-ordering on `serial_number` for faster lookups  
**File Compaction:** Auto-compaction runs after each load to optimize file sizes

---

## Configuration

### Configuration File

**Location:** `notebooks/config/{env}/tdet-conf.yaml`
