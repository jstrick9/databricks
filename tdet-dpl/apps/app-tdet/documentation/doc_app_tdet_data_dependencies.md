# TDET Streamlit App - Data Dependencies
---
**Created By:** Joshua Strickland  
**Created Date:** 2025-10-20     
**Last Updated By:** Joshua Strickland  
**Last Updated Date:** 2025-10-20
---
## Overview

The TDET Streamlit application depends on the `tdet_app_search` table created by an upstream ETL notebook. This document describes the dependency, ownership, and integration points.

---
## Quick Reference

**Source Table:** `{catalog}.silver.tdet_app_search`  
**Owner:** TDET Data Engineering Team  
**Contact:** joshua.strickland@uspto.gov  
**ETL Notebook:** `notebooks/python/silver/ntb_tdet_app_search`  
**Update Frequency:** Daily (Incremental)  
**Runtime:** ~10-20 minutes (varies by data volume)
---

## Table Locations

| Environment | Full Path |
|-------------|-----------|
| Dev | `tdet_dev.silver.tdet_app_search` |
| Test | `tdet_test.silver.tdet_app_search` |
| Prod | `tdet.silver.tdet_app_search` |
---

## Schema Details

**Partitioning:** `_created_date` (date partition)  
**Format:** Delta Lake  
**Typical Size:** 13-15 million active records  
**Update Pattern:** Incremental (SCD Type 2 - maintains history)

### Key Columns

| Column Name | Type | Description | Notes |
|-------------|------|-------------|-------|
| `serial_number` | INT | Trademark serial number | **Primary join key** |
| `mark_tx` | STRING | Trademark text | Display name |
| `filing_date` | DATE | Application filing date | From reporting.silver.milestone |
| `registration_number` | INT | Registration number | NULL if not registered |
| `registration_date` | DATE | Registration date | NULL if not registered |
| `owner_name` | STRING | Current owner name | From interested_party |
| `owner_email` | STRING | Current owner email | May be NULL |
| `attorney_name` | STRING | Current attorney | May be NULL |
| `correspondent_name` | STRING | Current correspondent | May be NULL |
| ... | ... | (48 total columns) | See ETL notebook for full list |
| `_is_record_active` | BOOLEAN | **Current record flag** | **CRITICAL: Always filter true** |
| `_created_date` | DATE | Record creation date | Partition column |
| `_created_timestamp` | TIMESTAMP | Record creation timestamp | ETL run timestamp |
| `_updated_timestamp` | TIMESTAMP | Last update timestamp | For change tracking |
| `_natural_key_hash` | STRING | Hash of serial_number | Deduplication key |
| `_record_data_hash` | STRING | Hash of data values | Change detection |

---

## Critical Requirements

### Always Filter for Active Records

```sql

-- ✅ **CORRECT:**

SELECT *  
FROM tdet_dev.silver.tdet_app_search  
WHERE serial_number = 87654321  
  AND _is_record_active = true;  **-- REQUIRED!**

-- ❌ **WRONG (will return duplicates):**

SELECT *  
FROM tdet_dev.silver.tdet_app_search  
WHERE serial_number = 87654321;