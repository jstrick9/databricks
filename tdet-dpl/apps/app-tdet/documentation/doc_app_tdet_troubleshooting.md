# Troubleshooting Guide
---
**Created By:** Joshua Strickland  
**Created Date:** 2025-10-20     
**Last Updated By:** Joshua Strickland  
**Last Updated Date:** 2025-10-20
---
## Common Issues

### 1. "Missing required connection settings"

**Cause:** Environment variables not set correctly

**Solution:**
- Verify secrets exist in Databricks secret scope ` secret_scope: "tdet_sql_scope"
      secret_key: "tdet_access_token"`
- Check databricks.yml has correct warehouse path variables
- Verify app-tdet.yml references variables correctly
---
### 2. "Source data table not found"

**Cause:** Upstream ETL has not created source table

**Solution:**
- Contact data team to create `{catalog}.silver.tdet_app_search`
- Verify table exists: `SELECT COUNT(*) FROM {catalog}.silver.tdet_app_search`
---
### 3. "Serial numbers not found"

**Cause:** Serial numbers don't exist in trademark database

**Solution:**
- Verify serial numbers are valid 8-digit integers
- Check source data freshness
- Confirm serial numbers exist in source system
---
### 4. "Failed to connect to Databricks SQL Warehouse"

**Cause:** Warehouse not running or permissions issue

**Solution:**
- Verify warehouse is running in Databricks workspace
- Check service principal has `CAN_USE` permission on warehouse
- Verify warehouse ID in GitLab CI variables
---
## Logs

View application logs:
```bash
databricks apps logs app-tdet --target <env> --tail 100