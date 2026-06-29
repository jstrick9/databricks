## ETL Workflow Details
---
**Created By:** Joshua Strickland  
**Created Date:** 2025-10-20     
**Last Updated By:** Joshua Strickland  
**Last Updated Date:** 2025-10-20
---
### Job Configuration

**Job Name:** `wf_tdet_app_search`  
**Location:** `jobs/wf_tdet_app_search/wf_tdet_app_search.yml`  
**Notebook:** `notebooks/python/silver/ntb_tdet_app_search`  
**Config:** `notebooks/config/{env}/tdet-conf.yaml`
---
### Schedule

**Cron Expression:** `40 0 7 * * ?`  
**Human Readable:** Every day at 7:40 AM Eastern Time  
**Timezone:** America/New_York  
**Status:** UNPAUSED (actively running)

**Expected Duration:** 10-20 minutes  
**Timeout:** 0.5 hours (1800 seconds)
---
### Best Time to Use TDET App

| Time (ET) | Data Status | Recommendation |
|-----------|-------------|----------------|
| Before 7:40 AM | Previous day's data | ✅ Use app (yesterday's data) |
| 7:40 AM - 8:15 AM | ETL running | ⚠️ Wait if possible (data refreshing) |
| After 8:15 AM | Fresh data | ✅ Best time to use app |
---
### Cluster Configuration

**Node Type:** i4i.xlarge  
**Autoscale:** 2-12 workers  
**Spark Version:** 16.4.x-scala2.12  
**Spot Instances:** Yes (with on-demand fallback)

**Cost Optimization:**
- Uses spot instances for 90% cost savings
- Auto-scales down when idle
- Terminates after job completion
---
### Monitoring the ETL Job

#### View Job Status in Databricks

1. Navigate to **Workflows** in Databricks workspace
2. Search for `wf_tdet_app_search`
3. Click on job name
4. View recent runs, logs, and metrics

#### Check Last Successful Run

```sql
-- Query to verify last ETL run

SELECT   
    MAX(_created_date) as last_etl_run,  
    DATEDIFF(CURRENT_DATE(), MAX(_created_date)) as days_since_last_run,  
    COUNT(*) as records_from_last_run  
FROM tdet_dev.silver.tdet_app_search  
WHERE _created_date = (SELECT MAX(_created_date) FROM tdet_dev.silver.tdet_app_search);