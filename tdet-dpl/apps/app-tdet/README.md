# TDET - Trademark Data Extraction Tool

Databricks Streamlit application for searching and exporting trademark data.

## Prerequisites

- Databricks workspace with Apps enabled
- Unity Catalog with `tdet_*` catalogs
- Source table: `{catalog}.silver.tdet_app_search`
- SQL Warehouse access
- Service Principal with appropriate permissions

## Deployment

### Using Databricks Asset Bundles:

```bash
# Deploy to dev
databricks bundle deploy -t dev

# Deploy to test
databricks bundle deploy -t test

# Deploy to prod
databricks bundle deploy -t prod