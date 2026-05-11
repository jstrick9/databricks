# TRM Domain - Complete Architecture

## Overview

Centralized Unity Catalog (`trm_domain`) with 5 schemas for USPTO Trademark data.

| Schema | Purpose | Prod? | Volumes |
|--------|---------|-------|---------|
| **operations** | Platform metadata | ✅ | - |
| **references** | Reference data | ✅ | - |
| **metrics** | Analytics/Gold layer | ✅ | - |
| **quality** | Data quality monitoring | ✅ | `audit_quality` (DQ configs) |
| **testing** | Dev/testing only | ❌ | `testing` (TGF output files) |

---

## Repository Structure

```
trm-domain-dpl/
├── .gitignore
├── .gitlab-ci.yml
├── databricks.yml                    # Matches existing trm-dpl pattern (lab/prod)
├── CONVENTIONS.md                    # Naming standards
├── README.md
│
├── data_quality/                     # YOUR EXISTING DQ FRAMEWORK
│   ├── config/
│   │   ├── dev/
│   │   │   └── trm_domain-conf.yaml
│   │   └── prod/
│   │       └── trm_domain-conf.yaml
│   ├── checks/
│   │   └── trm_domain/
│   │       ├── bronze/
│   │       ├── silver/
│   │       │   ├── trademark_checks.yml
│   │       │   └── ...
│   │       └── gold/
│   ├── transforms/
│   │   └── trm_domain/
│   │       ├── bronze/
│   │       ├── silver/
│   │       │   ├── trademark_canonical.py
│   │       │   └── ...
│   │       └── gold/
│   ├── custom_checks/
│   ├── allowed_values/
│   ├── hash_configs/
│   │   └── trm_domain/
│   │       ├── bronze/
│   │       ├── silver/
│   │       │   ├── trademark_hash.yml
│   │       │   └── ...
│   │       └── gold/
│   ├── engine/                        # Reused as-is
│   ├── utils/                         # Reused as-is
│   └── tests/
│
├── jobs/                              # Workflows
│   ├── trm_domain_bronze_ingest/
│   │   └── wf_bronze_ingest.yml
│   ├── trm_domain_silver_transform/
│   │   └── wf_silver_transform.yml
│   ├── trm_domain_gold_build/
│   │   └── wf_gold_build.yml
│   ├── trm_domain_quality_monitor/
│   │   └── wf_quality_monitor.yml
│   └── trm_domain_app_permissions/
│       └── wf_app_permissions.yml
│
├── notebooks/
│   ├── config/
│   │   ├── dev/trm_domain-conf.yaml
│   │   └── prod/trm_domain-conf.yaml
│   ├── python/
│   │   ├── bronze/
│   │   │   └── trademark/
│   │   │       └── ntb_ingest_trademark.ipynb
│   │   ├── silver/
│   │   │   └── trademark/
│   │   │       └── ntb_transform_trademark.ipynb
│   │   ├── gold/
│   │   │   └── trademark_mart/
│   │   │       └── ntb_build_trademark_mart.ipynb
│   │   └── shared/
│   │       ├── ntb_run_dq_checks.ipynb
│   │       └── ntb_manage_app_permissions.ipynb
│   └── sql/
│       ├── ddls/
│       │   ├── operations/
│       │   ├── references/
│       │   ├── metrics/
│       │   ├── quality/
│       │   └── testing/
│       └── views/
│
├── apps/                              # Databricks Apps
│   ├── config/
│   │   └── app_permissions.yaml       # Centralized permissions
│   ├── operations-hub/
│   ├── reference-hub/
│   ├── metrics-hub/
│   ├── quality-hub/                   # Creates/updates DQ configs in Volumes
│   └── testing-hub/                   # Dev only - Test File Generator
│
├── testing/                           # Test files & test configs
│   ├── test_data/                     # Sample test data files
│   │   ├── address_simple.csv
│   │   └── ...
│   ├── test_rules/                    # Test validation rules
│   └── tfg_configs/                   # Test File Generator configs
│       └── address_simple_config.yaml
│
├── templates/queries/
│   └── common_queries.sql
│
├── docs/
│   ├── quick-start.md
│   ├── troubleshooting.md
│   └── faq.md
│
└── scripts/
    └── setup_trm_domain.py
```

---

## Volume Structure

### DQ Configs Volume (`audit_quality`)

```
/Volumes/trm_domain_{env}/audit_quality/dq_configs/
├── checks/
│   └── trm_domain/
│       ├── bronze/
│       ├── silver/
│       │   ├── trademark_checks.yml
│       │   └── ...
│       └── gold/
├── hash_configs/
│   └── trm_domain/
│       ├── bronze/
│       ├── silver/
│       │   ├── trademark_hash.yml
│       │   └── ...
│       └── gold/
├── allowed_values/
│   ├── tm_status_codes.yml
│   ├── tm_filing_basis.yml
│   └── ...
└── transforms/
    └── trm_domain/
        ├── bronze/
        ├── silver/
        │   ├── trademark_canonical.py
        │   └── ...
        └── gold/
```

**Note**: DQ Hub App creates/updates these YAML files directly in the Volume.

### Testing Volume (`testing`)

```
/Volumes/trm_domain_dev/testing/
├── tfg_output_files/                  # Test File Generator output
│   ├── address_simple_20260317_165725.csv
│   ├── trademark_test_20260317_170000.csv
│   └── ...
├── test_tables/                       # Test data stored in tables
│   └── (managed by Testing Hub)
└── tfg_configs/                       # Test File Generator configs
    └── address_simple_config.yaml
```

**Note**: Testing Hub App generates test files and saves them to this Volume.

---

## Key Points

### 1. DQ Framework Integration
- Repo stores base configs
- DQ Hub App creates/updates configs in UC Volumes
- Volume path: `/Volumes/trm_domain_{env}/audit_quality/dq_configs/`

### 2. Testing Framework
- Testing schema exists only in dev (`trm_domain_dev`)
- Testing Hub App generates test data files
- Files stored in: `/Volumes/trm_domain_dev/testing/tfg_output_files/`
- Test data also stored in tables within `trm_domain_dev.testing` schema

### 3. databricks.yml
- Matches your existing `trm-dpl` pattern
- Same variables, same lab/prod targets, same permissions
- Includes `service_principal` variable for job execution

### 4. Notebook Organization
- Hybrid: Layer folders (bronze/silver/gold) with table sub-folders
- Shared notebooks for reusable logic (DQ checks, app permissions)

### 5. Centralized App Permissions
- Single YAML file: `apps/config/app_permissions.yaml`
- Defines all permissions for all 5 app hubs
- Deployed via CI/CD pipeline

---

## Quick Start

```bash
# 1. Clone
git clone https://gitlab.uspto.gov/trm/trm-domain-dpl.git
cd trm-domain-dpl

# 2. Setup catalog & schemas
python scripts/setup_trm_domain.py --environment dev

# 3. Deploy DQ configs to Volume
databricks fs cp -r data_quality/ dbfs:/Volumes/trm_domain_dev/audit_quality/dq_configs/ --overwrite

# 4. Deploy bundle
databricks bundle deploy -t lab

# 5. Run pipeline
databricks jobs run-now --job-id <silver-transform-job-id>
```

---

## Adding a New Table

1. DQ checks: `data_quality/checks/trm_domain/silver/{table}_checks.yml`
2. Transform: `data_quality/transforms/trm_domain/silver/{table}_canonical.py`
3. Hash config: `data_quality/hash_configs/trm_domain/silver/{table}_hash.yml`
4. Notebook: `notebooks/python/silver/{table}/ntb_transform_{table}.ipynb`
5. DDL: `notebooks/sql/ddls/silver/ddl_{table}.sql`
6. Pipeline: Update `jobs/trm_domain_silver_transform/wf_silver_transform.yml`

**Or use DQ Hub App** to create YAML files directly in Volumes.

---

## Adding Test Data

1. Use **Testing Hub App** to generate test files
2. Files saved to: `/Volumes/trm_domain_dev/testing/tfg_output_files/`
3. Test data stored in: `trm_domain_dev.testing` schema tables
4. Add test configs to: `testing/tfg_configs/` in repo (optional)

---

**Practical. Includes testing. Not over-engineered.**
