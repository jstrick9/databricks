# TRM Domain Naming Conventions & Best Practices

## Overview

This document defines naming conventions, coding standards, and best practices for the TRM Domain platform. Following these conventions ensures consistency, discoverability, and maintainability.

---

## Naming Conventions

### 1. Unity Catalog Objects

| Object Type | Convention | Example |
|-------------|------------|---------|
| **Catalog** | `trm_domain` | `trm_domain` |
| **Schema** | `{domain_name}` (lowercase, underscores) | `operations`, `references`, `metrics` |
| **Table** | `{entity_name}` (lowercase, underscores) | `trademark`, `tm_class`, `filing_trends` |
| **View** | `vw_{description}` | `vw_filing_trends`, `vw_status_summary` |
| **Column** | `snake_case` | `trademark_gid`, `filing_dt`, `serial_num_tx` |
| **Function** | `snake_case` | `calculate_quality_score`, `standardize_serial_number` |

### 2. DQ Framework Files

| File Type | Pattern | Example |
|-----------|---------|---------|
| **Schema Definition** | `{table}.schema.yml` | `trademark.schema.yml` |
| **DQ Checks** | `{table}_checks.yml` | `trademark_checks.yml` |
| **Transform** | `{table}_canonical.py` | `trademark_canonical.py` |
| **Hash Config** | `{table}_hash.yml` | `trademark_hash.yml` |

### 3. Notebook Naming

| Layer | Pattern | Example |
|-------|---------|---------|
| **Bronze** | `ntb_ingest_{source}_{entity}` | `ntb_ingest_tmngpdb_trademark` |
| **Silver** | `ntb_transform_{entity}` | `ntb_transform_trademark` |
| **Gold** | `ntb_build_{mart_name}` | `ntb_build_trademark_mart` |
| **Shared** | `ntb_{action}` | `ntb_run_dq_checks`, `ntb_debug_pipeline` |

### 4. Job/Workflow Naming

| Type | Pattern | Example |
|------|---------|---------|
| **Workflow** | `wf_{layer}_{action}` | `wf_silver_transform`, `wf_gold_build` |
| **Task** | `task_{action}_{entity}` | `task_transform_trademark`, `task_build_mart` |

### 5. App Naming

| App | Convention | Example |
|-----|------------|---------|
| **App Name** | `{domain}-hub` | `operations-hub`, `metrics-hub` |
| **App Folder** | `{domain}-hub/` | `operations-hub/`, `reference-hub/` |

---

## Coding Standards

### 1. Python Notebooks

#### Imports
```python
# Standard library
import os
import sys
from datetime import datetime

# Third-party
import yaml
import pandas as pd

# PySpark
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
```

#### Function Structure
```python
def transform_trademark(df: DataFrame) -> DataFrame:
    """
    Transform trademark data from bronze to silver.
    
    Args:
        df: Input DataFrame from bronze layer
        
    Returns:
        Transformed DataFrame ready for silver layer
    """
    # Implementation
    return df
```

#### Error Handling
```python
try:
    df = spark.table("trm_domain.bronze.trademark")
except Exception as e:
    print(f"❌ Failed to read bronze table: {e}")
    dbutils.notebook.exit("FAIL")
```

### 2. SQL Scripts

#### Formatting
```sql
-- Use uppercase for keywords, lowercase for identifiers
SELECT 
    trademark_gid,
    serial_num_tx,
    filing_dt,
    status_category
FROM trm_domain.silver.trademark
WHERE filing_dt >= '2024-01-01'
  AND status_category IN ('Registered', 'Pending')
ORDER BY filing_dt DESC;
```

#### Comments
```sql
-- Purpose: Create analytics-ready trademark mart
-- Owner: Trademark Operations Team
-- Update Frequency: Daily
-- Last Modified: 2024-01-15

CREATE OR REPLACE TABLE trm_domain.metrics.trademark_mart AS
SELECT
    trademark_gid,          -- Unique identifier for trademark
    serial_num_tx,          -- 8-digit serial number
    filing_dt,              -- Date application filed
    -- ... more columns
FROM trm_domain.silver.trademark;
```

### 3. YAML Files

#### Structure
```yaml
# Always include metadata at top
# Table: {table_name}
# Description: {description}
# Owner: {team}

key: value
nested:
  - item1
  - item2
  - item3
```

#### Quoting
```yaml
# Quote strings with special characters
"special-key": "value with spaces"
normal_key: value

# Always quote codes that might be interpreted as numbers
"601": "ABANDONED"
"1A": "USE IN COMMERCE"
```

---

## Best Practices

### 1. Data Quality

✅ **Always validate inputs before processing**
```python
def transform(df):
    assert df is not None, "Input DataFrame cannot be None"
    assert "trademark_gid" in df.columns, "Missing required column: trademark_gid"
    # ... continue
```

✅ **Use quality scores for every record**
```python
df = df.withColumn(
    "data_quality_score",
    calculate_quality_score(df)  # 0-1 scale
)
```

✅ **Log all DQ check results**
```python
# Write results to trm_domain.quality.dq_results
dq_results.write.mode("append").saveAsTable("trm_domain.quality.dq_results")
```

### 2. Performance

✅ **Partition by high-cardinality columns used in WHERE clauses**
```sql
PARTITIONED BY (filing_year)  -- Good for date range queries
CLUSTER BY (serial_num_tx)    -- Good for exact lookups
```

✅ **Use broadcast joins for small reference tables**
```python
from pyspark.sql.functions import broadcast

ref_df = broadcast(spark.table("trm_domain.references.ref_status_codes"))
result = df.join(ref_df, "status_code", "left")
```

✅ **Cache DataFrames used multiple times**
```python
df.cache()
df.count()  # Materialize cache
# ... use df multiple times
df.unpersist()
```

### 3. Error Handling & Debugging

✅ **Use structured logging**
```python
import logging

logger = logging.getLogger(__name__)
logger.info(f"Processing {row_count} records")
logger.warning(f"Quality score below threshold: {score}")
logger.error(f"Failed to process: {error}")
```

✅ **Include run metadata**
```python
df = df.withColumn("_run_id", lit(run_id))
df = df.withColumn("_run_timestamp", current_timestamp())
```

✅ **Validate outputs before writing**
```python
# Check row count
assert df.count() > 0, "Output DataFrame is empty"

# Check for nulls in key columns
null_count = df.filter(col("trademark_gid").isNull()).count()
assert null_count == 0, f"Found {null_count} null trademark_gids"
```

### 4. Git & Version Control

✅ **Use conventional commit messages**
```
feat: add trademark silver transform
fix: resolve null pointer in date parsing
docs: update data dictionary for tm_class
test: add unit tests for quality checks
chore: update DQ config version
```

✅ **Include table of contents in long documents**
```markdown
# Document Title

## Table of Contents
- [Overview](#overview)
- [Getting Started](#getting-started)
- [Usage](#usage)
```

✅ **Tag releases**
```bash
git tag -a v1.0.0 -m "Initial release"
git push origin v1.0.0
```

---

## Anti-Patterns to Avoid

❌ **Don't use hardcoded environment names**
```python
# BAD
spark.table("trm_domain_dev.silver.trademark")

# GOOD
dbx_env = dbutils.widgets.get("dbx_env")
spark.table(f"trm_domain_{dbx_env}.silver.trademark")
```

❌ **Don't skip quality checks**
```python
# BAD
df.write.mode("overwrite").saveAsTable("silver_table")

# GOOD
dq_results = run_quality_checks(df)
if dq_results.has_errors:
    raise Exception("Quality checks failed")
df.write.mode("overwrite").saveAsTable("silver_table")
```

❌ **Don't mix business logic with transforms**
```python
# BAD
def transform(df):
    # 500 lines of business logic mixed with Spark code
    return df

# GOOD
def transform(df):
    df = canonicalize_dates(df)
    df = standardize_codes(df)
    df = calculate_quality_score(df)
    return df
```

❌ **Don't use SELECT ***
```sql
-- BAD
SELECT * FROM trm_domain.silver.trademark

-- GOOD
SELECT trademark_gid, serial_num_tx, filing_dt FROM trm_domain.silver.trademark
```

---

## Code Review Checklist

Before submitting a merge request, verify:

- [ ] Naming conventions followed
- [ ] Quality checks added/updated
- [ ] Tests written (if applicable)
- [ ] Documentation updated
- [ ] No hardcoded environment names
- [ ] Error handling included
- [ ] Performance considerations addressed
- [ ] Commit message follows conventions
- [ ] DQ configs validated
- [ ] App permissions updated (if applicable)

---

## Questions?

If you're unsure about naming or conventions, ask in #trm-domain-support or review this document. When in doubt, **be consistent with existing code**.
