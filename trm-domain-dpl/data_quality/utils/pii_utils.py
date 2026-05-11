"""
PII Detection and Anonymization Utility.
Automatically detects likely PII columns using pattern matching and Unity Catalog tags.
Anonymizes PII data before sending to any AI/LLM endpoint.
"""
import hashlib
import re
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

spark = SparkSession.builder.getOrCreate()

# ===================================================================
# PII DETECTION PATTERNS
# Column name patterns that strongly suggest PII content
# ===================================================================
PII_COLUMN_PATTERNS = [
    r".*email.*",
    r".*phone.*",
    r".*mobile.*",
    r".*address.*",
    r".*addr.*",
    r".*ssn.*",
    r".*social.*security.*",
    r".*passport.*",
    r".*license.*",
    r".*birth.*",
    r".*dob.*",
    r".*name.*",
    r".*first_nm.*",
    r".*last_nm.*",
    r".*full_nm.*",
    r".*surname.*",
    r".*maiden.*",
    r".*gender.*",
    r".*race.*",
    r".*ethnicity.*",
    r".*salary.*",
    r".*income.*",
    r".*credit.*",
    r".*bank.*",
    r".*account.*",
    r".*routing.*",
    r".*zip.*",
    r".*postal.*",
    r".*ip_addr.*",
    r".*latitude.*",
    r".*longitude.*",
    r".*geo.*",
    r".*location.*",
    r".*biometric.*",
]

# Columns that sound like PII but are safe (allowlist)
PII_SAFE_ALLOWLIST = {
    "catalog_name",
    "schema_name",
    "table_name",
    "column_name",
    "address_type",
    "country_name",
    "state_name",
    "account_status",
    "account_type",
    "license_type",
    "bank_holiday",
    "location_code",
    "location_type",
}


def detect_pii_columns(
    df: DataFrame,
    catalog: str = None,
    schema: str = None,
    table_name: str = None
) -> list:
    """
    Automatically detect PII columns using:
    1. Column name pattern matching
    2. Unity Catalog column tags (if available)

    Returns a list of column names that are likely PII.
    """
    pii_cols = set()
    all_cols = [f.name.lower() for f in df.schema.fields if isinstance(f.dataType, StringType)]

    # Method 1: Column name pattern matching
    for col in all_cols:
        if col in PII_SAFE_ALLOWLIST:
            continue
        for pattern in PII_COLUMN_PATTERNS:
            if re.match(pattern, col, re.IGNORECASE):
                pii_cols.add(col)
                break

    # Method 2: Unity Catalog column tags
    if catalog and schema and table_name:
        try:
            tag_results = spark.sql(f"""
                SELECT column_name
                FROM system.information_schema.column_tags
                WHERE catalog_name = '{catalog}'
                  AND schema_name = '{schema}'
                  AND table_name = '{table_name}'
                  AND tag_name IN ('pii', 'PII', 'sensitive', 'personal_data', 'confidential')
            """).collect()
            for row in tag_results:
                pii_cols.add(row["column_name"].lower())
        except Exception:
            pass  # Unity Catalog tags not available — fallback to pattern matching only

    detected = sorted(list(pii_cols))
    if detected:
        print(f"PII columns detected: {detected}")
    return detected


def anonymize_dataframe(
    df: DataFrame,
    pii_columns: list,
    method: str = "hash"
) -> DataFrame:
    """
    Anonymize PII columns before sending data to AI endpoints.

    Methods:
      - 'hash':    SHA-256 hash the value (deterministic, reversible with key)
      - 'mask':    Replace with '***REDACTED***'
      - 'partial': Show first 2 chars only (e.g., 'jo***')
    """
    for col_name in pii_columns:
        if col_name not in [c.lower() for c in df.columns]:
            continue

        actual_col = next(c for c in df.columns if c.lower() == col_name.lower())

        if method == "hash":
            df = df.withColumn(
                actual_col,
                F.when(
                    F.col(actual_col).isNotNull(),
                    F.sha2(F.col(actual_col).cast("string"), 256)
                ).otherwise(F.lit(None))
            )
        elif method == "mask":
            df = df.withColumn(
                actual_col,
                F.when(
                    F.col(actual_col).isNotNull(),
                    F.lit("***REDACTED***")
                ).otherwise(F.lit(None))
            )
        elif method == "partial":
            df = df.withColumn(
                actual_col,
                F.when(
                    F.length(F.col(actual_col)) > 2,
                    F.concat(
                        F.substring(F.col(actual_col), 1, 2),
                        F.lit("***")
                    )
                ).otherwise(F.lit("***"))
            )

    return df


def get_safe_sample(
    df: DataFrame,
    pii_columns: list,
    n_rows: int = 100,
    anonymize_method: str = "hash"
) -> list:
    """
    Returns a PII-safe sample of the DataFrame as a list of dicts.
    Used for feeding data context to AI/LLM endpoints.
    """
    if pii_columns:
        safe_df = anonymize_dataframe(df, pii_columns, method=anonymize_method)
    else:
        safe_df = df

    return [row.asDict() for row in safe_df.limit(n_rows).collect()]


def tag_pii_columns_in_unity_catalog(
    catalog: str,
    schema: str,
    table_name: str,
    pii_columns: list
) -> None:
    """
    Formally tag detected PII columns in Unity Catalog.
    Run this once after onboarding a new table.
    """
    for col in pii_columns:
        try:
            spark.sql(f"""
                ALTER TABLE {catalog}.{schema}.{table_name}
                ALTER COLUMN {col}
                SET TAGS ('pii' = 'true', 'pii_auto_detected' = 'true')
            """)
            print(f"Tagged {catalog}.{schema}.{table_name}.{col} as PII")
        except Exception as e:
            print(f"Could not tag {col}: {e}")