# Databricks notebook source
# =============================================================================
# NOTEBOOK  : 00_setup
# PURPOSE   : One-time setup for the Test File Generator framework.
#             Creates all Delta tables and seeds regex patterns + masking rules.
# TARGET    : trm_domain_dev.testing (hardcoded)
# RUNTIME   : DBR 16.4 LTS (Spark 3.5.2, Scala 2.12)
# USAGE     : Run ONCE per environment. Safe to re-run (uses MERGE for seeds).
# =============================================================================

# COMMAND ----------

# DBTITLE 1,Framework Constants
FRAMEWORK_CATALOG = "trm_domain_dev"
FRAMEWORK_SCHEMA  = "testing"
FW = f"{FRAMEWORK_CATALOG}.{FRAMEWORK_SCHEMA}"

print(f"Framework DB: {FW}")

# COMMAND ----------

# DBTITLE 1,Create Catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {FRAMEWORK_CATALOG}")
print(f"✔ Catalog '{FRAMEWORK_CATALOG}' is ready.")

# COMMAND ----------

# DBTITLE 1,Create Schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FRAMEWORK_CATALOG}.{FRAMEWORK_SCHEMA}")
print(f"✔ Schema '{FRAMEWORK_SCHEMA}' is ready.")

# COMMAND ----------

# DBTITLE 1,Create Output Volume
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {FW}.tfg_output_files
    COMMENT 'TFG: Output volume for generated test files.'
""")
print(f"✔ Volume '{FW}.tfg_output_files' is ready.")
print(f"  Files accessible at: /Volumes/{FRAMEWORK_CATALOG}/{FRAMEWORK_SCHEMA}/tfg_output_files/")

# COMMAND ----------

# DBTITLE 1,TABLE: tfg_regex_patterns
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FW}.tfg_regex_patterns (
    regex_pattern_id         BIGINT    GENERATED ALWAYS AS IDENTITY,
    regex_pattern_name       STRING    NOT NULL  COMMENT 'Unique key for this pattern',
    regex_pattern            STRING    NOT NULL  COMMENT 'The actual regex expression',
    data_type_hint           STRING    COMMENT 'STRING | INTEGER | DATE | EMAIL | PHONE | SSN | etc.',
    description              STRING    COMMENT 'Human-readable description',
    positive_scenario_values STRING    COMMENT 'JSON array: [{{input, label}}, ...]',
    negative_scenario_values STRING    COMMENT 'JSON array: [{{input, label}}, ...]',
    is_active                BOOLEAN   NOT NULL DEFAULT TRUE,
    created_by               STRING    DEFAULT current_user(),
    created_ts               TIMESTAMP DEFAULT current_timestamp(),
    updated_ts               TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'TFG: Regex patterns with positive/negative test scenario values.'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")
print("✔ tfg_regex_patterns")

# COMMAND ----------

# DBTITLE 1,TABLE: tfg_source_field_mapping
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FW}.tfg_source_field_mapping (
    mapping_id          BIGINT    GENERATED ALWAYS AS IDENTITY,
    source_catalog      STRING    NOT NULL  COMMENT 'Unity Catalog catalog name',
    source_schema       STRING    NOT NULL  COMMENT 'Unity Catalog schema name',
    source_table        STRING    NOT NULL  COMMENT 'Source table name',
    source_field_name   STRING    NOT NULL  COMMENT 'Column in source table',
    regex_pattern_name  STRING    NOT NULL  COMMENT 'FK → tfg_regex_patterns.regex_pattern_name',
    is_pii              BOOLEAN   NOT NULL DEFAULT FALSE COMMENT 'If TRUE, auto-masks this field',
    masking_strategy    STRING    COMMENT 'FK → tfg_masking_rules.masking_strategy_name (used when is_pii=TRUE)',
    is_active           BOOLEAN   NOT NULL DEFAULT TRUE,
    created_by          STRING    DEFAULT current_user(),
    created_ts          TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'TFG: Maps source table fields to regex patterns and optional masking.'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")
print("✔ tfg_source_field_mapping")

# COMMAND ----------

# DBTITLE 1,TABLE: tfg_masking_rules
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FW}.tfg_masking_rules (
    masking_rule_id         BIGINT    GENERATED ALWAYS AS IDENTITY,
    masking_strategy_name   STRING    NOT NULL  COMMENT 'Unique strategy name',
    masking_category        STRING    NOT NULL  COMMENT 'HASH | NULLIFY | REDACT | PARTIAL_MASK | PII_* | SHUFFLE | TOKEN | CUSTOM',
    masking_description     STRING    COMMENT 'What this strategy does',
    masking_expression      STRING    COMMENT 'SQL expression template (use {{col}} as placeholder)',
    faker_provider          STRING    COMMENT 'Faker method (e.g. faker.email)',
    preserve_format         BOOLEAN   NOT NULL DEFAULT FALSE,
    is_reversible           BOOLEAN   NOT NULL DEFAULT FALSE,
    is_active               BOOLEAN   NOT NULL DEFAULT TRUE,
    created_by              STRING    DEFAULT current_user(),
    created_ts              TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'TFG: All available PII and data masking strategies.'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")
print("✔ tfg_masking_rules")

# COMMAND ----------

# DBTITLE 1,TABLE: tfg_test_profiles
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FW}.tfg_test_profiles (
    profile_id              BIGINT    GENERATED ALWAYS AS IDENTITY,
    profile_name            STRING    NOT NULL  COMMENT 'Unique profile name',
    profile_description     STRING    COMMENT 'What this profile generates',
    source_catalog          STRING    NOT NULL,
    source_schema           STRING    NOT NULL,
    source_table            STRING    NOT NULL,
    generation_mode         STRING    NOT NULL  COMMENT 'SIMPLE | SCENARIO | ADVANCED | FULL',
    row_count               INT       NOT NULL DEFAULT 100,
    volume_multiplier       INT       NOT NULL DEFAULT 1,
    apply_pii_masking       BOOLEAN   NOT NULL DEFAULT FALSE,
    include_nulls           BOOLEAN   NOT NULL DEFAULT FALSE,
    include_boundary_values BOOLEAN   NOT NULL DEFAULT FALSE,
    include_duplicates      BOOLEAN   NOT NULL DEFAULT FALSE,
    duplicate_count         INT       NOT NULL DEFAULT 1,
    include_schema_drift    BOOLEAN   NOT NULL DEFAULT FALSE,
    schema_drift_config     STRING    COMMENT 'JSON: [{{action,column,data_type,new_name}}]',
    output_format           STRING    NOT NULL DEFAULT 'CSV',
    output_path             STRING    COMMENT 'File path or catalog.schema.table for DELTA',
    include_header          BOOLEAN   NOT NULL DEFAULT TRUE,
    columns_to_exclude      STRING    COMMENT 'Comma-separated list',
    extra_columns           STRING    COMMENT 'JSON: [{{name,after_column,default_value}}]',
    is_active               BOOLEAN   NOT NULL DEFAULT TRUE,
    created_by              STRING    DEFAULT current_user(),
    created_ts              TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'TFG: Reusable named profiles for repeatable test runs.'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")
print("✔ tfg_test_profiles")

# COMMAND ----------

# DBTITLE 1,TABLE: tfg_run_log
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FW}.tfg_run_log (
    run_id              BIGINT    GENERATED ALWAYS AS IDENTITY,
    run_uuid            STRING    NOT NULL  COMMENT 'UUID for this run',
    profile_name        STRING    COMMENT 'Profile used (if any)',
    generation_mode     STRING    NOT NULL  COMMENT 'SIMPLE | SCENARIO | ADVANCED | FULL',
    source_table_fqn    STRING    NOT NULL  COMMENT 'catalog.schema.table',
    output_format       STRING,
    output_path         STRING,
    records_sampled     BIGINT    DEFAULT 0,
    records_generated   BIGINT    DEFAULT 0,
    records_masked      BIGINT    DEFAULT 0,
    records_error       BIGINT    DEFAULT 0,
    status              STRING    NOT NULL DEFAULT 'STARTED',
    error_message       STRING,
    started_by          STRING    DEFAULT current_user(),
    started_ts          TIMESTAMP DEFAULT current_timestamp(),
    completed_ts        TIMESTAMP,
    duration_seconds    DOUBLE
)
USING DELTA
COMMENT 'TFG: Master audit log for every test generation run.'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")
print("✔ tfg_run_log")

# COMMAND ----------

# DBTITLE 1,TABLE: tfg_test_result_log
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FW}.tfg_test_result_log (
    result_id                BIGINT    GENERATED ALWAYS AS IDENTITY,
    run_uuid                 STRING    NOT NULL  COMMENT 'FK → tfg_run_log.run_uuid',
    source_table_fqn         STRING    NOT NULL,
    source_field_name        STRING    NOT NULL  COMMENT 'Field modified for this test case',
    regex_pattern_name       STRING    COMMENT 'Pattern applied',
    scenario_type            STRING    NOT NULL  COMMENT 'positive | negative | null | boundary | duplicate | schema_drift',
    test_scenario_value      STRING    COMMENT 'Value injected',
    test_scenario_description STRING   COMMENT 'Human label',
    expected_outcome         STRING    COMMENT 'PASS | FAIL',
    created_ts               TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'TFG: Per-row scenario log for QA traceability.'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")
print("✔ tfg_test_result_log")

# COMMAND ----------

# =============================================================================
# SEED DATA: REGEX PATTERNS
# =============================================================================

# COMMAND ----------

# DBTITLE 1,Seed: Regex Patterns
import json

regex_patterns = [
    {
        "regex_pattern_name": "ALPHA_ONLY",
        "regex_pattern": "^[A-Za-z]+$",
        "data_type_hint": "STRING",
        "description": "Alphabetic characters only.",
        "positive_scenario_values": json.dumps([
            {"input": "Hello",  "label": "Simple word"},
            {"input": "ABCdef", "label": "Mixed case"},
            {"input": "Z",      "label": "Single char"}
        ]),
        "negative_scenario_values": json.dumps([
            {"input": "Hello World", "label": "Contains space"},
            {"input": "Hello1",      "label": "Contains digit"},
            {"input": "",            "label": "Empty string"},
            {"input": "Hello!",      "label": "Contains special char"}
        ])
    },
    {
        "regex_pattern_name": "ALPHANUMERIC",
        "regex_pattern": "^[A-Za-z0-9]+$",
        "data_type_hint": "STRING",
        "description": "Alphanumeric characters only.",
        "positive_scenario_values": json.dumps([
            {"input": "abc123", "label": "Mixed alphanumeric"},
            {"input": "ABC",    "label": "Alpha only"},
            {"input": "123",    "label": "Numeric only"}
        ]),
        "negative_scenario_values": json.dumps([
            {"input": "abc 123", "label": "Contains space"},
            {"input": "abc-123", "label": "Contains hyphen"},
            {"input": "",        "label": "Empty string"}
        ])
    },
    {
        "regex_pattern_name": "FREE_TEXT",
        "regex_pattern": "^.+$",
        "data_type_hint": "STRING",
        "description": "Any non-empty string.",
        "positive_scenario_values": json.dumps([
            {"input": "Hello World!", "label": "Normal text"},
            {"input": "12345",        "label": "Numeric string"},
            {"input": "!@#$%",        "label": "Special chars"}
        ]),
        "negative_scenario_values": json.dumps([
            {"input": "",   "label": "Empty string"},
            {"input": None, "label": "Null value"}
        ])
    },
    {
        "regex_pattern_name": "INTEGER_POSITIVE",
        "regex_pattern": "^[1-9][0-9]*$",
        "data_type_hint": "INTEGER",
        "description": "Positive integer, no leading zeros.",
        "positive_scenario_values": json.dumps([
            {"input": "1",       "label": "Single digit"},
            {"input": "42",      "label": "Two digits"},
            {"input": "1000000", "label": "Large number"}
        ]),
        "negative_scenario_values": json.dumps([
            {"input": "0",   "label": "Zero"},
            {"input": "-5",  "label": "Negative"},
            {"input": "01",  "label": "Leading zero"},
            {"input": "1.5", "label": "Decimal"},
            {"input": "abc", "label": "Non-numeric"}
        ])
    },
    {
        "regex_pattern_name": "DECIMAL_TWO_PLACES",
        "regex_pattern": "^-?\\d+(\\.\\d{1,2})?$",
        "data_type_hint": "DECIMAL",
        "description": "Decimal number with up to 2 decimal places.",
        "positive_scenario_values": json.dumps([
            {"input": "10.99", "label": "Two decimal places"},
            {"input": "100",   "label": "Whole number"},
            {"input": "-5.5",  "label": "Negative decimal"}
        ]),
        "negative_scenario_values": json.dumps([
            {"input": "10.999", "label": "Three decimal places"},
            {"input": "abc",    "label": "Non-numeric"},
            {"input": "",       "label": "Empty string"}
        ])
    },
    {
        "regex_pattern_name": "SSN_US",
        "regex_pattern": "^(?!000|666|9\\d{2})\\d{3}-(?!00)\\d{2}-(?!0000)\\d{4}$",
        "data_type_hint": "SSN",
        "description": "US Social Security Number (XXX-XX-XXXX).",
        "positive_scenario_values": json.dumps([
            {"input": "123-45-6789", "label": "Standard SSN"},
            {"input": "001-01-0001", "label": "Low range valid"},
            {"input": "899-99-9999", "label": "High valid range"}
        ]),
        "negative_scenario_values": json.dumps([
            {"input": "000-45-6789", "label": "Invalid area 000"},
            {"input": "666-45-6789", "label": "Invalid area 666"},
            {"input": "900-45-6789", "label": "Invalid area 9xx"},
            {"input": "123-00-6789", "label": "Invalid group 00"},
            {"input": "123456789",   "label": "No dashes"},
            {"input": "",            "label": "Empty string"}
        ])
    },
    {
        "regex_pattern_name": "DATE_ISO8601",
        "regex_pattern": "^\\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\\d|3[01])$",
        "data_type_hint": "DATE",
        "description": "ISO 8601 date: YYYY-MM-DD.",
        "positive_scenario_values": json.dumps([
            {"input": "2024-01-01", "label": "New Year"},
            {"input": "2023-12-31", "label": "End of year"},
            {"input": "2000-02-29", "label": "Leap year Feb 29"}
        ]),
        "negative_scenario_values": json.dumps([
            {"input": "01-01-2024", "label": "US format"},
            {"input": "2024/01/01", "label": "Slash separator"},
            {"input": "2024-13-01", "label": "Invalid month 13"},
            {"input": "2024-01-32", "label": "Invalid day 32"},
            {"input": "",           "label": "Empty string"}
        ])
    },
    {
        "regex_pattern_name": "DATE_US_FORMAT",
        "regex_pattern": "^(0[1-9]|1[0-2])/(0[1-9]|[12]\\d|3[01])/\\d{4}$",
        "data_type_hint": "DATE",
        "description": "US date: MM/DD/YYYY.",
        "positive_scenario_values": json.dumps([
            {"input": "01/01/2024", "label": "New Year US format"},
            {"input": "12/31/2023", "label": "End of year"},
            {"input": "06/15/1999", "label": "Mid-year"}
        ]),
        "negative_scenario_values": json.dumps([
            {"input": "2024-01-01", "label": "ISO format"},
            {"input": "13/01/2024", "label": "Invalid month"},
            {"input": "1/1/2024",   "label": "No zero padding"},
            {"input": "",           "label": "Empty string"}
        ])
    },
    {
        "regex_pattern_name": "ZIP_CODE_US",
        "regex_pattern": "^\\d{5}(-\\d{4})?$",
        "data_type_hint": "STRING",
        "description": "US ZIP code: 5 digits or ZIP+4.",
        "positive_scenario_values": json.dumps([
            {"input": "10001",      "label": "5-digit ZIP"},
            {"input": "10001-1234", "label": "ZIP+4"},
            {"input": "00501",      "label": "Leading zero ZIP"}
        ]),
        "negative_scenario_values": json.dumps([
            {"input": "1000",      "label": "Too short"},
            {"input": "100011",    "label": "Too long"},
            {"input": "ABCDE",     "label": "Alpha chars"},
            {"input": "",          "label": "Empty string"}
        ])
    },
    {
        "regex_pattern_name": "CREDIT_CARD_GENERIC",
        "regex_pattern": "^(4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12})$",
        "data_type_hint": "STRING",
        "description": "Major credit card formats (Visa, MC, Amex, Discover).",
        "positive_scenario_values": json.dumps([
            {"input": "4111111111111111", "label": "Visa test"},
            {"input": "5500005555555559", "label": "MC test"},
            {"input": "378282246310005",  "label": "Amex test"}
        ]),
        "negative_scenario_values": json.dumps([
            {"input": "1234567890123456", "label": "Invalid prefix"},
            {"input": "411111111111111",  "label": "Too short Visa"},
            {"input": "",                 "label": "Empty string"}
        ])
    },
    {
        "regex_pattern_name": "UUID_V4",
        "regex_pattern": "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$",
        "data_type_hint": "STRING",
        "description": "UUID version 4 format.",
        "positive_scenario_values": json.dumps([
            {"input": "550e8400-e29b-41d4-a716-446655440000", "label": "Valid UUID v4"},
            {"input": "f47ac10b-58cc-4372-a567-0e02b2c3d479", "label": "Another UUID v4"}
        ]),
        "negative_scenario_values": json.dumps([
            {"input": "550e8400e29b41d4a716446655440000",     "label": "No dashes"},
            {"input": "not-a-uuid-at-all-xxxx",              "label": "Invalid format"},
            {"input": "",                                     "label": "Empty string"}
        ])
    },
    {
        "regex_pattern_name": "IPV4_ADDRESS",
        "regex_pattern": "^((25[0-5]|2[0-4]\\d|[01]?\\d\\d?)\\.){3}(25[0-5]|2[0-4]\\d|[01]?\\d\\d?)$",
        "data_type_hint": "STRING",
        "description": "IPv4 address.",
        "positive_scenario_values": json.dumps([
            {"input": "192.168.1.1",    "label": "Private IP"},
            {"input": "0.0.0.0",        "label": "All zeros"},
            {"input": "255.255.255.255","label": "Broadcast"}
        ]),
        "negative_scenario_values": json.dumps([
            {"input": "256.0.0.1",      "label": "Octet out of range"},
            {"input": "192.168.1",      "label": "Missing octet"},
            {"input": "abc.def.ghi.jkl","label": "Alpha octets"},
            {"input": "",               "label": "Empty string"}
        ])
    },
    {
        "regex_pattern_name": "BOOLEAN_YN",
        "regex_pattern": "^[YN]$",
        "data_type_hint": "STRING",
        "description": "Y or N flag.",
        "positive_scenario_values": json.dumps([
            {"input": "Y", "label": "Yes"},
            {"input": "N", "label": "No"}
        ]),
        "negative_scenario_values": json.dumps([
            {"input": "y",   "label": "Lowercase"},
            {"input": "YES", "label": "Full word"},
            {"input": "1",   "label": "Numeric 1"},
            {"input": "",    "label": "Empty string"}
        ])
    },
    {
        "regex_pattern_name": "BOOLEAN_TF",
        "regex_pattern": "^(true|false|TRUE|FALSE)$",
        "data_type_hint": "STRING",
        "description": "Boolean: true/false or TRUE/FALSE.",
        "positive_scenario_values": json.dumps([
            {"input": "true",  "label": "Lowercase true"},
            {"input": "FALSE", "label": "Uppercase FALSE"}
        ]),
        "negative_scenario_values": json.dumps([
            {"input": "True", "label": "Mixed case"},
            {"input": "1",    "label": "Numeric"},
            {"input": "yes",  "label": "Yes instead"},
            {"input": "",     "label": "Empty string"}
        ])
    },
    {
        "regex_pattern_name": "CURRENCY_USD",
        "regex_pattern": "^\\$?\\d{1,3}(,\\d{3})*(\\.\\d{2})?$",
        "data_type_hint": "STRING",
        "description": "US dollar amount.",
        "positive_scenario_values": json.dumps([
            {"input": "$1,234.56", "label": "Full format"},
            {"input": "1234.56",   "label": "No dollar sign"},
            {"input": "$100",      "label": "Whole dollar"}
        ]),
        "negative_scenario_values": json.dumps([
            {"input": "$1234.567", "label": "Three decimal places"},
            {"input": "-$50.00",   "label": "Negative amount"},
            {"input": "abc",       "label": "Non-numeric"},
            {"input": "",          "label": "Empty string"}
        ])
    },
    {
        "regex_pattern_name": "US_STATE_CODE",
        "regex_pattern": "^(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC|PR|VI|GU|AS|MP)$",
        "data_type_hint": "STRING",
        "description": "US two-letter state/territory code.",
        "positive_scenario_values": json.dumps([
            {"input": "CA", "label": "California"},
            {"input": "NY", "label": "New York"},
            {"input": "TX", "label": "Texas"},
            {"input": "DC", "label": "District of Columbia"}
        ]),
        "negative_scenario_values": json.dumps([
            {"input": "ca",         "label": "Lowercase"},
            {"input": "ZZ",         "label": "Invalid code"},
            {"input": "California", "label": "Full name"},
            {"input": "",           "label": "Empty string"}
        ])
    },
    {
        "regex_pattern_name": "NOT_NULL_NOT_EMPTY",
        "regex_pattern": "^(?!\\s*$).+",
        "data_type_hint": "STRING",
        "description": "Any non-null, non-empty, non-whitespace string.",
        "positive_scenario_values": json.dumps([
            {"input": "A",     "label": "Single char"},
            {"input": "Hello", "label": "Normal word"},
            {"input": "123",   "label": "Numeric string"}
        ]),
        "negative_scenario_values": json.dumps([
            {"input": "",    "label": "Empty string"},
            {"input": "   ", "label": "Whitespace only"},
            {"input": None,  "label": "Null value"}
        ])
    }
]

# COMMAND ----------

# DBTITLE 1,Insert Regex Patterns (MERGE)
from pyspark.sql.types import StructType, StructField, StringType

regex_schema = StructType([
    StructField("regex_pattern_name",       StringType(), False),
    StructField("regex_pattern",            StringType(), False),
    StructField("data_type_hint",           StringType(), True),
    StructField("description",              StringType(), True),
    StructField("positive_scenario_values", StringType(), True),
    StructField("negative_scenario_values", StringType(), True),
])

regex_rows = [(p["regex_pattern_name"], p["regex_pattern"], p.get("data_type_hint"),
               p.get("description"), p.get("positive_scenario_values"),
               p.get("negative_scenario_values")) for p in regex_patterns]

spark.createDataFrame(regex_rows, schema=regex_schema).createOrReplaceTempView("regex_seed")

spark.sql(f"""
MERGE INTO {FW}.tfg_regex_patterns AS t
USING regex_seed AS s ON t.regex_pattern_name = s.regex_pattern_name
WHEN MATCHED THEN UPDATE SET
    t.regex_pattern = s.regex_pattern, t.data_type_hint = s.data_type_hint,
    t.description = s.description, t.positive_scenario_values = s.positive_scenario_values,
    t.negative_scenario_values = s.negative_scenario_values, t.updated_ts = current_timestamp()
WHEN NOT MATCHED THEN INSERT (regex_pattern_name, regex_pattern, data_type_hint, description,
    positive_scenario_values, negative_scenario_values, is_active, created_by, created_ts, updated_ts)
VALUES (s.regex_pattern_name, s.regex_pattern, s.data_type_hint, s.description,
    s.positive_scenario_values, s.negative_scenario_values, TRUE, current_user(), current_timestamp(), current_timestamp())
""")

print(f"✔ Regex patterns: {spark.sql(f'SELECT COUNT(*) c FROM {FW}.tfg_regex_patterns').first()['c']} rows")

# COMMAND ----------

# =============================================================================
# SEED DATA: MASKING RULES
# =============================================================================

# COMMAND ----------

# DBTITLE 1,Seed: Masking Rules
masking_rules = [
    ("HASH_MD5",            "HASH",         "Replace with MD5 hash.",                                 "md5(cast({col} as string))", None, False, False),
    ("HASH_SHA256",         "HASH",         "Replace with SHA-256 hash.",                             "sha2(cast({col} as string), 256)", None, False, False),
    ("NULLIFY",             "NULLIFY",      "Replace with NULL.",                                     "NULL", None, False, False),
    ("REDACT_FIXED",        "REDACT",       "Replace with 'REDACTED'.",                               "'REDACTED'", None, False, False),
    ("REDACT_ASTERISK",     "REDACT",       "Replace all chars with *, preserving length.",           "repeat('*', length(cast({col} as string)))", None, True, False),
    ("PARTIAL_MASK_LAST4",  "PARTIAL_MASK", "Show only last 4 chars, mask rest with *.",              "concat(repeat('*', greatest(0, length(cast({col} as string)) - 4)), right(cast({col} as string), 4))", None, True, False),
    ("PARTIAL_MASK_FIRST6", "PARTIAL_MASK", "Show only first 6 chars, mask rest with *.",             "concat(left(cast({col} as string), 6), repeat('*', greatest(0, length(cast({col} as string)) - 6)))", None, True, False),
    ("EMAIL_MASK_DOMAIN",   "PARTIAL_MASK", "Mask email local part: ***@domain.com.",                 "concat('***@', split(cast({col} as string), '@')[1])", None, True, False),
    ("FAKE_FULL_NAME",      "PII_NAME",     "Replace with a fake full name.",                         None, "faker.name", False, False),
    ("FAKE_FIRST_NAME",     "PII_NAME",     "Replace with a fake first name.",                        None, "faker.first_name", False, False),
    ("FAKE_LAST_NAME",      "PII_NAME",     "Replace with a fake last name.",                         None, "faker.last_name", False, False),
    ("FAKE_EMAIL",          "PII_EMAIL",    "Replace with a fake email address.",                     None, "faker.email", False, False),
    ("FAKE_PHONE_US",       "PII_PHONE",    "Replace with a fake US phone number.",                   None, "faker.phone_number", False, False),
    ("FAKE_SSN",            "PII_SSN",      "Replace with a fake SSN.",                               None, "faker.ssn", True, False),
    ("FAKE_ADDRESS",        "PII_ADDRESS",  "Replace with a fake address.",                           None, "faker.address", False, False),
    ("FAKE_STREET_ADDRESS", "PII_ADDRESS",  "Replace with a fake street address.",                    None, "faker.street_address", False, False),
    ("FAKE_CITY",           "PII_ADDRESS",  "Replace with a fake city.",                              None, "faker.city", False, False),
    ("FAKE_ZIP_CODE",       "PII_ADDRESS",  "Replace with a fake ZIP code.",                          None, "faker.zipcode", True, False),
    ("FAKE_DOB",            "PII_DOB",      "Replace with a fake date of birth.",                     None, "faker.date_of_birth", False, False),
    ("FAKE_CREDIT_CARD",    "PII_CARD",     "Replace with a fake credit card number.",                None, "faker.credit_card_number", False, False),
    ("FAKE_COMPANY",        "PII_NAME",     "Replace with a fake company name.",                      None, "faker.company", False, False),
    ("FAKE_USERNAME",       "PII_NAME",     "Replace with a fake username.",                          None, "faker.user_name", False, False),
    ("FAKE_IPV4",           "CUSTOM",       "Replace with a fake IPv4 address.",                      None, "faker.ipv4", True, False),
    ("SHUFFLE_COLUMN",      "SHUFFLE",      "Shuffle values within column across rows.",              None, None, True, False),
    ("DATE_SHIFT_RANDOM",   "PII_DOB",      "Shift date randomly ±365 days.",                        "date_add(cast({col} as date), cast((rand() * 730 - 365) as int))", None, True, False),
    ("TOKENIZE_UUID",       "TOKEN",        "Replace with a UUID token.",                             "cast(uuid() as string)", None, False, True),
    ("GENERALIZE_AGE_RANGE","CUSTOM",       "Replace age with range (e.g. 34 → '30-39').",           "concat(floor(cast({col} as int) / 10) * 10, '-', floor(cast({col} as int) / 10) * 10 + 9)", None, False, False),
    ("TRUNCATE_TO_YEAR",    "CUSTOM",       "Replace date with just the year.",                       "year(cast({col} as date))", None, False, False),
]

from pyspark.sql.types import BooleanType
mask_schema = StructType([
    StructField("masking_strategy_name", StringType(), False),
    StructField("masking_category",      StringType(), False),
    StructField("masking_description",   StringType(), True),
    StructField("masking_expression",    StringType(), True),
    StructField("faker_provider",        StringType(), True),
    StructField("preserve_format",       BooleanType(), False),
    StructField("is_reversible",         BooleanType(), False),
])

spark.createDataFrame(masking_rules, schema=mask_schema).createOrReplaceTempView("mask_seed")

spark.sql(f"""
MERGE INTO {FW}.tfg_masking_rules AS t
USING mask_seed AS s ON t.masking_strategy_name = s.masking_strategy_name
WHEN MATCHED THEN UPDATE SET
    t.masking_category = s.masking_category, t.masking_description = s.masking_description,
    t.masking_expression = s.masking_expression, t.faker_provider = s.faker_provider,
    t.preserve_format = s.preserve_format, t.is_reversible = s.is_reversible
WHEN NOT MATCHED THEN INSERT (masking_strategy_name, masking_category, masking_description,
    masking_expression, faker_provider, preserve_format, is_reversible, is_active, created_by, created_ts)
VALUES (s.masking_strategy_name, s.masking_category, s.masking_description,
    s.masking_expression, s.faker_provider, s.preserve_format, s.is_reversible,
    TRUE, current_user(), current_timestamp())
""")

print(f"✔ Masking rules: {spark.sql(f'SELECT COUNT(*) c FROM {FW}.tfg_masking_rules').first()['c']} rows")

# COMMAND ----------

# DBTITLE 1,Setup Summary
tables = ["tfg_regex_patterns", "tfg_source_field_mapping", "tfg_masking_rules",
          "tfg_test_profiles", "tfg_run_log", "tfg_test_result_log"]

print(f"\n{'='*60}")
print(f"  ✅ SETUP COMPLETE — {FW}")
print(f"{'='*60}")
for t in tables:
    cnt = spark.sql(f"SELECT COUNT(*) c FROM {FW}.{t}").first()["c"]
    print(f"  ✔ {FW}.{t}  ({cnt} rows)")
print(f"{'='*60}")
print(f"\n  Next: %run ./01_helpers  then run 02_test_file_generator")