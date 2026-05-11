# /data_quality/transforms/trm_reporting/silver/fpep_fact_canonical.py
from pyspark.sql import DataFrame
# from data_quality.transforms.common_transforms import (
#     canonicalize_serial_number,
#     uppercase_and_trim,
#     canonicalize_fpep_category,
#     empty_string_to_null
# )
from transforms.common_transforms import (
    canonicalize_serial_number,
    uppercase_and_trim,
    canonicalize_fpep_category,
    empty_string_to_null
)
def canonicalize_fpep_fact(df):
    df = canonicalize_serial_number(df, "SER_NUM")
    df = canonicalize_fpep_category(df, "CATEGORY")
    df = uppercase_and_trim(df, "FK_WRKR_ID")
    df = empty_string_to_null(df)
    return df