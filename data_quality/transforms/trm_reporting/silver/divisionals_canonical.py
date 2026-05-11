# /data_quality/transforms/trm_reporting/silver/divisionals_canonical.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
# from data_quality.transforms.common_transforms import (
#     safe_yyyymmdd_to_date,
#     empty_string_to_null
# )
from transforms.common_transforms import (
    safe_yyyymmdd_to_date,
    empty_string_to_null
)

def canonicalize_divisionals(df: DataFrame) -> DataFrame:
    df = df.withColumn("ser_num", F.trim(F.col("ser_num").cast("string")))
    df = df.withColumn("ref_ser_num", F.trim(F.col("ref_ser_num").cast("string")))
    
    df = safe_yyyymmdd_to_date(df, "dv_dt_rqst")
    df = safe_yyyymmdd_to_date(df, "dv_dt_complete")
    
    df = empty_string_to_null(df)
    return df