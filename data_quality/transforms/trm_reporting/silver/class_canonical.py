# /data_quality/transforms/trm_reporting/silver/class_canonical.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
# from data_quality.transforms.common_transforms import empty_string_to_null
from transforms.common_transforms import empty_string_to_null

def canonicalize_class(df: DataFrame) -> DataFrame:
    """
    Canonicalization for the class table. 
    Format standardization only — leaves invalid data intact for the DQ engine to catch.
    """
    
    # 1. Robust Zero-Padding for classes
    if "class" in df.columns:
        df = df.withColumn("class", F.lpad(F.col("class").cast("string"), 3, "0"))
    
    # 2. Cast Legacy Numeric Codes safely
    if "cl_cls_us_ct" in df.columns:
        df = df.withColumn("cl_cls_us_ct", F.col("cl_cls_us_ct").cast("int"))

    # 3. Final cleanup — empty strings → null
    df = empty_string_to_null(df)
    
    return df