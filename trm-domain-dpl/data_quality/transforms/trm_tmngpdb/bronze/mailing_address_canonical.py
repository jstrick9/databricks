# /data_quality/transforms/trm_tmngpdb/bronze/mailing_address_canonical.py
from pyspark.sql import DataFrame
# from data_quality.transforms.common_transforms import (
#     uppercase_and_trim,
#     standardize_street_address,
#     apply_country_canonicalization,
#     empty_string_to_null
# )
from transforms.common_transforms import (
    uppercase_and_trim,
    standardize_street_address,
    apply_country_canonicalization,
    empty_string_to_null
)
from pyspark.sql import functions as F

def remove_placeholder_names(df: DataFrame) -> DataFrame:
    """Remove known garbage recipient names — critical for delivery"""
    placeholders = ["ATTORNEY OF RECORD", "CORRESPONDENT", "SEE IMAGE", "UNKNOWN", "N/A", "TRADEMARK OWNER", "OWNER", "APPLICANT"]
    
    df = df.withColumn(
        "name_line_1_tx",
        F.when(
            F.upper(F.trim(F.col("name_line_1_tx"))).isin(placeholders),
            F.lit(None)
        ).otherwise(F.trim(F.upper(F.col("name_line_1_tx"))))
    )
    
    df = df.withColumn(
        "name_line_2_tx",
        F.when(
            F.trim(F.col("name_line_2_tx")).isin(["", None]),
            F.lit(None)
        ).otherwise(F.trim(F.upper(F.col("name_line_2_tx"))))
    )
    
    return df

def canonicalize_mailing_address(df: DataFrame) -> DataFrame:
    """Canonicalization of bronze.mailing_address."""
    
    df = remove_placeholder_names(df)
    
    if "name_line_1_tx" in df.columns: df = uppercase_and_trim(df, "name_line_1_tx")
    if "name_line_2_tx" in df.columns: df = uppercase_and_trim(df, "name_line_2_tx")

    if "street_line_1_tx" in df.columns: df = standardize_street_address(df, "street_line_1_tx")
    if "street_line_2_tx" in df.columns: df = standardize_street_address(df, "street_line_2_tx")

    if "city_nm" in df.columns: df = uppercase_and_trim(df, "city_nm")
    if "geographic_region_cd" in df.columns: df = uppercase_and_trim(df, "geographic_region_cd")

    # FIX: Country code + name PERFECT canonicalization MUST happen BEFORE postal codes
    df = apply_country_canonicalization(df, "country_cd", "country_nm")

    # Postal Code — clean aggressively (Now correctly recognizes standard "US")
    if "postal_cd" in df.columns:
        df = df.withColumn(
            "postal_cd",
            F.when(
                F.col("country_cd") == "US",
                F.regexp_replace(F.trim(F.col("postal_cd")), "[^0-9-]", "")
            ).otherwise(
                F.upper(F.regexp_replace(F.trim(F.col("postal_cd")), "\\s+", ""))
            )
        )

    if "department_nm" in df.columns: df = uppercase_and_trim(df, "department_nm")
    
    df = empty_string_to_null(df)
    return df