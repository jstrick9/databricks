# /data_quality/transforms/trm_reporting/silver/correspondence_canonical.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
# from data_quality.transforms.common_transforms import (
#     uppercase_and_trim,
#     clean_email,
#     apply_country_canonicalization,
#     empty_string_to_null
# )
from transforms.common_transforms import (
    uppercase_and_trim,
    clean_email,
    apply_country_canonicalization,
    empty_string_to_null
)

def canonicalize_correspondence(df: DataFrame) -> DataFrame:
    """
    Canonicalize silver.correspondence table — perfect for public TSDR and mail delivery.
    Uses only centralized common transforms — elite-tier architecture.
    """

    # 1. Name fields — uppercase + trim
    name_cols = ["cor_nm", "firm_nm", "atty_nm", "domestic_rep"]
    for col in name_cols:
        if col in df.columns:
            df = uppercase_and_trim(df, col)

    # 2. Country code + name — perfect canonicalization
    if "ctry_cd" in df.columns:
        df = apply_country_canonicalization(df, "ctry_cd", "ctry_nm")

    # 3. All email fields — lowercase + trim
    email_cols = [c for c in df.columns if "email" in c.lower()]
    for col in email_cols:
        df = clean_email(df, col)

    # 4. Address fields — clean and standardize
    address_cols = ["add_line1", "add_line2", "city_nm", "state_cd", "state_nm", "zipcode"]
    for col in address_cols:
        if col in df.columns:
            df = uppercase_and_trim(df, col)

    # 5. Legacy fields — clean up
    legacy_cols = ["ctry_name_caps", "country_or_area_name", "iso_alpha3_code"]
    for col in legacy_cols:
        if col in df.columns:
            df = uppercase_and_trim(df, col)

    # 6. Final cleanup — empty strings → null
    df = empty_string_to_null(df)

    return df