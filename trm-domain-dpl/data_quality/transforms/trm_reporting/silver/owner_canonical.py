# /data_quality/transforms/trm_reporting/silver/owner_canonical.py
from pyspark.sql import DataFrame
# from data_quality.transforms.common_transforms import (
#     uppercase_and_trim,
#     clean_email,
#     apply_country_canonicalization,
#     canonicalize_serial_number,
#     empty_string_to_null
# )
from transforms.common_transforms import (
    uppercase_and_trim,
    clean_email,
    apply_country_canonicalization,
    canonicalize_serial_number,
    empty_string_to_null
)

def canonicalize_owner(df: DataFrame) -> DataFrame:
    """
    Canonicalize silver.owner table — legal owner data for certificates, Madrid, and public record.
    """

    # 1. Owner name fields — uppercase + trim (appears on certificate)
    name_cols = ["name", "cor_nm", "firm_nm"]
    for col in name_cols:
        if col in df.columns:
            df = uppercase_and_trim(df, col)

    # 2. Country code + name — perfect canonicalization (Madrid-critical)
    if "ctry_cd" in df.columns or "ctry_nm" in df.columns:
        df = apply_country_canonicalization(df, "ctry_cd", "ctry_nm")

    # 3. Citizenship country — same rules as owner country
    if "citizenship" in df.columns:
        # Reuse the same function — citizenship uses same country list
        df = apply_country_canonicalization(df, "citizenship", None)  # name_col=None → only code

    # 4. All email fields — lowercase + trim
    email_cols = [c for c in df.columns if "email" in c.lower()]
    for col in email_cols:
        df = clean_email(df, col)

    # 5. Address fields — clean and standardize
    address_cols = ["address_1", "address_2", "city", "state_cd", "postal_cd"]
    for col in address_cols:
        if col in df.columns:
            df = uppercase_and_trim(df, col)

    # 6. Legacy/optional fields — clean up
    legacy_cols = ["country_or_area_name", "ctry_name_caps"]
    for col in legacy_cols:
        if col in df.columns:
            df = uppercase_and_trim(df, col)

    # 7. Serial number — canonical if present
    if "ser_num" in df.columns:
        df = canonicalize_serial_number(df, "ser_num")

    # 8. Final cleanup — empty strings → null (critical for joins)
    df = empty_string_to_null(df)

    return df