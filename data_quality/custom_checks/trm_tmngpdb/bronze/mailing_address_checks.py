# /data_quality/custom_checks/trm_tmngpdb/bronze/mailing_address_checks.py
# from data_quality.utils.dqx_compat import make_condition
from utils.dqx_compat import make_condition
import pyspark.sql.functions as F
# from data_quality.utils.path_utils import load_yaml
from utils.path_utils import load_yaml

# Load allowed values
US_STATE_CODES = load_yaml("us_state_codes.yml")   
ALLOWED_ADDRESS_TYPES = load_yaml("address_types.yml") 


def valid_us_domestic_address() -> F.Column:
    """
    US addresses must have complete, deliverable components.
    """
    condition = (
        (F.col("country_cd") == "US") &
        (
            F.col("name_line_1_tx").isNull() |
            F.col("street_line_1_tx").isNull() |
            ~F.col("street_line_1_tx").rlike("\\d+.*[A-Za-z]+") |  # must have number and street name
            F.col("city_nm").isNull() |
            F.col("geographic_region_cd").isNull() |
            ~F.col("geographic_region_cd").isin(US_STATE_CODES) |
            F.col("postal_cd").isNull() |
            ~F.col("postal_cd").rlike("^\\d{5}(-\\d{4})?$")
        )
    )
    return make_condition(
        condition,
        "US mailing address is incomplete or invalid will be returned by USPS",
        "undeliverable_us_address"
    )


def no_placeholder_name() -> F.Column:
    """
    name_line_1_tx must not be a generic placeholder (e.g., 'ATTORNEY OF RECORD').
    """
    placeholders = [
        "ATTORNEY OF RECORD",
        "CORRESPONDENT",
        "SEE IMAGE",
        "UNKNOWN",
        "N/A",
        "TRADEMARK OWNER",
        "OWNER",
        "APPLICANT"
    ]
    condition = F.upper(F.trim(F.col("name_line_1_tx"))).isin([p.upper() for p in placeholders])
    return make_condition(
        condition,
        "name_line_1_tx contains placeholder not a deliverable name",
        "placeholder_recipient_name"
    )


def valid_international_postal_code() -> F.Column:
    """
    Non-US postal codes must meet minimum standards (at least 3 characters).
    """
    condition = (
        (F.col("country_cd") != "US") &
        (F.col("postal_cd").isNull() | (F.length(F.col("postal_cd")) < 3))
    )
    return make_condition(
        condition,
        "International postal code missing or too short",
        "invalid_international_postal"
    )


def valid_address_type() -> F.Column:
    """
    address_type_ct must be one of the allowed address types from address_types.yml.
    """
    condition_ok = F.upper(F.trim(F.col("address_type_ct"))).isin(ALLOWED_ADDRESS_TYPES)
    condition = ~condition_ok
    return make_condition(
        condition,
        "address_type_ct must be a valid address type (e.g. CORRESPONDENCE, OWNER, DOMESTIC_REP, ATTORNEY, OTHER)",
        "address_type_ct_invalid"
    )