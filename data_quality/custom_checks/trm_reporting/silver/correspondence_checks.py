# /data_quality/custom_checks/trm_reporting/silver/correspondence_checks.py
# from data_quality.utils.dqx_compat import make_condition
from utils.dqx_compat import make_condition
import pyspark.sql.functions as F
# from data_quality.utils.path_utils import load_yaml
from utils.path_utils import load_yaml

# Load allowed values
US_STATES_CODES = load_yaml("us_state_codes.yml")

def valid_us_postal_address() -> F.Column:
    """US addresses must have street + city + state + zip"""
    condition = (
        (F.col("ctry_cd") == "US") &
        (
            F.col("add_line1").isNull() |
            ~F.col("add_line1").rlike("\\d+.*[A-Za-z]+") |  # has number and street name
            F.col("city_nm").isNull() |
            F.col("state_cd").isNull() |
            ~F.col("state_cd").isin(US_STATES_CODES) |
            F.col("zipcode").isNull() |
            ~F.col("zipcode").rlike("^\\d{5}(-\\d{4})?$")
        )
    )
    return make_condition(
        condition,
        "US correspondence address must be complete and valid",
        "invalid_us_correspondence_address"
    )

def electronic_correspondence_authorized() -> F.Column:
    """If email is provided, cr_email_auth must be 'Y'"""
    condition = (
        F.col("cr_email1").isNotNull() &
        (F.col("cr_email_auth") != "Y")
    )
    return make_condition(
        condition,
        "cr_email1 present but cr_email_auth != 'Y' – electronic delivery not authorized",
        "email_unauthorized"
    )

def valid_email_format(col_name: str) -> F.Column:
    regex = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    condition = F.col(col_name).rlike(regex) | F.col(col_name).isNull()
    return make_condition(
        ~condition,
        f"{col_name} must be valid email format",
        f"{col_name}_invalid_format"
    )