# /data_quality/custom_checks/trm_reporting/silver/divisionals_checks.py
# from data_quality.utils.dqx_compat import make_condition
from utils.dqx_compat import make_condition
import pyspark.sql.functions as F

def valid_madrid_divisional_filing_window() -> F.Column:
    """
    US filing_dt must be within 18 months of WIPO notification (ib_notification_dt)
    """
    months_diff = F.months_between(F.col("filing_dt"), F.col("ib_notification_dt"))
    condition = (months_diff > 18) | (F.col("filing_dt") < F.col("ib_notification_dt"))
    return make_condition(
        condition,
        "filing_dt must be within 18 months after ib_notification_dt",
        "madrid_divisional_deadline_missed"
    )

def divisional_request_to_us_filing_timely() -> F.Column:
    """
    From WIPO request (dv_dt_rqst) to US filing ≤ 90 days typical processing
    """
    condition = F.datediff(F.col("filing_dt"), F.col("dv_dt_rqst")) > 120  # allowing buffer
    return make_condition(
        condition,
        "Divisional request to US filing exceeds normal processing time",
        "divisional_processing_delay"
    )

def parent_ir_is_valid_madrid_number() -> F.Column:
    """
    ref_ser_num must be a valid WIPO International Registration number (7-10 digits)
    """
    condition = ~F.col("ref_ser_num").rlike("^[0-9]{7,10}$")
    return make_condition(
        condition,
        "ref_ser_num must be valid WIPO IR number (7-10 digits)",
        "invalid_madrid_parent_ir"
    )