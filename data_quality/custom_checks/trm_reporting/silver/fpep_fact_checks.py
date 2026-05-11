# /data_quality/custom_checks/trm_reporting/silver/fpep_fact_checks.py
# from data_quality.utils.dqx_compat import make_condition
from utils.dqx_compat import make_condition
import pyspark.sql.functions as F
# from data_quality.utils.path_utils import load_yaml
from utils.path_utils import load_yaml

# Load allowed FPEP categories
FPEP_RULES = load_yaml("fpep_categories.yml")  # e.g. {"FA": "FA", "FA-AMEND": "FA-Amend", ...}

def valid_fpep_category() -> F.Column:
    """
    CATEGORY must exist in the official FPEP category list (fpep_categories.yml)
    """
    valid_category = F.col("CATEGORY").isin(list(FPEP_RULES.keys()))
    condition = ~valid_category

    return make_condition(
        condition,
        "CATEGORY must be one of the allowed FPEP categories defined in fpep_categories.yml",
        "CATEGORY_invalid_fpep_category"
    )