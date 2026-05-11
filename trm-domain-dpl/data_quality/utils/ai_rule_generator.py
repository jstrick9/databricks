"""
AI-Powered Rule Generator.
Converts plain English descriptions into DQ check YAML configurations
using Databricks Foundation Model APIs.

Usage:
    from utils.ai_rule_generator import generate_rules_from_description
    yaml_output = generate_rules_from_description(
        "Flag any email that doesn't look real. 
         Serial numbers must be exactly 8 digits. 
         Country codes must be valid ISO codes.",
        table_columns=["email", "serial_number", "country_code"]
    )
"""
import json
import re
import yaml
from mlflow.deployments import get_deploy_client

# Databricks Foundation Model API endpoint name
DEFAULT_LLM_ENDPOINT = "databricks-meta-llama-3-1-70b-instruct"


SYSTEM_PROMPT = (
    "You are a Data Quality rule configuration expert for an enterprise PySpark "
    "Data Quality framework. Convert plain English requirements into valid YAML.\n\n"
    "Return ONLY YAML. No markdown. No commentary.\n\n"
    "YAML format:\n"
    "checks:\n"
    "  - check:\n"
    "      function: <function_name>\n"
    "      arguments:\n"
    "        col_name: <column_name>\n"
    "    criticality: <error|warning>\n\n"
    "Available functions:\n"
    "- is_not_null(col_name)\n"
    "- is_unique(col_name)\n"
    "- regex_match(col_name, regex or regex_name)\n"
    "- valid_iso_country_code(col_name)\n"
    "- valid_email_format(col_name)\n"
    "- values_in_0_or_1(col_name)\n"
    "- all_caps(col_name)\n"
    "- valid_ph_action_code(col_name)\n"
    "- created_before_last_modified(create_col, modified_col)\n"
    "- fiscal_year_matches_date(fiscal_year_col, date_col)\n\n"
    "Rules:\n"
    "- Only use column names from the provided list.\n"
    "- Use criticality=error for quarantine-worthy violations.\n"
    "- Use criticality=warning for flag-only violations.\n"
)


def _get_client():
    return get_deploy_client("databricks")


def generate_rules_from_description(
    description: str,
    table_columns: list,
    llm_endpoint: str = DEFAULT_LLM_ENDPOINT,
    pii_columns: list | None = None
) -> dict:
    """
    Convert plain English requirements into DQ checks YAML.

    Returns: {"checks": [ ... ]}
    """
    pii_columns = pii_columns or []
    pii_set = {c.lower() for c in pii_columns}

    col_lines = []
    for c in table_columns:
        suffix = " (PII)" if c.lower() in pii_set else ""
        col_lines.append(f"- {c}{suffix}")

    user_message = (
        "Convert the following requirements into YAML checks.\n\n"
        f"REQUIREMENTS:\n{description}\n\n"
        "AVAILABLE COLUMNS:\n" + "\n".join(col_lines) + "\n"
    )

    client = _get_client()
    resp = client.predict(
        endpoint=llm_endpoint,
        inputs={
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            "temperature": 0.1,
            "max_tokens": 2000,
        },
    )

    raw = resp["choices"][0]["message"]["content"].strip()

    # Strip code fences if the model included them
    raw = re.sub(r"^```yaml\s*", "", raw)
    raw = re.sub(r"^```\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw).strip()

    parsed = yaml.safe_load(raw) or {}
    if "checks" not in parsed or not isinstance(parsed["checks"], list):
        raise ValueError("LLM did not return a valid YAML document with a top-level 'checks:' list.")

    # Normalize criticality + argument keys to support your existing configs
    parsed["checks"] = normalize_checks_metadata(parsed["checks"])
    return parsed


def normalize_checks_metadata(checks: list) -> list:
    """
    Accept both legacy and new key styles:
      arguments: {column: X}  -> {col_name: X}
      criticality: warn       -> warning
    """
    out = []
    for item in checks:
        if not isinstance(item, dict):
            continue

        # Support nested format: {"check": {...}, "criticality": "..."}
        if "check" in item and isinstance(item["check"], dict):
            chk = item["check"]
        else:
            chk = item

        args = chk.get("arguments", {}) or {}

        # Normalize common arg naming variants
        if "column" in args and "col_name" not in args:
            args["col_name"] = args.pop("column")
        if "columns" in args and "col_names" not in args:
            args["col_names"] = args.pop("columns")

        chk["arguments"] = args

        crit = item.get("criticality", "warning")
        if crit == "warn":
            crit = "warning"
        item["criticality"] = crit

        # Ensure nested 'check' key exists
        if "check" not in item:
            item = {"check": chk, "criticality": crit}

        out.append(item)

    return out