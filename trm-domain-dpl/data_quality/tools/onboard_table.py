import os
import yaml
from pyspark.sql import SparkSession
from utils.path_utils import get_repo_root

spark = SparkSession.builder.getOrCreate()
repo_root = get_repo_root()

def onboard(catalog: str, schema: str, table_name: str, dbx_env: str = "dev", dry_run: bool = True) -> dict:
    """
    Create:
      - checks/<catalog>/<schema>/<table>_checks.yml
      - hash_configs/<catalog>/<schema>/<table>_hash_config.yml
      - transforms/<catalog>/<schema>/<table>_canonical.py

    Reads live schema from Unity Catalog for the given env + logical catalog.
    """
    from engine.dq_engine import load_env_config  # safe after lazy DQX import fix

    config = load_env_config(dbx_env, catalog)
    catalog_physical = config["trgt_catalog"]
    full_table = f"{catalog_physical}.{schema}.{table_name}"

    df = spark.table(full_table)
    cols = [f.name for f in df.schema.fields if not f.name.startswith("_")]

    # Minimal default checks (warning-level not_null)
    checks = {
        "checks": [
            {
                "check": {"function": "is_not_null", "arguments": {"col_name": c}},
                "criticality": "warn"
            }
            for c in cols[:10]  # keep small; user can expand
        ]
    }

    # Default hash config
    hash_cfg = {
        "table_name": table_name,
        "natural_key_columns": [cols[0]] if cols else [],
        "use_all_columns_for_data_hash": False,
        "deterministic_columns_for_data_hash": cols
    }

    canonical_py = f'''from pyspark.sql import DataFrame
from transforms.common_transforms import empty_string_to_null

def canonicalize_{table_name}(df: DataFrame) -> DataFrame:
    # TODO: Add format-only canonicalization here (trim, uppercase, date parsing, etc.)
    return empty_string_to_null(df)
'''

    paths = {
        "checks": os.path.join(repo_root, "checks", catalog, schema, f"{table_name}_checks.yml"),
        "hash_config": os.path.join(repo_root, "hash_configs", catalog, schema, f"{table_name}_hash_config.yml"),
        "canonical": os.path.join(repo_root, "transforms", catalog, schema, f"{table_name}_canonical.py"),
    }

    out = {}

    for k, p in paths.items():
        os.makedirs(os.path.dirname(p), exist_ok=True)

        if k == "checks":
            content = yaml.dump(checks, sort_keys=False)
        elif k == "hash_config":
            content = yaml.dump(hash_cfg, sort_keys=False)
        else:
            content = canonical_py

        out[k] = {"path": p, "content": content}

        if dry_run:
            continue

        if not os.path.exists(p):
            with open(p, "w") as f:
                f.write(content)

    return out