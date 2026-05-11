import os
import yaml

def get_repo_root() -> str:
    """
    Resolve the absolute path to the *runtime* data_quality root directory.

    Priority:
      1) DQ_RUNTIME_ROOT environment variable (recommended for Workflows/Jobs)
      2) Walk up from this file until we find a folder named 'data_quality'

    This makes the framework portable across different Databricks Repos paths like:
      /Workspace/Repos/<org>/<repo>/notebooks/python/data_quality
    """
    # 1) Environment override (best for Workflows/Jobs)
    env_root = os.getenv("DQ_RUNTIME_ROOT")
    if env_root:
        env_root = os.path.abspath(env_root)
        if os.path.basename(env_root) != "data_quality":
            raise RuntimeError(
                f"DQ_RUNTIME_ROOT must point to the data_quality directory itself. "
                f"Got: {env_root}"
            )
        if not os.path.isdir(env_root):
            raise RuntimeError(f"DQ_RUNTIME_ROOT does not exist or is not a directory: {env_root}")
        return env_root

    # 2) Walk up from this file
    current_file = os.path.abspath(__file__)
    path = os.path.dirname(current_file)

    for _ in range(30):  # allow deep repo nesting
        if os.path.basename(path) == "data_quality":
            return path

        parent = os.path.dirname(path)
        if parent == path:
            break
        path = parent

    raise RuntimeError(
        f"Cannot find data_quality root by walking up from {current_file}. "
        f"Set environment variable DQ_RUNTIME_ROOT to the full path of your data_quality directory."
    )


_REPO_ROOT = get_repo_root()

def load_yaml(file_name: str):
    """
    Load YAML file from allowed_values/<file_name> under the resolved repo root.
    """
    path = os.path.join(_REPO_ROOT, "allowed_values", file_name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"YAML not found: {path}")
    with open(path, "r") as f:
        return yaml.safe_load(f)