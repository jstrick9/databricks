"""
Shared pytest configuration for DQ tests.
"""
import sys
import os

sys.dont_write_bytecode = True

# dq_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# if dq_root not in sys.path:
#     sys.path.insert(0, dq_root)

# repo_root = os.path.abspath(os.path.join(dq_root, ".."))
# if repo_root not in sys.path:
#     sys.path.insert(0, repo_root)

DQ_ROOT = "/Workspace/Users/joshua.strickland@uspto.gov/data_quality"

if DQ_ROOT not in sys.path:
    sys.path.insert(0, DQ_ROOT)

import pytest


@pytest.fixture(scope="session")
def spark():
    """
    Reuse the existing Databricks SparkSession when running via pytest.main()
    inside a notebook. Falls back to creating a local session for CI runners.
    """
    from pyspark.sql import SparkSession

    # Try to get the existing Databricks session first
    existing = SparkSession.getActiveSession()
    if existing is not None:
        yield existing
        return  # Do NOT stop the Databricks session

    # Fallback: create a local session (GitLab CI / local dev)
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("dq_unit_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-test")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def _clean_temp_views(spark):
    """Drop any temp views created during a test."""
    yield
    for table in spark.catalog.listTables():
        if table.isTemporary:
            spark.catalog.dropTempView(table.name)