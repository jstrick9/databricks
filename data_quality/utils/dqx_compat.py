# /data_quality/utils/dqx_compat.py
from pyspark.sql.column import Column

def make_condition(condition: Column, message: str, name: str) -> Column:
    """
    Local shim for databricks-labs-dqx.make_condition.

    DQEngine in v0.9.x can work with simple boolean Columns as check results.
    We ignore message and name here; DQEngine will still evaluate the condition
    correctly and mark failing rows. Your own error_log builder uses the
    column_name and error_message coming from DQEngine, not this function.
    """
    return condition