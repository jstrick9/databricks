# Databricks notebook source
# DBTITLE 1,Imports
import logging
import json

# COMMAND ----------

# DBTITLE 1,Get Configuration
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=}, {dbx_env=}")

# COMMAND ----------

# DBTITLE 1,Common Functions
# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,Initialize: Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    force=True
)

logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
logging.getLogger("py4j.clientserver").setLevel(logging.ERROR)
log = logging.getLogger("bronze_loader")
log.setLevel(logging.INFO)

# COMMAND ----------

# DBTITLE 1,Helper Functions
def get_latest_update_predicate(
    catalog: str, schema: str, table: str, filter_column: str, mode: str
) -> str:
    """
    Helper to identify which changed records should be pulled by returning an
    appropriate predicate.

    When a timestamp is returned, the helper returns the timestamp. For none types, a
    dummy predicate is returned (1 = 1).
    """

    def get_timestamp_as_string(timestamp: datetime) -> str:
        """
        Local helper function to convert valid timestamps to strings.
        """
        return timestamp.strftime("%Y-%m-%d %H:%M:%S")

    filtered_modes = set(["merge", "append"])
    try:
        target_last_update_timestamp = (
            spark.sql(
                f"select max({filter_column}) target_last_update_timestamp from {catalog}.{schema}.{table}"
            )
            .collect()[0]
            .target_last_update_timestamp
        )
        if target_last_update_timestamp and mode in filtered_modes:
            log.info("The table appears to have valid CDC filter...")
            value: str = get_timestamp_as_string(target_last_update_timestamp)
            return f"{filter_column} > '{value}'"
        else:
            if mode == "merge":
                log.info(
                    "The table `mode` is appears to be a merge operation; however, the target table appears to be empty. Using dummy predicate..."
                )
            elif mode == "append":
                log.info(
                    "The table `mode` is appears to be a append operation; however, the target table appears to be empty. Using dummy predicate..."
                )
            else:
                log.info(
                    "The table `mode` appears to be a CDC operation. Using dummy predicate..."
                )
            return "1 = 1"
    except Exception as e:
        log.warning(
            f"An issue occured when trying to get and parse the latest timestamp for `{catalog}.{schema}.{table}`. Using dummy predicate..."
        )
        return "1 = 1"


def validate_input_structure(input_values: dict):
    """
    Helper to validate input values that have been passed down by the upstream task.
    """
    required_keys = {"name", "columns", "mode"}
    mode_required_keys = {"merge_condition"}
    valid_types = {
        "string",
        "long",
        "integer",
        "int",
        "double",
        "float",
        "boolean",
        "date",
        "timestamp",
        "binary",
        "short",
    }
    for key, document in enumerate(input_values):
        entry_id = document.get("name", key)
        missing_keys = required_keys - document.keys()
        log.info("Checking for missing keys...")
        if missing_keys:
            raise ValueError(
                f"Entry `{entry_id}` | Missing required keys: {missing_keys}"
            )
        log.info("No missing keys found.")
        columns = document.get("columns", {})
        log.info("Checking for general column issues...")
        if not isinstance(columns, dict):
            raise ValueError(f"Entry `{entry_id}` | `columns` must be a dictionary.")
        for column, _type in columns.items():
            if not isinstance(column, str) or not isinstance(_type, str):
                raise ValueError(
                    f"Entry `{entry_id}` | Column name and type must be strings. Found: {column} ({type(column)}): {_type} ({type(_type)})"
                )
            if _type.lower() not in valid_types:
                raise ValueError(
                    f"Entry `{entry_id}` | Column '{column}' has invalid Spark type '{_type}'. Valid types: {valid_types}"
                )
        log.info("No general column issues found.")
        mode = document.get("mode", {})
        log.info("Checking merge condition if applicable...")
        if mode.get("mode_type") == "merge":
            if "merge_condition" not in mode:
                raise ValueError(
                    f"Entry `{entry_id}` | `merge_condition` key required is required when using `mode_type` =  `merge`."
                )
            merge_condition = mode.get("merge_condition")
            if (
                not merge_condition
                or not isinstance(merge_condition, list)
                or len(merge_condition) == 0
            ):
                raise ValueError(
                    f"Entry `{entry_id}` | `merge_condition` key must have one or more values for `merge` type."
                )
            if any(
                not isinstance(column, str) or len(column) == 0
                for column in merge_condition
            ):
                raise ValueError(
                    f"Entry `{entry_id}` | All values in `merge_condition` must be non-empty strings for `merge` type."
                )
        log.info("No merge condition issue was found.")


def _get_jdbc_bounds(query: str, column: str) -> tuple[str, str]:
    """
    Helper to fetch the min and max values of the partition column
    from the source query using a single JDBC read
    """
    bounds_query = f"(select min({column}) as lower_bound, max({column}) as upper_bound from ({query}) as bounds_subquery) as bounds"
    row = (
        spark.read.format("jdbc")
        .option("url", URL)
        .option("dbtable", bounds_query)
        .option("user", USER)
        .option("password", PASSWORD)
        .option("driver", DRIVER)
        .load()
        .first()
    )
    bounds = str(row["lower_bound"]), str(row["upper_bound"])
    log.info(f"Using bounds for partitioning: {bounds}")
    return bounds

# COMMAND ----------

# DBTITLE 1,Set Configuration
common_configs = read_yaml(config_file)
reporting_catalog = common_configs["schema"]["trm_reporting_catalog"]
target_catalog = common_configs["schema"]["myuspto_catalog"]
run_env = dbx_env
print(target_catalog, reporting_catalog, run_env)

# COMMAND ----------

# DBTITLE 1,Globals
DRIVER: str = "com.mysql.cj.jdbc.Driver"

HOST: str = dbutils.secrets.get(scope="myuspto_scope", key="host")
PORT: str = dbutils.secrets.get(scope="myuspto_scope", key="port")
DATABASE = source_schema = dbutils.secrets.get(scope="myuspto_scope", key="database")
USER: str = dbutils.secrets.get(scope="myuspto_scope", key="user")
PASSWORD: str = dbutils.secrets.get(scope="myuspto_scope", key="password")

URL: str = f"jdbc:mysql://{HOST}:{PORT}/{DATABASE}"

NUM_PARTITIONS: int = 100

# COMMAND ----------

# DBTITLE 1,Details: Source Table
table_config = json.loads(dbutils.widgets.get("table"))
log.info("Validating table configuration from upstream task...")
validate_input_structure([table_config])
log.info("Table configuration from upstream task looks valid.")

table: str = table_config["name"]
filter_column: str = table_config["filter_column"]
columns: str = ", ".join([column for column in table_config["columns"]])
merge_condition: str = " AND ".join(
    [
        f"`target`.{column} = `source`.{column}"
        for column in table_config["mode"]["merge_condition"]
    ]
)
mode: str = table_config["mode"]["mode_type"]

predicate: str = get_latest_update_predicate(
    catalog=target_catalog,
    schema="bronze",
    table=table,
    filter_column=filter_column,
    mode=mode,
)

query = f"""
    select
        {columns}
    from
        {source_schema}.{table}
    where
        {predicate}
"""

lower_bound, upper_bound = _get_jdbc_bounds(query=query, column=filter_column)

# COMMAND ----------

# DBTITLE 1,Start Job
job_name = f"ntb_myuspto_bronze_load_{table}"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# DBTITLE 1,Setup: Source and Target
if lower_bound == "None" or upper_bound == "None": # _get_jdbc_bounds function returns tuple(str, str)
    source = (
        spark.read.format("jdbc")
        .option("url", URL)
        .option("dbtable", f"({query}) AS partitioned_source")
        .option("user", USER)
        .option("password", PASSWORD)
        .option("driver", DRIVER)
        .load()
    )
else:
    source = (
        spark.read.format("jdbc")
        .option("url", URL)
        .option("dbtable", f"({query}) AS partitioned_source")
        .option("user", USER)
        .option("password", PASSWORD)
        .option("driver", DRIVER)
        .option("partitionColumn", filter_column)
        .option("lowerBound", lower_bound)
        .option("upperBound", upper_bound)
        .option("numPartitions", NUM_PARTITIONS)
        .load()
    )
    
target = spark.sql(f"select * from {target_catalog}.bronze.{table}")
target_table_fqn: str = f"{target_catalog}.bronze.{table}"

# COMMAND ----------

# DBTITLE 1,Execute: Table Write Mode
mode_override = "overwrite" if target.count() == 0 else mode
if mode != mode_override:
    log.info("0 records were found in the target data source. `mode` will be set to `overwrite`.")
    mode = mode_override
if mode == "merge":
    source.createOrReplaceTempView("source")
    target.createOrReplaceTempView("target")
    log.info("Incoming source data will perform a merge against the target table.")
    spark.sql(f"""
        merge into `target`
        using `source`
        on {merge_condition}
        when matched then update set *
        when not matched then insert *
    """)
elif mode == "overwrite":
    log.info("Incoming source data will perform an overwrite against the target table.")
    source.write.mode("overwrite").saveAsTable(target_table_fqn)
elif mode == "append":
    log.info("Incoming source data will perform an append against the target table.")
    source.write.mode("append").saveAsTable(target_table_fqn)
else:
    log.error("No appropriate mode was specified.")
    raise ValueError("`mode` located `table_config.yml` must be one of: `merge`, `overwrite`, or `append`")

# COMMAND ----------

# DBTITLE 1,Execute: ANALYZE and OPTIMIZE
spark.sql(f"optimize {target_table_fqn}")
spark.sql(f"analyze table {target_table_fqn} compute statistics for all columns")

# COMMAND ----------

# DBTITLE 1,End Job
end_job_cntl(
    f"{reporting_catalog}.silver",
    job_name,
    job_start_ts,
    "completed",
    0,
    "job completed successfully",
)