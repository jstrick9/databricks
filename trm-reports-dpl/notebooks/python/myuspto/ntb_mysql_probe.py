# Databricks notebook source
# MAGIC %md
# MAGIC # Pre-flight / MySQL Probe
# MAGIC The purpose of this script is to probe MySQL before executing against it. This should limit the opportunity that a workflow should run (which uses the upstream tables). Moreover, we can contextualize errors we receive, and analyze the conditions and times that they are occurring.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# DBTITLE 1,Imports
import socket
import time
import logging
from typing import Optional
import functools
import yaml

# COMMAND ----------

# DBTITLE 1,Get Configuration
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name: str = "trmreports-conf.yaml"
config_file: str = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=}, {dbx_env=}")

# COMMAND ----------

# DBTITLE 1,Common Functions
# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,Table Configuration
table_config_file: str = "./table_config.yml"
table_configs = read_yaml(table_config_file)

# COMMAND ----------

# DBTITLE 1,Globals
WORKFLOW_TABLES: list[str] = [table["name"] for table in table_configs]

DRIVER: str = "com.mysql.cj.jdbc.Driver"

HOST: str = dbutils.secrets.get(scope="myuspto_scope", key="host")
PORT: str = dbutils.secrets.get(scope="myuspto_scope", key="port")
DATABASE: str = dbutils.secrets.get(scope="myuspto_scope", key="database")
USER: str = dbutils.secrets.get(scope="myuspto_scope", key="user")
PASSWORD: str = dbutils.secrets.get(scope="myuspto_scope", key="password")
URL: str = f"jdbc:mysql://{HOST}:{PORT}/{DATABASE}"

TIMEOUT: int = 5
RETRIES: int = 1
RETRY_DELAY: int = 5

CONNECTION_PROPERTIES: dict = {
    "url": URL,
    "user": USER,
    "password": PASSWORD,
    "driver": DRIVER,
    "numPartitions": 1,
}
KNOWN_PREFIXES: set[str] = set(["An error occurred while calling", "org.apache.spark"])

# COMMAND ----------

# DBTITLE 1,Initialize: Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)
log = logging.getLogger("preflight_probe")

# COMMAND ----------

# DBTITLE 1,Functions: Helpers
def retry(retries: int = RETRIES, delay: int = RETRY_DELAY):
    """
    Wrapper to retry with backoff during the probe.

    Note: This doesn't use exponential backoff.
    """

    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            previous_exception: Optional[Exception] = None
            for attempt in range(1, retries + 1):
                try:
                    return f(*args, **kwargs)
                except RuntimeError as e:
                    previous_exception = e
                    if attempt < retries:
                        log.warning(
                            f"Attempt {attempt}/{retries} failed. Retrying in {delay} seconds. ({e})"
                        )
                        time.sleep(delay)
                    else:
                        log.error(
                            f"All {retries} attempts ehausted for `{f.__name__}`."
                        )
            raise previous_exception

        return wrapper

    return decorator


def _get_jdbc_error(exception: Exception) -> str:
    """
    Attempts to pull the exception from the py4j.
    """
    exception_message: str = str(exception)
    if "\n" in exception_message:
        exception_message: str = exception_message.split("\n")[0]
    for prefix in KNOWN_PREFIXES:
        if prefix in exception_message:
            exception_message: str = exception_message[exception_message.find(prefix)]
    return exception_message.strip()


def _set_options(query: str, properties: dict = CONNECTION_PROPERTIES) -> dict:
    """
    Helper to add a query to the JDBC connection.
    """
    properties.update({"query": query})
    return properties


def report(failures: list[str]):
    """
    Reports out which steps failed as part of the pre-flight check. 
    As of now, this will failfast in the runner.
    """
    if not failures:
        log.info(
            "[RUNNER] [SUCCESS] Pre-flight / probe checks passed. Proceeding to workflow."
        )
        return
    log.error("[RUNNER] [FAILURE] Pre-flight / probe checks failed. Workflow delayed.")
    for i, exception_message in enumerate(failures, 1):
        log.error(f"{i}. {exception_message}")
    raise RuntimeError("Failing workflow.")

# COMMAND ----------

# DBTITLE 1,Functions: Tests
# Should put these in a runner class...
def check_driver(driver: str):
    """
    Helper to check that the MySQL driver exists.
    """

    log.info(f"[DRIVER] Checking for `{driver}` ...")
    try:
        spark._jvm.Class.forName(driver) # need to double check the driver class name
        log.info("[DRIVER] [SUCCESS] Driver is installed.")
    except Exception as e:
        raise RuntimeError(
            f"[DRIVER] [FAILURE] Driver is not installed. You will need to ensure {driver} is available before proceeding."
        )

@retry()
def test_dns_resolution(host: str):
    """
    Tests whether the DNS is resolvable.
    """
    log.info(f"[DNS] Resolving DNS... `{host}`")
    try:
        dummy = socket.getaddrinfo(
            host=host, port=None
        ) # I don't think there is a need to report the resolved IPs themselves since these will be dynamic, so I'll just count them
        is_resolvable: bool = len(dummy) > 0
        log.info(f"[DNS] [SUCCESS] Resolved IPs: {is_resolvable}.")
        return is_resolvable
    except socket.gaierror as e:
        raise RuntimeError(
            f"[DNS] [FAILURE] Failed to resolve any IPs for host `{host}` ({e})."
        ) from e


@retry()
def test_port_reachable(host: str, port: int, timeout: int = TIMEOUT):
    """
    Tests whether the port is reachable.
    """
    log.info(f"[TCP] Probing {host}:{port} (timeout={timeout})s")
    try:
        with socket.create_connection(address=(host, port), timeout=timeout):
            log.info(f"[TCP] [SUCCESS] `{host}:{port}` is open.")
    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        raise RuntimeError(
            f"[TCP] [FAILURE] Could not reach `{host}:{port}` ({e})"
        ) from e


@retry()
def test_authentication():
    """
    Tests whether the supplied authentication is successful.
    """
    log.info("[JDBC] Connecting to MySQL with authentication.")
    try:
        data = (
            spark.read.format("jdbc")
            .options(**_set_options("select 1 as active"))
            .load()
        )
        rows = data.collect()
        if rows and rows[0].active == 1:
            log.info("[JDBC] [SUCCESS] Connection established. Table accessible.")
        else:
            raise RuntimeError(
                "[JDBC] [FAILURE] Connection established but returned unexpected results."
            )
    except RuntimeError:
        raise
    except Exception as e:
        exception_message = _get_jdbc_error(e)
        raise RuntimeError(
            f"[JDBC] [FAILURE] Failed to establish a connection: {exception_message}"
        ) from e


@retry()
def test_table_accessible(table: str):
    """
    Test whether the table is accessible.
    """
    log.info(f"[TABLE] Checking access to `{table}`...")
    try:
        query = f"select 1 as active from {table} limit 1"
        data = spark.read.format("jdbc").options(**_set_options(query)).load()
        data.collect()
        log.info(f"[TABLE] [SUCCESS] `{table}` is accessible.")
    except Exception as e:
        exception_message = _get_jdbc_error(e)
        raise RuntimeError(f"[TABLE] [FAILURE] `{table}` is not accessible.") from e

# COMMAND ----------

# DBTITLE 1,Function: Runner
def execute_runner():
    log.info("Checking MySQL before execution...")

    failures: list[str] = []
    try:
        test_dns_resolution(HOST)
    except RuntimeError as e:
        failures.append(str(e))
        report(failures)
    try:
        test_port_reachable(HOST, PORT)
    except RuntimeError as e:
        failures.append(str(e))
        report(failures)
    try:
        test_authentication()
    except RuntimeError as e:
        failures.append(str(e))
        report(failures)

    for table in WORKFLOW_TABLES:
        try:
            test_table_accessible(table)
        except RuntimeError as e:
            failures.append(str(e))
        report(failures)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution

# COMMAND ----------

# DBTITLE 1,Execute: Pre-flight
execute_runner()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Downstream Propagation

# COMMAND ----------

# DBTITLE 1,Send: Table Details to Downstream Tasks
dbutils.jobs.taskValues.set(key="tables", value=table_configs)