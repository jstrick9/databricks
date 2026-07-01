# Databricks notebook source
# DBTITLE 1,Environment Settings
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")
dbutils.widgets.text("task", "initial")
task = dbutils.widgets.get("task")
dbutils.widgets.text("lookback", "1")
lookback = dbutils.widgets.get("lookback")
dbutils.widgets.text("backfill_date", "")
backfill_date = dbutils.widgets.get("backfill_date")

limit_clause = ""

if dbx_env == "dev":
    print("Development environment detected.")
    lookback = "3500"
    limit_clause = "limit 5"
    print(
        f"""
        Development data may not contain recent records. 
        Adjusting to accomodate test records. 
        lookback = {lookback} days
        A limit will be imposed as: {limit_clause}
    """
    )

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=}, {dbx_env=}, {task=}")

# COMMAND ----------

# DBTITLE 1,Shared Function
# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,Set Catalogs
common_configs = read_yaml(config_file)
reporting_catalog = common_configs["schema"]["trgt_catalog"]
tmngpdb_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
print(reporting_catalog, tmngpdb_catalog)

# COMMAND ----------

# DBTITLE 1,Begin Job
job_name = "ntb_trmreports_cmra_loader"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# DBTITLE 1,Imports
from delta.tables import DeltaTable
import json
from pydantic import BaseModel
from pyspark.sql.functions import col, current_timestamp, lit, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    MapType,
    ArrayType,
    Row
)
import requests
import time
from typing import Final, Optional, Dict, Any, Tuple, Literal

# COMMAND ----------

# DBTITLE 1,Typings
BatchStatus = Literal[
    "complete_batch_success", "partial_batch_success", "full_batch_failure"
]


class SuccessResponse(BaseModel):
    """
    Definition for success response. This should always have records returned
    from the API.
    """

    data: Dict[str, Any]
    response_time_ms: int
    status_code: int
    validated: bool = True


class ErrorResponse(BaseModel):
    """
    Definition for response containing an error. Note: this could be response error or
    something that happens *before* the API call.
    """

    error: str
    response_time_ms: int
    status_code: Optional[int] = None
    validated: bool = False


SuccessResponse.schema(), ErrorResponse.schema()

# COMMAND ----------

# DBTITLE 1,Helper
from dateutil.parser import parse

def validate_date_string(backfill_date: str) -> bool:
    try:
        datetime.date.fromisoformat(backfill_date)
        return True
    except ValueError:
        print("The backfill_date format, should be in the form of `YYYY-MM-DD`.")
        return False

# COMMAND ----------

# DBTITLE 1,Globals
VALID_TASKS: tuple[str] = ("initial", "retry")

AUTH_ID: str = dbutils.secrets.get(scope="smarty_streets_api", key="auth_id")
AUTH_TOKEN: str = dbutils.secrets.get(scope="smarty_streets_api", key="auth_token")
TIMEOUT: int = 60

# Note: this is the max batch size for the request, not the absolute
REQUEST_BATCH_SIZE: int = 100

# https://www.smarty.com/docs/http-status-code-error-meanings
ERROR_MESSAGES: dict = {
    400: "The request was malformed or incorrect",
    401: "Authentication failed. Please check the authorization ID and token.",
    402: "Payment required. Please check account has sufficient credits.",
    403: "Forbidden request. You do not have access to this type of request for this endpoint.",
    413: "Request was too large. Consider breaking the payload into smaller batches.",
    422: "Requested entity cannot be processed, likely because of lacking the required fields.",
    429: "Rate limit exceeded. Please slow down the number of requests you are sending.",
    500: "Internal server error. An issue occured on the backend.",
    503: "Backend is unreachable.  Please check that the SmartyStreets API is up.",
    504: "The upstream data provider took too long to process the request. This is out of your control.",
}

INIT_PARAMS: dict[str] = {
    "catalog": reporting_catalog,
    "schema": "silver",
    "request_status_table": "cmra_request_status",
    "case_status_table": "cmra_case_status",
}

RESPONSE_SCHEMA: Final = StructType(
    [
        StructField("input_id", StringType(), True),
        StructField("delivery_line_1", StringType(), True),
        StructField("last_line", StringType(), True),
        StructField("delivery_point_barcode", StringType(), True),
        StructField("components", MapType(StringType(), StringType()), True),
        StructField("metadata", MapType(StringType(), StringType()), True),
        StructField("analysis", MapType(StringType(), StringType()), True),
    ]
)

SCHEMA: Final = StructType(
    [
        StructField("serial_number", StringType(), True),
        StructField("data", RESPONSE_SCHEMA, True),
        StructField("response_time_ms", IntegerType(), True),
        StructField("validated", BooleanType(), True),
        StructField("http_status_code", IntegerType(), True),
        StructField("error", StringType(), True),
    ]
)

INIT_PARAMS

# COMMAND ----------

# DBTITLE 1,Clean Up Pre-request Failures
display(
  spark.sql(
    f"""
    update
      {reporting_catalog}.silver.cmra_request_status
    set
      status = 'failed',
      error_message = 'Job stuck too long in `pending`',
      update_ts = current_timestamp
    where
      unix_seconds(current_timestamp) - unix_seconds(update_ts) > 60
      and status = 'processing'
""")
)

# COMMAND ----------

# DBTITLE 1,Sanity Check: Task is Valid
if task not in VALID_TASKS:
    raise ValueError(
        f"Task type is not in the set of {VALID_TASKS}. Task supplied by widget = {task}"
    )

# COMMAND ----------

# DBTITLE 1,Branch Logic: Initial, Backfill, or Retryable
if backfill_date:
  print("Job is running a specific backfill date.")
  if validate_date_string(backfill_date):
    spark.sql(f"""
    with tm_party_role as (
      select
        *
      from
        (
          select
            *,
            row_number() over (
                partition by fk_trademark_gid, fk_interested_party_gid
                order by begin_effective_ts desc
              ) latest
          from
            {tmngpdb_catalog}.bronze.tm_party_role_h
          where
            date(begin_effective_ts) <= '{backfill_date}'
          qualify
            latest = 1
        )
      where
        action_ct != 'D'
    ),
    tm_mailing_addr as (
      select
        *
      from
        (
          select
            *,
            row_number() over (
                partition by fk_mailing_address_gid
                order by begin_effective_ts desc
              ) latest
          from
            {tmngpdb_catalog}.bronze.tm_mailing_addr_h
          where
            date(begin_effective_ts) <= '{backfill_date}'
          qualify
            latest = 1
        )
      where
        action_ct != 'D'
    ),
    mailing_address as (
      select
        *
      from
        (
          select
            *,
            row_number() over (partition by mailing_address_gid order by begin_effective_ts desc) latest
          from
            {tmngpdb_catalog}.bronze.mailing_address_h
          where
            date(begin_effective_ts) <= '{backfill_date}'
            and country_cd = 'US'
          qualify
            latest = 1
        )
      where
        action_ct != 'D'
    ),
    tm_milestone as (
      select
        *
      from
        (
          select
            *,
            row_number() over (partition by fk_trademark_gid order by begin_effective_ts desc) latest
          from
            {tmngpdb_catalog}.bronze.tm_milestone_h
          where
            date(begin_effective_ts) <= '{backfill_date}'
          qualify
            latest = 1
        )
      where
        action_ct != 'D'
    ),
    trademark as (
      select
        *
      from
        (
          select
            *,
            row_number() over (partition by trademark_gid order by begin_effective_ts desc) latest
          from
            {tmngpdb_catalog}.bronze.trademark_h
          where
            date(begin_effective_ts) = '{backfill_date}'
            and legacy_status_cd in (630, 631)
          qualify
            latest = 1
        )
      where
        action_ct != 'D'
    )
    select
      serial_num_tx serial_number,
      serial_num_tx input_id,
      trim(nvl(d.street_line_1_tx, '') || ' ' || nvl(d.street_line_2_tx, '')) input_street,
      trim(d.city_nm) input_city,
      trim(d.postal_cd) input_zipcode,
      trim(d.geographic_region_cd) input_state,
      to_json(
        struct(
          serial_num_tx input_id,
          1 as candidates,
          input_street street,
          input_city city,
          input_state state,
          input_zipcode zipcode,
          'enhanced' `match`
        )
      ) as payload
    from
      trademark a
        join tm_party_role b
          on a.trademark_gid = b.fk_trademark_gid
        join tm_mailing_addr c
          on b.tm_party_role_id = c.fk_tm_party_role_id
        join mailing_address d
          on c.fk_mailing_address_gid = d.mailing_address_gid
        join tm_milestone e
          on a.trademark_gid = e.fk_trademark_gid
        join {tmngpdb_catalog}.bronze.tm_party_role_owner f
          on a.trademark_gid = f.fk_trademark_gid
          and b.party_role_sequence_no = f.fk_party_role_sequence_no
    where
      f.fk_owner_type_id = 1
      and f.fk_tm_party_role_cd = 'OWNER'
      and d.address_type_ct = 'S'
      and f.fk_party_role_sequence_no = 1001
      and f.joint_owner_sequence_no = 1
      and not exists (
        select
          1
        from
          tm_milestone x
        where
          x.fk_tm_milestone_cd != 'FILED'
          and a.trademark_gid = x.fk_trademark_gid
      )
      and not exists (
        select
          1
        from
          {reporting_catalog}.silver.cmra_request_status y
        where
          a.serial_num_tx = y.serial_number
      )
      and not exists ( 
        select
          1
        from
          {tmngpdb_catalog}.bronze.business_event z
        where
          z.fk_business_event_reason_id = 873
          and date(z.effective_ts) <= '{backfill_date}'
          and a.trademark_gid = z.cfk_object_gid
      )
    {limit_clause}
    """).createOrReplaceTempView("new_input_records")

    display(spark.sql("select * from new_input_records"))

    display(
      spark.sql(f"""
      insert into {reporting_catalog}.silver.cmra_request_status (
          serial_number, input_id, input_street, input_city, input_zipcode, input_state, payload, status
        )
        select
          serial_number,
          input_id,
          input_street,
          input_city,
          input_zipcode,
          input_state,
          payload,
          'initialized' status
        from
          new_input_records;
      """
        )
    )
    predicate: str = "status = 'initialized'"

  else:
    end_job_cntl(
      f"{reporting_catalog}.silver",
      job_name,
      job_start_ts,
      "completed",
      0,
      "job completed successfully",
    )
    dbutils.notebook.exit(f"Job attempted to run with an invalid backfill date format.")
else:
  print("Job is not running a specific backfill date.")
  if task == "initial":
    print("Job is checking to see if new records are available to load.")
    spark.sql(f"""
    select
      b.ser_num serial_number,
      cast(b.ser_num as string) input_id,
      trim(nvl(o.address_1, '') || ' ' || nvl(o.address_2, '')) input_street,
      trim(o.city) input_city,
      trim(o.postal_cd) input_zipcode,
      trim(o.state_cd) input_state,
      to_json(
        struct(
          cast(b.ser_num as string) as input_id,
          1 as candidates,
          input_street street,
          input_city city,
          input_state state,
          input_zipcode zipcode,
          'enhanced' `match`
        )
      ) as payload
    from
      {reporting_catalog}.silver.milestone m
        inner join {reporting_catalog}.silver.bibliography b
          on m.ser_num = b.ser_num
        inner join {reporting_catalog}.silver.owner o
          on m.ser_num = o.ser_num
    where
      m.dock_dt is null
      and m.published_dt is null
      and m.registration_dt is null
      and m.abandonment_dt is null
      and m.noa_dt is null
      and b.am_stat in (630, 631)
      and o.current_owner = 'Y'
      and o.ctry_cd = 'US'
      and o.owner_num = 1
      and m.filing_dt >= (current_date - interval {lookback} day)
      and not exists (
        select
          1
        from
          {reporting_catalog}.silver.cmra_request_status b
        where
          m.ser_num = b.serial_number
      )
    {limit_clause}
    """).createOrReplaceTempView("new_input_records")

    display(spark.sql("select * from new_input_records"))

    display(
      spark.sql(f"""
      insert into {reporting_catalog}.silver.cmra_request_status (
          serial_number, input_id, input_street, input_city, input_zipcode, input_state, payload, status
        )
        select
          serial_number,
          input_id,
          input_street,
          input_city,
          input_zipcode,
          input_state,
          payload,
          'initialized' status
        from
          new_input_records;
      """
        )
    )
    predicate: str = "status = 'initialized'"
  else:
    print("Task is not loading new records. Eligible records will be retried instead.")
    predicate: str = "status = 'failed' and attempt_count != max_attempts"

# COMMAND ----------

# DBTITLE 1,Drop Duplicate Cases
spark.sql(
    f"""
select
  serial_number,
  count(1)
from
  {reporting_catalog}.silver.cmra_request_status
group by
  serial_number
having
  count(1) > 1
"""
).createOrReplaceTempView("duplicates")

display(
    spark.sql("""
        select 
            count(1) total_duplicates
        from 
            duplicates
    """)
)
spark.sql(
    f"""
    delete from {reporting_catalog}.silver.cmra_request_status a 
    where exists (
        select 
            * 
        from 
            duplicates b 
        where 
            a.serial_number = b.serial_number)
    """
)

# COMMAND ----------

# DBTITLE 1,Initialize: Task Logic Predicate
payload_columns: list[str] = [
    "serial_number",
    "input_street",
    "input_city",
    "input_state",
    "input_zipcode",
    "payload",
]
payload_projection: str = ", ".join(payload_columns)

payload_query: str = f"""
    select
        {payload_projection}
    from
        {reporting_catalog}.silver.cmra_request_status
    where
        {predicate}
"""

print(
    "The following query will be used as the basis for batch numbers:"
)
print(payload_query)

# COMMAND ----------

# DBTITLE 1,Split Batches For Payload
numerator: int = spark.sql(payload_query).count()
print(
    f"[{numerator}] records will {'attempt to be processed for the first time' if task == 'initial' else 'be retried'}."
)

num_batches: int = (
    (numerator // REQUEST_BATCH_SIZE) + 1
    if numerator % REQUEST_BATCH_SIZE > 0
    else (numerator // REQUEST_BATCH_SIZE)
)

if numerator == 0:
    end_job_cntl(
        f"{reporting_catalog}.silver",
        job_name,
        job_start_ts,
        "completed",
        0,
        "job completed successfully",
    )
    dbutils.notebook.exit(f"Job completed with 0 records.")
print(f"Number of batches: {num_batches}; Number of records: {numerator}")

payload_columns_with_batch: list[str] = payload_columns + [
    f"ntile({num_batches}) over (order by serial_number) batch"
]
payload_projection_with_batch: str = ", ".join(payload_columns_with_batch)
payload_with_batch_query: str = f"""
    select
        {payload_projection_with_batch}
    from
        {reporting_catalog}.silver.cmra_request_status
    where
        {predicate}
"""
print("The following query will be used to submit address verification to SmartyStreets:")
print(payload_with_batch_query)
print("Example records:")
display(spark.sql(payload_with_batch_query).limit(100))

# COMMAND ----------

# DBTITLE 1,CMRA Request Validator
class CMRAValidator:
    auth_id: str = AUTH_ID
    auth_token: str = AUTH_TOKEN
    timeout: int = TIMEOUT

    def __init__(
        self,
        catalog: str,
        schema: str,
        request_status_table: str,
        case_status_table: str,
        query: str,
    ):
        self.catalog = catalog
        self.schema = schema
        self.request_status_table = request_status_table
        self.case_status_table = case_status_table
        self.base_url = "https://us-street.api.smartystreets.com/street-address"
        self.request_fqn = f"{self.catalog}.{self.schema}.{self.request_status_table}"
        self.query = query

    def extract_nested_data(self, nested_data: dict) -> dict:
        """
        Helper to return nested data entry
        """
        return {
            "input_id": nested_data.get("input_id", None),
            "delivery_line_1": nested_data.get("delivery_line_1", None),
            "last_line": nested_data.get("last_line", None),
            "delivery_point_barcode": nested_data.get("delivery_point_barcode", None),
            "components": nested_data.get("components", None),
            "metadata": nested_data.get("metadata", None),
            "analysis": nested_data.get("analysis", None),
        }

    def create_dataframe_from_response(self, response_json: list[dict]) -> DataFrame:
        """
        Helper function to create a dataframe from a reponse.
        Input data MUST have an input id (serial number). Otherwise, we can't
        tie it back for retries if needed.
        """

        def extract_serial_number(nested_data: dict) -> dict:
            """
            Helper to return the serial number from the nested data entry
            """
            return self.extract_nested_data(nested_data).get("input_id", None)

        try:
            data = [
                {
                    "serial_number": extract_serial_number(
                        self.extract_nested_data(response_data.get("data"))
                    ),
                    "data": self.extract_nested_data(response_data.get("data")),
                    "response_time_ms": response_data.get("response_time_ms", None),
                    "validated": response_data.get("validated", None),
                    "http_status_code": response_data.get("http_status_code", None),
                    "error": response_data.get("error", None),
                }
                for response_data in response_json
            ]
            print(f"[create_dataframe_from_response] | Sample data: {data[:1]}")
            results_df: DataFrame = spark.createDataFrame(
                data,
                schema=SCHEMA,
            )
            return results_df
        except Exception as e:
            raise

    def get_eligible_requests(self) -> DataFrame:
        """
        Helper function to determine which records can be processed.
        """
        print("[get_eligible_requests] | Getting eligible requests.")
        try:
            rows_to_be_processed = spark.sql(self.query)

            num_rows_to_be_processed = rows_to_be_processed.count()
            print(f"({num_rows_to_be_processed}) records will be processed.")
            return rows_to_be_processed
        except Exception as e:
            print(f"An error occured getting the IDs ready for processing: {e}")
            raise

    def begin_processing_batch(self, batch: list, batch_number: int) -> None:
        """
        Function to mark cases that have intiated processing.
        This is used in case the job quits during processing, so
        records can be updated to failed status if they have been pending for too long.
        """
        try:
            print(f"[begin_processing_batch] | [{len(batch)}] records marked processed.")
            ids: int = ", ".join([record.serial_number for record in batch])
            print(f"Beginning process for IDs: {ids}")
            display(spark.sql(
                f"""
                UPDATE {self.request_fqn}
                SET
                    status = 'processing',
                    attempt_count = attempt_count + 1,
                    update_ts = current_timestamp
                WHERE
                    serial_number in ({ids})
            """
            ))
            print(f"Starting processing for batch: {batch_number}")
        except Exception as e:
            print(f"Could not begin records in batch: {batch_number}\n{e}")
            raise

    def mark_success_batch(self, batch_number: int, batch_results: DataFrame) -> None:
        """
        Helper to handle successfully processed requests.
        """
        print("[mark_success_batch] | Marking successful requests.")

        record_count: int = batch_results.count()
        batch_name: str = f"success_batch_{batch_number}"
        batch_results.createOrReplaceTempView(batch_name)

        try:
            display(spark.sql(
                f"""
                MERGE INTO {self.request_fqn} AS target
                USING {batch_name} AS source
                ON target.serial_number = source.serial_number
                WHEN MATCHED THEN
                UPDATE SET
                    target.status = 'completed',
                    target.http_status_code = source.http_status_code,
                    target.update_ts = current_timestamp,
                    target.response = to_json(source.data)
            """
            ))
        except Exception as e:
            print(f"Error marking completion of values in batch [{batch_number}]")
            raise

    def mark_failed_batch(self, batch_number: int, batch_results: DataFrame) -> None:
        """
        Function to mark failed requests
        """
        print("[mark_failed_batch] | Marking failed requests.")
        batch_name: str = f"failed_batch_{batch_number}"
        batch_results.createOrReplaceTempView(batch_name)
        record_count = batch_results.count()
        try:
            display(spark.sql(
                f"""
                MERGE INTO {self.request_fqn} AS target
                USING {batch_name} AS source
                ON target.serial_number = source.serial_number
                WHEN MATCHED THEN
                UPDATE SET 
                    target.status = 'failed',
                    target.http_status_code = source.http_status_code,
                    target.update_ts = current_timestamp,
                    target.response = to_json(source.data)
            """
            ))
            print(
                f"[mark_failed_batch] | [{record_count}] records unsuccessfully processed from batch [{batch_number}]"
            )
        except Exception as e:
            print(f"Error marking completion of values in batch [{batch_number}]")
            raise

    def process_request_batch(
        self, batch_number: int
    ) -> Tuple[BatchStatus, Optional[SuccessResponse], Optional[ErrorResponse]]:
        """
        Function to process new and failed responses of address records through the
        smartystreets API. A batch in this case is 1-100 address input records
        supplied as part of the payload.

        IMPORTANT: input_id must be a string, even if the underlying id is an integer.

        Example payload: {
            "input_id": "12345678",
            "candidates": 1,
            "street":"123 N. Some St Ste. 12345",
            "city":"Some City",
            "state":"MI",
            "zipcode":"12345",
            "match": "enhanced"
        }

        """
        batch_records: list[Row] = spark.sql(
            f"select payload from {self.request_fqn} where status = 'processing'"
        ).collect()
        print(f"[process_request_batch] | [{len(batch_records)}] records in batch.")
        payload: list[dict] = [json.loads(row.payload) for row in batch_records]
        print(f"[process_request_batch] | [{len(payload)}] records in payload.")
        print(f"[process_request_batch] | Example payload: {payload[:1]}")
        if not payload:
            print(
                f"[process_request_batch] | Payload cannot be empty for batch [{batch_number}]. There are currently no records showing up in the payload."
            )
            return
        try:
            print(f"[process_request_batch] | Made request for batch {batch_number}.")
            start_time = time.time()
            response: requests.Response = requests.post(
                url=self.base_url,
                params={
                    "auth-id": AUTH_ID,
                    "auth-token": AUTH_TOKEN,
                    "features": "component-analysis"
                },
                headers={
                    "Content-Type": "application/json; charset=utf-8",
                },
                json=payload,
                timeout=self.timeout,
            )

            response_time_ms = int((time.time() - start_time) * 1000)
            print(
                f"[process_request_batch] | Request for batch {batch_number} processed in {response_time_ms} ms."
            )

            response_data = response.json()
            try:
                print(
                    f"[process_request_batch] | [{len([record for record in response_data])}] records in response data (of [{len(payload)}] passed as payload)"
                )
            except:
                print("Unable to get payload ")
            if "errors" not in response_data:
                success_records: SuccessResponse = [
                    {
                        "data": {
                            "input_id": address.get("input_id", None),
                            "delivery_line_1": address.get("delivery_line_1", None),
                            "last_line": address.get("last_line", None),
                            "delivery_point_barcode": address.get(
                                "delivery_point_barcode", None
                            ),
                            "components": address.get("components", None),
                            "metadata": address.get("metadata", None),
                            "analysis": address.get("analysis", None),
                        },
                        "response_time_ms": response_time_ms,
                        "validated": True,
                        "http_status_code": response.status_code,
                        "error": None,
                    }
                    for address in response_data
                ]
                print(
                    f"[process_request_batch] | Sample of success records: {success_records[:1]}"
                )
                if len(response_data) == len(payload):
                    try:
                        return "complete_batch_success", success_records, None
                    except Exception as e:
                        print(
                            "[process_request_batch] | Unable to generate success records for complete_batch_success."
                        )
                        raise
                else:
                    # partial success means we have some records that returned a response
                    # but not all records; therefore, we need to get the delta
                    # between the records supplied in the complete batch and the records
                    # returned in the response; then we take the left anti of records not
                    # found and mark them failed
                    # TODO: add DQ process for records that have been pre-validated (i.e., addresses usable)
                    try:
                        print("[process_request_batch] | Generating delta...")
                        delta: list[str] = [
                            record["data"].get("input_id", None)
                            for record in success_records
                        ]
                        print(f"[process_request_batch] | Example delta: {delta[:1]}")
                        print(
                            f"Delta records identified ({len(delta)})"
                        )
                        failed_records = [
                            {
                                "data": {
                                    "input_id": row.get("input_id", None),
                                    "delivery_line_1": None,
                                    "last_line": None,
                                    "delivery_point_barcode": None,
                                    "components": None,
                                    "metadata": None,
                                    "analysis": None,
                                },
                                "response_time_ms": response_time_ms,
                                "validated": False,
                                "http_status_code": None,
                                "error": "Record was not returned in payload during multi-record request.",
                            }
                            for row in payload
                            if row.get("input_id", None) not in delta
                        ]
                        print(
                            f"[process_request_batch] | Sample of failed records: {failed_records[:1]}"
                        )
                        return "partial_batch_success", success_records, failed_records
                    except Exception as e:
                        print(
                            "[process_request_batch] | Unable to generate failed records for partial_batch_success."
                        )
                        raise

            else:
                # a full_batch_failure means that no records were returned with success
                # we mark all of these records failed
                error = str(response_data.get("errors"))
                try:
                    return (
                        "full_batch_failure",
                        None,
                        [
                            {
                                "data": {
                                    "input_id": row.get("input_id", None),
                                    "delivery_line_1": None,
                                    "last_line": None,
                                    "delivery_point_barcode": None,
                                    "components": None,
                                    "metadata": None,
                                    "analysis": None,
                                },
                                "response_time_ms": response_time_ms,
                                "validated": False,
                                "http_status_code": {
                                    response_data.get("status_code", None)
                                },
                                "error": ERROR_MESSAGES.get(
                                    {response_data.get("status_code", None)}
                                )
                                or error,
                            }
                            for row in payload
                        ],
                    )
                except Exception as e:
                    print(
                        "[process_request_batch] | Unable to generate failed records for full_batch_failure with defined error status code."
                    )
                    raise

        except Exception as e:
            try:
                print(f"Error not known. Error response unavailable.")
                return (
                    "full_batch_failure",
                    None,
                    [
                        {
                            "data": {
                                "input_id": row.get("input_id", None),
                                "delivery_line_1": None,
                                "last_line": None,
                                "delivery_point_barcode": None,
                                "components": None,
                                "metadata": None,
                                "analysis": None,
                            },
                            "response_time_ms": None,
                            "validated": False,
                            "http_status_code": None,
                            "error": e,
                        }
                        for row in payload
                    ],
                )
            except Exception as e:
                print(
                    "[process_request_batch] | Unable to generate failed records for full_batch_failure. An unexpected error occured."
                )
                raise

# COMMAND ----------

# DBTITLE 1,Initialize Validator
validator: CMRAValidator = CMRAValidator(**INIT_PARAMS, query=payload_with_batch_query)

# COMMAND ----------

# DBTITLE 1,Runner
def execute(validator: CMRAValidator) -> None:
    """
    Wrapper to execute the process as a whole.
    """
    try:
        print("Beginning address validation process.")
        records_to_process = validator.get_eligible_requests().collect()
        if not records_to_process:
            print("No records to process for the SmartyStreets API. Exiting.")
            return
        
        for batch_number in range(1, num_batches + 1):
            try:
                batch_records_to_process: list = [record for record in records_to_process if record.batch == batch_number]
                validator.begin_processing_batch(batch_records_to_process, batch_number)
                results = validator.process_request_batch(batch_number)
                if not results:
                    print(f"No results generated for batch [{batch_number}]")
                    continue
                outcome: BatchStatus = results[0]
                success_results: SuccessResponse = results[1]
                failed_results: ErrorResponse = results[2]
                if success_results:
                    success: DataFrame = validator.create_dataframe_from_response(
                        success_results
                    )
                    validator.mark_success_batch(
                        batch_number=batch_number, batch_results=success
                    )
                if failed_results:
                    failed: DataFrame = validator.create_dataframe_from_response(
                        failed_results
                    )
                    validator.mark_failed_batch(
                        batch_number=batch_number, batch_results=failed
                    )
            except Exception as e:
                print(f"Failed to process batch [{batch_number}] during the runner phase.")
                raise
    except Exception as e:
        print(f"An error occured in the runner: {e}")
        raise

execute(validator)

# COMMAND ----------

# DBTITLE 1,Show Sample
display(
  spark.sql(f"""
    select
      *
    from
      {reporting_catalog}.silver.cmra_request_status
    where
      status = 'completed'
    order by
      update_ts desc
    limit 5
  """)
)

# COMMAND ----------

# DBTITLE 1,End Job
completed_output_count = spark.sql(f"""
  select
    *
  from
    {reporting_catalog}.silver.cmra_request_status
  where
    create_ts = (
      select
        create_ts
      from
        {reporting_catalog}.silver.cmra_request_status
      where
        status = 'completed'
      order by
        create_ts desc
      limit 1
    )
"""
).count()

failed_output_count = spark.sql(f"""
  select
    *
  from
    {reporting_catalog}.silver.cmra_request_status
  where
    create_ts = (
      select
        create_ts
      from
        {reporting_catalog}.silver.cmra_request_status
      where
        status = 'failed'
        and date(create_ts) = current_date
      order by
        create_ts desc
      limit 1
    )
"""
).count()

end_job_cntl(
    f"{reporting_catalog}.silver",
    job_name,
    job_start_ts,
    "completed",
    completed_output_count + failed_output_count,
    "job completed successfully",
)
dbutils.notebook.exit(
    f"Job completed successfully with [{completed_output_count}] records. [{failed_output_count}] records failed to load an appropriate response and will be retried the next run."
)