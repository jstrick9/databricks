# Databricks notebook source
# DBTITLE 1,Imports
import json
import pandas as pd
import numpy as np
from pyspark import pandas as ps
from pyspark.sql import DataFrame
from io import BytesIO
import concurrent.futures
from typing import List, Tuple, Dict, Union, Any, Final
import base64
from datetime import datetime, timedelta
import time
import functools
import requests
from functools import wraps
import boto3
from enum import Enum
import smtplib, traceback, hashlib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import warnings
from pyspark.sql.functions import date_format

# COMMAND ----------

# DBTITLE 1,Ignore
warnings.filterwarnings("ignore")

# COMMAND ----------

# DBTITLE 1,Select Environment
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file = f"../../config/{dbx_env}/tdet-conf.yaml"

# COMMAND ----------

# DBTITLE 1,Run Common Definitions
# MAGIC %run ../../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,Read YAML
configs = read_yaml(config_file)
SOURCE_CATALOG: Final[str] = configs["schema"]["source_tmngpdb_catalog"]
TARGET_CATALOG: Final[str] = configs["schema"]["trgt_catalog"]
to_email = configs["alerting"]["email"]

# COMMAND ----------

# DBTITLE 1,Globals
WRITE_BUCKET: Final[str] = configs["s3"]["write_bucket"]

BASE_DIR: Final[str] = f"s3://{WRITE_BUCKET}"
INPUT_DIR: Final[str] = f"{BASE_DIR}/inputFolder"
REQUEST_DIR: Final[str] = f"{BASE_DIR}/requestFolder"
OUTPUT_DIR: Final[str] = f"{BASE_DIR}/outputFolder"
ARCHIVE_DIR: Final[str] = f"{REQUEST_DIR}/archive"
REQUEST_ERROR_DIR: Final[str] = f"{REQUEST_DIR}/error"
ERROR_DIR: Final[str] = f"{BASE_DIR}/errorFolder"

JSON_MAPPING: Final[Dict[str, List[Dict[str, bool]]]] = {
    "name": [
        {"column": "owner_name", "is_multivalued": False},
        {"column": "hist_owner_nm", "is_multivalued": True},
        {"column": "attorney_name", "is_multivalued": False},
        {"column": "hist_attorney_nm", "is_multivalued": True},
        {"column": "correspondent_name", "is_multivalued": False},
        {"column": "hist_cr_nm", "is_multivalued": True},
        {"column": "firm_name", "is_multivalued": False},
        {"column": "domestic_representative_name", "is_multivalued": False},
        {"column": "hist_dr_nm", "is_multivalued": True},
        {"column": "examiner_name", "is_multivalued": False},
    ],
    "email": [
        {"column": "owner_email", "is_multivalued": False},
        {"column": "hist_owner_email", "is_multivalued": True},
        {"column": "attorney_email", "is_multivalued": False},
        {"column": "hist_at_email", "is_multivalued": True},
        {"column": "correspondent_email", "is_multivalued": False},
        {"column": "hist_cr_email", "is_multivalued": True},
        {"column": "secondary_cor_email", "is_multivalued": False},
        {"column": "domestic_representative_email", "is_multivalued": False},
        {"column": "hist_dr_email", "is_multivalued": True},
    ],
    "phone": [
        {"column": "owner_phone", "is_multivalued": False},
        {"column": "attorney_phone", "is_multivalued": False},
        {"column": "correspondent_phone", "is_multivalued": False},
        {"column": "domestic_rep_phone", "is_multivalued": False},
    ],
    "attorneyMembershipNumber": [
        {"column": "attorney_membership_no", "is_multivalued": False},
    ],
    "mailingAddress": [
        {"column": "owner_address", "is_multivalued": False},
        {"column": "attorney_address", "is_multivalued": False},
        {"column": "correspondent_address", "is_multivalued": False},
    ],
    "url": [{"column": "specimen_url", "is_multivalued": False}],
    "status": [{"column": "status", "is_multivalued": False}],
    "phEntry": [{"column": "ph_action_code", "is_multivalued": False}],
}

PANDAS_CONVERSION_LIMIT: Final[int] = 250_000

AND_COLUMNS: Final[dict[str]] = {"status", "phEntry"}
API_UPDATE_ENDPOINT: Final[str] = configs["s3"]["api_update_endpoint"]

S3_BUCKET: Final[str] = configs["s3"]["certificate_bucket"]
SSL_TMP_FILENAME: Final[str] = "/tmp/ca_bundle_trust.crt"
SSL_CRT: Final[str] = "eds/certs/ca_bundle_trust.crt"

TDET_API_SCOPE: Final[str] = configs["secrets"]["tdet_api_scope"]
AUTH_URL: Final[str] = configs["okta"]["auth_url"]

GRANT_TYPE: Final[str] = "client_credentials"
SCOPE: Final[
    str
] = "uspto.trademark.det.application.search uspto.trademark.det.access uspto.trademark.det.application.history"
AUDIENCE: Final[str] = "trademarks"

CLIENT_ID: Final[str] = dbutils.secrets.get(scope=TDET_API_SCOPE, key="okta_client_id")
CLIENT_SECRET: Final[str] = dbutils.secrets.get(
    scope=TDET_API_SCOPE, key="okta_client_secret"
)

# Create a global variable to store the access token and expiration time
access_token_info = None

FILEHANDLER_ARGS = {
    "input_dir": INPUT_DIR,
    "output_dir": OUTPUT_DIR,
    "archive_dir": ARCHIVE_DIR,
    "request_dir": REQUEST_DIR,
    "request_error_dir": REQUEST_ERROR_DIR,
    "error_dir": ERROR_DIR,
    "json_mapping": JSON_MAPPING,
    "write_bucket": WRITE_BUCKET,
    "source_catalog": SOURCE_CATALOG,
    "target_catalog": TARGET_CATALOG,
    "api_update_endpoint": API_UPDATE_ENDPOINT,
    "ssl_tmp_filename": SSL_TMP_FILENAME,
    "to_email": to_email,
}

# COMMAND ----------

# DBTITLE 1,Request List
# TODO: Replace with GET OPEN REQUESTS
file_metadata_list = [
    f"{REQUEST_DIR}/{file.name}"
    for file in dbutils.fs.ls(REQUEST_DIR)
    if file.name.endswith(".json")
]
if len(file_metadata_list) == 0:
    dbutils.notebook.exit("No request files available to process.")
print("Files to be processed:")
print(file_metadata_list)

# COMMAND ----------

# DBTITLE 1,Retry Wrapper
def retry(max_attempts: int, wait_seconds: int):
    def decorator_retry(func):
        @functools.wraps(func)
        def wrapper_retry(*args, **kwargs):
            attempt = 0
            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    if attempt == max_attempts:
                        raise
                    print(
                        f"Retrying {func.__name__} in {wait_seconds} seconds... (Attempt {attempt}/{max_attempts})"
                    )
                    time.sleep(wait_seconds)

        return wrapper_retry

    return decorator_retry

# COMMAND ----------

# DBTITLE 1,Query Builder
def clean_request_values(request: dict) -> dict:
    cleaned_request = {}
    for key, value_info in request.items():
        if is_valid_value_info(value_info):
            value = clean_value(value_info["value"])
            if has_valid_subvalues(value):
                cleaned_request[key] = {
                    "values": process_values(value),
                    "fuzzyLogicIndicator": value_info.get("fuzzyLogicIndicator", False),
                }
    return cleaned_request


def is_valid_value_info(value_info) -> bool:
    return isinstance(value_info, dict) and "value" in value_info


def clean_value(value: str) -> str:
    return value.strip() if isinstance(value, str) else ""


def has_valid_subvalues(value: str) -> bool:
    return bool(value) and any(v.strip() for v in value.split(";"))


def process_values(values: str) -> list[str]:
    invalid_values = {"", '""', "''", "'"}
    cleaned_values = [
        v.strip()
        for v in values.split(";")
        if v.strip() and v.strip() not in invalid_values
    ]
    return list(set(cleaned_values))


def contains_single_quote(value: str) -> str:
    # If there are single quotes, return True
    return "'" in value


def generate_predicate(column: str, value: str, fuzzy: bool) -> str:
    if contains_single_quote(value):
        return (
            f"""{column} ilike "%{value}%\""""
            if fuzzy
            else f"""{column} = "{value}\""""
        )
    return f"{column} ilike '%{value}%'" if fuzzy else f"{column} = '{value}'"


def parse_single_predicate(json_request: dict, key: str) -> str:
    predicates = []
    for column_info in JSON_MAPPING.get(key, []):
        column = column_info["column"]
        if key in json_request:
            for value in json_request[key]["values"]:
                predicates.append(
                    generate_predicate(
                        column, value, json_request[key]["fuzzyLogicIndicator"]
                    )
                )
    return " OR ".join(predicates)


def combine_predicates(json_request: dict) -> str:
    or_clauses = []
    and_clauses = []

    for key in json_request:
        if key == "phEntry":
            continue
        predicate = parse_single_predicate(json_request, key)
        if predicate:  # Ensure we only add non-empty predicates
            if key not in AND_COLUMNS:
                or_clauses.append(predicate)
            else:
                and_clauses.append(predicate)

    combined_or = " OR ".join(or_clauses)
    combined_and = " AND ".join(and_clauses)

    if combined_or and combined_and:
        return f"({combined_or}) AND ({combined_and})"
    elif combined_or:
        return f"({combined_or})"
    elif combined_and:
        return f"({combined_and})"
    else:
        return ""


def wrap_value_match(value, fuzzy):
    return f"%{value}%" if fuzzy is True else value


def generate_match_case(
    column: str, values: list[str], fuzzy: bool, is_multivalued: bool
) -> str:
    match_type = "ilike" if fuzzy else "="
    formatted_values = [
        f'"{wrap_value_match(value, fuzzy)}"'
        if contains_single_quote(wrap_value_match(value, fuzzy))
        else f"'{wrap_value_match(value, fuzzy)}'"
        for value in values
    ]
    if is_multivalued:
        return f"""
            CASE
                WHEN {column} {match_type} {f" OR {column} {match_type} ".join(formatted_values)}
                THEN CONCAT(
                    '{column}: ',
                    ARRAY_JOIN(
                        FILTER(
                            SPLIT({column}, ';'),
                            element -> element {match_type} {f" OR element {match_type} ".join(formatted_values)}
                        ),
                        ';'
                    ),
                    '; '
                )
                ELSE ''
            END
        """
    else:
        return f"""
            CASE
                WHEN {column} {match_type} {f" OR {column} {match_type} ".join(formatted_values)}
                THEN CONCAT('{column}: ', {column}, '; ')
                ELSE ''
            END
        """


def parse_what_matched(json_request: dict) -> str:
    match_cases = [
        generate_match_cases_for_key(json_request, key) for key in json_request
    ]
    flattened_match_cases = [case for sublist in match_cases for case in sublist]
    if flattened_match_cases == []:
        return ""
    else:
        return f"CONCAT({', '.join(flattened_match_cases)}) AS what_matched"


def generate_match_cases_for_key(json_request: dict, key: str) -> list[str]:
    match_cases = []
    for column_info in JSON_MAPPING.get(key, []):
        if key == "phEntry":
            continue
        column = column_info["column"]
        values = json_request[key]["values"]
        fuzzy = json_request[key]["fuzzyLogicIndicator"]
        is_multivalued = column_info.get("is_multivalued", False)
        if len(values) > 0:
            match_cases.append(
                generate_match_case(column, values, fuzzy, is_multivalued)
            )
    return match_cases


def parse_event_match(json_request: dict) -> str:
    key = "phEntry"
    if key not in json_request:
        return ""
    column_date = "ph_action_date"
    match_cases = generate_event_match_cases(json_request, key, column_date)
    return format_event_match_sql(match_cases, column_date)


def generate_event_match_cases(
    json_request: dict, key: str, column_date: str
) -> list[str]:
    match_cases = []
    for column_info in JSON_MAPPING[key]:
        column = column_info["column"]
        values = json_request[key]["values"]
        fuzzy = json_request[key]["fuzzyLogicIndicator"]
        match_type = "ilike" if fuzzy else "="
        if len(values) > 0:
            match_cases.extend(
                [
                    f" {column} {match_type} '{wrap_value_match(value, fuzzy)}' THEN CONCAT({column}, ': ', {column_date})"
                    for value in values
                ]
            )
    return match_cases


def format_event_match_sql(match_cases: list[str], column_date: str) -> str:
    if len(match_cases) > 0:
        return f"""
            MAX(
                ARRAY_JOIN(
                    COLLECT_LIST(
                        CASE WHEN
                            {" WHEN ".join(match_cases)} 
                            ELSE NULL
                        END
                    ) OVER (PARTITION BY serial_number ORDER BY {column_date}),
                    '; '
                )
            ) OVER (PARTITION BY serial_number) AS event_match
        """
    else:
        return ""


def parse_event_match_join(catalog: str, json_request: dict) -> str:
    key = "phEntry"
    if key not in json_request:
        return ""

    predicate = parse_single_predicate(json_request, key)
    if predicate not in [None, ""]:
        return f"""
            INNER JOIN (
                SELECT DISTINCT
                    SPLIT(be.cfk_object_gid, ':')[2] AS serial_number,
                    sber.business_event_reason_cd AS ph_action_code,
                    DATE(be.effective_ts) AS ph_action_date
                FROM
                    {catalog}.bronze.business_event be
                    INNER JOIN {catalog}.bronze.stnd_business_event_reason sber
                    ON be.fk_business_event_reason_id = sber.business_event_reason_id
                WHERE
                    {predicate.replace("ph_action_code", "sber.business_event_reason_cd")}
            ) ph ON s.serial_num = ph.serial_number
        """
    else:
        return ""


def build_query(source_catalog: str, target_catalog: str, json_request: dict) -> str:
    json_request = clean_request_values(json_request)
    filter_criteria = combine_predicates(json_request)
    what_matched_criteria = parse_what_matched(json_request)
    event_match_join = parse_event_match_join(source_catalog, json_request)

    query = (
        f"""
            SELECT DISTINCT
                s.* EXCEPT(create_dt, create_user),
                {what_matched_criteria}
        """
        if what_matched_criteria not in ["", None]
        else f"""
            SELECT DISTINCT
                s.* EXCEPT(create_dt, create_user)
        """
    )

    if event_match_join not in ["", None]:
        query += f", {parse_event_match(json_request)}"
    if filter_criteria not in ["", None]:
        query += f"""
            FROM {target_catalog}.gold.search s
            {event_match_join}
            WHERE {filter_criteria}
        """
    else:
        query += f"""
            FROM {target_catalog}.gold.search s
            {event_match_join}
        """
    print(query)
    return query

# COMMAND ----------

# DBTITLE 1,Download Certificates
def download_certificates():
    s3_resource = boto3.resource("s3")
    s3_object = s3_resource.Object(bucket_name=S3_BUCKET, key=SSL_CRT)
    s3_objectfile = s3_object.download_file(SSL_TMP_FILENAME)
    print(f"SSL CRT :: {S3_BUCKET} - {SSL_CRT}")

# COMMAND ----------

# DBTITLE 1,Authorization
# Function to get authentication headers with token caching and refresh
def get_auth_headers():
    global access_token_info
    # Check if the access token is already in the cache and not expired
    if (
        access_token_info
        and datetime.datetime.utcnow() < access_token_info["expiration_time"]
    ):
        access_token = access_token_info["access_token"]
        print("Using cached token.")
    else:
        # If the cached token is expired or not present, obtain a new access token
        download_certificates()
        token = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode())
        headers = {
            "accept": "application/json",
            "cache-control": "no-cache",
            "content-type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {token.decode()}",
        }

        auth_response = requests.post(
            AUTH_URL,
            data={
                "grant_type": GRANT_TYPE,
                "scope": SCOPE,
                "audience": AUDIENCE,
            },
            headers=headers,
        )
        # Trigger Request Exception If Not OK
        auth_response.raise_for_status()
        auth_response_data = auth_response.json()

        access_token = auth_response_data.get("access_token")
        # Calculate the expiration time (e.g., 24 hour from now)
        expiration_time = datetime.datetime.utcnow() + timedelta(
            seconds=auth_response_data.get("expires_in")
        )

        # Cache the new access token and its expiration time
        access_token_info = {
            "access_token": access_token,
            "expiration_time": expiration_time,
        }
        print("Obtained new token.")
    # Create headers for subsequent API requests with the obtained or cached access token
    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    return headers

# COMMAND ----------

# DBTITLE 1,File Handler
class FileHandler:
    def __init__(
        self,
        json_mapping: Dict[str, List[Dict[str, bool]]],
        input_dir: str,
        output_dir: str,
        archive_dir: str,
        request_dir: str,
        request_error_dir: str,
        error_dir: str,
        write_bucket: str,
        source_catalog: str,
        target_catalog: str,
        api_update_endpoint: str,
        ssl_tmp_filename: str,
        to_email: str,
    ) -> None:
        """
        Initialize the FileHandler with a JSON mapping and a log folder.

        :param json_mapping: A dictionary mapping keys to column names.
        :param input_dir: The input directory for input files of requests.
        :param output_dir: The output directory for processed output files.
        :param archive_dir: The archive directory for processed request files.
        :param request_dir: The requests directory for incoming request files.
        :param request_error_dir: The requests directory for errored request files.
        :param error_dir: The error directory for error log files.
        :param write_bucket: The S3 bucket for processed output files.
        :param source_catalog: The TMNGPDB catalog used when prosecution history (PH) entries are present.
        :param target_catalog: The TDET catalog.
        :param api_update_endpoint: The endpoint used to POST an update to the job history table.
        :param ssl_tmp_filename: The ssl certification file used for the handshake when POSTing to the API server.
        :param to_email: The email address to send alerts to when processing fails for a request.
        """
        self.json_mapping = json_mapping
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.archive_dir = archive_dir
        self.request_dir = request_dir
        self.request_error_dir = request_error_dir
        self.error_dir = error_dir
        self.write_bucket = write_bucket
        self.api_update_endpoint = api_update_endpoint
        self.output_row_limit = PANDAS_CONVERSION_LIMIT
        self.ssl_file = ssl_tmp_filename
        self.target_catalog = target_catalog
        self.source_catalog = source_catalog
        self.to_email = to_email
        self.from_email = "noreply@uspto.gov"

    def json_to_sql(self, json_request: Dict[str, Any]) -> str:
        """
        Convert a JSON request to an SQL query.

        :param json_request: The JSON request to process.
        :return: The generated SQL query.
        :example:
            json_request = {"name": {"value": "John;Doe", "fuzzyLogicIndicator": True}}
            -> SELECT DISTINCT search.*, CONCAT_WS(', ', owner_name: 'John', hist_owner_nm: 'Doe') AS what_matched FROM search WHERE (owner_name ILIKE '%John%' OR owner_name ILIKE '%Doe%')
        """
        query = build_query(
            source_catalog=self.source_catalog,
            target_catalog=self.target_catalog,
            json_request=json_request,
        )
        return query

    def process_xlsx_file(self, file_name: str) -> str:
        try:
            input_df_pandas = ps.read_excel(
                io=f"{self.input_dir}/{file_name}",
                engine="openpyxl",
                usecols=[0],
                dtype=str,
            )

            input_df_pyspark = (
                input_df_pandas.to_spark()
                .distinct()
                .selectExpr("trim(serial_num) as serial_num")
            )

            tdet = spark.sql(
                f"select * except (create_dt, create_user) from {self.target_catalog}.gold.search",
            )

            output = (
                tdet.join(
                    other=input_df_pyspark.alias("user_input"),
                    on=[tdet.serial_num == input_df_pyspark.serial_num],
                    how="inner",
                )
                .selectExpr(
                    "* except(user_input.serial_num)",
                    "user_input.serial_num what_matched",
                )
                .limit(self.output_row_limit)
            )

            return output
        except Exception as e:
            raise e

    def process_json_file(self, file_name: str) -> str:
        """
        Process a JSON file and generate an SQL query.

        :param file_name: The name of the JSON file.
        :return: The generated SQL query.
        :example:
            file_name = "request.json"
            -> SELECT DISTINCT search.*, CONCAT_WS(', ', owner_name: 'John', hist_owner_nm: 'Doe')
            AS what_matched FROM search WHERE (owner_name ILIKE '%John%' OR owner_name ILIKE '%Doe%')
        """
        try:
            json_request = {
                k: v[0]
                for k, v in spark.read.option("multiline", True)
                .json(f"{self.input_dir}/{file_name}")
                .toPandas()
                .to_dict()
                .items()
            }
            return self.json_to_sql(json_request=json_request)
        except json.JSONDecodeError as e:  # FATAL
            raise e
        except KeyError as e:  # FATAL
            raise e
        except Exception as e:
            raise e

    def write_to_xlsx(self, output_df: DataFrame, output_file: str) -> int:
        """
        Write the DataFrame to an XLSX file and upload it to S3.

        :param output_df: The Spark DataFrame to write.
        :param output_file: The name of the output XLSX file.
        :return: The etag of the S3 upload operation - the etag will serve
        as a successfull upload response.
        :example:
            write_to_xlsx(output_df, "s3://tdet-dev/outputFolder/output.xlsx")
            -> "5d41402abc4b2a76b9719d911017c592"
        """
        try:
            pandas_dataframe = self.convert_df_to_pandas(
                output_df.distinct().limit(self.output_row_limit)
            )
            excel_bytes = self.convert_df_to_excel_bytes(pandas_dataframe)
            return self.upload_to_s3(key=output_file, body=excel_bytes)
        except Exception as e:
            raise e

    def convert_df_to_pandas(self, output_df: DataFrame) -> pd.DataFrame:
        """
        Convert a Spark DataFrame to a pandas DataFrame.

        :param output_df: The Spark DataFrame to convert.
        :return: A pandas DataFrame.
        """
        try:
            df = (
                output_df.limit(self.output_row_limit)
                .withColumn("filing_date", date_format("filing_date", "yyyy-MM-dd"))
                .withColumn(
                    "registration_date", date_format("registration_date", "yyyy-MM-dd")
                )
                .toPandas()
            )
            return df

        except Exception as e:
            raise e

    def convert_df_to_excel_bytes(self, df: pd.DataFrame) -> bytes:
        """
        Convert a pandas DataFrame to Excel bytes.

        :param df: The pandas DataFrame to convert.
        :return: The Excel bytes.
        """
        # engine_kwargs options can be found: https://xlsxwriter.readthedocs.io/workbook.html#Workbook
        try:
            with BytesIO() as stream:
                with pd.ExcelWriter(
                    stream,
                    engine="xlsxwriter",
                    engine_kwargs={
                        "options": {
                            "strings_to_urls": False,
                            "strings_to_formulas": False,
                        },
                    },
                ) as writer:
                    df.to_excel(excel_writer=writer, index=False, sheet_name="Sheet1")
                    try:
                        writer.sheets["Sheet1"].set_column("A:AV", 25)
                    except:
                        print("Could not adjust column width.")
                return stream.getvalue()
        except Exception as e:
            raise e

    @retry(max_attempts=2, wait_seconds=2)
    def upload_to_s3(self, key: str, body: bytes) -> str:
        """
        Upload a file to S3.

        :param key: The S3 key for the uploaded file.
        :param body: The bytes of the file to upload.
        :return: The etag of the S3 upload operation.
        :example:
            upload_to_s3(key="output.xlsx", body=file_bytes)
            -> "5d41402abc4b2a76b9719d911017c592"
        """
        try:
            s3 = boto3.resource("s3")
            response = s3.Bucket(self.write_bucket).put_object(
                Key=f"outputFolder/{key}", Body=body
            )
            etag = response.e_tag
            return etag
        except Exception as e:
            raise e

    @retry(max_attempts=3, wait_seconds=2)
    def send_email(self, subject: str, body: str, to_email: str) -> None:
        """
        Send an email notification.

        :param subject: The subject of the email.
        :param body: The body of the email.
        :param to_email: The recipient's email address.
        :example:
            subject = "Processing Failed"
            body = "Failed to process file."
            to_email = "benjamin.fielstra@uspto.gov"
        """
        try:
            print(
                f"Sending email to {to_email} with subject '{subject}' and body '{body}'"
            )
            msg = MIMEMultipart("alternative")
            msg["From"] = self.from_email
            msg["To"] = self.to_email
            msg["Subject"] = subject
            message = body
            msg.attach(MIMEText(message))

            mailserver = smtplib.SMTP("mailer.uspto.gov")
            mailserver.sendmail(self.from_email, self.to_email, msg.as_string())

            mailserver.quit()

        except Exception as e:
            raise e

    @retry(max_attempts=2, wait_seconds=2)
    def update_table(
        self,
        request_number: int,
        status: str,
        output_file: str = None,
        error_file: str = None,
        count: int = None,
    ) -> int:
        """
        Update the table with the processing status of a file by sending a POST request.

        :param request_number: The request number.
        :param status: The processing status ('COMPLETED', 'FAILED', 'INPROGRESS', 'FATAL', 'OPEN', 'READYTOBEPROCESSED').
        :param output_file: The output file name if the process was successful, otherwise None.
        :param error_file: The error file name if the process failed, otherwise None.
        :return: The HTTP status code of the response.
        :example:
            request_number = 1
            status = "COMPLETED"
            output_file = "output.xlsx"
            -> 200
        """
        # TODO: Add job history values
        try:
            valid_statuses = {
                "COMPLETED",
                "FAILED",  # <-- Needs to be added
                "ERROR",  # <-- Should be replaced with ERROR
                "INPROGRESS",
                "FATAL",  # <-- Needs to be added
                "OPEN",  # <-- Needs to be added
                "READYTOBEPROCESSED",  # <-- Needs to be added
            }

            if status not in valid_statuses:
                raise ValueError(
                    f"Invalid status: {status}. Must be one of {valid_statuses}"
                )

            payload = {
                "searchHistId": request_number,
                "requestStatusCd": status,
                "outputFileName": output_file,
                "errorFileName": error_file,
                "recordCount": count,
                "lastModUserId": "DBX_USER",
            }
            print(payload)
            headers = get_auth_headers()

            response = requests.post(
                url=self.api_update_endpoint,
                data=json.dumps(payload),
                headers=headers,
                verify=self.ssl_file,
            )

            print(f"Update table successful: {response.ok}")
            response.raise_for_status()
            return response.status_code
        except ValueError as e:
            raise e
        except Exception as e:
            raise e

    def write_error_log(self, file_name: str, error: str) -> None:
        """
        Write the error details to a log file in the specified folder using dbutils.fs.put.

        :param file_name: The name of the file that caused the error.
        :param error: The error message.
        :example:
            file_name = "request.json"
            error = "File not found"
        """
        try:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file_path = f"{self.error_dir}/{file_name}"
            log_message = (
                "An error occurred while processing the file on the backend. "
                "Please reach out to a Databricks admin.\n"
                f"Error processing file: {file_name}\n"
                f"Timestamp: {timestamp}\n"
            )
            dbutils.fs.put(log_file_path, log_message)
        except Exception as e:
            print(f"Failed to write error log: {str(e)}")

    def handle_exception(self, function_name: str, error: str) -> None:
        """
        Handle exceptions by logging the error details.

        :param function_name: The name of the function where the exception occurred.
        :param error: The error message.
        """
        print(f"Exception in {function_name}: {error}")
        self.write_error_log(file_name=function_name, error=error)

    def process_file(
        self, file_name: str, request_number: int, request_file: str
    ) -> Union[str, List[str]]:
        """
        Process a file and update the status, sending notifications if an exception occurs.

        :param file_name: The name of the file.
        :param request_number: The request number for the file.
        :return: The result of processing the file.
        :example:
            file_name = "request.json"
            request_number = 1
            -> SQL query or joined DataFrame
        """
        try:
            request_file_name = request_file.split("/")[-1]
            output_file = f"Req_{str(request_number).zfill(6)}_output.xlsx"

            if file_name.endswith(".json"):
                query = self.process_json_file(file_name=file_name)
                df = spark.sql(query).distinct().limit(self.output_row_limit)
                count = df.count()
                if count != 0:
                    self.write_to_xlsx(output_df=df, output_file=output_file)
                    self.update_table(
                        request_number=request_number,
                        status="COMPLETED",
                        output_file=output_file,
                        count=count,
                    )
                else:
                    self.update_table(
                        request_number=request_number,
                        status="COMPLETED",
                    )
                # TODO: remove need to i/o handled requests
                dbutils.fs.mv(
                    f"{self.request_dir}/{request_file_name}",
                    f"{self.archive_dir}/{request_file_name}",
                )
                return count
            elif file_name.endswith(".xlsx"):
                df = (
                    self.process_xlsx_file(file_name=file_name)
                    .distinct()
                    .limit(self.output_row_limit)
                )
                count = df.count()
                if count != 0:
                    self.write_to_xlsx(output_df=df, output_file=output_file)
                    self.update_table(
                        request_number=request_number,
                        status="COMPLETED",
                        output_file=output_file,
                        count=count,
                    )
                else:
                    print(f"No file written for {file_name}")
                    self.update_table(
                        request_number=request_number,
                        status="COMPLETED",
                    )
                # TODO: remove need to i/o handled requests
                dbutils.fs.mv(
                    f"{self.request_dir}/{request_file_name}",
                    f"{self.archive_dir}/{request_file_name}",
                )
                return count
            else:
                raise ValueError("Unsupported file type")
        except Exception as e:
            error_message = str(e)
            # TODO: remove need to i/o handled requests
            dbutils.fs.mv(
                f"{self.request_dir}/{request_file_name}",
                f"{self.request_error_dir}/{request_file_name}",
            )
            error_file_name = f"Req_{str(request_number).zfill(6)}_error.log"
            self.send_email(
                subject=f"TDET [{dbx_env}] | Processing Failed",
                body=f"Failed to process {file_name}: {error_message}",
                to_email=self.to_email,
            )
            self.update_table(
                request_number=request_number,
                status="ERROR",
                error_file=error_file_name,
            )
            self.write_error_log(
                file_name=error_file_name,
                error=error_message,
            )
            raise e


def parse_file_metadata(file_metadata: str) -> Tuple[int, str, str, str]:
    """
    Parse file metadata JSON string to extract request number, request type, file name, and request file name.

    :param file_metadata: The metadata JSON string containing request details.
    :return: A tuple containing request number, request type, file name, and request file name.
    :example:
        parse_file_metadata(file_metadata='{"requestNumber": 123, "requestType": "ReportSearch", "fileName": "input.json"}')
        -> (123, "ReportSearch", "input.json")
    """
    # TODO: should download/stream file rather than overkill with spark json read
    # metadata = json.loads(file_metadata)
    metadata = {
        k: v[0]
        for k, v in spark.read.option("multiline", True)
        .json(file_metadata)
        .toPandas()
        .to_dict()
        .items()
    }
    request_number = metadata["requestNumber"]
    request_type = metadata["requestType"]
    file_name = metadata["fileName"]
    return request_number, request_type, file_name, file_metadata


def process_files_concurrently(
    file_handler: FileHandler, file_metadata_list: List[str]
) -> None:
    """
    Process a list of files concurrently based on their metadata.

    :param file_handler: The file handler instance.
    :param file_metadata_list: A list of JSON strings containing file metadata.
    :example:
        file_metadata_list = [
            '{"requestNumber": 123, "requestType": "ReportSearch", "fileName": "input.json"}',
            '{"requestNumber": 124, "requestType": "SerialNumberSearch", "fileName": "input.xlsx"}'
        ]
        process_files_concurrently(file_handler=handler, file_metadata_list=file_metadata_list)
    """
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {}
        for file_metadata in file_metadata_list:
            request_number, request_type, file_name, request_file = parse_file_metadata(
                file_metadata=file_metadata,
            )
            futures[
                executor.submit(
                    file_handler.process_file,
                    file_name=file_name,
                    request_number=request_number,
                    request_file=request_file,
                )
            ] = file_name

        for future in concurrent.futures.as_completed(futures):
            file_name = futures[future]
            try:
                result = future.result()
                print(
                    f"Successfully processed {file_name}: {result} records written to S3."
                )
            except Exception as e:
                print(f"Failed to process {file_name}: {str(e)}")


handler = FileHandler(**FILEHANDLER_ARGS)
process_files_concurrently(handler, file_metadata_list)

# COMMAND ----------

# DBTITLE 1,Exit Notebook
dbutils.notebook.exit("Finished attempting to process all available request files.")
