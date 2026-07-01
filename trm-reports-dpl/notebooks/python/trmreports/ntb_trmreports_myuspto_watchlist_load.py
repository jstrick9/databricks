# Databricks notebook source
# MAGIC %md
# MAGIC Purpose:
# MAGIC - To pull records from a SharePoint excel spreadsheet (that serves as user input) to filter against our tables.
# MAGIC
# MAGIC Key notes:
# MAGIC - The `CLIENT_SECRET` associated with this workflow will require a refresh 05/01/2026
# MAGIC - This workflow pulls data from Documents / Databricks Sources / MyUSPTO Account Monitor
# MAGIC - Link to Excel sheet: https://usptogov.sharepoint.com/:x:/r/sites/O3G-TrademarkDataandAnalyticsProductGroup/Shared%20Documents/Databricks%20Sources/MyUSPTO%20Account%20Monitor/watch.xlsx?d=w6e34d510a55140f1a9442a111c18057a&csf=1&web=1&e=oglB50

# COMMAND ----------

# DBTITLE 1,Imports
import msal
from typing import Final
import requests
import time
from datetime import datetime, timedelta
import io
import pandas as pd
import numpy as np

# COMMAND ----------

# DBTITLE 1,Environment Settings
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

is_prod: bool = dbx_env == "prod"
print(f"{config_file=},{dbx_env=}")
if is_prod: print("Job is running in production, attempting to load SharePoint excel.")
else: print("Job is running in non-production, sample file will be generated.")

# COMMAND ----------

# DBTITLE 1,Import Shared Functions
# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,Set Configuration
common_configs = read_yaml(config_file)
reporting_catalog = common_configs["schema"]["trgt_catalog"]
print(reporting_catalog)

# COMMAND ----------

# DBTITLE 1,Begin Job
job_name = "ntb_trmreports_myuspto_watchlist_load"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# DBTITLE 1,Globals
COLUMN_NAMES: dict = ["patron_id", "alert"]
SCHEMA: pyspark.sql.types.StructType = StructType(
    [StructField(column_name, StringType(), True) for column_name in COLUMN_NAMES]
)
SCHEMA

# COMMAND ----------

# DBTITLE 1,Short Circuit Development Loads
if not is_prod:
    display(
        spark.sql(
            f"""
    insert into {reporting_catalog}.silver.myuspto_monitor_watchlist
    select
        col1 patron_id,
        col2 send_alert,
        col3 is_valid,
        col4 create_user,
        col5 create_timestamp
    from
        values
            ('00064E4B-4B5D-48D3-A31D-F0F608CABFCC', 'Y', 'Y', -1, current_timestamp),
            ('000F3C5E-E6D2-46C3-90C4-C29001281070', 'N', 'Y', -1, current_timestamp),
            ('003A662B-B2E2-406D-9631-0293A393A1DC', 'Y', 'Y', -1, current_timestamp),
            ('0006-4B5D-48D3-AD-FC', 'Y', 'N', -1, current_timestamp),
            ('123456789', 'Y', 'N', -1, current_timestamp),
            ('000F3C5E-E6D2-46C3-90C4-C29001281070', 'N', 'Y', -1, current_timestamp)
    """
        )
    )
    end_job_cntl(
        f"{reporting_catalog}.silver",
        job_name,
        job_start_ts,
        "completed",
        0,
        "job completed successfully",
    )
    dbutils.notebook.exit(f"Job completed. Inserted 6 sample records.")

# COMMAND ----------

# DBTITLE 1,File Extractor
class SharePointFileExtractor:
    """
    Class helper for finding and extracting a file from SharePoint Drive using
    the Microsoft Graph API.
    """

    TOKEN_SCOPE: list[str] = ["https://graph.microsoft.com/.default"]
    TENANT_ID: str = "ff4abfe9-83b5-4026-8b8f-fa69a1cad0b8"
    APPLICATION_TYPE: str = "application/json;odata.metadata=minimal"

    def __init__(
        self,
        site_id: str,
        client_id: str,
        client_secret: str,
        subdirectory: str,
        file_name: str,
    ):
        """
        Instantiates an extractor with the associated Service Principal and
        file metadata.

        site_id: str = "some-site-such-as-this"
        client_id: str = "some-client-id-like-this"
        client_secret: str = "aR4nd0m5st~ng0fch4raCt3rs~l1k3tH15"
        subdirectory: str = "some/file/directory/on/sharepoint"

        This is really just a helper to set up a reader as an object... I think
        in the future this might be more useful than right now because there aren't
        any pipelines that do this.
        """
        self._site_id: str = site_id
        self.__client_id: str = client_id
        self.__client_secret: str = client_secret
        self._subdirectory: str = subdirectory
        self._file_name: str = file_name
        self._graph_url = f"graph.microsoft.com/v1.0/sites/{self._site_id}"
        self._token = None
        self._token_expiry = None
        self._drive_id = None
        self._file_id = None
        self._data = None

    @property
    def token(self) -> dict:
        if not self._token or self._token_is_stale():
            self._refresh_token()
        return self._token

    @property
    def drive_id(self) -> str:
        """
        Helper to set the drive ID of the subfolder in which the file is located.
        For now, this points to the `Documents` folder.
        """
        if not self._drive_id:
            print("Drive ID not yet initialized.")
            self._get_drive_id()
        return self._drive_id

    @property
    def file_id(self) -> str:
        """
        Helper to set the file from the subfolder on SharePoint.
        """
        if not self._file_id:
            print("Fetching file...")
            self._get_file_id()
        return self._file_id

    @property
    def data(self):
        if not self._data:
            print("Creating input DF...")
            self._get_data()
        return self._data

    def _token_is_stale(self) -> bool:
        """
        Helper to check whether the token should be refreshed.
        """
        if not self._token_expiry:
            print("Token expiry not yet calculated.")
            return True
        if datetime.datetime.now() > self._token_expiry:
            print("Token requires refresh.")
            return True
        print("Token is fresh.")
        return False

    def _get_file_id(self) -> None:
        """
        Helper to get the file download from which we want to extract data.
        """
        current_token = self.token["access_token"]
        current_drive_id = self.drive_id
        response = requests.get(
            url=f"https://{self._graph_url}/drives/{current_drive_id}/root:/{self._subdirectory}:/children",
            headers={
                "Authorization": f"Bearer {current_token}",
                "Accept": self.APPLICATION_TYPE,
                "Content-Type": "application/json",
            },
        )
        file_id: str = [
            data for data in response.json()["value"] if data["name"] == self._file_name
        ][0]["id"]
        self._file_id = file_id

    def _refresh_token(self) -> None:
        """
        Helper to refresh the access token if it's either expired
        or if it has not yet been instantiated.
        """
        authority_url = f"https://login.microsoftonline.com/{self.TENANT_ID}"
        app = msal.ConfidentialClientApplication(
            authority=authority_url,
            client_id=self.__client_id,
            client_credential=self.__client_secret,
        )
        self._token = app.acquire_token_for_client(scopes=self.TOKEN_SCOPE)
        self._token_expiry = datetime.datetime.now() + timedelta(self._token["expires_in"])
        print("Token refreshed.")

    def _get_drive_id(self) -> None:
        """
        Helper for getting the drive ID for the API call to grab the file download
        url.
        """
        current_token = self.token["access_token"]
        response = requests.get(
            url=f"https://{self._graph_url}/drives",
            headers={
                "Authorization": f"Bearer {current_token}",
                "Accept": self.APPLICATION_TYPE,
                "Content-Type": "application/json",
            },
        )
        try:
            drive_id: str = [
                data for data in response.json()["value"] if data["name"] == "Documents"
            ][0]["id"]
            self._drive_id = drive_id
            print("Drive ID initialized.")
        except KeyError as e:
            print(response.json())
            raise e

    def _get_data(self) -> None:
        current_token = self.token["access_token"]
        current_drive_id = self.drive_id
        current_file_id = self.file_id
        response = requests.get(
            url=f"https://{self._graph_url}/drives/{current_drive_id}/items/{current_file_id}/content",
            headers={
                "Authorization": f"Bearer {current_token}",
                "Accept": self.APPLICATION_TYPE,
                "Content-Type": "application/json",
            },
        )
        self._data = response.content

# COMMAND ----------

# DBTITLE 1,Globals
SCOPE: str = "tm_dna_sharepoint"
SITE_ID: str = "c7e90280-ffd0-4bef-90b0-8e38f962f34e"
CLIENT_ID: str = dbutils.secrets.get(scope=SCOPE, key="client_id")
CLIENT_SECRET: str = dbutils.secrets.get(scope=SCOPE, key="client_secret")
SUBDIRECTORY: str = "Databricks Sources/MyUSPTO Account Monitor"
FILE_NAME: str = "watch.xlsx"

# COMMAND ----------

# DBTITLE 1,Instantiate Extractor
extractor: SharePointFileExtractor = SharePointFileExtractor(
    site_id=SITE_ID,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    subdirectory=SUBDIRECTORY,
    file_name=FILE_NAME,
)

# COMMAND ----------

# DBTITLE 1,Fetch Data
excel_buffer: bytes = extractor.data
user_input_data: io.BytesIO = io.BytesIO(excel_buffer)

# COMMAND ----------

# DBTITLE 1,Convert to Pandas DF
pandas_df = pd.read_excel(
    io=user_input_data, dtype=str, names=COLUMN_NAMES, usecols=range(len(COLUMN_NAMES))
).replace({np.nan: "NO VALUE"})
pandas_df.head(), pandas_df.info()

# COMMAND ----------

# DBTITLE 1,Convert DF and Show Raw Data
df = spark.createDataFrame(pandas_df, SCHEMA)
df.createOrReplaceTempView("user_input_data")
display(df)

# COMMAND ----------

# DBTITLE 1,Create Valid Data View
# MAGIC %sql
# MAGIC create or replace temp view valid_data as
# MAGIC select
# MAGIC   trim(patron_id) patron_id,
# MAGIC   max(
# MAGIC     case
# MAGIC       when upper(trim(alert)) = 'Y' then 'Y'
# MAGIC       else 'N'
# MAGIC     end
# MAGIC   ) as send_alert,
# MAGIC   'Y' is_valid,
# MAGIC   -1 as create_user,
# MAGIC   current_timestamp as create_timestamp
# MAGIC from
# MAGIC   user_input_data
# MAGIC where
# MAGIC   patron_id != 'NO VALUE'
# MAGIC   and len(patron_id) = 36
# MAGIC   and patron_id ilike '%-%-%-%-%'
# MAGIC group by
# MAGIC   patron_id
# MAGIC union
# MAGIC select
# MAGIC   patron_id,
# MAGIC   alert as send_alert,
# MAGIC   'N' is_valid,
# MAGIC   -1 as create_user,
# MAGIC   current_timestamp as create_timestamp
# MAGIC from
# MAGIC   user_input_data
# MAGIC where
# MAGIC   patron_id = 'NO VALUE'
# MAGIC   or len(patron_id) != 36
# MAGIC   or patron_id not ilike '%-%-%-%-%'

# COMMAND ----------

# DBTITLE 1,Show Input Data
display(spark.sql("select * from valid_data"))

# COMMAND ----------

# DBTITLE 1,Insert Values
"""
Business Rules
1. Only valid (36 character patron IDs) are considered..
Records without a patron ID (empty cells) are not valid.
2. Duplicate patron IDs send an alert as long as one.
instance has a positive alert status (`Y`).
3. Invalid alert statuses default to `N` when a valid
patron ID is present.
4. This table maintains a historical list of records. Batch
loads share the same timestamp, making it possible to see
which records have changed since the (n-)previous loads. In
the corresponding downstream report pipeline, the pipeline will
only consider the most recent batch insert as the cross-filter.
"""

display(
    spark.sql(
        f"""
    insert into {reporting_catalog}.silver.myuspto_monitor_watchlist
      select 
        * 
      from 
        valid_data
    """
    )
)

# COMMAND ----------

# DBTITLE 1,Count Records
counts = spark.sql(
    f"""
    select
        count(1) count,
        create_timestamp
    from
        {reporting_catalog}.silver.myuspto_monitor_watchlist
    group by
        create_timestamp
    order by
        create_timestamp desc
    limit 1
    """
).collect()[0][0]

# COMMAND ----------

# DBTITLE 1,Sample
# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   trm_reporting_dev.silver.myuspto_monitor_watchlist
# MAGIC order by
# MAGIC   create_timestamp desc
# MAGIC limit 5;

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
dbutils.notebook.exit(f"Job completed. Inserted {counts} records.")
