# Databricks notebook source
# DBTITLE 1,Purpose
#Purpose: This notebook contains common functions and parameters that will be set at spark level 
#Author: Pawanpreet Sangari
#Added email functionality 

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.types import *
import pyspark
from pyspark.sql import functions as f, DataFrame
from pyspark.sql.functions import current_timestamp, col, trim, when, regexp_replace, lit
import datetime
from pyspark.sql import DataFrame
import yaml
from pyspark.sql.types import StructType, StructField, StringType
import time
from py4j.protocol import Py4JJavaError
import re
import requests
import json
import traceback
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType
from datetime import timedelta

from delta.tables import *

import zipfile
from io import BytesIO
import os

import json
import secrets
import boto3
import requests

import pytz
from pyspark.sql.window import Window

default_aws_region = "us-east-1"

# COMMAND ----------

# DBTITLE 1,General Common Functions
def isEmptyString(empty_str: str) -> bool:
    if (empty_str is None) or (str(empty_str).strip()==""):
        return True
    else:
        return False
    return None


def read_yaml(file_path):
    with open(file_path, "r") as f:
        return yaml.safe_load(f)  # or full_load(f)

# COMMAND ----------

common_config_file_path = config_file
mysql_lom_scope = "mysql_bdr_server"
oracle_jbteasps_server = "oracle_jbteasps_server"

print(f'{common_config_file_path=},{mysql_lom_scope=},{oracle_jbteasps_server=}')

# COMMAND ----------

# DBTITLE 1,Stream Data Read function
def read_data_from_stream(schemaLoc:str, filePath: str)-> DataFrame:
    """ A common function to read data from cloud files using databricks auto loader"""
    try:
        df_data_reader = (spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "parquet")\
        .option("cloudFiles.schemaLocation", schemaLoc)\
        .option("cloudFiles.maxFilesPerTrigger", 1)\
        .option("rescuedDataColumn", "_rescue")
        .load(filePath))
    except:
        raise
        return None
    else:
        return df_data_reader

# COMMAND ----------

# DBTITLE 1,Stream Data Load Function
def load_stream_data_to_detla_tables(df_stream_data, checkpointLoc: str, catalogName: str,schemaName: str, tableName: str):
    """This Function loads stream data into delta tables"""
    try:
            df_stream_data.drop("Op").drop("_rescue").writeStream \
            .trigger(once=True) \
            .format("delta") \
            .option("checkpointLocation", checkpointLoc) \
            .table(f'{catalogName}.{schemaName}.{tableName}')
    except:
        raise
        return None
    else:
        return 

# COMMAND ----------

# DBTITLE 1,Begin job log and job control
def begin_job_cntl(data_quality_db,ctlg_db_name,job_name:str,job_start_ts):
    """This function creates an entry in job log table before starting the load process.
    And return max load_ts from job control table
    Also checks the config file and returns the dataload date"""
    
    job_log_id = spark.sql(f"""select nvl(max(job_log_id),0)+1 from {ctlg_db_name}.job_log 
    where job_nm='{job_name}' """).collect()[0][0]
    
    job_log_start_query = f"""
        insert into {ctlg_db_name}.job_log 
        PARTITION (job_nm = '{job_name}')
        (job_log_id,start_ts,end_ts,status_ct,src_cnt,trgt_cnt,comment_tx) 
        values (
            {job_log_id},cast('{job_start_ts}' as timestamp), null, 'started', 0,0, ''
            )
        """
    #print(job_log_start_query)
    spark.sql(job_log_start_query)
    
    df_cntl_dt=spark.sql(f"select max(load_ts) as max_cntl_dt from {ctlg_db_name}.job_control where job_nm = '{job_name}'")
    control_dt=df_cntl_dt.collect()[0][0]
    if(not(control_dt)):
        control_dt = "None"
        

    return(control_dt)
    
    

# COMMAND ----------

# DBTITLE 1,End Job Control and Job Log
def end_job_cntl(data_quality_db,ctlg_db_name,job_name,job_start_ts,proc_stus_cd,df_src_count,df_trgt_count,error):
    
    job_log_id = spark.sql(f"""select nvl(max(job_log_id),0) from {ctlg_db_name}.job_log 
    where job_nm='{job_name}' """).collect()[0][0]
    
    if proc_stus_cd == 'completed':
        job_control_query = f"""
            insert into {ctlg_db_name}.job_control
            PARTITION (job_nm = '{job_name}')
            (job_control_id, load_ts,create_ts,create_user_id,last_mod_ts,last_mod_user_id)
            values ({job_log_id}, '{job_start_ts}', current_timestamp(), 'etl', current_timestamp(),'etl') 
            """
        spark.sql(job_control_query)
        
        comment_text = error
    else:
        comment_text = re.sub('[\'"]+', '', f"FAILED TO LOAD DATA. AN EXCEPTION OF TYPE {type(error).__name__} OCCURED AT LINE {error.__traceback__.tb_lineno}. ERROR MESSAGE:{traceback.format_exception(type(error),error,error.__traceback__, limit=1, chain=True)}")
        
    #job_log_complete_query = f"""
    #    insert into {ctlg_db_name}.job_log values (
    #  '{job_name}', cast('{job_start_ts}' as timestamp), current_timestamp(), '{proc_stus_cd}', {df_count}, 'filing basis count'
    #    )
    #    """
    job_log_complete_query = f"""
        update {ctlg_db_name}.job_log 
        set 
            end_ts = current_timestamp(),
            status_ct = '{proc_stus_cd}',
            src_cnt = {df_src_count},
            trgt_cnt = {df_trgt_count},
            comment_tx = '{comment_text}'
        where job_nm = '{job_name}'
        and start_ts = '{job_start_ts}'
        """
    spark.sql(job_log_complete_query)
    return

# COMMAND ----------

# DBTITLE 1,MySQL connection details using Databricks secrets utility
def read_data_from_mysql_conn_dsu(sql_query: str, schema_name: str) -> DataFrame:
    """A common function to read data from mysql db"""
    
    pushdown_query = ("(" + sql_query + ") query_alias ")
    
    df_data = None
    
    try:
        df_data = (spark.read.format("mysql")\
                          .option("host", dbutils.secrets.get(scope=mysql_lom_scope, key="host"))\
                          .option("port",dbutils.secrets.get(scope=mysql_lom_scope, key="port"))\
                          .option("driver", "com.mysql.cj.jdbc.Driver")\
                          .option("database",schema_name)\
                          .option("dbtable",pushdown_query )\
                          .option("user", dbutils.secrets.get(scope=mysql_lom_scope, key="username"))\
                          .option("password", dbutils.secrets.get(scope=mysql_lom_scope, key="password"))\
                          .option("fetchsize","10000")\
                          .load())
    except Exception as e:
        print("Exception message: {}".format(e))
        
        return None
    else:
        return df_data

# COMMAND ----------

def load_mysql_table_dsu(df_table,schema_name,mysql_tbl_name):
    """This function appends data into mysql db table
    Receives 2 parameters: df_table( A dataframe with data to be written into the mysql table with columns ordered to match mysql db)
    and mysql_tbl_name (Name of mysql table writing data to)"""
    
    df_table_count = df_table.count()
    
    try:
        df_table.write.format("mysql")\
        .mode("append")\
        .option("batchsize","10000")\
        .option("numPatitions","1")\
        .option("host", dbutils.secrets.get(scope=mysql_lom_scope, key="host"))\
        .option("port",dbutils.secrets.get(scope=mysql_lom_scope, key="port"))\
        .option("driver", "com.mysql.cj.jdbc.Driver")\
        .option("user", dbutils.secrets.get(scope=mysql_lom_scope, key="username"))\
        .option("password", dbutils.secrets.get(scope=mysql_lom_scope, key="password"))\
        .option("database",schema_name)\
        .option("dbtable", mysql_tbl_name)\
        .save()
        
        print(f"Total records loaded in target table {mysql_tbl_name}: {df_table_count}")
    except Exception as e:
        print("Exception message: {}".format(e)) 
        return None
    return df_table_count

# COMMAND ----------

def read_data_from_postgres_conn(sql_query: str, scope_name="oracle_trmpvt_server") -> DataFrame:
    """A common function to read data from PostgreSQL db"""
    
    pushdown_query = "(" + sql_query + ") query_alias "
    host = dbutils.secrets.get(scope=scope_name, key="host")
    port = dbutils.secrets.get(scope=scope_name, key="port")
    db_name = dbutils.secrets.get(scope=scope_name, key="db_name")
    
    df_data = None
    
    try:
        df_data = (
            spark.read.format("jdbc")
            .option("url", f"jdbc:postgresql://{host}:{port}/{db_name}")
            .option("dbtable", pushdown_query)
            .option("user", dbutils.secrets.get(scope=scope_name, key="username"))
            .option("password", dbutils.secrets.get(scope=scope_name, key="password"))
            .option("driver", "org.postgresql.Driver")
            .option("fetchsize", "10000")
            .load()
        )
    except Exception as e:
        print("Exception message: {}".format(e))
        return None
    else:
        return df_data

# COMMAND ----------

def read_data_from_oracle_conn_dsu(sql_query: str, schema_name: str, secrets_name:str) -> DataFrame:
    """A common function to read data from mysql db"""
    
    pushdown_query = ("(" + sql_query + ") query_alias ")
    host = dbutils.secrets.get(scope=secrets_name, key="host")
    port = dbutils.secrets.get(scope=secrets_name, key="port")
    db_name = dbutils.secrets.get(scope=secrets_name, key="db_name")
    
    df_data = None
    
    try:
        df_data = (spark.read.format("jdbc")\
                          .option("url", "jdbc:oracle:thin:@"+host+":"+port+"/"+db_name)\
                          .option("dbtable",pushdown_query )\
                          .option("user", dbutils.secrets.get(scope=secrets_name, key="username"))\
                          .option("password", dbutils.secrets.get(scope=secrets_name, key="password"))\
                          .option("driver", "oracle.jdbc.OracleDriver")\
                          .option("fetchsize","10000")\
                          .load())
    except Exception as e:
        print("Exception message: {}".format(e))
        
        return None
    else:
        return df_data

# COMMAND ----------

def read_data_from_oracle_conn_dsu_cmn(sql_query: str, scope_name="oracle_trm_server") -> DataFrame:
    """A common function to read data from mysql db"""
    
    pushdown_query = ("(" + sql_query + ") query_alias ")
    host = dbutils.secrets.get(scope=scope_name, key="host")
    port = dbutils.secrets.get(scope=scope_name, key="port")
    db_name = dbutils.secrets.get(scope=scope_name, key="db_name")
    
    df_data = None
    
    try:
        df_data = (spark.read.format("jdbc")\
                          .option("url", "jdbc:oracle:thin:@"+host+":"+port+"/"+db_name)\
                          .option("dbtable",pushdown_query )\
                          .option("user", dbutils.secrets.get(scope=scope_name, key="username"))\
                          .option("password", dbutils.secrets.get(scope=scope_name, key="password"))\
                          .option("driver", "oracle.jdbc.OracleDriver")\
                          .option("fetchsize","10000")\
                          .load())
    except Exception as e:
        print("Exception message: {}".format(e))
        
        return None
    else:
        return df_data

# COMMAND ----------

def read_data_from_oracle_conn_dsu_opt(
    sql_query: str, scope_name="oracle_trm_server", options={"fetchsize": 10000}
) -> DataFrame:
    """A common function to read data from mysql db"""

    pushdown_query = "(" + sql_query + ") query_alias "
    host = dbutils.secrets.get(scope=scope_name, key="host")
    port = dbutils.secrets.get(scope=scope_name, key="port")
    db_name = dbutils.secrets.get(scope=scope_name, key="db_name")

    df_data = None

    try:
        df_data = (
            spark.read.format("jdbc")
            .option("url", "jdbc:oracle:thin:@" + host + ":" + port + "/" + db_name)
            .option("dbtable", pushdown_query)
            .option("user", dbutils.secrets.get(scope=scope_name, key="username"))
            .option("password", dbutils.secrets.get(scope=scope_name, key="password"))
            .option("driver", "oracle.jdbc.OracleDriver")
        )
        for option_key, option_value in options.items():
            df_data = df_data.option(option_key, option_value)
        df_data = df_data.load()
    except (Py4JJavaError) as e:
        print("A JVM error occurred when running the JDBC Load:")
        print(e.java_exception)
    except Exception as e:
        print("Exception message: {}".format(e))
        return None
    else:
        return df_data

# COMMAND ----------

def sample_data_match(
    proc_name: str,
    df_src: DataFrame,
    trgt_tbl_name: str,
    filter_col: str,
    sample_count,
    data_quality_catalog,
    job_name,
    pk_ind="Y",
    trgt_cnctn="DELTA_LAKE",
) -> str:
    """This function performs sample data match between source dataframe and Target table for random n number of records
    Receives 5 parameters: proc_name, source df name, target table name, column name to filter data on, count of records to filter sample data set on
    Loads the result of sample data match in CMN_PROC_VRFCTN_RSLT table
    Returns string at successful completion"""
    from io import StringIO
    import pandas as pd
    import pyspark.pandas as ps
    from datacompy import SparkSQLCompare
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    import warnings

    warnings.filterwarnings("ignore", category=UserWarning)
    try:
        # execute sample data match if new data is loaded in the target table else skip sample data match
        if df_src.count() > 1 and pk_ind == "Y":
            df_dq_sample = df_src.select(key_columns)
            # Create tuple for values to filter data on
            # concat key columns
            df_dq_sample = df_dq_sample.select(
                f.concat(*[f.col(col) for col in df_dq_sample.columns]).alias("concat")
            )
            # take sample of data
            dq_sample_val = tuple(
                map(lambda row: row[0], df_dq_sample.rdd.takeSample(False, sample_count))
            )
            spark.sql(f"set dq_sample_val = {dq_sample_val}")
            # Filter src dataframe for sample records and create a new sample src df
            filter_str = """ concat({0})  in {1}""".format(
                str(key_columns).replace("[", "").replace("]", "").replace("'", ""),
                dq_sample_val,
            )

            df_src_sample = df_src.filter(f.expr(filter_str))
            # Query Target table for sample records
            trgt_query_text = f"""(select * from {trgt_tbl_name} where {filter_str})"""
            if trgt_cnctn == "DELTA_LAKE":
                df_trgt_sample = spark.sql(trgt_query_text)
            elif trgt_cnctn == "MYSQL_TQR_LOM_DB":
                df_trgt_sample = read_data_from_mysql_conn_dsu(trgt_query_text, "tqr_lom")
            # Compare src and target df for sample data match
            if (
                df_src_sample.exceptAll(df_trgt_sample).count() == 0
                and df_trgt_sample.exceptAll(df_src_sample).count() == 0
            ):
                # sample data matches
                data_quality_result = "Source and Target Data Match"
                print("Source and Target Data Match")
            else:
                print("Source and Target Data Does Not Match")
                for col in df_src_sample.columns:
                    df_src_sample = df_src_sample.withColumnRenamed(col, col.lower())
                for col in df_trgt_sample.columns:
                    df_trgt_sample = df_trgt_sample.withColumnRenamed(col, col.lower())
                df_src_sample = ps.from_pandas(df_src_sample.toPandas())
                df_trgt_sample = ps.from_pandas(df_trgt_sample.toPandas())
                comparison = SparkSQLCompare(
                    spark_session=spark,df1=df_src_sample.to_spark(), df2=df_trgt_sample.to_spark(), join_columns=key_columns
                )
                data_quality_result = comparison.report()

        elif df_src.count() > 1 and pk_ind == "N":

            trgt_query_text = f"""(select * from {trgt_tbl_name})"""
            if trgt_cnctn == "DELTA_LAKE":
                df_trgt = spark.sql(trgt_query_text)
            elif trgt_cnctn == "MYSQL_TQR_LOM_DB":
                df_trgt = read_data_from_mysql_conn_dsu(trgt_query_text, "tqr_lom")
            if (
                df_src.exceptAll(df_trgt).count() == 0
                and df_trgt.exceptAll(df_src).count() == 0
            ):
                # sample data matches
                data_quality_result = "Source and Target Data Match."
            else:
                # sample data does not match
                data_quality_result = "Source and Target Data Does Not Match."
        else:
            # skip sample match if no new data is loaded in Target table
            data_quality_result = "Sample data match not performed as there is no new data loaded in the target table."

        spark.sql("set data_quality_result = " + str(data_quality_result))
    except Exception as e:
        print("Exception message: {}".format(e))
    
    # Insert results of sample data match in the CMN_PROC_VRFCTN_RSLT table
    try:
        print("Inserting results in DQ Tables")
        dq_insert_query = (f"""
      INSERT INTO
      {data_quality_catalog}.SILVER.CMN_PROC_VRFCTN_RSLT (
      PROC_ID,PROC_NAME,PROC_CTGRY_CD,QUERY_SET_ID,QUERY_DQ_CD,SRC_QUERY_NAME,TRGT_QUERY_NAME,
      JOB_LOG_ID,
      JOB_START_TS,RPTD_SRC_RSLT_CNT,RPTD_TRGT_RSLT_CNT,ERR_THRSHLD_PCT,RPTD_VRNC_PCT,DQ_RSLT_MSG,AUDT_INSRT_ID,AUDT_INSRT_TS,SRC_SYS_NAME
      )
      SELECT
      RFRNC.PROC_ID,
      RFRNC.PROC_NAME,
      RFRNC.PROC_CTGRY_CD,
      NULL AS QUERY_SET_ID,
      'SM' AS QUERY_DQ_CD,
      substring_index('{trgt_tbl_name}', '.', -1) AS SRC_QUERY_NAME,
      '{trgt_tbl_name}' AS TRGT_QUERY_NAME,
      RFRNC.JOB_LOG_ID,
      RFRNC.JOB_START_TS,
      {sample_count} AS RPTD_SRC_RSLT_CNT,
      {sample_count} AS RPTD_TRGT_RSLT_CNT,
      0 AS ERR_THRSHLD_PCT,
      0 AS RPTD_VRNC_PCT,
       regexp_replace('{data_quality_result}', "\\\"", "")  AS DQ_RSLT_MSG,
      'etl' as AUDT_INSRT_ID,
      current_timestamp() as AUDT_INSRT_TS,
      rfrnc.SRC_SYS_NAME
      from
      (
      SELECT
        rfrnc.SRC_SYS_NAME,
        rfrnc.proc_name,
        rfrnc.PROC_CTGRY_CD,
        rfrnc.PROC_ID,
        rfrnc.PROC_CNFG_FILE_PATH,
        max_by(job.job_log_id, job.start_ts) as job_log_id,
        max(job.start_ts) as job_start_ts
      from
        {data_quality_catalog}.SILVER.CMN_PROC_DEFN_RFRNC rfrnc
        inner join {trgt_catalog}.silver.job_log job on rfrnc.PROC_NAME = job.job_nm
      where
        rfrnc.proc_name = '{job_name}'
        and job.status_ct = 'completed'
      group by
        rfrnc.SRC_SYS_NAME,
        rfrnc.proc_name,
        rfrnc.PROC_CTGRY_CD,
        rfrnc.PROC_ID,
        rfrnc.PROC_CNFG_FILE_PATH
      ) RFRNC
      """)
        #print(dq_insert_query)
        spark.sql(dq_insert_query)

    except Exception as e:
        print("Exception message: {}".format(e))

    # data_quality_result.unpersist()
    return f"Sample Match results added to {data_quality_catalog}.SILVER.CMN_PROC_VRFCTN_RSLT Table. "

# COMMAND ----------

def full_data_match(PROC_CTGRY_CD: str,SRC_SYS_NAME: str,proc_name: str,src_query_text: str,trgt_query_text: str,trgt_cnctn="DELTA_LAKE") -> str:
    """This function performs full data match between source table and Target table
    Receives 6 parameters: PROC_CTGRY_CD,SRC_SYS_NAME, proc_name, source table name, target table name, target connection name
    Loads the result of full data match in CMN_PROC_VRFCTN_RSLT table
    Returns string at successful completion"""

    #execute sample data match if new data is loaded in the target table else skip sample data match
    try:
        src_query = src_query_text
        df_src = spark.sql(src_query)
        #Query Target table
        trgt_query = trgt_query_text
        if trgt_cnctn == "DELTA_LAKE":
            df_trgt = spark.sql(trgt_query)
        elif trgt_cnctn == "MYSQL_TQR_LOM_DB":
            df_trgt = read_data_from_mysql_conn_dsu(trgt_query, "tqr_lom")
        #Compare src and target df for sample data match
        if df_src.exceptAll(df_trgt).count() ==0 and df_trgt.exceptAll(df_src).count() ==0:
            #sample data matches
            data_quality_result =  "Source and Target Data Match"
        else:
            #sample data does not match
            data_quality_result =  "Source and Target Data Does Not Match"
    except Exception as e:
        print("Exception message: {}".format(e))
    
    spark.sql("set data_quality_result = "+str(data_quality_result))

    if SRC_SYS_NAME == 'LOM':
        proc_db = lom_db
    spark.sql(f"set proc_db = {proc_db}")  

    #Insert results of sample data match in the CMN_PROC_VRFCTN_RSLT table
    spark.sql(f"""
    INSERT INTO
    {data_quality_db}.SILVER.CMN_PROC_VRFCTN_RSLT (
    PROC_ID,PROC_NAME,PROC_CTGRY_CD,QUERY_SET_ID,QUERY_DQ_CD,SRC_QUERY_NAME,TRGT_QUERY_NAME,
    --JOB_LOG_ID,
    JOB_START_TS,RPTD_SRC_RSLT_CNT,RPTD_TRGT_RSLT_CNT,ERR_THRSHLD_PCT,RPTD_VRNC_PCT,DQ_RSLT_MSG,AUDT_INSRT_ID,AUDT_INSRT_TS,SRC_SYS_NAME
    )
    SELECT
    RFRNC.PROC_ID,
    RFRNC.PROC_NAME,
    RFRNC.PROC_CTGRY_CD,
    NULL AS QUERY_SET_ID,
    'SM' AS QUERY_DQ_CD,
    NULL AS SRC_QUERY_NAME,
    NULL AS TRGT_QUERY_NAME,
    --RFRNC.JOB_LOG_ID,
    RFRNC.JOB_START_TS,
    NULL AS RPTD_SRC_RSLT_CNT,
    NULL AS RPTD_TRGT_RSLT_CNT,
    0 AS ERR_THRSHLD_PCT,
    0 AS RPTD_VRNC_PCT,
    '{data_quality_result}' AS DQ_RSLT_MSG,
    'etl' as AUDT_INSRT_ID,
    current_timestamp() as AUDT_INSRT_TS,
    rfrnc.SRC_SYS_NAME
    from
    (
    SELECT
      rfrnc.SRC_SYS_NAME,
      rfrnc.proc_name,
      rfrnc.PROC_CTGRY_CD,
      rfrnc.PROC_ID,
      rfrnc.PROC_CNFG_FILE_PATH,
      max_by(job.job_log_id, job.start_ts) as job_log_id,
      max(job.start_ts) as job_start_ts
    from
      {data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC rfrnc
      inner join {proc_db}.silver.job_log job on rfrnc.PROC_NAME = job.job_nm
    where
      rfrnc.PROC_CTGRY_CD = '{PROC_CTGRY_CD}'
      and rfrnc.SRC_SYS_NAME = '{SRC_SYS_NAME}'
      and rfrnc.proc_name = '{proc_name}'
      and job.status_ct = 'completed'
    group by
      rfrnc.SRC_SYS_NAME,
      rfrnc.proc_name,
      rfrnc.PROC_CTGRY_CD,
      rfrnc.PROC_ID,
      rfrnc.PROC_CNFG_FILE_PATH
    ) RFRNC
    """)
    return f"Data Match results : {data_quality_result}. The results are added to {data_quality_db}.SILVER.CMN_PROC_VRFCTN_RSLT Table."

# COMMAND ----------

import smtplib, traceback, hashlib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


#emailid = config.get('tqr','EMAIL')

HEADER = '''
<html>
    <head>

    </head>
    <body>
'''
FOOTER = '''
    </body>
</html>
'''


class Notify(object):

    def render_template(self, rv, data):
        for key in data:
            rv = rv.replace(key,data[key])
        return rv

    def get_messages(self, templ, data):
        rich = self.render_template(templ, data)

        return {'html': rich}

    def compose_email(self, template_str, subj, to, data):
        template = f'<html><head/><body><h2>{template_str} </h2><hr> \
<p><b></b> <h2>INDEXED</h2> \
</body></html>'
        content = self.get_messages(template, data)

        from_addr = 'noreply@uspto.gov'

        msg = MIMEMultipart('alternative')
        msg['Subject'] = subj
        msg['From'] = from_addr
        msg['To'] = to

        part2 = MIMEText(content['html'], 'html')

        msg.attach(part2)

        return msg

    def send_mail(self, msg):
        s = smtplib.SMTP("mailer.uspto.gov")
        s.sendmail(msg['From'], msg['To'].split(','), msg.as_string())
        s.quit()

    def notify(self, template, subj, to, data):

        msg = self.compose_email(template, subj, to, data)
        self.send_mail(msg)

# COMMAND ----------

def get_current_datetime(timezone=None, date_pattern: str = "%Y-%m-%dT%H:%M:%S") -> str:
    """
    Generates current datetime value in associated timezone.
    """

    if timezone is None:
        timezone = pytz.timezone("US/Eastern")

    current_est_dt = datetime.datetime.now().astimezone(timezone)
    return current_est_dt.strftime(date_pattern)

# COMMAND ----------

def replace_zero_date_with_null(df: DataFrame, columns: list) -> DataFrame:
    """
    Replace dates with 0 values to null.
    """
    
    for column_name in columns:
        df = df.withColumn(
                column_name, when(col(column_name).cast(StringType()) != "0", col(column_name)).otherwise(lit(None))
        )

    return df

# COMMAND ----------

def replace_empty_string_with_null(df: DataFrame) -> DataFrame:
    """
    Replaces empty string with null values.
    Applicable only to string type columns.
    """
    
    EMPTY_STRING = ""
    STRING_TYPE = "string"

    for column_name, column_data_type in df.dtypes:
        if column_data_type != STRING_TYPE:
            continue

        df = df.withColumn(
            column_name,
            when(
                trim(col(column_name)) != EMPTY_STRING, col(column_name)
            ).otherwise(lit(None)),
        )
        
    return df

# COMMAND ----------

def replace_null_with_empty_string(df: DataFrame, columns: list) -> DataFrame:
    """
    Replace null values with empty string.
    """
    
    EMPTY_STRING = ""
    
    for column_name in columns:
        df = df.withColumn(
                column_name, when(col(column_name).isNull(), lit(EMPTY_STRING)).otherwise(col(column_name))
        )

    return df

# COMMAND ----------

def zip_file(file_path: str) -> bool:
    """
    Archive file from associated file_path to ZIP.
    """

    try:
        file_name = os.path.basename(file_path)

        with BytesIO() as zip_buffer:
            # Add all xml files in single zip file
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(file_path, arcname=file_name)

            zip_buffer.seek(0)

            # Writes zip file to dbfs
            with open(file_path.replace(".xml", ".zip"), "wb") as file:
                file.write(zip_buffer.read())

        return True
    except Exception as ex:
        return False

# COMMAND ----------

def upload_to_s3(source_path: str) -> bool:
    """
    Uploads file to s3 location.
    """

    source_file_path = source_path.replace("/dbfs/", "dbfs:/")
    zip_file_name = os.path.basename(source_path)
    destination_file_path = f"{s3_path}/{zip_file_name}"

    try:
        success = dbutils.fs.cp(source_file_path, destination_file_path)
        return success
    except Exception as ex:
        print(f"Uploading file: {source_file_path} to {destination_file_path} failed.")
        print("Exception message: ", ex)
        return False
