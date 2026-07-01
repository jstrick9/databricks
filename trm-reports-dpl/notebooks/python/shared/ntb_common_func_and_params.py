# Databricks notebook source
# DBTITLE 1,Purpose
#Purpose: This notebook contains common functions and parameters that will be set at spark level 
#Author: Pawanpreet Sangari
#Added email functionality 

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.types import *
import pyspark
from pyspark.sql import functions as f
from pyspark.sql.functions import current_timestamp
import datetime
from pyspark.sql.functions import col, lit
from pyspark.sql import DataFrame
import yaml
from pyspark.sql.types import StructType, StructField, StringType
import time
from pyspark.sql.window import Window

import re
import requests
import json
import traceback
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType
from datetime import timedelta
#from hyperleaup import HyperFile

from delta.tables import *

import json
import secrets
import time
#import boto3
import requests

from io import BytesIO
import smtplib
 
from email import encoders
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE

default_aws_region = "us-east-1"

# COMMAND ----------

from pyspark.sql.functions import col, create_map, lit, regexp_extract, concat_ws, collect_list, when, expr,regexp_replace,encode,length, split,trim,filter,to_date,date_format,last,lit,concat,size,min,max,first,last,month,year,current_timestamp,lit, upper,datediff, initcap, lag, add_months, date_add,countDistinct,lead,row_number,substring,avg,current_date,format_number,coalesce
from pyspark.sql.types import ShortType, IntegerType, LongType, StringType, DateType, TimestampType, StructField, StructType, DoubleType
from delta.tables import *
import yaml
import os
from pyspark.sql import Window
# from datetime import datetime
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import count as _count
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import col, expr, posexplode, sequence
from pyspark.sql.functions import col, round
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col, expr, abs as abs_col
from pyspark.sql.functions import collect_list, concat_ws

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
    

### below function is to clean tabs, newlines and dulication whitesplace from df columns-- Naval
def columns_to_clean(df,columns_in_list):
    for col_name in columns_in_list:
        df = df.withColumn(col_name,regexp_replace(col_name,r'\s+',' '))
    return df


# COMMAND ----------

common_config_file_path = config_file
mysql_lom_scope = "mysql_bdr_server"
oracle_jbteasps_server = "oracle_jbteasps_server"

print(f'{common_config_file_path=},{mysql_lom_scope=},{oracle_jbteasps_server=}')

# COMMAND ----------

# DBTITLE 1,Job start ts
import pytz
from pytz import timezone
job_start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

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
def begin_job_cntl(ctlg_db_name,job_name:str,job_start_ts):
    """This function creates an entry in job log table before starting the load process.
    And return max load_ts from job control table
    Also checks the config file and returns the dataload date"""
    
    job_log_id = spark.sql(f"""select nvl(max(job_log_id),0)+1 from {ctlg_db_name}.job_log 
    where job_nm='{job_name}' """).collect()[0][0]
 
    job_log_start_query = f"""
        insert into {ctlg_db_name}.job_log 
        PARTITION (job_nm = '{job_name}')
        (job_log_id,start_ts,end_ts,status_ct,record_qt,comment_tx) 
        values (
            {job_log_id},from_utc_timestamp('{job_start_ts}','America/New_York'), null, 'started', 0, ''
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
def end_job_cntl(ctlg_db_name,job_name,job_start_ts,proc_stus_cd,df_count,error, df_trgt_count=0):
    
    job_log_id = spark.sql(f"""select nvl(max(job_log_id),0) from {ctlg_db_name}.job_log 
    where job_nm='{job_name}' """).collect()[0][0]
    
    if proc_stus_cd == 'completed':
        job_control_query = f"""
            insert into {ctlg_db_name}.job_control
            PARTITION (job_nm = '{job_name}')
            (job_control_id,load_ts,create_ts,create_user_id,last_mod_ts,last_mod_user_id)
            values ( {job_log_id},from_utc_timestamp('{job_start_ts}','America/New_York'), from_utc_timestamp(current_timestamp(),'America/New_York'), 'etl', from_utc_timestamp(current_timestamp(),'America/New_York'),'etl') 
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
            end_ts = from_utc_timestamp(current_timestamp(),'America/New_York'),
            status_ct = '{proc_stus_cd}',
            record_qt = {df_count},
            comment_tx = '{comment_text}'
        where job_nm = '{job_name}'
        and start_ts = from_utc_timestamp('{job_start_ts}','America/New_York')
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
                          .option("driver", "com.mysql.cj.jdbc.Driver") \
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
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("host", dbutils.secrets.get(scope=mysql_lom_scope, key="host"))\
        .option("port",dbutils.secrets.get(scope=mysql_lom_scope, key="port"))\
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

def read_data_from_oracle_conn_dsu_cmn(sql_query: str, scope_name="oracle_trmpvt_server") -> DataFrame:
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

def read_data_from_oracle_conn_dsu_opt(sql_query: str, scope_name="oracle_trm_server", options= {"fetchsize":10000}) -> DataFrame:
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
                          #.option("numPartitions",10)\
                          #.option("fetchsize","10000")\
                          #a column that can be used that has a uniformly distributed range of values that can be used for parallelization
                          #.option("partitionColumn","LOCK_CONTROL_NO")\
                          #lowest value to pull data for with the partitionColumn
                          #.option("lowerBound",0)\
                          #max value to pull data for with the partitionColumn
                          #.option("upperBound",10000)\
                          #number of partitions to distribute the data into. Do not set this very large (~hundreds)
                          #.load()
                          )
        for option_key, option_value in options.items():
            df_data= df_data.option(option_key, option_value)
        df_data = df_data.load()
    except Exception as e:
        print("Exception message: {}".format(e))
        
        return None
    else:
        return df_data

# COMMAND ----------

def sample_data_match(proc_name: str,df_src: DataFrame,trgt_tbl_name: str,filter_col: str,sample_count,trgt_cnctn="DELTA_LAKE") -> str:
    """This function performs sample data match between source dataframe and Target table for random n number of records
    Receives 5 parameters: proc_name, source df name, target table name, column name to filter data on, count of records to filter sample data set on
    Loads the result of sample data match in CMN_PROC_VRFCTN_RSLT table
    Returns string at successful completion"""

    #execute sample data match if new data is loaded in the target table else skip sample data match
    if df_src.count()>0:
        df_dq_sample = df_src.select(filter_col)
        # Create tuple for values to filter data on
        dq_sample_val = tuple(map(lambda row: row[0], df_dq_sample.rdd.takeSample(False, sample_count)))
        spark.sql(f"set dq_sample_val = {dq_sample_val}")
        #Filter src dataframe for sample records and create a new sample src df
        filter_str = " {0} in {1}".format(filter_col,dq_sample_val)
        df_src_sample = df_src.filter(f.expr(filter_str))
        #Query Target table for sample records
        trgt_query_text = f"""(select * from {trgt_tbl_name} where {filter_str})"""
        if trgt_cnctn == "DELTA_LAKE":
            df_trgt_sample = spark.sql(trgt_query_text)
        elif trgt_cnctn == "MYSQL_TQR_LOM_DB":
            df_trgt_sample = read_data_from_mysql_conn_dsu(trgt_query_text, "tqr_lom")
        #Compare src and target df for sample data match
        if df_src_sample.exceptAll(df_trgt_sample).count() ==0 and df_trgt_sample.exceptAll(df_src_sample).count() ==0:
            #sample data matches
            data_quality_result =  "Source and Target Data Match"
        else:
            #sample data does not match
            data_quality_result =  "Source and Target Data Does Not Match"
    else:
        #skip sample match if no new data is loaded in Target table
        data_quality_result = "Sample data match not performed as there is no new data loaded in the target table."

    spark.sql("set data_quality_result = "+str(data_quality_result))
    #Insert results of sample data match in the CMN_PROC_VRFCTN_RSLT table
    spark.sql(f"""
    INSERT INTO
    DATA_QUALITY.SILVER.CMN_PROC_VRFCTN_RSLT (
    PROC_ID,PROC_NAME,PROC_CTGRY_CD,QUERY_SET_ID,QUERY_DQ_CD,SRC_QUERY_NAME,TRGT_QUERY_NAME,JOB_LOG_ID,JOB_START_TS,RPTD_SRC_RSLT_CNT,RPTD_TRGT_RSLT_CNT,ERR_THRSHLD_PCT,RPTD_VRNC_PCT,DQ_RSLT_MSG,AUDT_INSRT_ID,AUDT_INSRT_TS,SRC_SYS_NAME
    )
    SELECT
    RFRNC.PROC_ID,
    RFRNC.PROC_NAME,
    RFRNC.PROC_CTGRY_CD,
    NULL AS QUERY_SET_ID,
    'SM' AS QUERY_DQ_CD,
    NULL AS SRC_QUERY_NAME,
    NULL AS TRGT_QUERY_NAME,
    RFRNC.JOB_LOG_ID,
    RFRNC.JOB_START_TS,
    NULL AS RPTD_SRC_RSLT_CNT,
    NULL AS RPTD_TRGT_RSLT_CNT,
    0 AS ERR_THRSHLD_PCT,
    0 AS RPTD_VRNC_PCT,
    '{data_quality_result}' AS DQ_RSLT_MSG,
    'etl' as AUDT_INSRT_ID,
    from_utc_timestamp(current_timestamp(),'America/New_York') as AUDT_INSRT_TS,
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
      DATA_QUALITY.SILVER.CMN_PROC_DEFN_RFRNC rfrnc
      inner join lom.silver.job_log job on rfrnc.PROC_NAME = job.job_nm
    where
      rfrnc.proc_name = '{proc_name}'
      and job.status_ct = 'completed'
    group by
      rfrnc.SRC_SYS_NAME,
      rfrnc.proc_name,
      rfrnc.PROC_CTGRY_CD,
      rfrnc.PROC_ID,
      rfrnc.PROC_CNFG_FILE_PATH
    ) RFRNC
    """)
    return "Sample Match results added to LOM.SILVER.CMN_PROC_VRFCTN_RSLT Table. "

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
    PROC_ID,PROC_NAME,PROC_CTGRY_CD,QUERY_SET_ID,QUERY_DQ_CD,SRC_QUERY_NAME,TRGT_QUERY_NAME,JOB_LOG_ID,JOB_START_TS,RPTD_SRC_RSLT_CNT,RPTD_TRGT_RSLT_CNT,ERR_THRSHLD_PCT,RPTD_VRNC_PCT,DQ_RSLT_MSG,AUDT_INSRT_ID,AUDT_INSRT_TS,SRC_SYS_NAME
    )
    SELECT
    RFRNC.PROC_ID,
    RFRNC.PROC_NAME,
    RFRNC.PROC_CTGRY_CD,
    NULL AS QUERY_SET_ID,
    'SM' AS QUERY_DQ_CD,
    NULL AS SRC_QUERY_NAME,
    NULL AS TRGT_QUERY_NAME,
    RFRNC.JOB_LOG_ID,
    RFRNC.JOB_START_TS,
    NULL AS RPTD_SRC_RSLT_CNT,
    NULL AS RPTD_TRGT_RSLT_CNT,
    0 AS ERR_THRSHLD_PCT,
    0 AS RPTD_VRNC_PCT,
    '{data_quality_result}' AS DQ_RSLT_MSG,
    'etl' as AUDT_INSRT_ID,
    from_utc_timestamp(current_timestamp(),'America/New_York') as AUDT_INSRT_TS,
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
from email.mime.base import MIMEBase
from email import encoders


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
            rv = rv.replace(key, data[key])
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

    def compose_email_attach(self, template_str, subj, to,from_addr, data, filepaths):
        template = f'<html><head/><body><h2>{template_str} </h2><hr> \
</body></html>'
        content = self.get_messages(template, data)

        #from_addr = 'noreply@uspto.gov'
        cc = 'James.Nosal@USPTO.GOV'
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subj
        msg['From'] = from_addr
        msg['To'] = to
        msg['Cc'] = cc

        part2 = MIMEText(content['html'], 'html')
        msg.attach(part2)

        for filepath in filepaths:
            with open(filepath, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename= {filepath.split('/')[-1]}",
            )
            msg.attach(part)

        return msg
    
    ## Commented below line to add "Cc" field without modifying parameters   
    # def compose_email_attachment_with_html_body(self, html, subj, to,from_addr, filepaths, text=''): 
    def compose_email_attachment_with_html_body(self, html, subj, to, from_addr, filepaths, text='', **kwargs):
        Cc = kwargs.get('Cc', '') ## added to capture Cc field.
        msg = MIMEMultipart('alternative')
        part1 = MIMEText(text, 'plain')
        part2 = MIMEText(html, 'html')
        msg.attach(part1)
        msg.attach(part2)
        msg_mixed = MIMEMultipart('mixed')
        msg_mixed.attach(msg)
        
        for filepath in filepaths:
            with open(filepath, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename= {filepath.split('/')[-1]}",
            )
            
            msg_mixed.attach(part)
           
        msg_mixed['Subject'] = subj
        msg_mixed['From'] = from_addr
        msg_mixed['To'] = to
       # Added cc field to capture if anyone want to add additional emails address in Cc section
        if Cc:
            msg_mixed['Cc'] = Cc
        return msg_mixed

    #Commented the entire Defination and added new on which can check if cc recipient is present
    # def send_mail(self, msg):
    #     s = smtplib.SMTP("mailer.uspto.gov")
    #     # s.sendmail(msg['From'], msg['To'].split(','), msg.as_string()) 
    #     s.sendmail(msg['From'], msg['To'].split(',') + msg['Cc'].split(','), msg.as_string()) # added this code to capture cc field
    #     s.quit()
    ## Below Code added at 14/01/2025
    def send_mail(self, msg):
        s = smtplib.SMTP("mailer.uspto.gov")
        to_recipients = msg['To'].split(',')
        cc_recipients = msg['Cc'].split(',') if 'Cc' in msg else []
        all_recipients = to_recipients + cc_recipients
        s.sendmail(msg['From'], all_recipients, msg.as_string())
        s.quit()

    def notify(self, template, subj, to, data):
        msg = self.compose_email(template, subj, to, data)
        self.send_mail(msg)

# COMMAND ----------

def df_match(df1, df2):

    # schema match check
    clist1 = [x.lower() for x in sorted(df1.columns)]
    clist2 = [x.lower() for x in sorted(df2.columns)]
    if set(clist1) != set(clist2):
        return f'Failed - columns do not match \nColumns in df1 but not df2: {[x for x in clist1 if x not in clist2]} \nColumns in df2 but not df1: {[x for x in clist2 if x not in clist1]}'
    
    if df1.exceptAll(df2).count() == 0 and df2.exceptAll(df1).count() == 0:
        return 'Perfect match'
    
    count1 = df1.count()
    count2 = df2.count()

    if count1 != count2:
        return f'Failed - counts do not match\nCount1: {count1}\nCount2: {count2}'
    else:
        return 'Failed - counts match but content does not'

def column_comparison (df1, df2, keycolumn):
    dict_matches = {}
    for c in df1.drop(keycolumn).columns:
        if df1.select(keycolumn, c).exceptAll(df2.select(keycolumn, c)).count() == 0:
            print(c + ' matches')
            dict_matches[c] = 'match'
        else:
            print(c + ' does not match')
            dict_matches[c] = 'non_match'
    return dict_matches

def column_comparison_ck (df1, df2, keycolumns):
    dict_matches = {}
    for c in df1.drop(*keycolumns).columns:
        if df1.select(*keycolumns, c).exceptAll(df2.select(*keycolumns, c)).count() == 0:
            print(c + ' matches')
            dict_matches[c] = 'match'
        else:
            print(c + ' does not match')
            dict_matches[c] = 'non_match'
    return dict_matches

# COMMAND ----------

## return boolean check on exact schema match between two dataframes
def schema_match(df1, df2):
    return df1.select(sorted(df1.columns)).dtypes == df2.select(sorted(df2.columns)).dtypes

## return boolean check on column name match between two dataframes
def col_match(df1, df2):
    return sorted(df1.columns) == sorted(df2.columns)

## return detailed differences between schemas of two dataframes
def schema_diffs(df1, df2):
    schema1 = df1.select(sorted(df1.columns)).dtypes
    schema2 = df2.select(sorted(df2.columns)).dtypes

    cols1 = [tup[0] for tup in schema1]
    cols2 = [tup[0] for tup in schema2]

    gap1 = [x for x in schema1 if x not in schema2]
    gap2 = [x for x in schema2 if x not in schema1]

    gap_cols1 = [c for c in cols1 if c not in cols2]
    gap_cols2 = [c for c in cols2 if c not in cols1]

    dtype_mismatch1 = [x for x in gap1 if x[0] in list(set(gap_cols1).intersection(set(gap_cols2)))]
    dtype_mismatch2 = [x for x in gap2 if x[0] in list(set(gap_cols1).intersection(set(gap_cols2)))]

    rslt = {'df1_only_columns': gap_cols1,
        'df2_only_columns': gap_cols2,
        'data_type_mismatches1': dtype_mismatch1,
        'data_type_mismatches2': dtype_mismatch2}
    
    return json.loads(json.dumps(rslt))

# COMMAND ----------

## return percentage match for each column between two dataframes
def column_comparison_pct (df1, df2, keycolumns):
    base_cnt = df1.count()
    dict_matches = {}
    for c in df1.drop(*keycolumns).columns:
        col_non_match_cnt = df1.select(*keycolumns, c).exceptAll(df2.select(*keycolumns, c)).count()
        if col_non_match_cnt == 0:
            print(c + ' matches')
            dict_matches[c] = '100% match'
        else:
            print(c + ' does not match')
            pct_non_match = "%0.2f" % ((base_cnt - col_non_match_cnt) / base_cnt * 100) ## replaced round function with float formatting to avoid import conflicts
            dict_matches[c] = f'{pct_non_match}% match'
    return dict_matches

# COMMAND ----------

def insert_to_dq(job_nm, table1, table2, proc_ctgry_cd, src_sys_name, rslt, dq_catalog):

    proc_id = spark.sql(f"select proc_id from {dq_catalog}.silver.CMN_PROC_DEFN_RFRNC where proc_name='{job_nm}'").collect()[0][0]
    table1_cnt = spark.table(f'{table1}').count()
    table2_cnt = spark.table(f'{table2}').count()

    df = spark.sql(f"""
        select
        '{proc_id}' AS PROC_ID,
        '{job_nm}' AS PROC_NAME,
        '{proc_ctgry_cd}' as PROC_CTGRY_CD,
        1 as QUERY_SET_ID,
        'DM' as QUERY_DQ_CD,
        '{table1}' as SRC_QUERY_NAME,
        '{table2}' as TRGT_QUERY_NAME,
        null as JOB_LOG_ID,
        from_utc_timestamp(current_timestamp(),'America/New_York') as JOB_START_TS,
        {table1_cnt} as RPTD_SRC_RSLT_CNT,
        {table2_cnt} as RPTD_TRGT_RSLT_CNT,
        0 as ERR_THRSHLD_PCT,
        CASE
        when {table1_cnt} is null or  {table2_cnt} is null then -100
            WHEN {table1_cnt} = 0
            AND  {table2_cnt} != 0 THEN -100
            WHEN {table1_cnt} = 0 THEN 0.0000
            ELSE round(ABS(
            (
                (
                (
                    FLOAT({table1_cnt}) - FLOAT( {table2_cnt})
                ) * 100.0000
                ) / FLOAT({table1_cnt})
            )
            ),2)
        END AS RPTD_VRNC_PCT,
        null AS DQ_RSLT_MSG,
        'ETL' AS AUDT_INSRT_ID,
        from_utc_timestamp(current_timestamp(),'America/New_York') as AUDT_INSRT_TS,
        '{src_sys_name}' as SRC_SYS_NAME
    """)

    df = df.withColumn(
        'DQ_RSLT_MSG', lit(rslt)
    )

    df.write.mode('append').format("delta").insertInto(f"{dq_catalog}.SILVER.CMN_PROC_VRFCTN_RSLT")


# COMMAND ----------

def detailed_data_match(df_altrx, df_dbx, pks):

    rslt = {}

    altrx_cnt = df_altrx.count()
    dbx_cnt = df_dbx.count()
    
    ## check schema match
    if col_match(df_altrx, df_dbx) == False:
        rslt['result'] = 'Failed'
        rslt['result_detail'] = 'Table schemas do not match'
        rslt['schema_diffs'] = schema_diffs(df_altrx, df_dbx)
    elif df_altrx.exceptAll(df_dbx).count() == 0 and df_dbx.exceptAll(df_altrx).count() == 0:
        rslt['result'] = 'Success'
        rslt['result_detail'] = 'Tables are a perfect match'
    elif altrx_cnt != dbx_cnt:
        rslt['result'] = 'Failed'
        rslt['result_detail'] = 'Table counts do not match'
        rslt['count_diffs'] = {'Alteryx': altrx_cnt, 'DBX': dbx_cnt}
    else:
        rslt['result'] = 'Failed'
        rslt['result_detail'] = 'Table counts match but contents do not'
        rslt['column_diffs'] = column_comparison_pct(df_altrx, df_dbx, pks)

    return rslt

# COMMAND ----------

def alteryx_data_match(alteryx_table, dbx_table, pks, job_nm, dq_catalog):

    proc_ctgry_cd = 'REPORTS'
    src_sys_name = 'TRM_REPORTS'

    df_altrx = spark.sql(f"select * from {alteryx_table}").drop('create_ts', 'create_user_id', 'update_ts', 'update_user_id')
    df_dbx = spark.sql(f"select * from {dbx_table}").drop('create_ts', 'create_user_id', 'update_ts', 'update_user_id')

    df_altrx = df_altrx.select(sorted([x.lower() for x in df_altrx.columns]))
    df_dbx = df_dbx.select(sorted([x.lower() for x in df_dbx.columns]))

    rslt = detailed_data_match(df_altrx, df_dbx, pks)

    insert_to_dq(job_nm, alteryx_table, dbx_table, proc_ctgry_cd, src_sys_name, str(rslt), dq_catalog)
    
    return rslt

# COMMAND ----------

## Commented this code and upgraded csv file formate to this defination 
# def get_bytestream(df: pd.core.frame.DataFrame):
#     with BytesIO() as stream:
#         with pd.ExcelWriter(
#             stream,
#             engine="xlsxwriter",
#             engine_kwargs={
#                 "options": {
#                     "strings_to_urls": False,
#                     "strings_to_formulas": False,
#                 }
#             },
#         ) as writer:
#             df.to_excel(
#                 excel_writer=writer,
#                 index=False,
#             )
#             stream.seek(0)
#         return stream.getvalue()

def get_bytestream(df: pd.core.frame.DataFrame, output_type: str = 'excel'):
    if output_type == 'excel': 
        with BytesIO() as stream:
            with pd.ExcelWriter(
                stream,
                engine="xlsxwriter",
                engine_kwargs={
                    "options": {
                        "strings_to_urls": False,
                        "strings_to_formulas": False,
                    }
                },
            ) as writer:
                df.to_excel(
                    excel_writer=writer,
                    index=False,
                )
                stream.seek(0)
            return stream.getvalue()
    elif output_type == 'csv':
         with BytesIO() as stream:
            df.to_csv(
                path_or_buf=stream,
                index=False,
            )
            stream.seek(0)
            return stream.getvalue()
    else:
        raise TypeError("Valid output types are: [`excel`, `csv`]")
    
def send_mail(
    send_from: str,
    send_to: str,
    send_to_cc: str,
    subject: str,
    text: str,
    data_to_attach,
    attachment_name: str,
    server: str = "mailer.uspto.gov",
):
    try:
        msg = MIMEMultipart()
        msg["From"] = send_from
        msg["To"] = COMMASPACE.join(send_to.split(","))
        msg['Cc'] = COMMASPACE.join(send_to_cc.split(","))
        msg["Subject"] = subject

        msg.attach(MIMEText(text))
        
        data_to_attach = data_to_attach.toPandas()

        part = MIMEApplication(get_bytestream(data_to_attach))
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            "attachment",
            filename=attachment_name,
        )
        msg.attach(part)

        smtp = smtplib.SMTP(server)
        # smtp.sendmail(send_from, send_to.split(","), msg.as_string())
        # Updated code to include cc reciepint 
        rcpt = send_to.split(",") + (send_to_cc.split(",") if send_to_cc else []) 
        smtp.sendmail(send_from, rcpt , msg.as_string())
        smtp.close()
    except Exception as error:
        print("An issue occured during the email sending process.")
        raise error


# COMMAND ----------

def send_mail_no_attachment(
    send_from: str,
    send_to: str,
    send_to_cc: str,
    subject: str,
    text: str,
    server: str = "mailer.uspto.gov",
):
    try:
        msg = MIMEMultipart()
        msg["From"] = send_from
        msg["To"] = COMMASPACE.join(send_to.split(","))
        msg['Cc'] = COMMASPACE.join(send_to_cc.split(","))
        msg["Subject"] = subject

        msg.attach(MIMEText(text, 'html'))

        smtp = smtplib.SMTP(server)
        smtp.sendmail(send_from, send_to.split(","), msg.as_string())
        smtp.close()
    except Exception as error:
        print("An issue occurred during the email sending process.")
        raise error

# COMMAND ----------

def export_to_file(output_file_name, df, file_format, export_path, delimiter=None, header_yn="yes"):
    """
    Export a DataFrame to various file formats with configurable options.

    Args:
        output_file_name (str): The name of the output file.
        df (DataFrame): The Spark DataFrame to export.
        file_format (str): The file format (json, ndjson, xlsx, csv, txt).
        export_path (str): Path for exporting the file.
        delimiter (str): The delimiter to use for CSV or TXT files. Defaults to a comma.
        header_yn (str): Indicates if the header should be included ("yes" or "no").

    Raises:
        ValueError: If an unsupported file format is specified.
    """

    # Ensure the delimiter is specified correctly, defaulting to a comma
    delimiter = delimiter if delimiter else ","

    # Ensure the header parameter is either 'true' or 'false'
    header_option = "true" if header_yn.lower() == "yes" else "false"

    # Convert Spark DataFrame to Pandas DataFrame for non-Spark-supported formats
    pandas_df = df.toPandas()

    # Convert file_format to lower case
    file_format = file_format.lower()

    # Define file path
    # dbutils.fs.mkdirs(export_path)
    file_path = f"{export_path}{output_file_name}.{file_format}"
    print(f"Exporting file to: {file_path}\n")

    # Export logic based on file format
    if file_format == "json":
        pandas_df.to_json(file_path, orient="records", lines=False)
    elif file_format == "ndjson":
        pandas_df.to_json(file_path, orient="records", lines=True)
    elif file_format == "xlsx":
        pandas_df.to_excel(file_path, index=False)
    elif file_format == "csv":
        pandas_df.to_csv(file_path, index=False, header=(header_option == "true"), sep=delimiter)
    elif file_format == "txt":
        pandas_df.to_csv(file_path, index=False, header=(header_option == "true"), sep=delimiter)
    else:
        raise ValueError(f"Unsupported output format: {file_format}\n")

    print(f"File successfully exported as {file_format}.\n")

# COMMAND ----------

#### Consolidated Email Function ######

def send_email_report(
    job_nm: str,
    subject: str,
    send_from: str,
    send_to: str,
    send_to_cc = '',
    html_body = '',
    plain_text_body = '',
    attachments = []
):
    """
    Send an email with optional cc recipients, html content, body text and file attachments.

    Args:
        subject (str): Email subject line.
        send_from (DataFrame):  Email address from which the email will be sent.
        send_to (str): Email address of the recipients in a comma separated list.
        send_to_cc (str): Optional: Email address of the CC recipients in a comma separated list.
        html_content (str): Optional: HTML formatted content for the email body. Defaults to blank.
        plain_text (str): Optional: Plain text for the email body in case the HTML body cannot be rendered. Will be overwritten by the HTML body in normal circumstances. Defaults to blank.
        attachments (list): Optional: List of attachments to include in the email. Accepts either a list of DBFS filepaths, or a list of tuples in format (DataFrame, filename, filetype) where filetype is either csv or xlsx.
    """
    try:
        curr_dt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).strftime('%B %d, %Y')
        footer = f"Generated on {curr_dt} - Databricks Workflow: {job_nm}. For any questions/comments, please utilize the <a href='https://apps.gov.powerapps.us/play/e/default-ff4abfe9-83b5-4026-8b8f-fa69a1cad0b8/a/ea18fb4c-aa64-4056-b0d5-f5ad4097cf0d?tenantId=ff4abfe9-83b5-4026-8b8f-fa69a1cad0b8&sourcetime=1731596262267&source=portal'>TMDnA Request Form</a>."
        msg = MIMEMultipart('alternative')
        part1 = MIMEText(plain_text_body + "\n\n" + footer, 'plain')
        part2 = MIMEText(html_body + "<br><br>" + footer, 'html')
        msg.attach(part1)
        msg.attach(part2)
        msg_mixed = MIMEMultipart('mixed')
        msg_mixed.attach(msg)

        msg_mixed["From"] = send_from
        msg_mixed["To"] = COMMASPACE.join(send_to.split(","))
        msg_mixed['Cc'] = COMMASPACE.join(send_to_cc.split(","))
        msg_mixed["Subject"] = subject
        

        if len(attachments) > 0:
            if type(attachments[0]) is tuple: #### for when sending just dataframes
                for (df, filename, filetype) in attachments:
                    data_to_attach = df.toPandas()

                    part = MIMEApplication(get_bytestream(data_to_attach, filetype))
                    encoders.encode_base64(part)
                    part.add_header(
                        "Content-Disposition",
                        "attachment",
                        filename=filename,
                    )
                    msg_mixed.attach(part)

            elif type(attachments[0]) is str: #### for when getting filepaths
                for filepath in attachments:
                    with open(filepath, "rb") as attachment:
                        part = MIMEBase("application", "octet-stream")
                        part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        "Content-Disposition",
                        f"attachment; filename= {filepath.split('/')[-1]}",
                    )
                    
                    msg_mixed.attach(part)

        smtp = smtplib.SMTP("mailer.uspto.gov")
        rcpt = send_to.split(",") + (send_to_cc.split(",") if send_to_cc else []) 
        smtp.sendmail(send_from, rcpt , msg_mixed.as_string())
        smtp.close()

    except Exception as error:
        print("An issue occured during the email sending process.")
        raise error


# COMMAND ----------

## Defination to create upper case all the coumns of th dataframe
def uppercase_columns(df):
    return df.select([col(c).alias(c.upper()) for c in df.columns])

# COMMAND ----------

def count_empty(table_counts: list[int]) -> int:
    """
    Helper to count the number of empty table failures.
    """
    num_empty_tables: int = sum([1 for count in table_counts if count == 0])
    if not num_empty_tables: print("No empty load detected. Moving to next task.")
    return num_empty_tables

# COMMAND ----------

print("LOADED.")
