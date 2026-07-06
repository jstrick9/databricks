# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")
dbutils.widgets.text("rundate","")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME").rstrip()
src_name = SRC_SYS_NAME.lower()
config_file_name = src_name+"-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name

import pytz
from pytz import timezone
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

from datetime import date, timedelta

rundate = dbutils.widgets.get("rundate")
if rundate == '':
    rdate = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).date() - timedelta(days=1)
    rday = rdate.strftime("%A")
    rdate_yyyy_mm_dd = rdate
    rdate = rdate.strftime('%d-%b-%y')
else:
    rdate = rundate
    import datetime
    rdate = datetime.datetime.strptime(rundate, '%Y-%m-%d') - timedelta(days=1)
    rday = rdate.strftime("%A")
    rdate_yyyy_mm_dd = rdate
    rdate = rdate.strftime('%d-%b-%y')
    
print(rday, rdate)
spark.conf.set('conf.rdate', str(rdate))

# COMMAND ----------

if rundate == '':
    formatted_rundate = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).date() - timedelta(days=1) 
    formatted_rundate = formatted_rundate.strftime('%d-%b-%Y')
else:
    formatted_rundate = rdate
formatted_rundate

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
bdx_api = common_configs['schema']['bdx_api']
print(f'{trgt_catalog=},{dbx_env=},{bdx_api=}')



# COMMAND ----------

rdate = dbutils.widgets.get("rundate").rstrip()

query = f"""
SELECT COUNT(*) as recs_count 
FROM {trgt_catalog}.silver.tmappl_daily_consolidated_vw 
WHERE pulldt = '{rdate_yyyy_mm_dd}'
"""

result = spark.sql(query)
recs_count = result.collect()[0]['recs_count']

# COMMAND ----------

import requests

url = f"{bdx_api}{formatted_rundate}"

headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}
if recs_count>0:
    try:
        response = requests.post(url, headers=headers, timeout=60)  # Timeout set to 60 seconds
        # To display the response content
        print(response.text)
        #print(url)
    except requests.exceptions.ConnectTimeout:
        print("The request timed out. Please try again later or increase the timeout.")
else:
    print("BDX Daily TMAPPL XML job not executed as recs_count = 0.")