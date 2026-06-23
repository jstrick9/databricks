# Databricks notebook source
# MAGIC %md
# MAGIC ### Calls the tmapplser incremental Load file
# MAGIC - This change will allow for back filling
# MAGIC - This will check if  start and end dates are supplied, if not uses current date has a run date

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("SRC_SYS_NAME", "TMNGPDB")
dbutils.widgets.text("start_date","")
dbutils.widgets.text("end_date","")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME").rstrip()
start_date = dbutils.widgets.get("start_date").rstrip()
end_date = dbutils.widgets.get("end_date").rstrip()
src_name = SRC_SYS_NAME.lower()
config_file_name = src_name+"-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name

import pytz
from pytz import timezone
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']


spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.trgt_catalog', trgt_catalog.lower()) 
spark.conf.set('config.dbx_env', dbx_env.lower())

# COMMAND ----------

job_name = 'ntb_silver_tmapplser_inc_call_txn_discrepancies'

start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
print(f'{start_ts=}')

# COMMAND ----------

from datetime import datetime, timedelta

# Function to run another notebook with a date parameter
def run_notebook(date, env):
    print({"rundate": date.strftime("%Y-%m-%d")})
    dbutils.notebook.run("./ntb_silver_tmapplser_inc_load_based_on_txn_tables_discrepancies", 0, {"rundate": date.strftime("%Y-%m-%d"), "dbx_env": env, "SRC_SYS_NAME" : "TMNGPDB"})


if start_date == '' or  end_date == '':
    current_date = datetime.now().astimezone(pytz.timezone('US/Eastern'))
    run_notebook(current_date, dbx_env)
else:
    # start_date 
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    # End date 
    end_date =  datetime.strptime(end_date, "%Y-%m-%d")
    # Loop from start date to the day before today
    current_date = start_date
    while current_date <= end_date:
        run_notebook(current_date, dbx_env)
        current_date += timedelta(days=1)  # Increment day by day
        print(f"processed date {current_date}")

dbutils.notebook.exit(f"Completed call for tmapplser incremental procedure")

# COMMAND ----------


