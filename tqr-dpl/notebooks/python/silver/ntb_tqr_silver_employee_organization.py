# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE WIDGET TEXT dbx_env DEFAULT "dev"

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file = "../../../notebooks/config/"+dbutils.widgets.get("dbx_env").rstrip()+"/tqr-conf.yaml"
#config_file = '/Workspace/Users/Pawanpreet.Sangari@USPTO.GOV/bdr-ng-tqr-dpl-lo/notebooks/config/dev/tqr-conf.yaml'
print(f'{config_file=}')
config_file_path = config_file

# COMMAND ----------

config_file_path = config_file

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Load Logic
# MAGIC 1. query src_trm_application table where filing date> data load dt
# MAGIC 2. Join with emp_org table to get examiner_employee_number not present in emp_org table or status_ct !='completed'
# MAGIC 3. Pass the examiner_emp_num to the getLawcode function to get the lawcode or 404 error as "organization code"
# MAGIC append the data into employee_organization table
# MAGIC ###Enhancements
# MAGIC 1. ntb_tqr_silver_employee_organization notebook,for a given employee_no  In employee_organization table  if the record exists with status_ct = 'error' ,we will update last_mod_ts to current_timestamp and we will not insert duplicate records.
# MAGIC  
# MAGIC 2. We have user defined function for getting employee lawcode. Instead of using udf can you use vectorized udf ? 

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

common_configs = read_yaml(config_file)

tqr_catalog = common_configs['schema']['tqr_catalog']
trm_catalog = common_configs['schema']['trm_catalog']
trm_worker_catalog = common_configs['schema']['trm_worker_catalog']
trm_tmprodvty_catalog = common_configs['schema']['trm_tmprodvty_catalog']

src_tqr_db = trm_catalog+'.bronze'
trm_worker_db = trm_worker_catalog+'.bronze'
trm_tmprodvty_db = trm_tmprodvty_catalog+'.bronze'
stg_tqr_db = tqr_catalog+'.silver'

#Job variables
job_name = 'ntb_tqr_silver_employee_organization'
trgt_tbl_name = 'employee_organization'

#job_start_ts = datetime.datetime.now()
import pytz
from pytz import timezone
job_start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

print(f'{src_tqr_db=},{stg_tqr_db=},{job_start_ts=}')
spark.sql(f"set src_tqr_db = {src_tqr_db}")
spark.sql(f"set stg_tqr_db = {stg_tqr_db}")
spark.sql(f"set trm_worker_db = {trm_worker_db}")
spark.sql(f"set trm_tmprodvty_db = {trm_tmprodvty_db}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start Job Control

# COMMAND ----------

# DBTITLE 1,Create entry in job log table and get max dt from job control table
control_dt = begin_job_cntl(stg_tqr_db, job_name, job_start_ts)
print(f'{control_dt=}')

# COMMAND ----------

# DBTITLE 1,Get dataload date from config file
configs = read_yaml(config_file_path)
dataload_dt =  configs['schema']['dataload_dt']
print(f'{dataload_dt=}')
lawcode_host = configs['lawcode']['lawcode_host']
print(f'{lawcode_host=}')

# COMMAND ----------

# DBTITLE 1,ETL Logic
try:
    examiner_employee_num_query = f""" 
    select * from
    (select distinct 
        app.examiner_employee_no as employee_no,
        '' as organization_cd,
        '' as original_organization_cd,
        '' as status_ct,
        from_utc_timestamp(current_timestamp(),'America/New_York') as create_ts,
        'etl' as create_user_id,
        from_utc_timestamp(current_timestamp(),'America/New_York') as last_mod_ts,
        'etl' as last_mod_user_id,
        case when trgt.status_ct ='error' then 'U' 
             when trgt.status_ct ='completed' then 'E'--excluded
             else 'I' end as rec_stus_cd
    from {stg_tqr_db}.src_trm_application app
    left outer join
        (select distinct employee_no ,status_ct
        from {stg_tqr_db}.employee_organization
        ) trgt
    ON app.examiner_employee_no=trgt.employee_no 
    where   app.filing_dt > to_date('{dataload_dt}')
    ) where rec_stus_cd != 'E'
    """

    df_examiner_employee_num = spark.sql(examiner_employee_num_query)
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(stg_tqr_db, job_name, job_start_ts,'failed',0,e)
    raise

# COMMAND ----------

import pandas as pd
import requests
import json
import traceback
from concurrent.futures import ThreadPoolExecutor

employee_num = 'employee_no'
schema = df_examiner_employee_num.schema

def fetch_lawcode(employee_no):
    try:
        url = f"{lawcode_host}/{employee_no}"
        response = requests.get(url, verify=False, timeout=10)
        if response.ok:
            data = json.loads(response.text)
            lawcode = str(data['primaryOrganization']['shortName'])
            return (employee_no, lawcode[-3:], 'completed', lawcode)
        else:
            return (employee_no, "404", 'error', "404")
    except Exception:
        return (employee_no, "404", 'error', "404")

def getLawcode(pdf: pd.DataFrame) -> pd.DataFrame:
    employee_nos = pdf[employee_num].tolist()
    results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(fetch_lawcode, employee_nos))
    result_df = pd.DataFrame(results, columns=[employee_num, 'organization_cd', 'status_ct', 'original_organization_cd'])
    pdf = pdf.drop(columns=['organization_cd', 'status_ct', 'original_organization_cd'], errors='ignore')
    merged = pd.merge(pdf, result_df, on=employee_num, how='left')
    return merged

# COMMAND ----------

df_employee_organization = df_examiner_employee_num.groupby("employee_no").applyInPandas(getLawcode, schema=schema)
#df_employee_organization.display()
df_employee_organization.createOrReplaceTempView("employee_organization_temp")
#df_count = df_employee_organization.count()

# COMMAND ----------

# DBTITLE 1,Merge Query
from pyspark.sql.functions import col

try:
    # Write the entire DataFrame to a temp view at once (no batching)
    df_employee_organization.createOrReplaceTempView("employee_organization_temp_batch")
    merge_query = """
    MERGE INTO ${stg_tqr_db}.employee_organization trgt
    USING employee_organization_temp_batch src
    ON trgt.employee_no = src.employee_no
    WHEN MATCHED THEN 
      UPDATE SET
        trgt.organization_cd = src.organization_cd,
        trgt.status_ct = src.status_ct,
        trgt.last_mod_ts = src.last_mod_ts,
        trgt.last_mod_user_id = src.last_mod_user_id
    WHEN NOT MATCHED THEN 
      INSERT (employee_no, organization_cd, status_ct, create_ts, create_user_id, last_mod_ts, last_mod_user_id)
      VALUES(src.employee_no, src.organization_cd, src.status_ct, src.create_ts, src.create_user_id, src.last_mod_ts, src.last_mod_user_id)
    """
    spark.sql(merge_query)
    #end_job_cntl(stg_tqr_db, job_name, job_start_ts,'completed', df_employee_organization.count(),"job completed successfully")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(stg_tqr_db, job_name, job_start_ts,'failed',0,e)
    raise

# COMMAND ----------

try:
    merge_query = """
    MERGE INTO ${stg_tqr_db}.employee_organization trgt USING (
      SELECT
        DISTINCT worker_no AS worker_num,
        substr(
          first(dn_worker_tm_organization_cd) OVER (
            PARTITION BY worker_no
            ORDER BY max(EP.transaction_effective_dt) DESC
          ),
        -3) AS current_lo
      FROM
        ${trm_worker_db}.worker W
        LEFT JOIN ${trm_tmprodvty_db}.production_transaction EP
          ON worker_no = dn_worker_no
          AND delete_in = 'N'
        LEFT JOIN employee_organization_temp temp
          ON temp.employee_no = worker_no
      GROUP BY
        worker_no,
        dn_worker_tm_organization_cd
    ) LO
    ON trgt.employee_no = LO.worker_num
      AND trgt.organization_cd != LO.current_lo
    WHEN MATCHED THEN
      UPDATE SET
        trgt.organization_cd = LO.current_lo,
        trgt.last_mod_ts = CURRENT_TIMESTAMP(),
        trgt.last_mod_user_id = 'etl'
    """
    df_updates = spark.sql(merge_query)
    #end_job_cntl(stg_tqr_db, job_name, job_start_ts,'completed', df_count,"job completed successfully")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(stg_tqr_db, job_name, job_start_ts,'failed',0,e)
    raise

# COMMAND ----------

df_updates.display()

# COMMAND ----------

dbutils.notebook.exit(f"Completed Loading {stg_tqr_db}.{trgt_tbl_name} ")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Unit test cells below
