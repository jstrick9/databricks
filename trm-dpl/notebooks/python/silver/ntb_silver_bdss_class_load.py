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

# DBTITLE 1,define rundate
from datetime import date

rundate = dbutils.widgets.get("rundate")
if rundate == '':
    #rdate = date.today()
    rdate = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).date()
    #rdate = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).date()- timedelta(days=1) # unit test
    rday = rdate.strftime("%A")
else:
    rdate = rundate
    import datetime
    rdate = datetime.datetime.strptime(rundate, '%Y-%m-%d').date() 
    rday = rdate.strftime("%A")
print(rday)
spark.conf.set('conf.rdate', str(rdate) )

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
foreign_oracle_catalog = common_configs['schema']['foreign_oracle_catalog']
foreign_oracle_db = common_configs['schema']['src_db_name']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
src_db_name = common_configs['schema']['src_db_name'].upper()
trm_scope = common_configs['secrets']['trm_scope']
ptas_scope = common_configs['secrets']['ptas_scope']

spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.trgt_catalog', trgt_catalog.lower()) 
spark.conf.set('config.trm_scope', trm_scope.lower()) 
spark.conf.set('config.ptas_scope', ptas_scope.lower())
spark.conf.set('config.dbx_env', dbx_env.lower())

if trgt_catalog.count("_") == 1:
    env = ""
else:
    env = "_"+trgt_catalog.split("_",2)[-1]

print(f'{src_db_name=},{trgt_catalog=}, {data_quality_catalog=},{trm_scope=},{ptas_scope=},{dbx_env=},{env=}')
from pyspark.sql.functions import col, lit

# COMMAND ----------

job_name = 'ntb_silver_bdss_class_load'

#control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,job_start_ts)
start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
print(f'{start_ts=}')
control_dt = begin_job_cntl(f'{data_quality_catalog}',f'{trgt_catalog}.silver',job_name,start_ts)

# COMMAND ----------

# concat_ws('', a.first_use_anywhere_year_no,a.first_use_anywhere_month_no,a.first_use_anywhere_day_no ) AS dt_1_use,
# concat_ws('', a.first_use_in_commerce_year_no,a.first_use_in_commerce_month_no,a.first_use_in_commerce_day_no ) AS dt_1_use_comm,

df_bdss_class = spark.sql(
  f"""
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS cl_ser_num,
      cl_cls_intl_ct,
      cl_cls_us_ct,
      cls_intl,
      cls_us,
      cl_stat AS cls_stat,
      dt_stat,
      dt_1_use,
      dt_1_use_comm,
      prime_cls,
      CURRENT_TIMESTAMP() AS create_ts,
      'etl' AS create_user_id
    FROM
    (
      SELECT a.fk_trademark_gid,
        1 cl_cls_intl_ct,
        (
          --- how many us classes does it map to
          SELECT COUNT(*)
          FROM {foreign_oracle_catalog}.{foreign_oracle_db}.tm_class_reference c
          WHERE c.fk_class_id = a.fk_class_id AND c.fk_trademark_gid = a.fk_trademark_gid
        ) AS cl_cls_us_ct,
        b.class_no AS cls_intl,
        (
          SELECT CONCAT_WS('', SORT_ARRAY(COLLECT_LIST(e.class_no), true))
          FROM {foreign_oracle_catalog}.{foreign_oracle_db}.tm_class_reference d
          INNER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.stnd_class e
          ON d.fk_referenced_class_id = e.class_id 
          WHERE d.fk_class_id = a.fk_class_id AND d.fk_trademark_gid = a.fk_trademark_gid
        ) AS cls_us,
        a.fk_tm_class_status_cd AS cl_stat,
        date_format(status_dt, 'yyyyMMdd') AS dt_stat,
        (
          CONCAT(
            '' || a.first_use_anywhere_year_no || '',
            CASE WHEN a.first_use_anywhere_month_no >= 10
              THEN '' || a.first_use_anywhere_month_no || ''
              ELSE '0' || a.first_use_anywhere_month_no 
            END
          ) ||  CASE WHEN a.first_use_anywhere_day_no >= 10
                  THEN '' || a.first_use_anywhere_day_no || ''
                  ELSE '0' || a.first_use_anywhere_day_no
                END
        ) AS dt_1_use,
        (
          CONCAT(
          '' || a.first_use_in_commerce_year_no || '',
          CASE WHEN a.first_use_in_commerce_month_no >= 10
            THEN '' ||  a.first_use_in_commerce_month_no || ''
            ELSE '0' || a.first_use_in_commerce_month_no 
          END
          ) ||  CASE WHEN a.first_use_in_commerce_day_no >= 10
                THEN '' || a.first_use_in_commerce_day_no || ''
                ELSE '0' || a.first_use_in_commerce_day_no
                END
        ) AS dt_1_use_comm,
        b.class_no AS prime_cls 
      FROM {foreign_oracle_catalog}.{foreign_oracle_db}.tm_class a
      INNER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.stnd_class b ON a.fk_class_id = b.class_id
      WHERE b.fk_class_schedule_cd = 'INTL' 

      UNION ALL

      SELECT a.fk_trademark_gid,
        (
          SELECT COUNT(*)
          FROM {foreign_oracle_catalog}.{foreign_oracle_db}.tm_class_reference c
          WHERE c.fk_class_id = a.fk_class_id AND c.fk_trademark_gid = a.fk_trademark_gid
        ) AS cl_cls_intl_ct,
        1 cl_cls_us_ct,
          (
          SELECT CONCAT_WS('', SORT_ARRAY(COLLECT_LIST(e.class_no), true))
          FROM {foreign_oracle_catalog}.{foreign_oracle_db}.tm_class_reference d
          INNER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.stnd_class e
          ON d.fk_referenced_class_id = e.class_id 
          WHERE d.fk_class_id = a.fk_class_id AND d.fk_trademark_gid = a.fk_trademark_gid
        ) AS cls_intl,
        b.class_no AS cls_us,
        a.fk_tm_class_status_cd AS cl_stat,
        date_format(status_dt, 'yyyyMMdd') AS dt_stat,
        (
          CONCAT(
            '' || a.first_use_anywhere_year_no || '',
            CASE WHEN a.first_use_anywhere_month_no >= 10
              THEN '' || a.first_use_anywhere_month_no || ''
              ELSE '0' || a.first_use_anywhere_month_no 
            END
          ) ||  CASE WHEN a.first_use_anywhere_day_no >= 10
                  THEN '' || a.first_use_anywhere_day_no || ''
                  ELSE '0' || a.first_use_anywhere_day_no
                END
        ) AS dt_1_use,
        (
          CONCAT(
          '' || a.first_use_in_commerce_year_no || '',
          CASE WHEN a.first_use_in_commerce_month_no >= 10
            THEN '' ||  a.first_use_in_commerce_month_no || ''
            ELSE '0' || a.first_use_in_commerce_month_no 
          END
          ) ||  CASE WHEN a.first_use_in_commerce_day_no >= 10
                THEN '' || a.first_use_in_commerce_day_no || ''
                ELSE '0' || a.first_use_in_commerce_day_no
                END
        ) AS dt_1_use_comm,
        b.class_no AS prime_cls
      FROM {foreign_oracle_catalog}.{foreign_oracle_db}.tm_class a
      INNER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.stnd_class b
      ON a.fk_class_id = b.class_id
      WHERE b.fk_class_schedule_cd IN ('US', 'CMM', 'CRT')

      UNION ALL

      SELECT a.fk_trademark_gid,
        (
          SELECT COUNT(DISTINCT class_no) + 3  /*** for 200, A and B classes**/
          FROM {foreign_oracle_catalog}.{foreign_oracle_db}.stnd_class e  
          WHERE fk_class_schedule_cd IN ('INTL') AND class_no > '000'
          AND current_date() BETWEEN begin_effective_dt AND end_effective_dt
        ) AS cl_cls_intl_ct,
        (
          SELECT COUNT(DISTINCT class_no)
          FROM {foreign_oracle_catalog}.{foreign_oracle_db}.stnd_class e 
          WHERE fk_class_schedule_cd IN ('US', 'CRT', 'CMM') AND class_no > '000'
        ) AS cl_cls_us_ct,
        (
          SELECT CONCAT_WS('', SORT_ARRAY(COLLECT_LIST(DISTINCT e.class_no), true)) || '200A B '
          FROM {foreign_oracle_catalog}.{foreign_oracle_db}.stnd_class e
          WHERE fk_class_schedule_cd IN ('INTL') AND class_no > '000'
          AND CURRENT_DATE() BETWEEN begin_effective_dt AND end_effective_dt
        ) AS cls_intl,
        (
          SELECT CONCAT_WS('', SORT_ARRAY(COLLECT_LIST(DISTINCT e.class_no), true))
          FROM {foreign_oracle_catalog}.{foreign_oracle_db}.stnd_class e 
          WHERE fk_class_schedule_cd IN ('US', 'CRT', 'CMM') AND class_no > '000'
        ) AS cls_us,
        a.fk_tm_class_status_cd AS cl_stat,
        date_format(status_dt, 'yyyyMMdd') AS dt_stat,
        (
          CONCAT(
            '' || a.first_use_anywhere_year_no || '',
            CASE WHEN a.first_use_anywhere_month_no >= 10
              THEN '' || a.first_use_anywhere_month_no || ''
              ELSE '0' || a.first_use_anywhere_month_no 
            END
          ) ||  CASE WHEN a.first_use_anywhere_day_no >= 10
                  THEN '' || a.first_use_anywhere_day_no || ''
                  ELSE '0' || a.first_use_anywhere_day_no
                END
        ) AS dt_1_use,
        (
          CONCAT(
          '' || a.first_use_in_commerce_year_no || '',
          CASE WHEN a.first_use_in_commerce_month_no >= 10
            THEN '' ||  a.first_use_in_commerce_month_no || ''
            ELSE '0' || a.first_use_in_commerce_month_no 
          END
          ) ||  CASE WHEN a.first_use_in_commerce_day_no >= 10
                THEN '' || a.first_use_in_commerce_day_no || ''
                ELSE '0' || a.first_use_in_commerce_day_no
                END
        ) AS dt_1_use_comm,
        b.class_no AS prime_cls
      FROM {foreign_oracle_catalog}.{foreign_oracle_db}.tm_class a
      INNER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.stnd_class b
      ON a.fk_class_id = b.class_id
      WHERE b.fk_class_schedule_cd = 'NRN' 
    ) d
  """
)

# COMMAND ----------

try:
    df_bdss_class.write.mode("overwrite").format("delta").insertInto(f'{trgt_catalog}.silver.BDSS_CLASS')
    recs_count = df_bdss_class.count()
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'completed',0,recs_count,"job completed successfully")
    #dbutils.fs.rm(CHK_POINT_DIR,True)
    dbutils.notebook.exit(f"Completed Loading {recs_count} records into BDSS_CLASS Table ")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'failed',0,0,e)
    #dbutils.fs.rm(CHK_POINT_DIR,True)
    raise
dbutils.notebook.exit(f"Completed loading BDSS_CLASS Table ")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Unit test cells below

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from trm_tmngpdb_dev.silver.BDSS_CLASS

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trm_tmngpdb_dev.silver.BDSS_CLASS
