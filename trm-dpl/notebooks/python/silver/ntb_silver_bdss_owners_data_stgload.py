# Databricks notebook source
# MAGIC %md
# MAGIC ##Query6

# COMMAND ----------

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

from datetime import date

rundate = dbutils.widgets.get("rundate")
if rundate == '':
    #rdate = date.today()
    rdate = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).date()
    #rdate = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).date()- timedelta(days=1) # unit test

else:
    rdate = rundate
    import datetime
    rdate = datetime.datetime.strptime(rundate, '%Y-%m-%d').date() 

print(rdate)
spark.conf.set('conf.rdate', str(rdate))

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
foreign_oracle_catalog = common_configs['schema']['foreign_oracle_catalog']
foreign_oracle_db = common_configs['schema']['src_db_name']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
src_db_name = common_configs['schema']['src_db_name'].upper()

spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.trgt_catalog', trgt_catalog.lower()) 
spark.conf.set('config.dbx_env', dbx_env.lower())

if trgt_catalog.count("_") == 1:
    env = ""
else:
    env = "_"+trgt_catalog.split("_",2)[-1]

print(f'{src_db_name=},{trgt_catalog=}, {data_quality_catalog=},{dbx_env=},{env=}')
from pyspark.sql.functions import col, lit

# COMMAND ----------

job_name = 'ntb_silver_bdss_owners_data_stgload'

start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
print(f'{start_ts=}')
control_dt = begin_job_cntl(f'{data_quality_catalog}',f'{trgt_catalog}.silver',job_name,start_ts)

# COMMAND ----------

df_owners_data_stg = spark.sql(
    f"""
        SELECT
            cast(split(TM_PARTY_ROLE.fk_trademark_gid, ':')[2] AS INTEGER) AS sernum,
            SUBSTR(PARTY_ROLE_SEQUENCE_NO, 4, 1) AS PY_ENT_NUM,
            TRIM(INTERESTED_PARTY_NM) AS NAM,
            TRIM(TO_CHAR(SUBSTR(PARTY_ROLE_SEQUENCE_NO, 1, 2), '00')) AS PARTY_TYPE,
            TRIM(STREET_LINE_1_TX) AS ADDR_1,
            TRIM(STREET_LINE_2_TX) AS ADDR_2,
            TRIM(CITY_NM) AS CITY,
            CASE WHEN TRIM(MAILING_ADDRESS.GEOGRAPHIC_REGION_CD) IS NOT NULL
                THEN TRIM(MAILING_ADDRESS.GEOGRAPHIC_REGION_CD)
                WHEN TRIM(MAILING_ADDRESS.COUNTRY_CD) IS NOT NULL AND LENGTH(TRIM(MAILING_ADDRESS.COUNTRY_CD)) < 3
                THEN TRIM(MAILING_ADDRESS.COUNTRY_CD) || 'X'
            END AS STE_CTRY_CD,
            TRIM(MAILING_ADDRESS.POSTAL_CD) AS ZIP_CD,
            TRIM(TO_CHAR(INTERESTED_PARTY.FK_LEGAL_ENTITY_TYPE_CD, '00')) AS ENTITY_TYPE,
            CASE WHEN LENGTH(TRIM(STREET_LINE_1_TX)) > 40
                THEN 1
                ELSE 0
            END AS PY_FLG_ADDR_1,
            CASE WHEN LENGTH(TRIM(STREET_LINE_2_TX)) > 40
                THEN 1
                ELSE 0
            END AS PY_FLG_ADDR_2,
            CASE WHEN TRIM(PARTY_COMPOSITION_TX) IS NOT NULL
                THEN 1
                ELSE 0
            END AS PY_FLG_CMP_STMT,
            CASE WHEN TRIM(ASSUMED_NM) IS NOT NULL
                THEN 1
                ELSE 0
            END AS PY_FLG_DBA_AKA,
            CASE WHEN TRIM(LEGAL_ENTITY_STATEMENT_TX) IS NOT NULL
                THEN 1
                ELSE 0
            END AS PY_FLG_ENTITY,
            CASE WHEN TRIM(INTERESTED_PARTY_NM) IS NOT NULL
                THEN 1
                ELSE 0
            END AS PY_FLG_NAM_TEXT,
            TRIM(TO_CHAR(SUBSTR(PARTY_ROLE_SEQUENCE_NO, 1, 2), '00')) || '0' || 1 AS PTYPEENUMBER,
            CASE WHEN LENGTH(TRIM(MAILING_ADDRESS.GEOGRAPHIC_REGION_CD)) < 3
                THEN TRIM(MAILING_ADDRESS.GEOGRAPHIC_REGION_CD)
            END AS STATE,
            CASE WHEN LENGTH(TRIM(INTERESTED_PARTY.GEOGRAPHIC_REGION_CD)) < 3
                THEN TRIM(INTERESTED_PARTY.GEOGRAPHIC_REGION_CD)
            END AS CITIZEN_STATE,
            INTERESTED_PARTY.COUNTRY_CD AS CITIZEN_COUNTRY,
            INTERESTED_PARTY.COUNTRY_CD AS CITIZEN_OTHER,
            '' AS CITIZENSHIP,
            '' AS COUNTRY
        FROM {foreign_oracle_catalog}.{foreign_oracle_db}.INTERESTED_PARTY
            INNER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.TM_PARTY_ROLE
            ON INTERESTED_PARTY.INTERESTED_PARTY_GID = TM_PARTY_ROLE.FK_INTERESTED_PARTY_GID
            INNER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.TM_MAILING_ADDR
            ON tm_mailing_addr.FK_TM_PARTY_ROLE_ID = TM_PARTY_ROLE.TM_PARTY_ROLE_ID
            INNER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.MAILING_ADDRESS
            ON tm_mailing_addr.FK_MAILING_ADDRESS_GID = MAILING_ADDRESS.MAILING_ADDRESS_GID
            LEFT JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.INTERESTED_PARTY_ASSUMED_NM
            ON INTERESTED_PARTY.INTERESTED_PARTY_GID = INTERESTED_PARTY_ASSUMED_NM.FK_INTERESTED_PARTY_GID
        WHERE fk_tm_party_role_cd = 'OWNER'
            AND SUBSTR(PARTY_ROLE_SEQUENCE_NO, 4, 1) IS NOT NULL
            AND TM_PARTY_ROLE.fk_trademark_gid IN (
                SELECT DISTINCT concat('Trademark:0:', sernum)
                FROM {trgt_catalog}.silver.tmappl_daily_consolidated_vw
            )
        ORDER BY SUBSTR(PARTY_ROLE_SEQUENCE_NO, 1, 2) DESC
  """)

# COMMAND ----------

try:
    df_owners_data_stg.write.mode("overwrite").format("delta").saveAsTable(f'{trgt_catalog}.silver.bdss_owners_data_daily_stg')
    recs_count = df_owners_data_stg.count()
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'completed',0,recs_count,"job completed successfully")
    dbutils.notebook.exit(f"Completed Loading {recs_count} records into bdss_owners_data_daily_stg Table ")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'failed',0,0,e)
    raise
dbutils.notebook.exit(f"Completed loading bdss_owners_data_daily_stg Table ")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trm_tmngpdb_dev.silver.bdss_owners_data_daily_stg
