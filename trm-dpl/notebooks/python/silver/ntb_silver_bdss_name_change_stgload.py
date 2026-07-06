# Databricks notebook source
# MAGIC %md
# MAGIC ##Query5

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

job_name = 'ntb_silver_bdss_name_change_stgload'

start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
print(f'{start_ts=}')
control_dt = begin_job_cntl(f'{data_quality_catalog}',f'{trgt_catalog}.silver',job_name,start_ts)

# COMMAND ----------

consolidated_vw = (
    spark.read.table(f"{trgt_catalog}.silver.tmappl_daily_consolidated_vw")
        .select(
            col("SERNUM")
        ).distinct()
)
consolidated_vw.createOrReplaceTempView("consolidated_vw")

# COMMAND ----------

df_name_change_stg_chunk1 = spark.sql(
    f"""
    SELECT
        legal_entity_statement_tx AS vt_text, 
        regexp_substr(fk_trademark_gid,'[^:]+$') AS vt_ser_num, 
        'EN' || cast(b.party_role_sequence_no AS VARCHAR(5)) AS vt_text_type,
        'EN' || cast(b.party_role_sequence_no AS VARCHAR(5)) || ltrim(to_char(1, '0000')) || legal_entity_statement_tx AS name_change_text
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.interested_party a 
    INNER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.tm_party_role b 
    ON a.interested_party_gid = b.fk_interested_party_gid
    WHERE legal_entity_statement_tx IS NOT NULL
    AND fk_legal_entity_type_cd IN (98, 99)
    AND TRIM(legal_entity_statement_tx) != ''
    AND regexp_substr(fk_trademark_gid,'[^:]+$') IN (SELECT SERNUM FROM consolidated_vw)

    --DR
    UNION

    SELECT
        RTRIM(IP.INTERESTED_PARTY_NM) AS VT_TEXT,
        regexp_substr(fk_trademark_gid,'[^:]+$') AS vt_ser_num, 
        'DR0000' AS VT_TEXT_TYPE, 
        'DR0000'||ltrim(to_char(1, '0000'))||RTRIM(IP.INTERESTED_PARTY_NM) AS name_change_text
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TM_PARTY_ROLE PR
    INNER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.INTERESTED_PARTY IP
    ON PR.fk_interested_party_gid = IP.interested_party_gid 
    AND PR.FK_TM_PARTY_ROLE_CD = 'DR'
    AND regexp_substr(fk_trademark_gid,'[^:]+$') IN (SELECT SERNUM FROM consolidated_vw)

    --AT
    UNION

    SELECT
        RTRIM(IP.INTERESTED_PARTY_NM) AS VT_TEXT,
        regexp_substr(fk_trademark_gid,'[^:]+$') vt_ser_num, 
        'AT0000' as VT_TEXT_TYPE, 
        'AT0000'||ltrim(to_char(1, '0000'))||RTRIM(IP.INTERESTED_PARTY_NM) AS name_change_text
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TM_PARTY_ROLE PR
    INNER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.INTERESTED_PARTY IP
    ON PR.fk_interested_party_gid = IP.interested_party_gid 
    AND PR.FK_TM_PARTY_ROLE_CD = 'AT'
    AND regexp_substr(fk_trademark_gid,'[^:]+$') IN (SELECT SERNUM FROM consolidated_vw)

    --PN---
    UNION

    SELECT DISTINCT
        substring(TRIM(IP.INTERESTED_PARTY_NM),121) AS VT_TEXT,
        regexp_substr(fk_trademark_gid,'[^:]+$') vt_ser_num,
        ('PN' || cast(PR.party_role_sequence_no AS VARCHAR(5))) VT_TEXT_TYPE,
        ('PN' || cast(PR.party_role_sequence_no AS VARCHAR(5))) || ltrim(to_char(1, '0000')) || substring(RTRIM(IP.INTERESTED_PARTY_NM),121) AS name_change_text
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.interested_party IP
    INNER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.tm_party_role PR
    ON IP.interested_party_gid = PR.fk_interested_party_gid
    WHERE substring(TRIM(IP.INTERESTED_PARTY_NM),120) != ''
    AND interested_party_ct = 'O'
    AND regexp_substr(fk_trademark_gid,'[^:]+$') IN (SELECT SERNUM FROM consolidated_vw)

    --MK
    UNION

    SELECT
        SUBSTR(literal_element_tx, 41) AS VT_TEXT,
        CAST(split(FK_TRADEMARK_GID, ':')[2] AS INTEGER) AS VT_SER_NUM,
        'MK0000' AS VT_TEXT_TYPE, 
        'MK0000' || ltrim(to_char(1, '0000')) || SUBSTR(literal_element_tx, 41) AS name_change_text
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TM_LITERAL
    WHERE LENGTH(literal_element_tx) > 40
    AND regexp_substr(fk_trademark_gid,'[^:]+$') IN (SELECT SERNUM FROM consolidated_vw)

    UNION

    SELECT 
        SUBSTR(standard_character_tx, 41) AS VT_TEXT,
        CAST(split(TRADEMARK_GID, ':')[2] AS INTEGER) AS VT_SER_NUM,
        'MK0000' AS VT_TEXT_TYPE, 
        'MK0000' || ltrim(to_char(1, '0000')) || SUBSTR(standard_character_tx, 41) AS name_change_text
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TRADEMARK
    WHERE LENGTH(standard_character_tx) > 40
    AND regexp_substr(trademark_gid,'[^:]+$') IN (SELECT SERNUM FROM consolidated_vw)
    """
)

# COMMAND ----------

df_name_change_stg_chunk2 = spark.sql(
  f"""
    --EN
    --AI
    SELECT 
      SUBSTR(street_line_1_tx, 41) AS VT_TEXT,
      regexp_substr(fk_trademark_gid,'[^:]+$') AS vt_ser_num,
      ('AI' || cast(PR.party_role_sequence_no AS VARCHAR(5))) AS VT_TEXT_TYPE,
      ('AI' || cast(PR.party_role_sequence_no AS VARCHAR(5))) || ltrim(to_char(1, '0000')) || SUBSTR(street_line_1_tx, 41) AS name_change_text
    FROM  {foreign_oracle_catalog}.{foreign_oracle_db}.TM_PARTY_ROLE PR
    INNER JOIN   {foreign_oracle_catalog}.{foreign_oracle_db}.tm_mailing_addr TMA
    on TM_PARTY_ROLE_ID = FK_TM_PARTY_ROLE_ID 
    INNER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.mailing_address MA
    ON TMA.FK_MAILING_ADDRESS_GID = MA.MAILING_ADDRESS_GID
    WHERE PR.FK_TM_PARTY_ROLE_CD = 'OWNER'
    AND trim(street_line_1_tx) != ''
    AND LENGTH(street_line_1_tx) > 40
    AND regexp_substr(fk_trademark_gid,'[^:]+$') IN (SELECT SERNUM FROM consolidated_vw)

    --AS
    UNION

    SELECT
      SUBSTR(street_line_2_tx, 41) AS VT_TEXT,
      regexp_substr(fk_trademark_gid,'[^:]+$') AS vt_ser_num,
      ('AS' || cast(PR.party_role_sequence_no AS VARCHAR(5))) AS VT_TEXT_TYPE,
      ('AS' || cast(PR.party_role_sequence_no AS VARCHAR(5))) || ltrim(to_char(1, '0000')) || SUBSTR(street_line_2_tx, 41) AS name_change_text
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TM_PARTY_ROLE PR
    INNER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.tm_mailing_addr TMA
    ON TM_PARTY_ROLE_ID = FK_TM_PARTY_ROLE_ID 
    INNER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.mailing_address MA
    ON TMA.FK_MAILING_ADDRESS_GID = MA.MAILING_ADDRESS_GID
    WHERE PR.FK_TM_PARTY_ROLE_CD = 'OWNER'
    AND TRIM(street_line_2_tx) != ''
    AND LENGTH(street_line_2_tx) > 40
    AND regexp_substr(fk_trademark_gid,'[^:]+$') IN (SELECT SERNUM FROM consolidated_vw)

    --DB
    UNION

    SELECT
      RTRIM(IP.assumed_nm) AS VT_TEXT,
      regexp_substr(fk_trademark_gid,'[^:]+$') AS vt_ser_num, 
      'DB' || cast(PR.party_role_sequence_no AS VARCHAR(5)) AS vt_text_type, 
      'DB' || cast(PR.party_role_sequence_no AS VARCHAR(5)) || ltrim(to_char(1, '0000')) || RTRIM(IP.assumed_nm) AS name_change_text
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TM_PARTY_ROLE PR
    INNER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.INTERESTED_PARTY_ASSUMED_NM IP
    ON PR.fk_interested_party_gid = IP.fk_interested_party_gid 
    WHERE TRIM(IP.assumed_nm) != ''
    AND regexp_substr(fk_trademark_gid,'[^:]+$') IN (SELECT SERNUM FROM consolidated_vw)

    --CO
    UNION

    SELECT
      RTRIM(IP.PARTY_COMPOSITION_TX) AS VT_TEXT,
      regexp_substr(fk_trademark_gid,'[^:]+$') AS vt_ser_num, 
      'CO' || cast(PR.party_role_sequence_no AS VARCHAR(5)) AS vt_text_type, 
      'CO' || cast(PR.party_role_sequence_no AS VARCHAR(5)) || ltrim(to_char(1, '0000')) || RTRIM(IP.PARTY_COMPOSITION_TX) AS name_change_text
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TM_PARTY_ROLE PR
    INNER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.INTERESTED_PARTY IP
    ON PR.fk_interested_party_gid = IP.interested_party_gid 
    WHERE TRIM(IP.PARTY_COMPOSITION_TX) != ''
    AND regexp_substr(fk_trademark_gid,'[^:]+$') IN (SELECT SERNUM FROM consolidated_vw)

    --NC
    UNION

    SELECT
      trim(legacy_assignment_tx) AS vt_text,
      regexp_substr(fk_trademark_gid, '[^:]+$') AS vt_ser_num,
      'NC' || substr(CAST(fk_party_role_sequence_no AS VARCHAR(6)),1,2) || '00' AS vt_text_type,
      'NC' || substr(CAST(fk_party_role_sequence_no AS VARCHAR(6)),1,2) || '00' || ltrim(to_char(1, '0000')) || trim(legacy_assignment_tx) AS name_change_text
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.tm_party_role_owner
    WHERE fk_tm_party_role_cd = 'OWNER'
    AND legacy_assignment_tx IS NOT NULL
    AND regexp_substr(fk_trademark_gid,'[^:]+$') IN (SELECT SERNUM FROM consolidated_vw)
  """
)

# COMMAND ----------

# Avoid recomputation
df_name_change_stg_chunk1 = df_name_change_stg_chunk1.persist()
df_name_change_stg_chunk2 = df_name_change_stg_chunk2.persist()

df_name_change_stg = df_name_change_stg_chunk1.union(df_name_change_stg_chunk2)

# COMMAND ----------

try:
    df_name_change_stg.repartition(24).write.mode("overwrite").format("delta").saveAsTable(f'{trgt_catalog}.silver.bdss_name_change_daily_stg')
    recs_count = df_name_change_stg.count()
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'completed',0,recs_count,"job completed successfully")
    dbutils.notebook.exit(f"Completed Loading {recs_count} records into bdss_name_change_daily_stg Table ")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'failed',0,0,e)
    raise
dbutils.notebook.exit(f"Completed loading bdss_name_change_daily_stg Table ")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trm_tmngpdb_dev.silver.bdss_name_change_daily_stg
