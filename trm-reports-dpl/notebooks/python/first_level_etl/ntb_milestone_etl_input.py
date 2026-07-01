# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will gives us the Input for Milestone ETL which will be used by successor notebook which has ETL code for Milestone.

# COMMAND ----------

# DBTITLE 1,Setting environment
# dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

# DBTITLE 1,config file widget
# dbx_env = dbutils.widgets.get("dbx_env").rstrip()
# config_file_name = "trmreports-conf.yaml"
# config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Imports
# MAGIC %run ./ntb_comm_imports_altx $config_file = config_file

# COMMAND ----------

# DBTITLE 1,Setting Config Param
common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
tmngpdb_catalog = common_configs['schema']['tmngpdb_src_catalog']
tmintltm_catalog = common_configs['schema']['tmintltm_src_catalog']
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
data_layer = "bronze"
schema_silver = "silver"
table_silver= "milestone"

# COMMAND ----------

ip1_query = f'''SELECT   CAST(split(AM.TRADEMARK_GID,':')[2] AS INTEGER) AS AM_SER_NUM ,
  TM.AM_DT_FIL,
  RI.NOTIFICATION_DT AS RI_NOTIF_DT,
  RI.IB_PUBLICATION_DT AS RI_IB_PUB_DT,
  TM.AM_DT_ABAN AS AM_DT_ABAN,
  TM.AM_DT_CNCL AS AM_DT_CNCL,
  TM.AM_DT_AMND_REG AS AM_DT_AMND_REG,
  TM.AM_DT_PUB AS AM_DT_PUB,
  TM.AM_DT_PUB_12C AS AM_DT_PUB_12C,
  TM.AM_DT_REG AS AM_DT_REG,
  TM.AM_DT_RNWL AS AM_DT_RNWL,
  BUS.AM_DT_SUSP_CHECK AS AM_DT_SUSP_CHECK,
  (CASE WHEN TF.CURRENT_IN ='Y' THEN 1 ELSE 0 END)  AS AM_FLG_66A_CUR,
  (CASE WHEN TF.FILED_IN ='Y' THEN 1 ELSE 0 END)  AS AM_FLG_66A_FIL,
  TM.IU_DT_NOA AS IU_DT_NOA,
  TOA.FIRST_EA_ACTION_COUNTED_DT AS AM_1_ACTN_CT_DT,
  CLS.AM_CLS_CT_ACTV AS AM_CLS_CT_ACTV,
  (CASE WHEN TI.ITU_CASE_PUBD_FOR_OPSTN_IN ='Y' THEN 1 ELSE 0 END) AS AM_FLG_ITU_PUBO,
  AM.LAST_MOD_TS AS LAST_MODIFIED_DATE
   FROM {tmngpdb_catalog}.{data_layer}.TRADEMARK AM
  LEFT JOIN ( SELECT fk_trademark_gid
,COLLECT_SET(AM_DT_FIL)[0] AS AM_DT_FIL
,COLLECT_SET(AM_DT_REG)[0] AS AM_DT_REG
,COLLECT_SET(AM_DT_PUB)[0] AS AM_DT_PUB
,COLLECT_SET(AM_DT_CNCL)[0] AS AM_DT_CNCL
,COLLECT_SET(AM_DT_ABAN)[0] AS AM_DT_ABAN
,COLLECT_SET(AM_DT_PUB_12C)[0] AS AM_DT_PUB_12C
,COLLECT_SET(AM_DT_RNWL)[0] AS AM_DT_RNWL
,COLLECT_SET(IU_DT_NOA)[0] AS IU_DT_NOA
,COLLECT_SET(AM_DT_AMND_REG)[0] AS AM_DT_AMND_REG
,COLLECT_SET(AM_TRMNT_DT)[0] AS AM_TRMNT_DT
FROM(
select  fk_trademark_gid, 
CASE WHEN   FK_TM_MILESTONE_CD = 'FILED' THEN  MILESTONE_DT END AS  AM_DT_FIL,
CASE WHEN   FK_TM_MILESTONE_CD = 'REG' THEN  MILESTONE_DT END AS  AM_DT_REG,
CASE WHEN   FK_TM_MILESTONE_CD = 'PUB' THEN  MILESTONE_DT END AS  AM_DT_PUB,
CASE WHEN   FK_TM_MILESTONE_CD = 'CNCL' THEN  MILESTONE_DT END AS  AM_DT_CNCL,
CASE WHEN   FK_TM_MILESTONE_CD = 'ABAND' THEN  MILESTONE_DT END AS  AM_DT_ABAN,
CASE WHEN   FK_TM_MILESTONE_CD = 'PUB12' THEN  MILESTONE_DT END AS  AM_DT_PUB_12C,
CASE WHEN   FK_TM_MILESTONE_CD = 'RENEW' THEN  MILESTONE_DT END AS  AM_DT_RNWL,
CASE WHEN   FK_TM_MILESTONE_CD = 'NOA' THEN  MILESTONE_DT END AS  IU_DT_NOA,
CASE WHEN   FK_TM_MILESTONE_CD = 'AMNDR' THEN  MILESTONE_DT END AS  AM_DT_AMND_REG,
CASE WHEN   FK_TM_MILESTONE_CD = 'TRMNT' THEN  MILESTONE_DT END AS  AM_TRMNT_DT
from {tmngpdb_catalog}.{data_layer}.TM_MILESTONE )ML_AM
 group by fk_trademark_gid
               ) TM
  ON AM.TRADEMARK_GID = TM.FK_TRADEMARK_GID
 LEFT JOIN (SELECT *  FROM {tmngpdb_catalog}.{data_layer}.TM_FILING_BASIS WHERE FK_FILING_BASIS_CD='66(a)') TF
 ON AM.TRADEMARK_GID = TF.FK_TRADEMARK_GID
 LEFT JOIN {tmngpdb_catalog}.{data_layer}.TM_ITU TI
 ON AM.TRADEMARK_GID = TI.FK_TRADEMARK_GID
 LEFT JOIN {tmintltm_catalog}.{data_layer}.INTERNATIONAL_REG_TM RI 
ON AM.TRADEMARK_GID = RI.CFK_TRADEMARK_GID
LEFT JOIN( 
  SELECT CFK_OBJECT_GID, max(EFFECTIVE_TS) as AM_DT_SUSP_CHECK
 FROM  {tmngpdb_catalog}.{data_layer}.BUSINESS_EVENT BE
 INNER JOIN  (SELECT BUSINESS_EVENT_REASON_CD,BUSINESS_EVENT_REASON_ID 
 from {tmngpdb_catalog}.{data_layer}.STND_BUSINESS_EVENT_REASON
 WHERE BUSINESS_EVENT_REASON_CD IN ('RCSCS','RCCKS')
 )BR
 ON BE.FK_BUSINESS_EVENT_REASON_ID = BR.BUSINESS_EVENT_REASON_ID
 GROUP BY CFK_OBJECT_GID)BUS
  ON AM.TRADEMARK_GID = BUS.CFK_OBJECT_GID
  LEFT JOIN {tmngpdb_catalog}.{data_layer}.TM_OFFICE_ACTIONS TOA
  ON AM.TRADEMARK_GID = TOA.FK_TRADEMARK_GID
  LEFT JOIN (select FK_TRADEMARK_GID,COUNT(DISTINCT FK_CLASS_ID) AS AM_CLS_CT_ACTV
 from {tmngpdb_catalog}.{data_layer}.TM_CLASS
where FK_TM_CLASS_STATUS_CD='6'
GROUP BY FK_TRADEMARK_GID)CLS
 ON AM.TRADEMARK_GID = CLS.FK_TRADEMARK_GID'''

ip1_df = spark.sql(ip1_query)

#ip1_df.show()


# COMMAND ----------

ip2_query = f'''SELECT RI.NOTIFICATION_DT AS RI_NOTIF_DT
   ,AM.FILING_DT AS AM_DT_FIL
   ,FIRST_USE_ANYWHERE_YEAR_NO||TRIM(lpad(FIRST_USE_ANYWHERE_MONTH_NO,2,'0'))||TRIM(lpad(FIRST_USE_ANYWHERE_DAY_NO,2,'0')) AS CL_DT_1_USE
   ,FIRST_USE_IN_COMMERCE_YEAR_NO||TRIM(lpad(FIRST_USE_IN_COMMERCE_MONTH_NO,2,'0') )||TRIM(lpad(FIRST_USE_IN_COMMERCE_DAY_NO,2,'0') )AS CL_DT_1_USE_COMM 
   ,CAST(split(CL.FK_TRADEMARK_GID,':')[2] AS INTEGER) as CL_SER_NUM
   ,CAST(split(DVC.FK_TRADEMARK_GID,':')[2] AS INTEGER) AS DV_PRNT_SER_NUM
   ,DVC.TM_DIVISIONAL_STATUS_DT AS DV_DT_PRCS_CMPLT
   ,DVC.FK_TM_DIVISIONAL_STATUS_CD AS DV_STAT
   ,CAST(split(DVC1.FK_CHILD_TRADEMARK_GID,':')[2] AS INTEGER) AS DV_CHLD_SER_NUM1
   ,DVC1.TM_DIVISIONAL_STATUS_DT AS DV_DT_PRCS_CMPLT1
   ,DVC1.FK_TM_DIVISIONAL_STATUS_CD AS DV_STAT1
   ,DVC1.LAST_MOD_TS AS LAST_MODIFIED_DATE
   ,DVC.UNIT_RECEIVED_DT AS DV_DT_CHLD_RQST
   FROM {tmngpdb_catalog}.{data_layer}.TM_CLASS CL 
   RIGHT JOIN {tmngpdb_catalog}.{data_layer}.TRADEMARK AM
   ON AM.TRADEMARK_GID = CL.FK_TRADEMARK_GID
   LEFT JOIN {tmngpdb_catalog}.{data_layer}.TM_DIVISIONAL_CHILD DVC
   ON AM.TRADEMARK_GID =  DVC.FK_CHILD_TRADEMARK_GID
   LEFT JOIN {tmngpdb_catalog}.{data_layer}.TM_DIVISIONAL_CHILD DVC1
   ON AM.TRADEMARK_GID = DVC1.FK_TRADEMARK_GID
   LEFT JOIN {tmintltm_catalog}.{data_layer}.INTERNATIONAL_REG_TM RI 
   ON AM.TRADEMARK_GID = RI.CFK_TRADEMARK_GID'''

ip2_df = spark.sql(ip2_query)

#ip2_df.show()


# COMMAND ----------

ip3_query = f'''SELECT 
CREATE_TS AS LAST_MODIFIED_DATE,
1 AS VT_ENT_NUM,
CAST(split(TMR.FK_PARENT_TRADEMARK_GID, ':')[2] AS INTEGER) AS VT_SER_NUM,
CAST(split(TMR.FK_RELATED_TRADEMARK_GID, ':')[2] AS INTEGER) AS VT_TEXT,
'TNSF00' AS VT_TEXT_TYPE
from {tmngpdb_catalog}.{data_layer}.TM_RELATIONSHIP TMR
WHERE TMR.FK_RELATIONSHIP_TYPE_CD = \'TNSF\''''

ip3_df = spark.sql(ip3_query)

#ip3_df.show()


# COMMAND ----------

ip4_query = f'''SELECT AM_SER_NUM, AM_DT_FIL, RI_NOTIF_DT, CM_ENT_CD, CM_ENT_DT, CM_ENT_NUM, CM_ENT_TYPE FROM
(SELECT CAST(split(TM.TRADEMARK_GID, ':')[2] AS INTEGER) AS AM_SER_NUM, 
TM.FILING_DT AS AM_DT_FIL,
IRT.NOTIFICATION_DT AS RI_NOTIF_DT
FROM {tmngpdb_catalog}.{data_layer}.TRADEMARK TM
LEFT JOIN {tmintltm_catalog}.{data_layer}.INTERNATIONAL_REG_TM IRT
ON TM.TRADEMARK_GID = IRT.CFK_TRADEMARK_GID) AM
INNER JOIN
(
SELECT to_date(BE.EFFECTIVE_TS) AS CM_ENT_DT,
BER.LEGACY_CM_ENT_CD AS CM_ENT_CD,
CAST(split(BE.CFK_OBJECT_GID, ':')[2] AS INTEGER) AS CM_SER_NUM,
BE.ORDER_NO AS CM_ENT_NUM,
BER.LEGACY_CM_ENT_TYPE_CD AS CM_ENT_TYPE
FROM {tmngpdb_catalog}.{data_layer}.BUSINESS_EVENT BE
LEFT JOIN {tmngpdb_catalog}.{data_layer}.STND_BUSINESS_EVENT_REASON BER
ON BE.FK_BUSINESS_EVENT_REASON_ID = BER.BUSINESS_EVENT_REASON_ID
Where (BER.LEGACY_CM_ENT_CD Like 'EXT_')
)CM
ON AM.AM_SER_NUM = CM.CM_SER_NUM'''

ip4_df = spark.sql(ip4_query)

#ip4_df.show()


# COMMAND ----------

ip5_query = f'''SELECT AM_SER_NUM, AM_DT_FIL, RI_NOTIF_DT, CM_ENT_CD, CM_ENT_DT, CM_ENT_NUM FROM
(SELECT CAST(SPLIT(TM.TRADEMARK_GID, ':')[2] AS INTEGER) AS AM_SER_NUM, 
TM.FILING_DT AS AM_DT_FIL,
IRT.NOTIFICATION_DT AS RI_NOTIF_DT
FROM {tmngpdb_catalog}.{data_layer}.TRADEMARK TM
LEFT JOIN {tmintltm_catalog}.{data_layer}.INTERNATIONAL_REG_TM IRT
ON TM.TRADEMARK_GID = IRT.CFK_TRADEMARK_GID) AM
LEFT JOIN
(
SELECT TO_DATE(BE.EFFECTIVE_TS) AS CM_ENT_DT,
BER.LEGACY_CM_ENT_CD AS CM_ENT_CD,
CAST(SPLIT(BE.CFK_OBJECT_GID, ':')[2] AS INTEGER) AS CM_SER_NUM,
BE.ORDER_NO AS CM_ENT_NUM
FROM {tmngpdb_catalog}.{data_layer}.BUSINESS_EVENT BE
LEFT JOIN {tmngpdb_catalog}.{data_layer}.STND_BUSINESS_EVENT_REASON BER
ON BE.FK_BUSINESS_EVENT_REASON_ID = BER.BUSINESS_EVENT_REASON_ID
)CM
ON AM.AM_SER_NUM = CM.CM_SER_NUM'''

ip5_df = spark.sql(ip5_query)

#ip5_df.show()


# COMMAND ----------

ip6_query = f'''SELECT CAST(SPLIT(AM.AM_SER_NUM, ':')[2] AS INTEGER) AS AM_SER_NUM,
AM_DT_FIL,
RI_NOTIF_DT,
CM_ENT_CD,
CM_ENT_DT,
CM_ENT_NUM,
CM_ENT_TYPE
FROM
(
SELECT TM.TRADEMARK_GID AS AM_SER_NUM, 
TM.FILING_DT AS AM_DT_FIL,
IRT.NOTIFICATION_DT AS RI_NOTIF_DT
FROM {tmngpdb_catalog}.{data_layer}.TRADEMARK TM
LEFT JOIN {tmintltm_catalog}.{data_layer}.INTERNATIONAL_REG_TM IRT
ON (TM.TRADEMARK_GID = IRT.CFK_TRADEMARK_GID)
) AM

LEFT JOIN

(
SELECT TO_DATE(BE.EFFECTIVE_TS) AS CM_ENT_DT,
BER.LEGACY_CM_ENT_CD AS CM_ENT_CD,
BE.CFK_OBJECT_GID AS CM_SER_NUM,
BE.ORDER_NO AS CM_ENT_NUM,
BER.LEGACY_CM_ENT_TYPE_CD AS CM_ENT_TYPE
FROM {tmngpdb_catalog}.{data_layer}.BUSINESS_EVENT BE
LEFT JOIN {tmngpdb_catalog}.{data_layer}.STND_BUSINESS_EVENT_REASON BER
ON BE.FK_BUSINESS_EVENT_REASON_ID = BER.BUSINESS_EVENT_REASON_ID
WHERE BE.FK_OBJECT_TYPE_CD = 'Trademark' AND BER.LEGACY_CM_ENT_CD NOT IN ('MIG0','WEBE','XSCE','XSSE') AND BER.LEGACY_CM_ENT_CD IS NOT NULL
) CM
ON AM.AM_SER_NUM = CM.CM_SER_NUM'''

ip6_df = spark.sql(ip6_query)

#ip6_df.show()


# COMMAND ----------

ip7_query = f'''SELECT CAST(split(AM.AM_SER_NUM, ':')[2] AS INTEGER) AS AM_SER_NUM,
AM.AM_DT_FIL,
CM.CM_ENT_CD,
CM.CM_ENT_DT,
CM.CM_ENT_NUM,
AM.AM_DT_ABAN,
IU.IU_DT_NOA

-- AM
FROM
(
select TM.TRADEMARK_GID AS AM_SER_NUM,
TM.FILING_DT AS AM_DT_FIL,
TMM.MILESTONE_DT AS AM_DT_ABAN
from
{tmngpdb_catalog}.{data_layer}.TRADEMARK TM
left join 
(select distinct TM_MILESTONE.FK_TRADEMARK_GID, TM_MILESTONE.MILESTONE_DT from {tmngpdb_catalog}.{data_layer}.TM_MILESTONE
left join {tmngpdb_catalog}.{data_layer}.STND_TM_MILESTONE STMM
on TM_MILESTONE.fk_tm_milestone_cd = stmm.tm_milestone_cd
where STMM.TM_MILESTONE_CD = 'ABAND') TMM
on tm.trademark_gid = tmm.fk_trademark_gid
) AM

LEFT JOIN 

-- CM
(
SELECT to_date(BE.EFFECTIVE_TS) AS CM_ENT_DT,
BER.LEGACY_CM_ENT_CD AS CM_ENT_CD,
BE.CFK_OBJECT_GID AS CM_SER_NUM,
BE.ORDER_NO AS CM_ENT_NUM
FROM {tmngpdb_catalog}.{data_layer}.BUSINESS_EVENT BE
LEFT JOIN {tmngpdb_catalog}.{data_layer}.STND_BUSINESS_EVENT_REASON BER
ON BE.FK_BUSINESS_EVENT_REASON_ID = BER.BUSINESS_EVENT_REASON_ID
WHERE BE.FK_OBJECT_TYPE_CD = 'Trademark' AND BER.LEGACY_CM_ENT_CD NOT IN ('MIG0','WEBE','XSCE','XSSE') AND BER.LEGACY_CM_ENT_CD IS NOT NULL
) CM
ON (AM.AM_SER_NUM = CM.CM_SER_NUM)

LEFT JOIN

-- IU
(
select ITU.FK_TRADEMARK_GID AS IU_SER_NUM,
tmm.milestone_dt AS IU_DT_NOA
from {tmngpdb_catalog}.{data_layer}.TM_ITU ITU
left join {tmngpdb_catalog}.{data_layer}.tm_milestone tmm
on itu.fk_trademark_gid = tmm.fk_trademark_gid
left join {tmngpdb_catalog}.{data_layer}.stnd_tm_milestone stmm
on tmm.fk_tm_milestone_cd = stmm.tm_milestone_cd
where stmm.tm_milestone_cd = 'NOA'
) IU

ON (AM.AM_SER_NUM = IU.IU_SER_NUM)'''

ip7_df = spark.sql(ip7_query)

#ip7_df.show()


# COMMAND ----------

ip8_query = f'''SELECT
PR.FK_TM_PARTY_ROLE_CD||'0000' as VT_TEXT_TYPE, 
RTRIM(IP.INTERESTED_PARTY_NM) as VT_TEXT,
CAST(split(PR.FK_TRADEMARK_GID, ':')[2] AS INTEGER) AS VT_SER_NUM
from {tmngpdb_catalog}.{data_layer}.TM_PARTY_ROLE PR
INNER JOIN
{tmngpdb_catalog}.{data_layer}.INTERESTED_PARTY IP
ON PR.fk_interested_party_gid = IP.interested_party_gid 
WHERE PR.FK_TM_PARTY_ROLE_CD='DR' OR PR.FK_TM_PARTY_ROLE_CD=\'AT\''''
ip8_df = spark.sql(ip8_query)

#ip8_df.show()


# COMMAND ----------

ip9_query = f'''SELECT to_date(BE.EFFECTIVE_TS) AS CM_ENT_DT,
BER.LEGACY_CM_ENT_CD AS CM_ENT_CD,
CAST(split(BE.CFK_OBJECT_GID, ':')[2] AS INTEGER) AS CM_SER_NUM
FROM {tmngpdb_catalog}.{data_layer}.BUSINESS_EVENT BE
LEFT JOIN {tmngpdb_catalog}.{data_layer}.STND_BUSINESS_EVENT_REASON BER
ON BE.FK_BUSINESS_EVENT_REASON_ID = BER.BUSINESS_EVENT_REASON_ID
WHERE BER.LEGACY_CM_ENT_CD IN ('ARAA', 'WOAG')'''

ip9_df = spark.sql(ip9_query)

#ip9_df.show()


# COMMAND ----------

ip10_query = f'''SELECT AM_SER_NUM, AM_DT_FIL, RI_NOTIF_DT, CM_ENT_CD, CM_ENT_DT, CM_ENT_NUM, CM_ENT_TYPE FROM
(SELECT CAST(SPLIT(TM.TRADEMARK_GID, ':')[2] AS INTEGER) AS AM_SER_NUM, 
TM.FILING_DT AS AM_DT_FIL,
IRT.NOTIFICATION_DT AS RI_NOTIF_DT
FROM {tmngpdb_catalog}.{data_layer}.TRADEMARK TM
LEFT JOIN {tmintltm_catalog}.{data_layer}.INTERNATIONAL_REG_TM IRT
ON TM.TRADEMARK_GID = IRT.CFK_TRADEMARK_GID) AM
INNER JOIN
(
SELECT TO_DATE(BE.EFFECTIVE_TS) AS CM_ENT_DT,
BER.LEGACY_CM_ENT_CD AS CM_ENT_CD,
CAST(SPLIT(BE.CFK_OBJECT_GID, ':')[2] AS INTEGER) AS CM_SER_NUM,
BE.ORDER_NO AS CM_ENT_NUM,
BER.LEGACY_CM_ENT_TYPE_CD AS CM_ENT_TYPE
FROM {tmngpdb_catalog}.{data_layer}.BUSINESS_EVENT BE
LEFT JOIN {tmngpdb_catalog}.{data_layer}.STND_BUSINESS_EVENT_REASON BER
ON BE.FK_BUSINESS_EVENT_REASON_ID = BER.BUSINESS_EVENT_REASON_ID
Where (BER.LEGACY_CM_ENT_CD Like 'CNSL') Or (BER.LEGACY_CM_ENT_CD Like 'GNSL')
)CM
ON AM.AM_SER_NUM = CM.CM_SER_NUM'''
ip10_df = spark.sql(ip10_query)

#ip10_df.show()


# COMMAND ----------

ip11_query = f'''SELECT CAST(SPLIT(AM.AM_SER_NUM, ':')[2] AS INTEGER) AS AM_SER_NUM,
AM_DT_FIL,
RI_NOTIF_DT,
CM_ENT_CD,
CM_ENT_DT,
CM_ENT_NUM
FROM
(
SELECT TM.TRADEMARK_GID AS AM_SER_NUM, 
TM.FILING_DT AS AM_DT_FIL,
IRT.NOTIFICATION_DT AS RI_NOTIF_DT
FROM {tmngpdb_catalog}.{data_layer}.TRADEMARK TM
LEFT JOIN {tmintltm_catalog}.{data_layer}.INTERNATIONAL_REG_TM IRT
ON (TM.TRADEMARK_GID = IRT.CFK_TRADEMARK_GID)
) AM

LEFT JOIN

(
SELECT TO_DATE(BE.EFFECTIVE_TS) AS CM_ENT_DT,
BER.LEGACY_CM_ENT_CD AS CM_ENT_CD,
BE.CFK_OBJECT_GID AS CM_SER_NUM,
BE.ORDER_NO AS CM_ENT_NUM
FROM {tmngpdb_catalog}.{data_layer}.BUSINESS_EVENT BE
LEFT JOIN {tmngpdb_catalog}.{data_layer}.STND_BUSINESS_EVENT_REASON BER
ON BE.FK_BUSINESS_EVENT_REASON_ID = BER.BUSINESS_EVENT_REASON_ID
WHERE BE.FK_OBJECT_TYPE_CD = 'Trademark' AND BER.LEGACY_CM_ENT_CD NOT IN ('MIG0','WEBE','XSCE','XSSE') AND BER.LEGACY_CM_ENT_CD IS NOT NULL
) CM
ON AM.AM_SER_NUM = CM.CM_SER_NUM'''

ip11_df = spark.sql(ip11_query)

#ip11_df.show()


# COMMAND ----------

ist_actn_blk_file1 = f"s3://{cdc_bucket}/eds/static_files/milestone/1st_action_date_block_additional_input1.csv"
ist_actn_blk_file2 = f"s3://{cdc_bucket}/eds/static_files/milestone/1st_action_date_block_additional_input2.csv"
flage_blk_file = f"s3://{cdc_bucket}/eds/static_files/milestone/milestone_flag_input.csv"
print(f'{ist_actn_blk_file1=},{ist_actn_blk_file2=},{flage_blk_file}')
file_type = "csv"

# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = ","

# Creating the Dataframe for input and output
ist_actn_blk_ip1 = spark.read.format(file_type) \
  .option("schema",infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("encoding", "windows-1252") \
  .load(ist_actn_blk_file1)

ist_actn_blk_ip2 = spark.read.format(file_type) \
  .option("schema",infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("encoding", "windows-1252") \
  .load(ist_actn_blk_file2)

flage_blk_ip = spark.read.format(file_type) \
  .option("schema",infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("encoding", "windows-1252") \
  .load(flage_blk_file)

