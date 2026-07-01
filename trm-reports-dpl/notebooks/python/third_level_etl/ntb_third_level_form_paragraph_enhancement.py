# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Set config file
dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file = f"../../config/{dbx_env}/trmreports-conf.yaml"
print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Execute common function ntbk
# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

# DBTITLE 1,Set parameter values
common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['reporting_catalog']
tmngpdb_src_catalog = common_configs['schema']['tmngpdb_src_catalog']
tmworker_catalog = common_configs['schema']['tmworker_catalog']

# COMMAND ----------

# DBTITLE 1,Start Job Control
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_third_level_form_paragraph_enhancement'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

df_fp = spark.sql(f"""
    SELECT 
    aa.class,
    aa.ser_num AS ser_num_class,
    bb.class_no,
    bb.modification_no,
    bb.title_tx,
    bb.INTL_CLASS_SHORT_TITLE_TX,
    aa.goods_and_services_desc,
    cc.serial_number,
    cc.law_office,
    cc.country_or_area_name,
    dd.Category AS FPEPCategory,
    dd.fk_fp_category_id AS FPEPCategoryID,
    dd.FP_YEAR AS FPEPYEAR,
    dd.fk_wrkr_id AS FPEPWorkerID,
    dd.action_count AS FPEPActionCt,
    dd.COMPLETED_DT AS FPEPCompletedDt,
    dd.GROUP_NAME AS FPEPGroup,
    dd.fp_id AS FPEPFPID,
    dd.SER_NUM AS FPEPSerNum,
    dd.fk_fp_group_id AS FPGroupID,
    dd.TITLE_TX as FPEPTitle,
    dd.TRANSACTIONAL_LITERAL AS FPEPTransLit,
    ee.CREATE_USER_ID,
    ee.FK_USER_ROLE_ID,
    gg.title_tx AS User_Role,
    ee.FK_TM_ORGANIZATION_GID,
    ff.tm_organization_gid,
    ff.organization_cd,
    ff.organization_nm,
    a.worker_no AS tmworkerNo,
    a.active_in,
    a.worker_nm,
    b.worker_no AS tmngpdbWorkerNo,
    b.grade_cd,
    b.brs_user_id,
    c.serial_number AS FPDSerialNum,
    c.action_type as FPDActionType
FROM
    {reporting_catalog}.silver.fpep_fact dd
JOIN
    {reporting_catalog}.silver.class aa ON dd.SER_NUM = aa.ser_num
JOIN
    {tmngpdb_src_catalog}.bronze.stnd_class bb ON aa.class = bb.class_no
JOIN
    {reporting_catalog}.gold.post_reg_dashboard cc ON aa.ser_num = cc.Serial_number
JOIN
   {tmworker_catalog}.bronze.worker_role ee ON dd.FK_WRKR_ID = ee.CREATE_USER_ID
JOIN
   {tmworker_catalog}.bronze.tm_organization ff ON ee.FK_TM_ORGANIZATION_GID = ff.tm_organization_gid
JOIN
   {tmworker_catalog}.bronze.user_role gg ON ee.FK_USER_ROLE_ID = gg.USER_ROLE_ID
JOIN
   {tmworker_catalog}.bronze.worker a ON a.worker_no = dd.FK_WRKR_ID
JOIN
   {tmngpdb_src_catalog}.bronze.worker b ON dd.fk_wrkr_id = b.worker_no
JOIN
   {reporting_catalog}.gold.form_paragraph_dashboard c on cc.serial_number = c.serial_number
WHERE
    dd.COMPLETED_DT BETWEEN '2020-10-01' AND CURRENT_DATE() 
    AND bb.modification_no = 7 
    AND a.active_in = 'Y'
GROUP BY
    aa.class,
    aa.ser_num,
    bb.class_no,
    bb.modification_no,
    bb.title_tx,
    bb.INTL_CLASS_SHORT_TITLE_TX,
    aa.goods_and_services_desc,
    cc.serial_number,
    cc.law_office,
    cc.country_or_area_name,
    dd.Category,
    dd.fk_fp_category_id,
    dd.FP_YEAR,
    dd.fk_wrkr_id,
    dd.action_count,
    dd.COMPLETED_DT,
    dd.GROUP_NAME,
    dd.fp_id,
    dd.SER_NUM,
    dd.TITLE_TX,
    dd.TRANSACTIONAL_LITERAL,
    ee.CREATE_USER_ID,
    ee.FK_USER_ROLE_ID,
    gg.title_tx,
    ee.FK_TM_ORGANIZATION_GID,
    ff.tm_organization_gid,
    ff.organization_cd,
    ff.organization_nm,
    a.worker_no,
    a.grade_ct,
    a.active_in,
    a.worker_nm,
    b.worker_no,
    b.grade_cd,
    b.brs_user_id,
    c.serial_number, 
    c.action_type,
    dd.fk_fp_group_id
                  
                  """)

# COMMAND ----------

df_fp = df_fp.withColumn(
    "FPEPTitle", trim(regexp_replace(col("FPEPTitle"), r"[^\x20-\x7e]", "" ))
)

# COMMAND ----------

# set column order
df_fp = df_fp.select(
    'class',
    'ser_num_class',
    'class_no',
    'modification_no',
    'title_tx',
    'INTL_CLASS_SHORT_TITLE_TX',
    'goods_and_services_desc',
    'serial_number',
    'law_office',
    'country_or_area_name',
    'FPEPCategory',
    'FPEPCategoryID',
    'FPEPYEAR',
    'FPEPWorkerID',
    'FPEPActionCt',
    'FPEPCompletedDt',
    'FPEPGroup',
    'FPEPFPID',
    'FPEPSerNum',
    'FPGroupID',
    'FPEPTitle',
    'FPEPTransLit',
    'CREATE_USER_ID',
    'FK_USER_ROLE_ID',
    'User_Role',
    'FK_TM_ORGANIZATION_GID',
    'tm_organization_gid',
    'organization_cd',
    'organization_nm',
    'tmworkerNo',
    'active_in',
    'worker_nm',
    'tmngpdbWorkerNo',
    'grade_cd',
    'brs_user_id',
    'FPDSerialNum',
    'FPDActionType'
)

# COMMAND ----------

df_fp.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.form_paragraph_enhancement")

# COMMAND ----------

# end job control
recs_count = df_fp.count()
end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")
