# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
tmngpdb_catalog = common_configs['schema']['tmngpdb_src_catalog']
tmngfpepp_catalog = common_configs['schema']['tmngfpepp_catalog']
tmprodvty_catalog = common_configs['schema']['tmprodvty_catalog']
print(reporting_catalog)
print(tmngpdb_catalog)
print(tmngfpepp_catalog)
print(tmprodvty_catalog)
schema_bronze = "bronze"
schema_silver = "silver"

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import coalesce

# use original logic prior to 2024-04-01
df_form_para_pre_2024 = spark.sql(f"""                 
select                   
  CAST(split(wio.cfk_object_gid,':')[2] AS INTEGER) AS SER_NUM,
  upper(fpg.title_tx) as group_name,
  date(oa.issue_dt) as completed_dt,
  swit.title_tx as transactional_literal,
  coalesce(
    pac.productivity_action_cd, 
    CASE 
      WHEN swit.title_tx = 'Final Action' THEN '6330'
      WHEN swit.title_tx = 'Non-Final Action' THEN '6320'
      WHEN swit.title_tx = 'Non-Final Action - Full Refusal' THEN '6320'
      WHEN swit.title_tx = 'Combined Examiner''s Amendment and Priority Action' THEN '6326'
      WHEN swit.title_tx = 'Examiner''s Amendment' THEN '6328'
      WHEN swit.title_tx = 'Request for Reconsideration Denied' THEN '6330'
      WHEN swit.title_tx = 'Subsequent Final Action' THEN '6330'
      WHEN swit.title_tx = 'Notice of Non-Responsive Amendment' THEN '6320'
      WHEN swit.title_tx = "Combined Examiner's Amendment and Priority Action" THEN '0210'
      WHEN swit.title_tx = "Examiner's Amendment" THEN '0200'
      ELSE null
    END
  ) as transaction_no,
  case 
    when date(oa.issue_dt) = date(oas.first_ea_action_counted_dt) and trim(FIRST_EA_ACTION_COUNTED_IN) = 'Y' then 1
    when nvl(pac.dn_action_no, oa.action_no) is not null then nvl(pac.dn_action_no, oa.action_no)
    else 1 
  end as action_count,
  oa.issue_empe_no as fk_wrkr_id,
  fpv.fk_fp_call_number_tx as fp_id,
  trim(fpv.paragraph_title_tx) as title_tx,
  fpv.fk_form_paragraph_group_id as fk_fp_group_id,
  fpv.fk_form_paragraph_category_id as fk_fp_category_id,
  fpc.title_tx as category,
  case when month(oa.issue_dt) > 9 then year(oa.issue_dt) + 1 else year(oa.issue_dt) end as fp_year,
  ddvcf.CREATE_TS as completed_ts,
  NULL AS TM_ANALYTICS_TS 
from
  {tmngpdb_catalog}.bronze.OFFICE_ACTIVITY oa
  join {tmngpdb_catalog}.bronze.work_item wi on oa.fk_work_item_gid = wi.work_item_gid
  join {tmngpdb_catalog}.bronze.work_item_object wio on wio.fk_work_item_gid = wi.work_item_gid
  left join {tmngpdb_catalog}.bronze.TM_OFFICE_ACTIONS oas on wio.cfk_object_gid = oas.fk_trademark_gid
  left join (
    select pt.cfk_object_gid, pt.Create_ts, pt.dn_worker_no, pa.productivity_action_cd, pt.dn_action_no
    from {tmprodvty_catalog}.bronze.production_transaction pt 
    join {tmprodvty_catalog}.bronze.productivity_action pa 
      on pt.fk_generating_prodvty_actn_id = pa.productivity_action_id
    and DELETE_IN = 'N'
  ) pac 
    on date(oa.issue_dt) = date(pac.Create_ts) 
    and oa.issue_empe_no = pac.dn_worker_no 
    and wio.cfk_object_gid = pac.cfk_object_gid
  join {tmngpdb_catalog}.bronze.STND_WORK_ITEM_TYPE swit on wi.FK_WORK_ITEM_TYPE_CD = swit.WORK_ITEM_TYPE_CD
  join {tmngpdb_catalog}.bronze.office_activity_draft_document oadd on oa.fk_work_item_gid = oadd.fk_work_item_gid
  join {tmngpdb_catalog}.bronze.draft_document dd on oadd.fk_draft_document_id = dd.draft_document_id
  join {tmngpdb_catalog}.bronze.draft_document_version ddv on dd.draft_document_id = ddv.fk_draft_document_id
  join {tmngpdb_catalog}.bronze.draft_document_version_compnt ddvc on ddv.fk_draft_document_id = ddvc.fk_draft_document_id
    and ddv.draft_document_mod_no = ddvc.fk_draft_document_mod_no
  join {tmngpdb_catalog}.bronze.draft_doc_ver_compnt_fpv ddvcf on ddvc.fk_draft_document_id = ddvcf.fk_draft_document_id
    and ddvc.fk_draft_document_mod_no = ddvcf.fk_draft_document_mod_no
    and ddvc.fk_document_component_id = ddvcf.fk_document_component_id
  join {tmngfpepp_catalog}.bronze.form_paragraph_version fpv on fpv.form_paragraph_version_gid = ddvcf.cfk_form_paragraph_version_gid
  join {tmngfpepp_catalog}.bronze.stnd_form_paragraph_category fpc on fpv.fk_form_paragraph_category_id = fpc.form_paragraph_category_id
  join {tmngfpepp_catalog}.bronze.stnd_form_paragraph_group fpg on fpv.fk_form_paragraph_group_id = fpg.form_paragraph_group_id
where ddvcf.CREATE_TS < '2024-04-01'
""")

df_form_para_pre_2024 = df_form_para_pre_2024.withColumn(
  'title_tx', regexp_replace(col("title_tx"), r'[\uFFFD]', "")
).withColumn(
  'title_tx', regexp_replace(col("title_tx"), r'[\u201C|\u201D]', '"')
).withColumn(
  'title_tx', regexp_replace(col("title_tx"), r'[\u2013]', '-')
).withColumn(
  'title_tx', regexp_replace(col("title_tx"), r'[\u2019]', "'")
)

# COMMAND ----------

### use html tags post 2024 changes

df_form_para_post_2024 = spark.sql(f"""                 
select                   
  CAST(split(wio.cfk_object_gid,':')[2] AS INTEGER) AS SER_NUM ,
  --upper(fpg.title_tx) as group_name,
  date(oa.issue_dt) as completed_dt,
  swit.title_tx as transactional_literal,
  pac.productivity_action_cd as transaction_no,
  case when date(oa.issue_dt) = date(oas.first_ea_action_counted_dt) and trim(FIRST_EA_ACTION_COUNTED_IN) = 'Y' then 1
  when nvl(pac.dn_action_no,oa.action_no) is not null then nvl(pac.dn_action_no,oa.action_no) 
  else 1 end as action_count,
  oa .issue_empe_no as fk_wrkr_id,
  --fpv.fk_fp_call_number_tx as fp_id,
  --TRIM(fpv.paragraph_title_tx) as title_tx,
  --fpv.fk_form_paragraph_group_id as fk_fp_group_id,
  --fpv.fk_form_paragraph_category_id as fk_fp_category_id,
  --ddvc.rank_order_no as position_order_no,---column not used in etl
  -- fpc.title_tx as category,
  case when month(oa.issue_dt)>9 then year(oa.issue_dt)+1 else year(oa.issue_dt) end as fp_year,
  -- ddvcf.CREATE_TS as completed_ts,
  NULL AS TM_ANALYTICS_TS ,
  ddvc.fk_document_component_id
from
  {tmngpdb_catalog}.bronze.OFFICE_ACTIVITY oa
  join {tmngpdb_catalog}.bronze.work_item wi on oa.fk_work_item_gid = wi.work_item_gid
  join  {tmngpdb_catalog}.bronze.work_item_object wio on wio.fk_work_item_gid = wi.work_item_gid --get tradmark_gid
  left join  {tmngpdb_catalog}.bronze.TM_OFFICE_ACTIONS oas on wio.cfk_object_gid = oas.fk_trademark_gid
  left join (select pt.cfk_object_gid, pt.Create_ts, pt.dn_worker_no, pa.productivity_action_cd , pt.dn_action_no
  from {tmprodvty_catalog}.bronze.production_transaction pt 
  join  {tmprodvty_catalog}.bronze.productivity_action pa 
  on pt.fk_generating_prodvty_actn_id = pa.productivity_action_id
  and DELETE_IN = 'N')pac 
  on date(oa.issue_dt) = date(pac.Create_ts) and oa.issue_empe_no  = pac.dn_worker_no and wio.cfk_object_gid=pac.cfk_object_gid
  join  {tmngpdb_catalog}.bronze.STND_WORK_ITEM_TYPE SWIT on WI.FK_WORK_ITEM_TYPE_CD = SWIT.WORK_ITEM_TYPE_CD
  join {tmngpdb_catalog}.bronze.office_activity_draft_document oadd on oa.fk_work_item_gid = oadd.fk_work_item_gid
  join  {tmngpdb_catalog}.bronze.draft_document dd on oadd.fk_draft_document_id = dd.draft_document_id
  join  {tmngpdb_catalog}.bronze.draft_document_version ddv on dd.draft_document_id = ddv.fk_draft_document_id
  join  {tmngpdb_catalog}.bronze.draft_document_version_compnt ddvc on ddv.fk_draft_document_id = ddvc.fk_draft_document_id-----------
  and ddv.draft_document_mod_no = ddvc.fk_draft_document_mod_no""")

df_doc_txt = spark.sql(f""" select dd.draft_document_id, dd.draft_document_nm, document_component_tx,  dd.create_ts, dd.last_mod_ts, dc.document_component_id
from {tmngpdb_catalog}.bronze.draft_document dd join {tmngpdb_catalog}.bronze.draft_document_version_compnt ddvc on dd.draft_document_id=fk_draft_document_id
join {tmngpdb_catalog}.bronze.document_component dc on ddvc.fk_document_component_id=dc.document_component_id
where draft_document_status_ct='C' and FK_DOCUMENT_COMPONENT_TYPE_CD='FREE' """)

df_doc_txt = df_doc_txt.withColumn(
  'fpv_id', explode(regexp_extract_all(col('document_component_tx'), lit(r'(?<=fp-version-gid=")(FPV:\d*:\d*)')))
).select(
  "document_component_id", "fpv_id", col("create_ts").alias('completed_ts')
).distinct()

df_fpv = spark.sql(f"""select form_paragraph_version_gid,
                    upper(fpg.title_tx) as group_name,
                    fpv.fk_fp_call_number_tx as fp_id,
                    TRIM(fpv.paragraph_title_tx) as title_tx,
                    fpv.fk_form_paragraph_group_id as fk_fp_group_id,
                    fpv.fk_form_paragraph_category_id as fk_fp_category_id,
                    fpc.title_tx as category                    
                    from {tmngfpepp_catalog}.bronze.form_paragraph_version fpv 
                    join {tmngfpepp_catalog}.bronze.stnd_form_paragraph_category fpc on fpv.fk_form_paragraph_category_id = fpc.form_paragraph_category_id
                    join {tmngfpepp_catalog}.bronze.stnd_form_paragraph_group fpg on fpv.fk_form_paragraph_group_id = fpg.form_paragraph_group_id""")

df_joined = df_form_para_post_2024.join(df_doc_txt, df_form_para_post_2024.fk_document_component_id==df_doc_txt.document_component_id)

df_final = df_joined.join(df_fpv, df_joined.fpv_id == df_fpv.form_paragraph_version_gid ).drop('fk_document_component_id', 'document_component_id', 'form_paragraph_version_gid', 'fpv_id')

df_final = df_final.filter(col('completed_ts') >= '2024-04-01')

df_final = df_final.withColumn(
  'title_tx', regexp_replace(col("title_tx"), r'[\uFFFD]', "") ## remove unicode unknown replacement character
).withColumn(
  'title_tx', regexp_replace(col("title_tx"), r'[\u201C|\u201D]', '"') ## replace unicode left and right specific quotation marks with standard quotation mark
).withColumn(
  'title_tx', regexp_replace(col("title_tx"), r'[\u2013]', '-') ## replace unicode 'EN DASH' with regular hyphen
).withColumn(
  'title_tx', regexp_replace(col("title_tx"), r'[\u2019]', "'") ## replace unicode right single quote with standard single quote
)

# COMMAND ----------

from pyspark.sql.window import Window

df_out = df_form_para_pre_2024.unionByName(df_final)


# COMMAND ----------

df_out.createOrReplaceTempView("temp_form_para_fpep_fact")
#df_out.count()

# COMMAND ----------

df_fpep_fact_merge = spark.sql(f"""
MERGE INTO {reporting_catalog}.silver.fpep_fact trgt
USING temp_form_para_fpep_fact src
ON src.ser_num = trgt.ser_num
and src.fp_id= trgt.fp_id
and src.completed_ts = trgt.completed_ts--date_trunc('SECOND', trgt.completed_ts)
--and src.action_count = trgt.action_count
WHEN NOT MATCHED THEN INSERT (CATEGORY,FK_FP_CATEGORY_ID,FK_FP_GROUP_ID,TITLE_TX,CURRENT_TITLE,SER_NUM,FP_YEAR,FK_WRKR_ID,ACTION_COUNT,TRANSACTION_NO,TRANSACTIONAL_LITERAL,COMPLETED_DT,GROUP_NAME,FP_ID,COMPLETED_TS,TM_ANALYTICS_TS)
VALUES (SRC.CATEGORY,SRC.FK_FP_CATEGORY_ID,SRC.FK_FP_GROUP_ID,SRC.TITLE_TX,'',
SRC.SER_NUM,SRC.FP_YEAR,SRC.FK_WRKR_ID,SRC.ACTION_COUNT,SRC.TRANSACTION_NO,SRC.TRANSACTIONAL_LITERAL,
SRC.COMPLETED_DT,SRC.GROUP_NAME,SRC.FP_ID,SRC.completed_ts,SRC.TM_ANALYTICS_TS )
""")
#There are records with -1 ser_num in fpep_fact calgary file
#There are duplicate records in calfary file 77100572
#df_fpep_fact_merge.display()

# COMMAND ----------

from delta.tables import DeltaTable

# Make a DataFrame copy of the table
df_fpep_fact = spark.table(f"{reporting_catalog}.silver.fpep_fact")

# For each fp_id, get the latest title_tx by completed_ts
from pyspark.sql.window import Window

window_spec = Window.partitionBy("fp_id").orderBy(col("completed_ts").desc())
df_latest_title = df_fpep_fact.withColumn(
    "current_title_new",
    first("title_tx", ignorenulls=True).over(window_spec)
).select("fp_id", "current_title_new").distinct()

# Join to get the new current_title for each row
df_to_update = df_fpep_fact.join(
    df_latest_title, on="fp_id", how="left"
).withColumn(
    "CURRENT_TITLE", col("current_title_new")
).drop("current_title_new")

# Use case when logic for CATEGORY column
df_category = df_to_update.withColumn(
    "CATEGORY",
    when(
        (lower(col("category")).like("%fee%")) | (lower(col("category")).like("%translation%")),
        concat(
            upper(substring(lower(col("CATEGORY")), 1, 1)),
            lower(substring(col("CATEGORY"), 2, length(col("CATEGORY")) - 1))
        )
    ).otherwise(col("CATEGORY"))
)

# Write back to Delta table (overwrite mode)
df_category.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{reporting_catalog}.silver.fpep_fact")

# COMMAND ----------

dbutils.notebook.exit(f"Completed initial data load of fpep_fact Table ")
