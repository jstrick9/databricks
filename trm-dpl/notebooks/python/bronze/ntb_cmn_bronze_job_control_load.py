# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")
dbutils.widgets.text("data_load_group", "", "data_load_group")#group1
#TMBUSCALENDAR,TMINTLTM,TMNGPDB,DATABRIDGE,EOGADMIN,JBTEASPS,PROCEEDING,TMPRODVTY,TMREVIEWS,TRMWORKER, TMNGFPEPP, EFOIAP, TMNGIDMP
#scope DBRPRODS & JBTEASPS

# COMMAND ----------

# DBTITLE 1,Config file widget
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME").rstrip()
src_name = SRC_SYS_NAME.lower()
config_file_name = src_name+"-conf.yaml" 
#config_file_name = "tmbuscalendar-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Execute common function ntbk
# MAGIC %run  ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,Execute Table list metadata ntbk
# MAGIC %run ../shared/ntb_tm_brnz_table_list

# COMMAND ----------

# DBTITLE 1,Set Parameter Values
common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
src_db_name = common_configs['schema']['src_db_name'].upper()

if SRC_SYS_NAME == 'TMNGPDB':
    data_load_group = dbutils.widgets.get("data_load_group")
    src_folder = common_configs['cdc']['src_csv_files']+"/"+data_load_group + "/" +src_db_name
    schema_metadata = src_name+"_metadata_"+data_load_group
else:
    src_folder = common_configs['cdc']['src_csv_files']
    schema_metadata = src_name+"_metadata"

src_database = common_configs['cdc']['src_database']
trm_scope = common_configs['secrets']['trm_scope']

spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.trgt_catalog', trgt_catalog.lower()) 
spark.conf.set('config.trm_scope', trm_scope.lower()) 

spark.sql(f"set SRC_SYS_NAME = SRC_SYS_NAME")
database = 'bronze'
control_table = 'cdc_batch_job_control'


spark.conf.set('config.schema_metadata', schema_metadata.lower())
print(f'{src_db_name=},{trgt_catalog=}, {data_quality_catalog=},{trm_scope=},{schema_metadata=},{src_folder=} ')

# COMMAND ----------

# DBTITLE 1,Create Dataframe with all table names from Oracle TMNGPDB schema. 
all_tables_query = f"""
(SELECT TBLS.TABLE_NAME , nvl(CONS.primary_keys,'') as primary_keys
FROM
(select  TABLE_NAME FROM ALL_TABLES where OWNER = '{src_db_name}'
)TBLS
LEFT JOIN
(select CONS.TABLE_NAME,
listagg(cols.column_name, ', ') within group (order by CONS.TABLE_NAME) as primary_keys
from ALL_constraints CONS, all_cons_columns cols
where CONS.OWNER = '{src_db_name}'
AND CONS.CONSTRAINT_TYPE = 'P'
AND cons.constraint_name = cols.constraint_name
AND cons.owner = cols.owner
AND CONS.TABLE_NAME = COLS.TABLE_NAME
group by  CONS.TABLE_NAME)CONS
ON TBLS.TABLE_NAME = CONS.TABLE_NAME) 
"""
#print(all_tables_query)
df_src_pk = read_data_from_oracle_conn_dsu_cmn(all_tables_query,trm_scope)
print(df_src_pk.count())
df_src_pk.createOrReplaceTempView("temp_oracle_metadata")

# COMMAND ----------

# DBTITLE 1,Create Dataframe with table names from metadata ntbk list
schema_def = ["TABLE_GROUP_NAME","TABLE_NAME","FULL_LOAD","DQ_FLTR"]
df_schema_metadata = spark.createDataFrame(data = eval(schema_metadata), schema = schema_def)
df_schema_metadata = df_schema_metadata.select(f.upper('TABLE_NAME').alias("TABLE_NAME"),'FULL_LOAD').distinct()
df_schema_metadata.display()
df_schema_metadata.createOrReplaceTempView("temp_schema_metadata")
#tmbuscalendar_metadata_table_list = [row.TABLE_NAME for row in df_tmbuscalendar_metadata.collect()]
#tmbuscalendar_metadata_table_list = tuple(tmbuscalendar_metadata_table_list)

# COMMAND ----------

# DBTITLE 1,load data in Control table
if SRC_SYS_NAME == 'TMNGPDB':
    df_merge = spark.sql(f""" \
    MERGE INTO {trgt_catalog}.{database}.{control_table} AS trgt_table \
    using( \
    select \
    '{src_folder}/' || src.TABLE_NAME as src_folder, \
    '{trgt_catalog}' as catalog_name, \
    '{database}' as database_name, \
    '{data_load_group}' as group_name, \
    lower(src.TABLE_NAME) as table_name, \
    '{src_database}' as source_db_name, \
    src.TABLE_NAME as source_table_name, \
    nvl(src.PRIMARY_KEYS,'') as primary_keys, \
    temp_md.FULL_LOAD as full_load, \
    True as initial_load_finished \
    from \
    temp_oracle_metadata src \
        inner join temp_schema_metadata temp_md \
        on upper(src.TABLE_NAME) = temp_md.TABLE_NAME) as src_table \
    on upper(trgt_table.source_table_name) = upper(src_table.TABLE_NAME) \
        and lower(trgt_table.group_name) = '{data_load_group}'
      
    when matched and (nvl(upper(trgt_table.primary_keys), '') != nvl(upper(src_table.primary_keys), '')  or 
    nvl(upper(trgt_table.FULL_LOAD), 'N') != nvl(upper(src_table.FULL_LOAD), 'N'))\
    then update \
    set trgt_table.primary_keys = src_table.primary_keys,  trgt_table.FULL_LOAD = src_table.FULL_LOAD\

    when not matched and src_table.TABLE_NAME is not null then \
    insert (src_folder,catalog_name,database_name,group_name,table_name,source_db_name,source_table_name,primary_keys,full_load,initial_load_finished) \
    values (src_table.src_folder, src_table.catalog_name, src_table.database_name,src_table.group_name,src_table.table_name, src_table.source_db_name, src_table.source_table_name, \
        primary_keys,src_table.full_load,src_table.initial_load_finished) \
    """)
else:
    df_merge = spark.sql(f"""
    MERGE INTO {trgt_catalog}.{database}.{control_table} AS trgt_table \
    using( \
    select \
    '{src_folder}/' || src.TABLE_NAME as src_folder, \
    '{trgt_catalog}' as catalog_name, \
    '{database}' as database_name, \
    lower(src.TABLE_NAME) as table_name, \
    '{src_database}' as source_db_name, \
    src.TABLE_NAME as source_table_name, \
    nvl(src.PRIMARY_KEYS,'') as primary_keys, \
    temp_md.FULL_LOAD as full_load, \
    False as initial_load_finished \
    from \
    temp_oracle_metadata src \
        inner join temp_schema_metadata temp_md \
        on upper(src.TABLE_NAME) = temp_md.TABLE_NAME) as src_table \
    on upper(trgt_table.source_table_name) = upper(src_table.TABLE_NAME) \
        
    when matched and (nvl(upper(trgt_table.primary_keys), '') != nvl(upper(src_table.primary_keys), '')or 
    nvl(upper(trgt_table.FULL_LOAD), 'N') != nvl(upper(src_table.FULL_LOAD), 'N'))\
     then update \
    set trgt_table.primary_keys = src_table.primary_keys, trgt_table.FULL_LOAD = src_table.FULL_LOAD\

    when not matched and src_table.TABLE_NAME is not null then \
    insert (src_folder,catalog_name,database_name,table_name,source_db_name,source_table_name,primary_keys,full_load,initial_load_finished) \
    values (src_table.src_folder, src_table.catalog_name, src_table.database_name,src_table.table_name, src_table.source_db_name, src_table.source_table_name, \
        primary_keys,src_table.full_load,src_table.initial_load_finished) \
    """)
df_merge.display()

# COMMAND ----------

# DBTITLE 1,Verify duplicate entries in control table
df_check_duplicates = spark.sql(
    f"""
    select * from(select distinct (table_name), count(*) c_tables from {trgt_catalog}.{database}.{control_table} group by table_name)
    where c_tables>1"""
    )
if df_check_duplicates.count() > 0:
    raise ValueError("There are duplicate entries in control table")
else:
    print("There are no duplicate enteries found")

# COMMAND ----------

dbutils.notebook.exit(f"Completed Loading {trgt_catalog}.{database}.{control_table} ")
