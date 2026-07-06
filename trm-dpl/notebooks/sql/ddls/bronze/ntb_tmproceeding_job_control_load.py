# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE WIDGET TEXT dbx_env DEFAULT "dev"

# COMMAND ----------

# DBTITLE 1,Config file widget
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file = "../../../config/"+dbutils.widgets.get("dbx_env").rstrip()+"/proceeding-conf.yaml"
print(f'{config_file=}')
if dbx_env == "qa":
    dbutils.widgets.text("env", "test")
    print(f'{dbx_env=}')
else:
    dbutils.widgets.text("env", dbx_env)
    print(f'{dbx_env=}')

# COMMAND ----------

# DBTITLE 1,Execute common function ntbk
# MAGIC %run ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,Execute Table list metadata ntbk
# MAGIC %run ./ntb_proceeding_table_list

# COMMAND ----------

# DBTITLE 1,Set Parameter Values
common_configs = read_yaml(config_file)
proceeding_catalog = common_configs['schema']['proceeding_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{proceeding_catalog=}, {data_quality_catalog=} ')
src_folder = common_configs['cdc']['src_csv_files']
src_database = common_configs['cdc']['src_database']
spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.proceeding_catalog', proceeding_catalog.lower()) 
database = 'bronze'
control_table = 'cdc_batch_job_control'

# COMMAND ----------

# DBTITLE 1,Create Dataframe with all table names from Oracle TMNGPDB schema. 
all_tables_query = f"""
(SELECT TBLS.TABLE_NAME , CONS.primary_keys
FROM
(select  TABLE_NAME FROM ALL_TABLES where OWNER = 'TMPROCEEDING'
)TBLS
LEFT JOIN
(select CONS.TABLE_NAME,
listagg(cols.column_name, ', ') within group (order by CONS.TABLE_NAME) as primary_keys
from ALL_constraints CONS, all_cons_columns cols
where CONS.OWNER = 'TMPROCEEDING'
AND CONS.CONSTRAINT_TYPE = 'P'
AND cons.constraint_name = cols.constraint_name
AND cons.owner = cols.owner
AND CONS.TABLE_NAME = COLS.TABLE_NAME
group by  CONS.TABLE_NAME)CONS
ON TBLS.TABLE_NAME = CONS.TABLE_NAME) tab
"""
#replace the connection with secrets
df_src_pk = (spark.read.format("jdbc")\
      .option("url", "jdbc:oracle:thin:@pvt-tmng-db-4.pvt.uspto.gov:1602/TRMPVT")\
      .option("dbtable", all_tables_query)\
      .option("user", "BIGDATAREAD")\
      .option("password", "BigDataTRM#23May24")\
      .option("driver", "oracle.jdbc.OracleDriver")\
      .option("fetchSize",1)\
      .load())
print(df_src_pk.count())
#df_src_pk.display()

# COMMAND ----------

df_src_pk.display()

# COMMAND ----------

# MAGIC %run ./ntb_proceeding_table_list

# COMMAND ----------

# DBTITLE 1,Create Dataframe with table names from metadata ntbk list
Schema = ["TABLE_GROUP_NAME","TABLE_NAME"]
df_tmproceeding_metadata = spark.createDataFrame(data = proceedingdb_metadata, schema = Schema)
df_tmproceeding_metadata = df_tmproceeding_metadata.select('TABLE_NAME').distinct()

# COMMAND ----------

df_src_pk.createOrReplaceTempView("temp_oracle_metadata")
df_brz_job_control = spark.sql(f"select '{src_folder}/'||TABLE_NAME as src_folder,'{proceeding_catalog}' as catalog_name,'{database}' as database_name,\
    lower(TABLE_NAME) as table_name,'{src_database}' as source_db_name,TABLE_NAME as source_table_name,PRIMARY_KEYS as primary_keys, False as initial_load_finished\
    from temp_oracle_metadata ")
brz_job_control_load = df_brz_job_control.alias("df_brz").join(df_tmproceeding_metadata.alias("df_md"),(f.col("df_brz.source_table_name") == f.col("df_md.TABLE_NAME")),"inner").select('df_brz.*')
#brz_job_control_load.display()
brz_job_control_load.write.mode('overwrite').format("delta").insertInto(f'{proceeding_catalog}.{database}.{control_table}')

# COMMAND ----------

brz_job_control_load.count()

# COMMAND ----------

# DBTITLE 1,Verify duplicate entries
df_check_duplicates = spark.sql(
    f"""
    select * from(select distinct (table_name), count(*) c_tables from {proceeding_catalog}.{database}.{control_table} group by table_name)
    where c_tables>1"""
    )
if df_check_duplicates.count() > 0:
    raise ValueError("There are duplicate entries in control table")
else:
    print("There are no duplicate enteries found")   

# COMMAND ----------

dbutils.notebook.exit(f"Completed Loading {proceeding_catalog}.{database}.{control_table}. Number of rows inserted {brz_job_control_load.count()} ")
