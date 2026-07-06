# Databricks notebook source
# MAGIC %md
# MAGIC <pre>
# MAGIC SCN is a huge number with two components to it: SCN Base & SCB Wrap.
# MAGIC
# MAGIC SCN is a 6 byte (48 bits) number. Out of these 48 bits, SCN_WRAP is a 16 bit (2 Bytes) number and SCN_BASE is a 32 bit (4 Bytes) number. Both BASE & WRAP are used to control the SCN’s increment and to ensure that the database won’t run out of it. SCN_WRAP is incremented by 1 when SCN_BASE reaches the value of 4 Billion and SCN_BASE becomes 0.
# MAGIC
# MAGIC From Oracle Version 12c, the SCN number is an 8 byte number.
# MAGIC </pre>

# COMMAND ----------

# DBTITLE 1,Create widgets
dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")
dbutils.widgets.text("data_load_group", "", "data_load_group")#group1
dbutils.widgets.text("PROC_CTGRY_CD", "SRC_TO_BRNZ", "PROC_CTGRY_CD")
#dbutils.widgets.text("config_file","../config/dev/tmngpdb-conf.yaml")
dbutils.widgets.text("dbx_env","dev")
#comments
#TMBUSCALENDAR,TMINTLTM,TMNGPDB,EOGADMIN,JBTEASPS,PROCEEDING,TMPRODVTY,TMREVIEWS,TMWORKER, TMNGFPEPP, EFOIAP, TMNGIDMP
#DATABRIDGE

# COMMAND ----------

#dbx_env = dbutils.widgets.get("config_file").rstrip().split("/", 3)[2]
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME")
PROC_CTGRY_CD = dbutils.widgets.get("PROC_CTGRY_CD")
data_load_group = dbutils.widgets.get("data_load_group")

src_name = SRC_SYS_NAME.lower()
config_file_name = src_name+"-conf.yaml"


config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{SRC_SYS_NAME=},{PROC_CTGRY_CD=},{config_file_name=},{config_file=},{dbx_env=}')

# COMMAND ----------

# DBTITLE 1,Run common functions ntbk
# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,Define Table list
# MAGIC %run ../shared/ntb_tm_brnz_table_list

# COMMAND ----------

spark.sql("set SRC_SYS_NAME = " + dbutils.widgets.get("SRC_SYS_NAME"))
spark.sql("set PROC_CTGRY_CD = " + dbutils.widgets.get("PROC_CTGRY_CD"))
#job_name = 'ntb_'+SRC_SYS_NAME.lower()+'_dq_data_vrfctn_frmwrk'
#job_name = f'ntb_{src_name}_{current_table}_brnz_load'
#spark.sql("set job_name = "+str(job_name))
#print(f'{job_name=}')
job_start_ts = datetime.datetime.now()

# COMMAND ----------

# DBTITLE 1,Define variables from config file
common_configs = read_yaml(config_file)
src_db = common_configs['schema']['src_db_name']
trgt_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
receiver_email = common_configs['data_quality']['receiver_email']
brz_allowed_variance = common_configs['data_quality']['brz_allowed_variance']
trm_scope = common_configs['secrets']['trm_scope']

database = 'bronze'


#proc_id = spark.sql(f"""select proc_id from {data_quality_catalog}.silver.CMN_PROC_DEFN_RFRNC where proc_name='{job_name}'""").collect()[0][0]
#spark.conf.set('conf.proc_id', proc_id)
spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.trgt_catalog', trgt_catalog.lower()) 
spark.conf.set('config.src_db', src_db.lower()) 
spark.conf.set('conf.database', database)
#spark.conf.set('conf.job_name', job_name)
spark.conf.set('conf.brz_allowed_variance', brz_allowed_variance)
spark.conf.set('config.trm_scope', trm_scope.lower()) 


emailid = receiver_email
env = dbx_env.upper()
#if SRC_SYS_NAME == 'TMNGPDB':
#    groups = common_configs['DMS']['groups']
#    #groups=1
#    spark.conf.set('conf.groups', groups)
#    schema_metadata =""
#    for i in range(1,groups+1):
#        schema_metadata = schema_metadata + "tmngpdb_metadata_group"+str(i) +"+"
#    schema_metadata = schema_metadata[:-1]
if SRC_SYS_NAME == 'TMNGPDB':
    schema_metadata = src_name+"_metadata_"+data_load_group
else:
    schema_metadata = src_name+"_metadata"

print(f'{schema_metadata=},{src_db=},{trgt_catalog=}, {data_quality_catalog=} ,{receiver_email=},{brz_allowed_variance=},{trm_scope=}')

# COMMAND ----------

# DBTITLE 1,Define schema from table list
Schema = ["TABLE_GROUP_NAME","TABLE_NAME","FULL_LOAD","DQ_FLTR"]
df_metadata = spark.createDataFrame(data = eval(schema_metadata), schema = Schema)
#src_scope_name = "oracle_trmpvt_server"
#df_metadata.display()    
df_metadata = df_metadata.select(f.upper('TABLE_NAME').alias("TABLE_NAME"),f.upper('DQ_FLTR').alias("DQ_FLTR")).distinct()#.filter(f.col('TABLE_NAME')=="TM_CLASS")
df_metadata = df_metadata.withColumn('SRC_SYS_NAME',f.lit(SRC_SYS_NAME))

df_metadata.display()
SRC_SCHEMA = src_db
TRGT_SCHEMA = trgt_catalog+'.bronze'

# COMMAND ----------

# DBTITLE 1,Calculate source and target table counts
if (PROC_CTGRY_CD == 'SRC_TO_BRNZ'):
    query_cnt_rows =[]

    for r in df_metadata.collect():
        dct = r.asDict()
        tbl_nm = dct["TABLE_NAME"]
        tbl_nm = dct["TABLE_NAME"]
        try:
            print("\n")
            print(f"Performing data load frequency check for {tbl_nm}:")
            #checks refresh frequency from past 1 day
            pushdown_query = f"""(SELECT SCN_TO_TIMESTAMP(MAX(ora_rowscn)) from {SRC_SCHEMA}.{tbl_nm})"""
            pushdown_query_other = f"""(SELECT  case when max_scn is not null then SCN_TO_TIMESTAMP(max_scn) else null end as max_scn from (SELECT case when count(*) >=1 then max(ora_rowscn) else null end as max_scn 
FROM {SRC_SCHEMA}.{tbl_nm} where ora_rowscn>=TIMESTAMP_TO_SCN (current_timestamp-1)))"""
                #src_query =  f"""(SELECT SCN_TO_TIMESTAMP(MAX(ora_rowscn)) from {SRC_SCHEMA}.{tbl_nm})"""
                          
            try:
                try:
                    print("Excecuting query 1")
                    df_read_src_tbl = read_data_from_oracle_conn_dsu_cmn(pushdown_query, trm_scope)
                    if df_read_src_tbl.count()>=1:
                        src_last_refresh_time = (df_read_src_tbl.collect()[0][0])
                    else:
                        src_last_refresh_time = None
                except:
                    print("Excecuting query 2")
                    df_read_src_tbl = read_data_from_oracle_conn_dsu_cmn(pushdown_query_other, trm_scope)
                    if df_read_src_tbl.count()>=1:
                        src_last_refresh_time = (df_read_src_tbl.collect()[0][0])
                    else:
                        src_last_refresh_time = None
                
                dct['RPTD_SRC_LAST_REFRESH_TS'] = src_last_refresh_time
            except Exception as e:
                #raise
                data_quality_result =  "Exception message: {}".format(e)
                src_last_refresh_time = None
                dct['RPTD_SRC_LAST_REFRESH_TS'] = src_last_refresh_time
                print("Exception message: {}".format(e))
            query_cnt_rows.append(dct)
        
        except Exception as e:
            raise
            print("Exception message: {}".format(e))

# COMMAND ----------

query_cnt_rows

# COMMAND ----------

# DBTITLE 1,Create temp table
df_dq_query_counts = spark.createDataFrame(query_cnt_rows,schema='TABLE_NAME string, DQ_FLTR string, SRC_SYS_NAME string, RPTD_SRC_LAST_REFRESH_TS timestamp').withColumn("LAST_UPDT_TS",f.from_utc_timestamp(current_timestamp(),'America/New_York'))
#df_dq_query_counts.createOrReplaceTempView("temp_dq_src_trgt_counts")
if SRC_SYS_NAME != 'TMNGPDB':
    df_dq_query_counts.select("SRC_SYS_NAME","TABLE_NAME","RPTD_SRC_LAST_REFRESH_TS","LAST_UPDT_TS").write.mode("append").saveAsTable(f"{data_quality_catalog}.silver.{SRC_SYS_NAME}_refresh_freq")
else:
    df_dq_query_counts.select("SRC_SYS_NAME","TABLE_NAME","RPTD_SRC_LAST_REFRESH_TS","LAST_UPDT_TS").write.mode("append").saveAsTable(f"{data_quality_catalog}.silver.{SRC_SYS_NAME}_{data_load_group}_refresh_freq")

# COMMAND ----------

dbutils.notebook.exit(f"Completed data refresh verification for  {trgt_catalog}.{database} tables")
