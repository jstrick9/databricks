# Databricks notebook source
# MAGIC %md
# MAGIC ##Datacompy
# MAGIC https://capitalone.github.io/datacompy/spark_usage.html

# COMMAND ----------

#pip install datacompy

# COMMAND ----------

#dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")
dbutils.widgets.text("data_load_group", "", "data_load_group")#group1
dbutils.widgets.text("full_data_refresh", "N", "full_data_refresh")#group1
#TMBUSCALENDAR,TMINTLTM,TMNGPDB,DATABRIDGE,EOGADMIN,JBTEASPS,PROCEEDING,TMPRODVTY,TMREVIEWS,TRMWORKER, TMNGFPEPP, EFOIAP, TMNGIDMP
#scope DBRPRODS & JBTEASPS

# COMMAND ----------

# DBTITLE 1,Config file widget
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME").rstrip()
full_data_refresh = dbutils.widgets.get("full_data_refresh")
src_name = SRC_SYS_NAME.lower()
config_file_name = src_name+"-conf.yaml" 
#config_file_name = "tmbuscalendar-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
#config_file = "/Workspace/Users/Pawanpreet.Sangari@USPTO.GOV/bdr-ng-trm-dpl-mysql-jar/notebooks/config/dev/tmngpdb-conf.yaml"
print(f'{config_file=}')

import pytz
#import datacompy
from pytz import timezone

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
    data_load_group = dbutils.widgets.get("data_load_group")

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

# DBTITLE 1,Filter Tables from Control Table where Full Load = 'N' and initial load finished ='Y'
if SRC_SYS_NAME == 'TMNGPDB':
    df_control_table = spark.sql(f"""select catalog_name,database_name,table_name,source_db_name,source_table_name,primary_keys,full_load,decode(initial_load_finished,false,0,1) as initial_load_finished from {trgt_catalog}.bronze.cdc_batch_job_control
    where group_name = '{data_load_group}'
--and full_load = 'N'
--and initial_load_finished= true
--and lower(source_table_name) in ('stnd_object_type') 
  """)
else:
    df_control_table = spark.sql(f"""select catalog_name,database_name,table_name,source_db_name,source_table_name,primary_keys, full_load,decode(initial_load_finished,false,0,1) as initial_load_finished from {trgt_catalog}.bronze.cdc_batch_job_control """)


df_control_table.display()


# COMMAND ----------

display(df_control_table)

# COMMAND ----------

# DBTITLE 1,Get CDC Timestamp Column name from table list ntbk
schema_def = ["TABLE_GROUP_NAME","TABLE_NAME","FULL_LOAD","DQ_FLTR"]
df_schema_metadata = spark.createDataFrame(data = eval(schema_metadata), schema = schema_def)
df_schema_metadata = df_schema_metadata.select(f.upper('TABLE_NAME').alias("TABLE_NAME"),'DQ_FLTR').distinct()
job_control_df = df_control_table.alias("df_cntl").join(df_schema_metadata.alias("df_dq_fltr"),(f.col("df_cntl.source_table_name") == f.col("df_dq_fltr.TABLE_NAME")),"inner")
#job_control_df.display()
job_control_parameters = job_control_df.collect()

# COMMAND ----------

# DBTITLE 1,Define Merge Function
#########################################################################
## Function for merging CDC input to main table
#########################################################################
def merge_cdc_to_main(target_catalog, target_db, target_table, cdc_df, key_columns, all_columns,composite_key_ind):
    tempview_name = f"{target_table}_updates"
    cdc_df.createOrReplaceGlobalTempView(tempview_name)

    other_columns = all_columns.copy()    
    key_condition = " "

    q_1 = f"""
        MERGE INTO {target_catalog}.{target_db}.{target_table} t
        USING global_temp.{tempview_name} u 
        ON
        """

    # build key comparing conditions
    
    for k in key_columns:
        other_columns.remove(k)
        
    firstColumn = True
    for column in key_columns:
        if firstColumn:
            key_condition += f" t.{column} = u.{column}"
            firstColumn = False
        else:
            key_condition += f" AND t.{column} = u.{column}"

    if composite_key_ind == 'Y':
        t_other_columns = list(map(lambda x: 't.' + x, other_columns))
        t_other_columns = ','.join(t_other_columns)
        u_other_columns = list(map(lambda x: 'u.' + x, other_columns))
        u_other_columns = ','.join(u_other_columns)

        q_2 = f"""
            WHEN MATCHED AND CONCAT({t_other_columns}) != CONCAT({u_other_columns})
            THEN UPDATE SET 
        """

    else:
        q_2 = """
            WHEN MATCHED 
            THEN UPDATE SET 
        """  


    # build update statement
    update_statement = " "
    firstColumn = True
    for column in other_columns:
        if firstColumn:
            update_statement += f" {column}=u.{column}"
            firstColumn = False
        else:
            update_statement += f", {column}=u.{column}"

    q_3 = """
            WHEN NOT MATCHED 
            THEN INSERT 
    """  
    

    # build insert statement
    insert_statement = "("
    firstColumn = True
    for column in all_columns:
        if firstColumn:
            insert_statement += f"{column}"
            firstColumn = False
        else:
            insert_statement += f", {column}"

    insert_statement += """)
    VALUES
    (
    """

    firstColumn = True
    for column in all_columns:
        if firstColumn:
            insert_statement += f"u.{column}"
            firstColumn = False
        else:
            insert_statement += f", u.{column}"


    merge_query = q_1 + key_condition + q_2 + update_statement + q_3 + insert_statement + ")"
    #print(f"merge_query = {merge_query}")

    try:
        df_merge = spark.sql(merge_query)
        df_merge.display()
    except Exception as e:
        print("Exception message: {}".format(e))
        print(f"Unable to Merge Data into {target_catalog}.{target_db}.{target_table}")
     
#########################################################################
## The End of Function merge_cdc_to_main
#########################################################################
    
    

# COMMAND ----------

# DBTITLE 1,Delete and Merge Inserts/Updates into Bronze delta tables
data_load_failed_tables = []
for job_control in job_control_parameters:
    #if job_control['table_name'] == 'office_activity_h':
    current_cat = job_control['catalog_name']
    current_db  = job_control['database_name']
    current_table = job_control['table_name']
    primary_keys = job_control['primary_keys']
    src_db = job_control['source_db_name']
    src_table = job_control['source_table_name']
    cdc_date_col = job_control['DQ_FLTR']
    full_load_ind = job_control['full_load']
    initial_load_ind = job_control['initial_load_finished']
    job_name = f'ntb_{src_name}_{current_table}_brnz_load'
    print(f"******************************************** \n Now processing: {src_db}.{src_table}")
    
    key_columns = [item.strip().lower() for item in primary_keys.split(",")]

    all_columns = spark.table(f"{current_cat}.{current_db}.{current_table}").columns
    all_columns = [x.lower() for x in all_columns]
    print(f"all_columns= {all_columns}")
    print(f"key_columns= {primary_keys}")
    print(f"{cdc_date_col=}")

    start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
    print(f'{start_ts=}')
    control_dt = begin_job_cntl(f'{data_quality_catalog}',f'{trgt_catalog}.silver',job_name,start_ts)
    
    try:
        if full_data_refresh == 'Y' or full_load_ind == 'Y' or initial_load_ind == 0: #Performing full data refresh for all tables or only tables marked in cdc job control table
            print(f"Performing full load for {current_cat}.{current_db}.{current_table}")
            cdc_table_query = f"""select * from {src_db}.{src_table} """
            #Load data into Target Table
            df_src_cdc = read_data_from_oracle_conn_dsu_cmn(cdc_table_query,trm_scope)
            df_src_cdc.cache()
            src_count = int(df_src_cdc.count())
            print(f"Number of full load records: {src_count}")
            try:
                df_src_cdc.write.mode("overwrite").format("delta").insertInto(f'{current_cat}.{current_db}.{current_table}')
                
            #Update batch control table
                update_sql = f"""
                    UPDATE {current_cat}.{current_db}.cdc_batch_job_control
                    SET initial_load_finished = true 
                    WHERE source_table_name = '{src_table}'
                    and group_name = '{data_load_group}'
                    """
                spark.sql(update_sql)
                print("Updated control table")
            except Exception as e:
                print("Exception message: {}".format(e))
                data_load_failed_tables.append(f'{current_table} "Exception message: {e}"')
            #sample data Match
            #sample_count = round(src_count*.8)
            #if sample_count>1000: sample_count = 1000
            #print(f"Executing sample data match on {sample_count} rows")
            #if primary_keys !='':
            #    sample_data_match(job_name,df_src_cdc,f"{current_cat}.{current_db}.{current_table}",f"{primary_keys}",sample_count,"DELTA_LAKE")
            #try:
            #    if primary_keys !='':
            #        sample_data_match(job_name,df_src_cdc,f"{current_cat}.{current_db}.{current_table}",f"{primary_keys}",sample_count,data_quality_catalog,job_name,"Y","DELTA_LAKE") 
            #    else:
            #        sample_data_match(job_name,df_src_cdc,f"{current_cat}.{current_db}.{current_table}",f"{primary_keys}",sample_count,data_quality_catalog,job_name,"N","DELTA_LAKE")
            #except Exception as e:
            #    print("Exception message: {}".format(e))
            #    print(f"Unable to complete data load for {current_cat}.{current_db}.{current_table}")
            #    data_load_failed_tables.append(f'{current_table}')
            df_src_cdc.unpersist()

        elif cdc_date_col!='' :# cdc date column present in source database
            print(f"Performing cdc load for {current_cat}.{current_db}.{current_table}")
            max_LAST_MOD_TS = spark.sql(f"""select nvl(SUBSTRING_INDEX((cast(max({cdc_date_col})as string)),'.',1),'1900-01-01') 
                                        FROM {current_cat}.{current_db}.{current_table}""").collect()[0][0]
            #cdc_table_query = f"""select * from {src_db}.{src_table} 
            #where {cdc_date_col} >=to_timestamp('{max_LAST_MOD_TS}','YYYY-MM-DD HH24:MI:SS')"""
            table_count_query = f"""select * from {src_db}.{src_table} """
            cmpst_key_ind = 'N'
            print(f'{max_LAST_MOD_TS=}')

            #Apply Delete rows in Target table where PK's are not found in source DB
            if max_LAST_MOD_TS!= '1900-01-01':
                deleted_rec_table_query = f"""select {primary_keys} from {src_db}.{src_table} """
                df_src_full_pk = read_data_from_oracle_conn_dsu_cmn(deleted_rec_table_query,trm_scope)
                df_src_full_pk.createOrReplaceTempView("temp_oracle_deleted")
                
                df_deleted_rec = spark.sql(f"""delete from {current_cat}.{current_db}.{current_table}
                    where concat({primary_keys}) not in (select concat(*) from temp_oracle_deleted)""")
                print(f"Number of deleted records: {df_deleted_rec.count()}")
                
            #Merge data into Target table    
            #df_src_cdc = read_data_from_oracle_conn_dsu_cmn(cdc_table_query,trm_scope)
            #print(f"Number of cdc records: {df_src_cdc.count()}")
            df_src_count = read_data_from_oracle_conn_dsu_cmn(table_count_query,trm_scope)
            df_src_count.cache()
            src_count = int(df_src_count.count())  
            
            df_src_count.createOrReplaceTempView("temp_full_table_data")
            df_src_cdc = spark.sql(f"""select * from temp_full_table_data
            where {cdc_date_col} >=to_timestamp('{max_LAST_MOD_TS}')""")
            print(f"Number of cdc records: {df_src_cdc.count()}")
            
            try:
                merge_cdc_to_main(current_cat, current_db, current_table, df_src_cdc, key_columns, all_columns,cmpst_key_ind)
            except Exception as e:
                print("Exception message: {}".format(e))
                data_load_failed_tables.append(f'{current_table} "Exception message: {e}"')
            #sample data match
            #sample_count = round(src_count*.8)
            #if sample_count>1000:
            #    sample_count = 1000
            #print(f"Executing sample data match on {sample_count} rows")
            #sample_data_match(job_name,df_src_count,f"{current_cat}.{current_db}.{current_table}",f"{primary_keys}",sample_count,"DELTA_LAKE")
            #try:
            #    sample_data_match(job_name,df_src_count,f"{current_cat}.{current_db}.{current_table}",f"{primary_keys}",sample_count,data_quality_catalog,job_name, "Y","DELTA_LAKE")
            #except Exception as e:
            #    print("Exception message: {}".format(e))
            #    print(f"Unable to comlete data load for {current_cat}.{current_db}.{current_table}")
            #    data_load_failed_tables.append(f'{current_table}')
            df_src_count.unpersist()


        elif cdc_date_col=='': # No cdc date column present in source database
            print(f"Performing composite key cdc load for {current_cat}.{current_db}.{current_table}")
            cdc_table_query = f"""select * from {src_db}.{src_table} """
            cmpst_key_ind = 'Y'

            #Apply Delete rows in Target table where PK's are not found in source DB
            deleted_rec_table_query = f"""select {primary_keys} from {src_db}.{src_table} """
            df_src_full_pk = read_data_from_oracle_conn_dsu_cmn(deleted_rec_table_query,trm_scope)
            df_src_full_pk.createOrReplaceTempView("temp_oracle_deleted")
            df_deleted_rec = spark.sql(f"""delete from {current_cat}.{current_db}.{current_table}
                    where concat({primary_keys}) not in (select concat(*) from temp_oracle_deleted)""")
            print(f"Number of deleted records: {df_deleted_rec.count()}")
                
            #Merge data into target table
            df_src_cdc = read_data_from_oracle_conn_dsu_cmn(cdc_table_query,trm_scope)
            df_src_cdc.cache()
            
            src_count = int(df_src_cdc.count())   
            print(f"Number of cdc records: {src_count}")
            try:     
                merge_cdc_to_main(current_cat, current_db, current_table, df_src_cdc, key_columns, all_columns,cmpst_key_ind)
            except Exception as e:
                print("Exception message: {}".format(e))
                data_load_failed_tables.append(f'{current_table} "Exception message: {e}"')
            #sample data Match
            #sample_count = round(src_count*.8)
            #if sample_count>1000: sample_count = 1000
            #print(f"Executing sample data match on {sample_count} rows")
            #sample_data_match(job_name,df_src_cdc,f"{current_cat}.{current_db}.{current_table}",f"{primary_keys}",sample_count,"DELTA_LAKE")
            #try:
            #    sample_data_match(job_name,df_src_cdc,f"{current_cat}.{current_db}.{current_table}",f"{primary_keys}",sample_count,data_quality_catalog,job_name, "Y","DELTA_LAKE")
            #except Exception as e:
            #    print("Exception message: {}".format(e))
            #    print(f"Unable to comlete data load for {current_cat}.{current_db}.{current_table}")
            #    data_load_failed_tables.append(f'{current_table}')
            #df_src_cdc.write.mode("overwrite").format("delta").insertInto(f'{current_cat}.{current_db}.{current_table}')
            df_src_cdc.unpersist()
        
        else:
            dbutils.notebook.exit(f"No criteria satisfied for data load of {current_cat}.{current_db}.{current_table}  ")
        
        end_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
        print(f'{end_ts=}')
        time_elapsed = (end_ts-start_ts)
        print(f'{time_elapsed=}')

        #capture counts after merge:
        #pushdown_query = f"""(select count(*)  from {src_db}.{src_table})"""
        #try:
            #df_read_src_tbl = read_data_from_oracle_conn_dsu_cmn(pushdown_query, trm_scope)
            #src_count = int(df_read_src_tbl.collect()[0][0])
        #except Exception as e:
                    #src_count = None
        
        trgt_query =  f"""(select count(*) from {current_cat}.{current_db}.{current_table})"""
        df_read_trgt_tbl = spark.sql(trgt_query)
        trgt_count = df_read_trgt_tbl.collect()[0][0]

        print(f'{src_count=},{trgt_count=}')
        end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'completed', src_count,trgt_count,"job completed successfully")
    except Exception as e:
        print("Exception message: {}".format(e))
        print(f"Unable to complete data load for {current_cat}.{current_db}.{current_table}")
        end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'failed',0,0,e)
        data_load_failed_tables.append(f'{current_table} "Exception message: {e}" ')
if len(data_load_failed_tables)>0:
    raise Exception(f'Data load failed for {data_load_failed_tables}')

# COMMAND ----------

dbutils.notebook.exit(f"Completed Loading {trgt_catalog}.{database} ")

# COMMAND ----------


