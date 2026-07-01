# Databricks notebook source
# DBTITLE 1,Declare parameters
dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("data_load_group", "", "data_load_group")#group1, group2 or group3-6 
dbutils.widgets.text("full_data_refresh", "N", "full_data_refresh")
dbutils.widgets.text("use_thread_pool_executor_yn", "N", "use_thread_pool_executor_yn")

# COMMAND ----------

# DBTITLE 1,Get Widgets
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
full_data_refresh = dbutils.widgets.get("full_data_refresh")
use_thread_pool_executor_yn = dbutils.widgets.get("use_thread_pool_executor_yn")
config_file_name = "trmreports-conf.yaml"
if dbx_env == "qa":
    dbx_env = "test"
config_file = f"../../config/{dbx_env}/{config_file_name}"
print(f'{config_file=}')

# COMMAND ----------

import pytz
from pytz import timezone
import datacompy
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

# DBTITLE 1,Execute Common Function ntbk
# MAGIC %run  ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# MAGIC %run ../shared/ntb_ttab_brnz_table_list_optmz

# COMMAND ----------

# DBTITLE 1,Set configuration variables
common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
src_db_name = common_configs['schema']['src_db_name']
src_name = "ttabp"
rdate = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).date()
rday = rdate.strftime("%A")

data_load_group = dbutils.widgets.get("data_load_group")
schema_metadata = src_name+"_metadata_"+data_load_group
ttab_scope = common_configs['secrets']['ttab_ttabp_scope']
 
spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.trgt_catalog', trgt_catalog.lower()) 
spark.conf.set('config.ttab_scope', ttab_scope.lower()) 
spark.sql(f"set SRC_SYS_NAME = src_db_name")
database = 'bronze'
control_table = 'cdc_batch_job_control'

spark.conf.set('config.schema_metadata', schema_metadata.lower())
print(f'{src_db_name=},{trgt_catalog=}, {data_quality_catalog=},{ttab_scope=},{schema_metadata=}')

# COMMAND ----------

# DBTITLE 1,,Filter Tables from Control Table where Full Load = 'N' and initial load finished ='Y'
df_control_table = spark.sql(f"""select catalog_name,database_name,table_name,source_db_name,source_table_name,primary_keys,full_load,decode(initial_load_finished,false,0,1) as initial_load_finished from {trgt_catalog}.bronze.cdc_batch_job_control
where group_name = '{data_load_group}'
--and full_load = 'N'
--and initial_load_finished= true 
""")

df_control_table.display()


# COMMAND ----------

# DBTITLE 1,Get CDC Timestamp Column name from table list ntbk
schema_def = ["TABLE_GROUP_NAME","TABLE_NAME","FULL_LOAD","DQ_FLTR","LARGE_TABLE_IND","ZORDER_Columns","NUM_PARTITIONS","FETCH_SIZE",'PART_COLUMN','LOWER_BOUND','UPPER_BOUND']
df_schema_metadata = spark.createDataFrame(data = eval(schema_metadata), schema = schema_def)
# removed following code as load groups are already seperated by daily and weekly
#if rday !='Sunday':
    #df_schema_metadata= df_schema_metadata.filter("TABLE_GROUP_NAME = 'daily_load'")
df_schema_metadata = df_schema_metadata.select(f.upper('TABLE_NAME').alias("TABLE_NAME"),'DQ_FLTR',"NUM_PARTITIONS","FETCH_SIZE",'PART_COLUMN','LOWER_BOUND','UPPER_BOUND').distinct()
job_control_df = df_control_table.alias("df_cntl").join(df_schema_metadata.alias("df_dq_fltr"),(f.col("df_cntl.source_table_name") == f.col("df_dq_fltr.TABLE_NAME")),"inner")
job_control_df.display()
job_control_parameters = job_control_df.collect()

# COMMAND ----------

# DBTITLE 1,Define Merge Function
#########################################################################
## Function for merging CDC input to main table
#########################################################################
def merge_cdc_to_main(target_catalog, target_db, target_table, cdc_df, key_columns, all_columns,composite_key_ind):
    """
    Merges Change Data Capture (CDC) updates into the main table.

    This function performs an upsert operation (update and insert) by comparing 
    records in the CDC input DataFrame (`cdc_df`) with the target table. The 
    merge is based on the specified key columns and other attributes.

    Parameters:
        target_catalog (str): The catalog name of the target table.
        target_db (str): The database name of the target table.
        target_table (str): The name of the target table.
        cdc_df (DataFrame): The CDC input DataFrame containing new and updated records.
        key_columns (list): List of primary key columns for uniquely identifying records.
        all_columns (list): List of all column names in the target table.
        composite_key_ind (str): Indicator ('Y' or 'N') specifying if composite key comparison is needed.

    Returns:
        None: Displays the result of the merge operation or logs an error if the operation fails.
    """
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

# DBTITLE 1,Except group7
# Execute the custom CDC load logic for all groups except group7
if data_load_group != "group7":
    data_load_failed_tables = []
    for job_control in job_control_parameters:
        current_cat = job_control['catalog_name']
        current_db  = job_control['database_name']
        current_table = job_control['table_name']
        primary_keys = job_control['primary_keys']
        src_db = job_control['source_db_name']
        src_table = job_control['source_table_name']
        cdc_date_col = job_control['DQ_FLTR']
        numPartitions = job_control['NUM_PARTITIONS']
        fetchsize = job_control['FETCH_SIZE']
        partitionColumn = job_control['PART_COLUMN']
        lowerBound = str(job_control['LOWER_BOUND'])  # fix isdigit check
        upperBound = str(job_control['UPPER_BOUND'])  # fix isdigit check
        full_load_ind = job_control['full_load']
        initial_load_ind = job_control['initial_load_finished']
        job_name = f'ntb_{src_name}_{current_table}_brnz_load'
        numPartitions = 0 if numPartitions in ["", None] else int(numPartitions)

        print(f"******************************************** \n Now processing: {src_db}.{src_table}")

        start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
        print(f'{start_ts=}')
        control_dt = begin_job_cntl(f'{trgt_catalog}.silver', job_name, start_ts)

        try:
            key_columns = [item.strip().lower() for item in primary_keys.split(",")]

            all_columns = spark.table(f"{current_cat}.{current_db}.{current_table}").columns
            all_columns = [x.lower() for x in all_columns]
            print(f"all_columns= {all_columns}")
            print(f"key_columns= {primary_keys}")
            print(f"{cdc_date_col=}")

            print(f"partitionColumn= {partitionColumn}")
            print(f"numPartitions= {numPartitions}")
            print(f"lowerBound= {lowerBound}")
            print(f"upperBound= {upperBound}")

            if numPartitions >= 1 and full_load_ind == 'Y':
                if lowerBound.isdigit() and upperBound.isdigit():
                    lowerBound = int(lowerBound)
                    upperBound = int(upperBound)
                    fetchsize = fetchsize if fetchsize not in ["", None] else 10000
                    options = {
                        "numPartitions": numPartitions,
                        "fetchsize": fetchsize,
                        "partitionColumn": partitionColumn,
                        "lowerBound": lowerBound,
                        "upperBound": upperBound,
                    }
                else:
                    options = {
                        "numPartitions": numPartitions,
                        "fetchsize": fetchsize,
                        "partitionColumn": partitionColumn,
                        "lowerBound": lowerBound,
                        "upperBound": upperBound,
                        "sessionInitStatement": "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'",
                    }
            else:
                options = {"fetchsize": 10000}
        except Exception as e:
            print("Exception message: {}".format(e))
            print(f"Unable to complete data load for {current_cat}.{current_db}.{current_table} because there was an issue with the partition or primary key configuration.")
            end_job_cntl(f"{trgt_catalog}.silver", job_name, start_ts, 'failed', 0, e, 0)
            data_load_failed_tables.append(f'{current_table}')
            continue

        try:
            if full_data_refresh == 'Y' or full_load_ind == 'Y' or initial_load_ind == 0:
                print(f"Performing full load for {current_cat}.{current_db}.{current_table}")
                cdc_table_query = f"select * from {src_db}.{src_table}"
                df_src_cdc = read_data_from_oracle_conn_dsu_opt(cdc_table_query, ttab_scope, options)
                df_src_cdc.cache()
                src_count = int(df_src_cdc.count())
                print(f"Number of full load records: {src_count}")

                target_columns = spark.table(f"{current_cat}.{current_db}.{current_table}").columns
                for c in df_src_cdc.columns:
                    df_src_cdc = df_src_cdc.withColumnRenamed(c, c.replace('$', ''))
                df_src_cdc = df_src_cdc.select(*target_columns)

                try:
                    df_src_cdc.write.mode("overwrite").format("delta").insertInto(f'{current_cat}.{current_db}.{current_table}')
                    update_sql = f"""
                        UPDATE {current_cat}.{current_db}.cdc_batch_job_control
                        SET initial_load_finished = true 
                        WHERE source_table_name = '{src_table}'
                    """
                    spark.sql(update_sql)
                    print("Updated control table")
                except Exception:
                    data_load_failed_tables.append(f'{current_table}')
                df_src_cdc.unpersist()

            elif cdc_date_col != '':
                print(f"Performing cdc load for {current_cat}.{current_db}.{current_table}")
                max_LAST_MOD_TS = spark.sql(f"""
                    select nvl(SUBSTRING_INDEX((cast(max({cdc_date_col}) as string)),'.',1),'1900-01-01')
                    FROM {current_cat}.{current_db}.{current_table}
                """).collect()[0][0]

                table_count_query = f"select * from {src_db}.{src_table}"
                cmpst_key_ind = 'N'
                print(f'{max_LAST_MOD_TS=}')

                if max_LAST_MOD_TS != '1900-01-01':
                    deleted_rec_table_query = f"select {primary_keys} from {src_db}.{src_table}"
                    df_src_full_pk = read_data_from_oracle_conn_dsu_opt(deleted_rec_table_query, ttab_scope, options)
                    df_src_full_pk.createOrReplaceTempView("temp_oracle_deleted")
                    df_deleted_rec = spark.sql(f"""
                        delete from {current_cat}.{current_db}.{current_table}
                        where concat({primary_keys}) not in (select concat(*) from temp_oracle_deleted)
                    """)
                    print(f"Number of deleted records: {df_deleted_rec.count()}")

                df_src_count = read_data_from_oracle_conn_dsu_opt(table_count_query, ttab_scope, options)
                df_src_count.cache()
                src_count = int(df_src_count.count())
                df_src_count.createOrReplaceTempView("temp_full_table_data")
                df_src_cdc = spark.sql(f"""
                    select * from temp_full_table_data
                    where {cdc_date_col} >= to_timestamp('{max_LAST_MOD_TS}')
                """)
                print(f"Number of cdc records: {df_src_cdc.count()}")

                try:
                    merge_cdc_to_main(current_cat, current_db, current_table, df_src_cdc, key_columns, all_columns, cmpst_key_ind)
                except Exception:
                    data_load_failed_tables.append(f'{current_table}')
                df_src_count.unpersist()

            else:
                print(f"Performing composite key cdc load for {current_cat}.{current_db}.{current_table}")
                cdc_table_query = f"select * from {src_db}.{src_table}"
                cmpst_key_ind = 'Y'

                deleted_rec_table_query = f"select {primary_keys} from {src_db}.{src_table}"
                df_src_full_pk = read_data_from_oracle_conn_dsu_opt(deleted_rec_table_query, ttab_scope, options)
                df_src_full_pk.createOrReplaceTempView("temp_oracle_deleted")
                df_deleted_rec = spark.sql(f"""
                    delete from {current_cat}.{current_db}.{current_table}
                    where concat({primary_keys}) not in (select concat(*) from temp_oracle_deleted)
                """)
                print(f"Number of deleted records: {df_deleted_rec.count()}")

                df_src_cdc = read_data_from_oracle_conn_dsu_opt(cdc_table_query, ttab_scope, options)
                df_src_cdc.cache()
                src_count = int(df_src_cdc.count())
                print(f"Number of cdc records: {src_count}")

                try:
                    merge_cdc_to_main(current_cat, current_db, current_table, df_src_cdc, key_columns, all_columns, cmpst_key_ind)
                except Exception:
                    data_load_failed_tables.append(f'{current_table}')
                df_src_cdc.unpersist()

            end_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
            print(f'{end_ts=}')
            time_elapsed = (end_ts - start_ts)
            print(f'{time_elapsed=}')

            trgt_query = f"(select count(*) from {current_cat}.{current_db}.{current_table})"
            trgt_count = spark.sql(trgt_query).collect()[0][0]

            print(f'{src_count=},{trgt_count=}')
            end_job_cntl(f"{trgt_catalog}.silver", job_name, start_ts, 'completed',
                         src_count, "job completed successfully", trgt_count)
        except Exception as e:
            print("Exception message: {}".format(e))
            print(f"Unable to complete data load for {current_cat}.{current_db}.{current_table}")
            end_job_cntl(f"{trgt_catalog}.silver", job_name, start_ts, 'failed', 0, e, 0)
            data_load_failed_tables.append(f'{current_table}')

    if data_load_failed_tables:
        raise Exception(f'Data load failed for {data_load_failed_tables}')

# COMMAND ----------

# DBTITLE 1,ttabwf- Group7
# Execute the custom CDC load logic only for group7
if data_load_group == "group7":
    data_load_failed_tables = []
    for job_control in job_control_parameters:
        current_cat = job_control['catalog_name']
        current_db  = job_control['database_name']
        current_table = job_control['table_name']
        primary_keys = job_control['primary_keys']
        src_db = job_control['source_db_name']
        src_table = job_control['source_table_name']
        cdc_date_col = job_control['DQ_FLTR']
        numPartitions = job_control['NUM_PARTITIONS']
        fetchsize = job_control['FETCH_SIZE']
        partitionColumn = job_control['PART_COLUMN']
        lowerBound = str(job_control['LOWER_BOUND'])
        upperBound = str(job_control['UPPER_BOUND'])
        full_load_ind = job_control['full_load']
        initial_load_ind = job_control['initial_load_finished']
        job_name = f'ntb_{src_name}_{current_table}_brnz_load'
        numPartitions = 0 if numPartitions in ["", None] else int(numPartitions)

        print(f"******************************************** \n Now processing: {src_db}.{src_table}")

        start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
        print(f'{start_ts=}')
        control_dt = begin_job_cntl(f'{trgt_catalog}.silver', job_name, start_ts)

        try:
            key_columns = [item.strip().lower() for item in primary_keys.split(",")]

            all_columns = spark.table(f"{current_cat}.{current_db}.{current_table}").columns
            all_columns = [x.lower() for x in all_columns]
            print(f"all_columns= {all_columns}")
            print(f"key_columns= {primary_keys}")
            print(f"{cdc_date_col=}")

            print(f"partitionColumn= {partitionColumn}")
            print(f"numPartitions= {numPartitions}")
            print(f"lowerBound= {lowerBound}")
            print(f"upperBound= {upperBound}")

            if numPartitions >= 1 and full_load_ind == 'Y':
                if lowerBound.isdigit() and upperBound.isdigit():
                    lowerBound = int(lowerBound)
                    upperBound = int(upperBound)
                    fetchsize = fetchsize if fetchsize not in ["", None] else 20000
                    options = {
                        "numPartitions": numPartitions,
                        "fetchsize": fetchsize,
                        "partitionColumn": partitionColumn,
                        "lowerBound": lowerBound,
                        "upperBound": upperBound,
                    }
                else:
                    options = {
                        "numPartitions": numPartitions,
                        "fetchsize": fetchsize,
                        "partitionColumn": partitionColumn,
                        "lowerBound": lowerBound,
                        "upperBound": upperBound,
                        "sessionInitStatement": "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'",
                    }
            else:
                options = {"fetchsize": 20000}
        except Exception as e:
            print("Exception message: {}".format(e))
            print(f"Unable to complete data load for {current_cat}.{current_db}.{current_table} because there was an issue with the partition or primary key configuration.")
            end_job_cntl(f"{trgt_catalog}.silver", job_name, start_ts, 'failed', 0, e, 0)
            data_load_failed_tables.append(f'{current_table}')
            continue

        try:
            if full_data_refresh == 'Y' or full_load_ind == 'Y' or initial_load_ind == 0:
                print(f"Performing full load for {current_cat}.{current_db}.{current_table}")
                cdc_table_query = f"select * from {src_db}.{src_table}"
                df_src_cdc = read_data_from_oracle_conn_dsu_opt(cdc_table_query, ttab_scope, options)
                src_count = int(df_src_cdc.count())
                print(f"Number of full load records: {src_count}")

                target_columns = spark.table(f"{current_cat}.{current_db}.{current_table}").columns
                for c in df_src_cdc.columns:
                    df_src_cdc = df_src_cdc.withColumnRenamed(c, c.replace('$', ''))
                df_src_cdc = df_src_cdc.select(*target_columns)

                try:
                    df_src_cdc.write.mode("overwrite").format("delta").insertInto(f'{current_cat}.{current_db}.{current_table}')
                    update_sql = f"""
                        UPDATE {current_cat}.{current_db}.cdc_batch_job_control
                        SET initial_load_finished = true 
                        WHERE source_table_name = '{src_table}'
                    """
                    spark.sql(update_sql)
                    print("Updated control table")
                except Exception:
                    data_load_failed_tables.append(f'{current_table}')

            elif cdc_date_col != '':
                print(f"Performing cdc load for {current_cat}.{current_db}.{current_table}")
                max_LAST_MOD_TS = spark.sql(f"""
                    select nvl(SUBSTRING_INDEX((cast(max({cdc_date_col}) as string)),'.',1),'1900-01-01')
                    FROM {current_cat}.{current_db}.{current_table}
                """).collect()[0][0]

                table_count_query = f"select * from {src_db}.{src_table}"
                cmpst_key_ind = 'N'
                print(f'{max_LAST_MOD_TS=}')

                if max_LAST_MOD_TS != '1900-01-01':
                    deleted_rec_table_query = f"select {primary_keys} from {src_db}.{src_table}"
                    df_src_full_pk = read_data_from_oracle_conn_dsu_opt(deleted_rec_table_query, ttab_scope, options)
                    df_src_full_pk.createOrReplaceTempView("temp_oracle_deleted")
                    df_deleted_rec = spark.sql(f"""
                        delete from {current_cat}.{current_db}.{current_table}
                        where concat({primary_keys}) not in (select concat(*) from temp_oracle_deleted)
                    """)
                    print(f"Number of deleted records: {df_deleted_rec.count()}")

                df_src_count = read_data_from_oracle_conn_dsu_opt(table_count_query, ttab_scope, options)
                src_count = int(df_src_count.count())
                df_src_count.createOrReplaceTempView("temp_full_table_data")
                df_src_cdc = spark.sql(f"""
                    select * from temp_full_table_data
                    where {cdc_date_col} >= to_timestamp('{max_LAST_MOD_TS}')
                """)
                print(f"Number of cdc records: {df_src_cdc.count()}")

                try:
                    merge_cdc_to_main(current_cat, current_db, current_table, df_src_cdc, key_columns, all_columns, cmpst_key_ind)
                except Exception:
                    data_load_failed_tables.append(f'{current_table}')

            else:
                print(f"Performing composite key cdc load for {current_cat}.{current_db}.{current_table}")
                cdc_table_query = f"select * from {src_db}.{src_table}"
                cmpst_key_ind = 'Y'

                deleted_rec_table_query = f"select {primary_keys} from {src_db}.{src_table}"
                df_src_full_pk = read_data_from_oracle_conn_dsu_opt(deleted_rec_table_query, ttab_scope, options)
                df_src_full_pk.createOrReplaceTempView("temp_oracle_deleted")
                df_deleted_rec = spark.sql(f"""
                    delete from {current_cat}.{current_db}.{current_table}
                    where concat({primary_keys}) not in (select concat(*) from temp_oracle_deleted)
                """)
                print(f"Number of deleted records: {df_deleted_rec.count()}")

                df_src_cdc = read_data_from_oracle_conn_dsu_opt(cdc_table_query, ttab_scope, options)
                src_count = int(df_src_cdc.count())
                print(f"Number of cdc records: {src_count}")

                try:
                    merge_cdc_to_main(current_cat, current_db, current_table, df_src_cdc, key_columns, all_columns, cmpst_key_ind)
                except Exception:
                    data_load_failed_tables.append(f'{current_table}')

            end_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
            print(f'{end_ts=}')
            time_elapsed = (end_ts - start_ts)
            print(f'{time_elapsed=}')

            trgt_query = f"(select count(*) from {current_cat}.{current_db}.{current_table})"
            trgt_count = spark.sql(trgt_query).collect()[0][0]

            print(f'{src_count=},{trgt_count=}')
            end_job_cntl(f"{trgt_catalog}.silver", job_name, start_ts, 'completed', src_count,
                         "job completed successfully", trgt_count)
        except Exception as e:
            print("Exception message: {}".format(e))
            print(f"Unable to complete data load for {current_cat}.{current_db}.{current_table}")
            end_job_cntl(f"{trgt_catalog}.silver", job_name, start_ts, 'failed', 0, e, 0)
            data_load_failed_tables.append(f'{current_table}')

    if data_load_failed_tables:
        raise Exception(f'Data load failed for {data_load_failed_tables}')

# COMMAND ----------

dbutils.notebook.exit(f"Completed Loading {trgt_catalog}.{database} ")
