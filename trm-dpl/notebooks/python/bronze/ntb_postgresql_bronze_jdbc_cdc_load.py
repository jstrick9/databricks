# Databricks notebook source
pip install datacompy

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports
import pytz
from pytz import timezone
from delta.tables import DeltaTable
import datetime
import yaml
import builtins
from pyspark.sql.functions import col, upper
import concurrent.futures, traceback

# COMMAND ----------

# DBTITLE 1,Widgets
# Define widgets
dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")
dbutils.widgets.text("full_data_refresh", "N", "full_data_refresh")

# Retrieve widget values
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME").rstrip()
src_name = SRC_SYS_NAME.lower()
full_data_refresh = dbutils.widgets.get("full_data_refresh")
dbutils.widgets.getAll()

# COMMAND ----------

# DBTITLE 1,Config file widget
# Load config YAML from mounted path
config_file = f"../../config/{dbx_env}/{SRC_SYS_NAME.lower()}-conf.yaml"
with open(config_file, 'r') as f:
    common_configs = yaml.safe_load(f)
print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Execute common function ntbk
# MAGIC %run  ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# MAGIC %run ../shared/ntb_tm_brnz_table_list

# COMMAND ----------

# DBTITLE 1,Set Parameter Values
trgt_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
src_db_name = common_configs['schema']['src_db_name'].upper()
cdc_path = common_configs['cdc']['src_csv_files']


src_folder = common_configs['cdc']['src_csv_files']
schema_metadata = f"{SRC_SYS_NAME.lower()}_metadata"


src_database = common_configs['cdc']['src_database']
trm_scope = common_configs['secrets']['trm_scope']

spark.conf.set('config.data_quality_db', common_configs['schema']['data_quality_catalog'].lower())
spark.conf.set('config.trgt_catalog', common_configs['schema']['trgt_catalog'].lower())
spark.conf.set('config.trm_scope', trm_scope.lower())
spark.conf.set('config.schema_metadata', schema_metadata.lower())

print(f"src_db_name={common_configs['schema']['src_db_name'].upper()}, trgt_catalog={common_configs['schema']['trgt_catalog']}, data_quality_catalog={common_configs['schema']['data_quality_catalog']}, trm_scope={trm_scope}, schema_metadata={schema_metadata}, src_folder={src_folder}")

# COMMAND ----------

df_control = spark.sql(f"""
        SELECT catalog_name,
               database_name,
               table_name,
               source_db_name,
               source_table_name,
               primary_keys,
               full_load,
               decode(initial_load_finished,false,0,1) AS initial_load_finished
        FROM {trgt_catalog}.bronze.cdc_batch_job_control
"""
)

# COMMAND ----------

display(df_control)

# COMMAND ----------

eval(schema_metadata)

# COMMAND ----------

# Define schema and create DataFrame
schema_def = ["TABLE_GROUP_NAME", "TABLE_NAME", "FULL_LOAD", "DQ_FLTR"]
schema_metadata_list = eval(schema_metadata)
df_schema_metadata = spark.createDataFrame(data=schema_metadata_list, schema=schema_def)

# Rename 'TABLE_NAME' to 'schema_table_name' to prevent ambiguity
df_schema_metadata = df_schema_metadata.withColumnRenamed(
    "TABLE_NAME", "schema_table_name"
)

# Select and transform necessary columns
df_schema_metadata = df_schema_metadata.select(
    upper("schema_table_name").alias("schema_table_name"),
    "DQ_FLTR",
).distinct()

# Perform the join using the renamed column
job_control_df = df_control.alias("c").join(
    df_schema_metadata.alias("m"),
    upper(f.col("c.source_table_name")) == col("m.schema_table_name"),
    "inner",
)

# Convert to list of dicts
job_control_parameters = [row.asDict() for row in job_control_df.collect()]

# COMMAND ----------

display(df_schema_metadata)

# COMMAND ----------

# DBTITLE 1,Define Merge Function
def merge_cdc_to_main(target_catalog: str,
                      target_db: str,
                      target_table: str,
                      cdc_df,
                      key_columns: list,
                      all_columns: list,
                      composite_key_ind: str):
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
    # register global temp view
    cdc_df.createOrReplaceGlobalTempView(tempview_name)

    # prepare columns
    other_cols = all_columns.copy()
    for k in key_columns:
        other_cols.remove(k)

    # Build ON condition
    on_cond = " AND ".join([f"t.{k}=u.{k}" for k in key_columns])

    # Build WHEN MATCHED clause
    if composite_key_ind == 'Y':
        t_cols = ",".join([f"t.{c}" for c in other_cols])
        u_cols = ",".join([f"u.{c}" for c in other_cols])
        match_clause = f"WHEN MATCHED AND CONCAT({t_cols}) != CONCAT({u_cols}) THEN UPDATE SET "
    else:
        match_clause = "WHEN MATCHED THEN UPDATE SET "

    # Build update assignments
    update_assigns = ", ".join([f"{c}=u.{c}" for c in other_cols])

    # Build INSERT columns & values
    insert_cols = ",".join(all_columns)
    insert_vals = ",".join([f"u.{c}" for c in all_columns])

    merge_sql = f"""
    MERGE INTO {target_catalog}.{target_db}.{target_table} t
    USING global_temp.{tempview_name} u
      ON {on_cond}
      {match_clause} {update_assigns}
      WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """

    try:
        result_df = spark.sql(merge_sql)
        display(result_df)
    except Exception as e:
        print(f"Exception merging into {target_catalog}.{target_db}.{target_table}: {e}")

# COMMAND ----------

def process_table(job_control):
    """
    Processes a specific table based on job control parameters. This includes:
    - Performing full data refresh or CDC (Change Data Capture) operations.
    - Handling data deletion and merging.
    - Validating data through sample matching.
    - Managing job control updates and logging.

    Args:
        job_control (dict): A dictionary containing the following keys:
            - catalog_name (str): Target catalog name.
            - database_name (str): Target database name.
            - table_name (str): Target table name.
            - primary_keys (str): Comma-separated list of primary key column names.
            - source_db_name (str): Source database name.
            - source_table_name (str): Source table name.
            - DQ_FLTR (str): CDC filter column name for date-based filtering.
            - full_load (str): Indicator ('Y' or 'N') for performing a full load.
            - initial_load_finished (int): Indicator (0 or 1) for whether the initial load is complete.

    Raises:
        Exception: If data load fails for one or more tables.
    """
    try:
        data_load_failed_tables = []
        current_cat = job_control["catalog_name"]
        current_db = job_control["database_name"]
        current_table = job_control["table_name"]
        primary_keys = job_control["primary_keys"]
        src_db = job_control["source_db_name"]
        src_table = job_control["source_table_name"]
        cdc_date_col = job_control["DQ_FLTR"]
        full_load_ind = job_control["full_load"]
        initial_load_ind = job_control["initial_load_finished"]
        job_name = f"ntb_{src_name}_{current_table}_brnz_load"
        print(
            f"******************************************** \nNow processing: {src_db}.{src_table} \n"
        )

        key_columns = [item.strip().lower() for item in primary_keys.split(",")]
        target_table_path = f"{current_cat}.{current_db}.{current_table}"
        all_columns = spark.table(target_table_path).columns
        all_columns = [x.lower() for x in all_columns]
        print(f"{target_table_path} all_columns= {all_columns} \n")
        print(f"{target_table_path} key_columns= {primary_keys} \n")
        print(f"{target_table_path} {cdc_date_col=} \n")

        start_ts = datetime.datetime.now().astimezone(pytz.timezone("US/Eastern"))
        print(f"{target_table_path} {start_ts=} \n")
        control_dt = begin_job_cntl(
            f"{data_quality_catalog}", f"{trgt_catalog}.silver", job_name, start_ts
        )

        if (
            full_data_refresh == "Y"
            or full_load_ind == "Y"
            or initial_load_ind == 0
        ):  
            print(
                f"Performing full load for {target_table_path}"
            )
            cdc_table_query = f"""select * from {src_db}.{src_table} """
            df_src_cdc = read_data_from_postgres_conn(
                cdc_table_query, trm_scope
            )
            df_src_cdc.cache()
            src_count = int(df_src_cdc.count())
            print(f"Number of full load records: {src_count}")
            try:
                df_src_cdc.write.mode("overwrite").format("delta").insertInto(
                    f"{target_table_path}"
                )

                update_sql = f"""
                    UPDATE {current_cat}.{current_db}.cdc_batch_job_control
                    SET initial_load_finished = true 
                    WHERE source_table_name = '{src_table}'
                    """
                spark.sql(update_sql)
                print("Updated control table")
            except:
                data_load_failed_tables.append(f"{current_table}")

            # sample_count = round(src_count * 0.8)
            # if sample_count > 1000:
            #     sample_count = 1000
            # print(f"Executing sample data match on {sample_count} rows")
            # try:
            #     if primary_keys != "":
            #         sample_data_match(
            #             job_name,
            #             df_src_cdc,
            #             f"{target_table_path}",
            #             f"{primary_keys}",
            #             sample_count,
            #             data_quality_catalog,
            #             job_name,
            #             "Y",
            #             "DELTA_LAKE",
            #         )
            #     else:
            #         sample_data_match(
            #             job_name,
            #             df_src_cdc,
            #             f"{target_table_path}",
            #             f"{primary_keys}",
            #             sample_count,
            #             data_quality_catalog,
            #             job_name,
            #             "N",
            #             "DELTA_LAKE",
            #         )
            # except Exception as e:
            #     print("Exception message: {}".format(e))
            #     print(
            #         f"Unable to comlete data load for {target_table_path}"
            #     )
            #     data_load_failed_tables.append(f"{current_table}")

            df_src_cdc.unpersist()

        elif cdc_date_col != "":
            print(
                f"Performing cdc load for {target_table_path}"
            )
            max_LAST_MOD_TS = spark.sql(
                f"""select nvl(SUBSTRING_INDEX((cast(max({cdc_date_col})as string)),'.',1),'1900-01-01') 
                                        FROM {target_table_path}"""
            ).collect()[0][0]
            table_count_query = f"""select * from {src_db}.{src_table} """
            cmpst_key_ind = "N"
            print(f"{max_LAST_MOD_TS=}")

            if max_LAST_MOD_TS != "1900-01-01":
                deleted_rec_table_query = (
                    f"""select {primary_keys} from {src_db}.{src_table} """
                )
                df_src_full_pk = read_data_from_postgres_conn(
                    deleted_rec_table_query, trm_scope
                )
                df_src_full_pk.createOrReplaceTempView("temp_oracle_deleted")

                df_deleted_rec = spark.sql(
                    f"""delete from {target_table_path}
                    where concat({primary_keys}) not in (select concat(*) from temp_oracle_deleted)"""
                )
                print(f"Number of deleted records: {df_deleted_rec.count()}")

            df_src_count = read_data_from_postgres_conn(
                table_count_query, trm_scope
            )
            df_src_count.cache()
            src_count = int(df_src_count.count())

            df_src_count.createOrReplaceTempView("temp_full_table_data")
            df_src_cdc = spark.sql(
                f"""select * from temp_full_table_data
            where {cdc_date_col} >=to_timestamp('{max_LAST_MOD_TS}')"""
            )
            print(f"Number of cdc records: {df_src_cdc.count()}")

            try:
                merge_cdc_to_main(
                    current_cat,
                    current_db,
                    current_table,
                    df_src_cdc,
                    key_columns,
                    all_columns,
                    cmpst_key_ind,
                )
            except:
                data_load_failed_tables.append(f"{current_table}")

            # sample_count = round(src_count * 0.8)
            # if sample_count > 1000:
            #     sample_count = 1000
            # print(f"Executing sample data match on {sample_count} rows")
            # try:
            #     sample_data_match(
            #         job_name,
            #         df_src_count,
            #         f"{target_table_path}",
            #         f"{primary_keys}",
            #         sample_count,
            #         data_quality_catalog,
            #         job_name,
            #         "Y",
            #         "DELTA_LAKE",
            #     )
            # except Exception as e:
            #     print("Exception message: {}".format(e))
            #     print(
            #         f"Unable to comlete data load for {target_table_path}"
            #     )
            #     data_load_failed_tables.append(f"{current_table}")
            
            df_src_count.unpersist()

        elif cdc_date_col == "":  
            print(
                f"Performing composite key cdc load for {target_table_path}"
            )
            cdc_table_query = f"""select * from {src_db}.{src_table} """
            cmpst_key_ind = "Y"

            deleted_rec_table_query = (
                f"""select {primary_keys} from {src_db}.{src_table} """
            )
            df_src_full_pk = read_data_from_postgres_conn(
                deleted_rec_table_query, trm_scope
            )
            df_src_full_pk.createOrReplaceTempView("temp_oracle_deleted")
            df_deleted_rec = spark.sql(
                f"""delete from {target_table_path}
                    where concat({primary_keys}) not in (select concat(*) from temp_oracle_deleted)"""
            )
            print(f"Number of deleted records: {df_deleted_rec.count()}")

            df_src_cdc = read_data_from_postgres_conn(
                cdc_table_query, trm_scope
            )
            df_src_cdc.cache()

            src_count = int(df_src_cdc.count())
            print(f"Number of cdc records: {src_count}")
            try:
                merge_cdc_to_main(
                    current_cat,
                    current_db,
                    current_table,
                    df_src_cdc,
                    key_columns,
                    all_columns,
                    cmpst_key_ind,
                )
            except:
                data_load_failed_tables.append(f"{current_table}")

            # sample_count = round(src_count * 0.8)
            # if sample_count > 1000:
            #     sample_count = 1000
            # print(f"Executing sample data match on {sample_count} rows")

            # try:
            #     sample_data_match(
            #         job_name,
            #         df_src_cdc,
            #         f"{target_table_path}",
            #         f"{primary_keys}",
            #         sample_count,
            #         data_quality_catalog,
            #         job_name,
            #         "Y",
            #         "DELTA_LAKE",
            #     )
            # except Exception as e:
            #     print("Exception message: {}".format(e))
            #     print(
            #         f"Unable to complete data load for {target_table_path}\n"
            #     )
            #     data_load_failed_tables.append(f"{current_table}")

            df_src_cdc.unpersist()

        else:
            dbutils.notebook.exit(
                f"No criteria satisfied for data load of {target_table_path}\n"
            )

        # Verify counts
        trgt_count = spark.table(target_table_path).count()
        print(f"Verification: src_count={src_count}, trgt_count={trgt_count}")
        
        # if the target count is less than 95% of the source count, fail immediately
        if trgt_count < src_count * 0.95:
            raise ValueError(f"Count mismatch: src={src_count} tgt={trgt_count}")

        end_ts = datetime.datetime.now().astimezone(pytz.timezone("US/Eastern"))
        print(f"Load successful at {end_ts} (elapsed {end_ts - start_ts})")
        end_job_cntl(
            data_quality_catalog, f"{trgt_catalog}.silver", job_name,
            start_ts, 'completed', src_count, trgt_count,
            'job completed successfully'
        )
        return f"Processing complete for {target_table_path}\n"

        if len(data_load_failed_tables) > 0:
            raise Exception(f"Data load failed for {data_load_failed_tables}")

    except Exception as e:
        return f"Error processing {job_control['table_name']}: {str(e)} \n"

# COMMAND ----------

def process_table_wrapper(jc: dict):
    """
    Runs process_table, capturing success/failure.
    Returns: (table_name: str, success: bool, message: str)
    """
    tbl = jc["table_name"]
    try:
        msg = process_table(jc)
        # Check if the message indicates an error
        if isinstance(msg, str) and msg.startswith("Error processing"):
            return tbl, False, msg
        return tbl, True, msg
    except Exception as e:
        tb = traceback.format_exc()
        # log failure control
        end_job_cntl(
          data_quality_catalog,
          f"{trgt_catalog}.silver",
          f"ntb_{src_name}_{tbl}_brnz_load",
          datetime.datetime.now(timezone("US/Eastern")),
          "failed", 0, 0,
          str(e)
        )
        return tbl, False, f"{e}\n{tb}"

# COMMAND ----------

max_threads = min(max(2, spark.sparkContext.defaultParallelism//2), 8)

print(f"Number of threads: {max_threads}")

# COMMAND ----------

# Set source_db_name to TURM for JBTEASPS
for jc in job_control_parameters:
    if SRC_SYS_NAME.upper() == "JBTEASPS":
        jc["source_db_name"] = "TURM"

results = {}

with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
    future_to_table = {
        executor.submit(process_table_wrapper, jc): jc["table_name"]
        for jc in job_control_parameters
    }
    for future in concurrent.futures.as_completed(future_to_table):
        tbl = future_to_table[future]
        try:
            tbl_name, success, msg = future.result()
        except Exception as e:
            success, msg = False, f"UNEXPECTED exception: {e}"
        results[tbl] = (success, msg)
        print(f"[{'OK' if success else 'ERR'}] {tbl}: {msg}")

to_rerun = [jc for jc in job_control_parameters if not results[jc["table_name"]][0]]

if to_rerun:
    print(f"\nRe-processing {len(to_rerun)} failed tables sequentially…\n")
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future_to_table = {
            executor.submit(process_table_wrapper, jc): jc["table_name"]
            for jc in to_rerun
        }
        for future in concurrent.futures.as_completed(future_to_table):
            tbl = future_to_table[future]
            try:
                tbl_name, success, msg = future.result()
            except Exception as e:
                success, msg = False, f"UNEXPECTED exception: {e}"
            results[tbl] = (success, msg)
            print(f"[{'OK' if success else 'ERR'}] retry {tbl}: {msg}")

# COMMAND ----------

summary = ", ".join(f"{tbl}:{'OK' if ok else 'ERR'} - {msg}"
                    for tbl, (ok, msg) in results.items())

dbutils.notebook.exit(f"Completed Loading {trgt_catalog}.bronze. Results: {summary}")
