# Databricks notebook source
# Install required packages
%pip install --upgrade pip tableauhyperapi tableauserverclient hyperleaup pantab certifi --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import ssl
import urllib3
import warnings

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

# Override default SSL context
ssl._create_default_https_context = ssl._create_unverified_context

# patch tableauserverclient to disable SSL verification
import tableauserverclient as TSC
_original_server_init = TSC.Server.__init__

def _patched_server_init(self, *args, **kwargs):
    _original_server_init(self, *args, **kwargs)
    self.add_http_options({'verify': False})

TSC.Server.__init__ = _patched_server_init

# COMMAND ----------

# DBTITLE 1,Set writable directories for Tableau Hyper
import os

# Save original directory
NOTEBOOK_DIR = os.getcwd()
print(f"Notebook directory: {NOTEBOOK_DIR}")

# Set writable temp directories for Hyper process
os.environ['TMPDIR'] = '/tmp'
os.environ['TEMP'] = '/tmp'
os.environ['TMP'] = '/tmp'

# Create directories for Hyper logs and run files
os.makedirs('/tmp/hyper_logs', exist_ok=True)
os.makedirs('/tmp/hyper_run', exist_ok=True)

print(f"Current working directory: {os.getcwd()}")
print("Hyper temp directories configured successfully")

# COMMAND ----------

# DBTITLE 1,Define ntbk variables
dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file = f"../../config/{dbx_env}/trmreports-conf.yaml"
print(f'{config_file=}')

# COMMAND ----------

## input table list to migrate only those tables to Tableau, if no list is provided all tables will be processed by default
## Input tables as a comma separaetd list of DBX table names from notebooks/python/shared/ntb_tm_hyperfile_list (third column in trm_hyperfile_table_map)

dbutils.widgets.text("table_list","")
table_list = dbutils.widgets.get("table_list").split(",")
table_list = [tbl.strip() for tbl in table_list]

if len(table_list) == 1 and table_list[0] == "":
    table_list = None

# COMMAND ----------

# DBTITLE 1,Execute common functions ntbk
# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,Execute hyperfile list ntbk
# MAGIC %run ../shared/ntb_tm_hyperfile_list

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Define credentials- store in DBX secrets for PROD env
#%run ./creds

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
src_catalog = common_configs['schema']['tmngpdb_src_catalog']

spark.conf.set('conf.catalog', trgt_catalog)
spark.conf.set('conf.src_catalog', src_catalog)
spark.conf.set('conf.dbx_env', dbx_env)
print(f"{trgt_catalog=},{src_catalog=}")

if dbx_env == 'prod':
  tableau_scope = common_configs['secrets']['tableau_scope']

  spark.conf.set('config.tableau_scope', tableau_scope.lower()) 
  tableau_server = dbutils.secrets.get(scope=tableau_scope, key="host")
  username = dbutils.secrets.get(scope=tableau_scope, key="username")
  password = dbutils.secrets.get(scope=tableau_scope, key="password")
  project_name = 'Trademark'

  tableau_server = tableau_server.replace("https://", "").replace("http://", "").rstrip("/")
  tableau_server = f"https://{tableau_server}"

# COMMAND ----------

# DBTITLE 1,Start Job log
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trm_third_level_publish_hyperfiles'

control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,starttime)



# COMMAND ----------

# DBTITLE 1,Read list of hyperfiles into dataframe
schema_def = ["HYPERFILE_NAME","DBX_SCHEMA_NAME","DBX_TABLE_NAME"]
hypefile_metadata = 'trm_hyperfile_table_map'
spark.conf.set('config.hypefile_metadata', hypefile_metadata.lower())
print(f'{hypefile_metadata=}')
df_schema_metadata = spark.createDataFrame(data = eval(hypefile_metadata), schema = schema_def)
df_schema_metadata = df_schema_metadata.select('HYPERFILE_NAME','DBX_SCHEMA_NAME','DBX_TABLE_NAME').distinct()

## apply table list filter if provided, otherwise process all tables

if table_list is not None:
    job_control_df = df_schema_metadata.filter(col('DBX_TABLE_NAME').isin(table_list))
else:
    job_control_df = df_schema_metadata

job_control_df.display()
job_control_parameters = job_control_df.collect()

# COMMAND ----------

# DBTITLE 1,Create hyperfiles and Publish to Tableau server
# Tables that bypass hyperleaup (parquet-based) and use pantab (direct hyper write) instead
# to avoid Hyper API parquet page index overflow errors
tables_with_parquet_issue = ['form_paragraph_dashboard']

for job_control in job_control_parameters:
    dbx_schema_name = job_control['DBX_SCHEMA_NAME']
    dbx_tbl_name = job_control['DBX_TABLE_NAME']
    hyperfile_name  = job_control['HYPERFILE_NAME']
    print(f"******************************************** \n Now processing: {dbx_tbl_name}")
    schema_metadata = dbx_tbl_name+"_column_map"
    spark.conf.set('config.schema_metadata', schema_metadata.lower())
    spark.conf.set('config.dbx_tbl_name', dbx_tbl_name)
    spark.conf.set('config.dbx_schema_name', dbx_schema_name)
    print(f'{schema_metadata=}')

    try:
        schema_def = ["hyperfile_col_name","dbx_col_name"]
        df_schema_metadata = spark.createDataFrame(data = eval(schema_metadata), schema = schema_def)
        dbx_columns = df_schema_metadata.select('dbx_col_name').rdd.flatMap(lambda x: x).collect()
        hyperfile_columns = df_schema_metadata.select('hyperfile_col_name').rdd.flatMap(lambda x: x).collect()

        df_read_dbx_table = spark.sql(f"select * from {trgt_catalog}.{dbx_schema_name}.{dbx_tbl_name}")

        from pyspark.sql import functions as f
        from pyspark.sql.window import Window

        if dbx_tbl_name in ['quality_dashboard', 'quality_dashboard_pivot']:
            for col in dict(df_read_dbx_table.dtypes):
                if (dict(df_read_dbx_table.dtypes)[col]) == 'boolean' and col != 'final_compliance':
                    df_read_dbx_table = df_read_dbx_table.withColumn(col, f.col(col).cast('integer'))

        for col in dict(df_read_dbx_table.dtypes):            
            if (dict(df_read_dbx_table.dtypes)[col]) == 'float':
                df_read_dbx_table = df_read_dbx_table.withColumn(col, f.col(col).cast('double'))

        if dbx_tbl_name == 'inventory_unexamined_hstry':
            date_win = Window().orderBy(f.col("unexamined_date").desc())
            max_date = df_read_dbx_table.withColumn(
                "rn", f.row_number().over(date_win)
            ).filter(f.col("rn") == 1).select(
                "unexamined_date", f.col("unexamined_cases").alias("today_cases"), f.col("unexamined_classes").alias("today_unexamined")
            )

            today_cases = max_date.select("today_cases").collect()[0][0]
            today_unexamined = max_date.select("today_unexamined").collect()[0][0]

            df_read_dbx_table = df_read_dbx_table.withColumn(
                "today_cases", f.lit(today_cases)
            ).withColumn(
                "today_unexamined", f.lit(today_unexamined)
            )

            dbx_columns.append("today_cases")
            dbx_columns.append("today_unexamined")
            hyperfile_columns.append("Today_Cases")
            hyperfile_columns.append("Today_Unexamined")

        df_hyperfile = df_read_dbx_table.select(*dbx_columns)
        df_hyperfile = df_hyperfile.toDF(*hyperfile_columns)

        os.chdir('/tmp')

        if dbx_tbl_name in tables_with_parquet_issue:
            # Use CSV + Hyper API COPY command (bypasses both parquet and toPandas)
            # This avoids parquet page index overflow AND driver memory overflow
            import tableauserverclient as TSC
            from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, TableDefinition, TableName, SqlType, Nullability
            import glob
            import shutil

            print(f"  Using CSV + Hyper API COPY for {dbx_tbl_name}...")

            # Step 1: Write DataFrame to CSV via Spark (distributed write)
            spark_csv_path = f"/tmp/{dbx_tbl_name}_csv"
            df_hyperfile.write.mode('overwrite').option('header', 'true').option('nullValue', '').option('sep', '\t').csv(spark_csv_path)
            print(f"  CSV written to {spark_csv_path}")

            # Step 2: Copy CSV part files from cluster FS to driver local /tmp/ using dbutils
            local_csv_path = f"/tmp/{dbx_tbl_name}_csv_local"
            # Clean up any stale files from previous runs
            if os.path.exists(local_csv_path):
                shutil.rmtree(local_csv_path)
            os.makedirs(local_csv_path, exist_ok=True)
            fs_files = dbutils.fs.ls(spark_csv_path)
            part_files = [f for f in fs_files if f.name.startswith('part-')]
            print(f"  Found {len(part_files)} CSV part files in cluster FS")
            for pf in part_files:
                local_file = f"file:{local_csv_path}/{pf.name}"
                dbutils.fs.cp(pf.path, local_file)
            print(f"  Copied {len(part_files)} files to driver local: {local_csv_path}")

            # Step 3: Map Spark types to Hyper SQL types
            def spark_type_to_hyper(spark_type):
                type_map = {
                    'string': SqlType.text(),
                    'int': SqlType.int(),
                    'integer': SqlType.int(),
                    'bigint': SqlType.big_int(),
                    'long': SqlType.big_int(),
                    'double': SqlType.double(),
                    'float': SqlType.double(),
                    'boolean': SqlType.bool(),
                    'date': SqlType.date(),
                    'timestamp': SqlType.timestamp_tz(),
                    'decimal': SqlType.double(),
                }
                # Handle decimal(precision, scale) types
                if spark_type.startswith('decimal'):
                    return SqlType.double()
                return type_map.get(spark_type, SqlType.text())

            # Step 3: Build Hyper table definition from DataFrame schema
            hyper_columns = []
            for field in df_hyperfile.schema:
                hyper_type = spark_type_to_hyper(field.dataType.simpleString())
                hyper_columns.append(TableDefinition.Column(field.name, hyper_type, Nullability.NULLABLE))

            table_def = TableDefinition(TableName("Extract", "Extract"), columns=hyper_columns)

            # Step 4: Create .hyper file and COPY from CSV files
            hyper_path = f"/tmp/{hyperfile_name}.hyper"
            with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                with Connection(hyper.endpoint, hyper_path, CreateMode.CREATE_AND_REPLACE) as conn:
                    conn.catalog.create_schema("Extract")
                    conn.catalog.create_table(table_def)

                    # Find CSV part files on local filesystem
                    csv_files = sorted(glob.glob(f"{local_csv_path}/part-*"))
                    # Filter out hidden files, _SUCCESS, and .crc files
                    csv_files = [f for f in csv_files if not os.path.basename(f).startswith(('_', '.'))]
                    print(f"  Found {len(csv_files)} CSV part files")
                    if csv_files:
                        print(f"  Sample file: {os.path.basename(csv_files[0])}")

                    for csv_file in csv_files:
                        conn.execute_command(f"""
                            COPY "Extract"."Extract" FROM '{csv_file}'
                            WITH (FORMAT CSV, HEADER, DELIMITER E'\t', NULL '')
                        """)
                    row_count = conn.execute_scalar_query(f'SELECT COUNT(*) FROM "Extract"."Extract"')
                    print(f"  Hyper file created with {row_count} rows at {hyper_path}")

            # Step 5: Publish to Tableau Server
            tableau_auth = TSC.TableauAuth(username, password)
            server = TSC.Server(tableau_server, use_server_version=True)
            with server.auth.sign_in(tableau_auth):
                all_projects, _ = server.projects.get()
                target_project = next((p for p in all_projects if p.name == project_name), None)
                if target_project is None:
                    raise ValueError(f"Project '{project_name}' not found on Tableau Server")
                datasource = TSC.DatasourceItem(target_project.id, name=hyperfile_name)
                datasource = server.datasources.publish(datasource, hyper_path, mode=TSC.Server.PublishMode.Overwrite)
                print(f"  Published Hyper File as datasource luid: {datasource.id}")

            # Clean up
            os.remove(hyper_path)
            shutil.rmtree(local_csv_path, ignore_errors=True)
            dbutils.fs.rm(spark_csv_path, recurse=True)
        else:
            # Use hyperleaup for all other tables (parquet-based flow)
            from hyperleaup import HyperFileConfig
            from hyperleaup import HyperFile

            hf_config = HyperFileConfig(timestamp_with_timezone=True,
                                        allow_nulls=True,
                                        convert_decimal_precision=True)
            hf_name = hyperfile_name
            hf = HyperFile(name=hf_name, df=df_hyperfile, is_dbfs_enabled=True, config=hf_config)

            datasource_name = hf.name

            luid = hf.publish(tableau_server_url=tableau_server,
                              username=username,
                              password=password,
                              project_name=project_name,
                              datasource_name=datasource_name
                              )
            print(f'Published Hyper File as new datasource luid: {luid}')

        os.chdir(NOTEBOOK_DIR)

    except Exception as e:
        os.chdir(NOTEBOOK_DIR)
        print(f"Error processing {dbx_tbl_name}: {e}")
        continue

# COMMAND ----------

dbutils.notebook.exit(f"Completed publishing hyper files")
