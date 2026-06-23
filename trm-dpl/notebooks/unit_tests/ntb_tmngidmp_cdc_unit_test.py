# Databricks notebook source
import oracledb

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT dbx_env DEFAULT "dev"

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()

if dbx_env == 'test':
  config_file = "../config/qa/tmngidmp-conf.yaml"
else:
  config_file = "../config/"+dbutils.widgets.get("dbx_env").rstrip()+"/tmngidmp-conf.yaml"
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ../python/shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE trm_tmngidmpvt_dev.bronze.emp4 (
# MAGIC   EMP_ID INT NOT NULL,
# MAGIC   NAME STRING,
# MAGIC   DEPT_ID INT
# MAGIC )

# COMMAND ----------

emp4_df = read_data_from_oracle_conn_dsu_cmn("select * from bigdataread.emp4")
emp4_df = emp4_df.withColumn('EMP_ID', col('EMP_ID').cast(IntegerType())).withColumn('DEPT_ID', col('DEPT_ID').cast(IntegerType()))
emp4_df.write.mode("overwrite").saveAsTable("trm_tmngidmpvt_dev.bronze.emp4")

# COMMAND ----------

emp4_og = spark.sql("select * from trm_tmngidmpvt_dev.bronze.emp4")

# COMMAND ----------

# %sql
# insert into trm_tmngidmpvt_dev.bronze.cdc_batch_job_control (src_folder, catalog_name, database_name, table_name, source_db_name, source_table_name, primary_keys, full_load, initial_load_finished)
# values ('s3a://bdr-databricks-app-dev/eds/DMS/TMNGIDMPVT/emp4','trm_tmngidmpvt_dev','bronze','emp4','bigdataread','EMP4','EMP_ID','N',true)

# COMMAND ----------

scope_name = "oracle_trmpvt_server"
host = dbutils.secrets.get(scope=scope_name, key="host")
port = dbutils.secrets.get(scope=scope_name, key="port")
db_name = dbutils.secrets.get(scope=scope_name, key="db_name")
user = dbutils.secrets.get(scope=scope_name, key="username")
pwd = dbutils.secrets.get(scope=scope_name, key="password")

# COMMAND ----------

connection = oracledb.connect(user=user, password=pwd, host=host, port=port, service_name="TRMPVT")
cursor = connection.cursor()

# COMMAND ----------

# insert
q_insert1 = "insert into bigdataread.emp4 (EMP_ID, NAME, DEPT_ID) values(1, 'TestInsert1',2)"
cursor.execute(q_insert1)
connection.commit()

q_insert2 = "insert into bigdataread.emp4 (EMP_ID, NAME, DEPT_ID) values(2, 'TestInsert2',2)"
cursor.execute(q_insert2)
connection.commit()

q_insert3 = "insert into bigdataread.emp4 (EMP_ID, NAME, DEPT_ID) values(3, 'TestInsert3',2)"
cursor.execute(q_insert3)
connection.commit()

# COMMAND ----------

# update1
q_upd1 = "update bigdataread.emp4 set DEPT_ID = 3 where EMP_ID = 1"
cursor.execute(q_upd1)
connection.commit()

# COMMAND ----------

# update 2
q_upd2 = "update bigdataread.emp4 set DEPT_ID = 4 where EMP_ID = 2"
cursor.execute(q_upd2)
connection.commit()

# COMMAND ----------

###################
# wait some time for first cdc file to generate
###################

# COMMAND ----------

# update value to null
q_updn = "update bigdataread.emp4 set NAME = Null where EMP_ID = 1"
cursor.execute(q_updn)
connection.commit()

# COMMAND ----------

# close connection
cursor.close()
connection.close()

# COMMAND ----------

#######################
# wait for second cdc file
#######################

# COMMAND ----------

# %run ../python/bronze/ntb_tmngidmp_cdc_batch_load $dbx_env=dev

# run not working from this notebook for unknown reason, go run cdc batch load notebook manually and return here

# COMMAND ----------

### check cdc inserts and updates properly applied

# COMMAND ----------

# df post cdc updates
emp4_brnz = spark.sql("select * from trm_tmngidmpvt_dev.bronze.emp4")

# COMMAND ----------

df_test_rows = spark.createDataFrame([[1,None,3],[2,'TestInsert2',4],[3,'TestInsert3',2]], emp4_brnz.schema)

# COMMAND ----------

df_comp = emp4_og.filter(~col("EMP_ID").isin([x.EMP_ID for x in df_test_rows.select('EMP_ID').collect()])).union(df_test_rows)

# COMMAND ----------

emp4_brnz.display()
df_comp.display()

# COMMAND ----------

comp_fail = emp4_brnz.exceptAll(df_comp)
assert comp_fail.count() == 0, "Expected failure: null column issue"

# COMMAND ----------

emp4_brnz.filter(col("EMP_ID") != 1).display()
df_comp.filter(col("EMP_ID") != 1).display()

# COMMAND ----------

comp_pass = emp4_brnz.filter(col("EMP_ID") != 1).exceptAll(df_comp.filter(col("EMP_ID") != 1))
assert comp_pass.count() == 0, "Unexpected failure: recheck logic"

# COMMAND ----------

# check deletes

# COMMAND ----------

connection = oracledb.connect(user=user, password=pwd, host=host, port=port, service_name="TRMPVT")
cursor = connection.cursor()

# COMMAND ----------

# delete test records
q_del1 = "delete from bigdataread.emp4 where EMP_ID = 1"
cursor.execute(q_del1)
connection.commit()

q_del2 = "delete from bigdataread.emp4 where EMP_ID = 2"
cursor.execute(q_del2)
connection.commit()


# COMMAND ----------

# close connection
cursor.close()
connection.close()

# COMMAND ----------

#######
# wait for cdc file then run batch
#######

# COMMAND ----------

comp_del = emp4_brnz.exceptAll(df_comp.filter(~col("EMP_ID").isin([1,2])))
assert comp_del.count() == 0, "Unexpected failure: recheck logic"
