# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../shared/ntb_tm_brnz_table_list

# COMMAND ----------

# MAGIC %md
# MAGIC from pyspark.sql import SQLContext
# MAGIC from pyspark.sql.types import StructField, StructType, StringType
# MAGIC sc = spark.sparkContext
# MAGIC schema_def = StructType([StructField('TABLE_GROUP_NAME', StringType(),False),StructField('TABLE_NAME', StringType(), True),StructField('FULL_LOAD', StringType(), True),StructField('DQ_FLTR', StringType(), True),StructField('LARGE_TABLE_IND', StringType(), True),StructField('ZORDER', StringType(), True)])
# MAGIC df_schema_metadata_all = sqlContext.createDataFrame(sc.emptyRDD(), schema = schema_def)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType

spark = SparkSession.builder.getOrCreate()

schema_def = StructType([StructField('TABLE_GROUP_NAME', StringType(),False),StructField('TABLE_NAME', StringType(), True),StructField('FULL_LOAD', StringType(), True),StructField('DQ_FLTR', StringType(), True),StructField('LARGE_TABLE_IND', StringType(), True),StructField('ZORDER', StringType(), True)])
df_schema_metadata_all = spark.createDataFrame([], schema = schema_def)

# COMMAND ----------

list_schema_metadata = ["tmngpdb_metadata_group1","tmngpdb_metadata_group2","tmngpdb_metadata_group3","tmngpdb_metadata_group4","tmngpdb_metadata_group5","tmngpdb_metadata_group6","tmngpdb_metadata_group7","tmngpdb_metadata_group8","tmngpdb_metadata_group9","tmngpdb_metadata_group10","tmngpdb_metadata_group11","tmngpdb_metadata_group12","tmworker_metadata","tmbuscalendar_metadata","tmintltm_metadata","tmngfpepp_metadata","eogadmin_metadata","jbteasps_metadata","proceeding_metadata","tmprodvty_metadata","tmreviews_metadata","tmworker_metadata","tmngidmp_metadata","efoiap_metadata","tmrefdata_metadata"]
#list_schema_metadata = ["tmrefdata_metadata"]#tmbuscalendar_metadata
for schema_metadata in list_schema_metadata:
    df_schema_metadata = spark.createDataFrame(data = eval(schema_metadata), schema = schema_def)
    df_schema_metadata = df_schema_metadata.withColumn("CATALOG",split(lit(schema_metadata),'_')[0])
    df_schema_metadata_all = df_schema_metadata_all.unionByName(df_schema_metadata, allowMissingColumns=True)
df_schema_metadata_all = df_schema_metadata_all.select(lower("TABLE_NAME").alias("TABLE_NAME"),"ZORDER","CATALOG")
df_schema_metadata_all.display()

# COMMAND ----------

catalog_df = spark.sql(f"show catalogs")
#catalog_df = catalog_df.filter(catalog_df.catalog == "trm_tmbuscalendar_dev")
if dbx_env !='prod':
    catalog_df = catalog_df.filter(catalog_df.catalog.like(f'trm%_{dbx_env}'))
else:
    catalog_df = catalog_df.filter(catalog_df.catalog.like(f'trm_%'))
#catalog_df.display()

# COMMAND ----------

for row in catalog_df.select("catalog").collect():
    ct = row["catalog"]
    print(f"Applying liquid clustering to catalog: {ct}")
    spark.catalog.setCurrentCatalog(f"{ct}")
    schema_df = spark.sql(f"show schemas in {ct}")
    schema_df = schema_df.filter(schema_df.databaseName != "information_schema")
    #schema_df.display()
    for row in schema_df.select("databaseName").collect():
        db = row["databaseName"]
        #print(db)
        table_df = spark.sql(f"show tables in {ct}.{db}")
        table_df = table_df.withColumn("catalog",lit(ct))
        #table_df.display()
        job_control_df = table_df.alias("table_df").join(df_schema_metadata_all.alias("df_dq_fltr"),(col("table_df.tableName") == col("df_dq_fltr.TABLE_NAME")) & expr("table_df.catalog like (CONCAT('%' ,df_dq_fltr.CATALOG,'%'))"),"inner")
        cnt = 0
        for row in job_control_df.select("database","tableName","ZORDER").collect():
            db, table, zorder = row["database"], row["tableName"], row["ZORDER"]
            if zorder:
                cmd = ("ALTER TABLE " + ct + "." +db+ "." +table+ " CLUSTER BY (" +zorder+ ");")
            else:
                print("No column to cluster on.")
                break
            
            try:
                print(cmd)
                spark.sql(f"{cmd}")
            except Exception as e:
                print("Error:", e.__class__,"occured.")
                print("failed " + cmd )

        

# COMMAND ----------

dbutils.notebook.exit(f"Completed running optimize command on tm catalogs")
