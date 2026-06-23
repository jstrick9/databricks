# Databricks notebook source
# MAGIC %md
# MAGIC <pre>
# MAGIC Vacuum
# MAGIC VACUUM is used to clean up unused and stale data files that are taking up unnecessary storage space. Removing these files can help reduce storage costs. 
# MAGIC When you run VACUUM on a Delta table it removes the following files from the underlying file system:
# MAGIC   Any data files that are not maintained by Delta Lake
# MAGIC   Removes stale data files (files that are no longer referenced by a Delta table) and are older than 7 days.
# MAGIC Running VACUUM daily helps keep storage costs in check, especially for larger tables
# MAGIC </pre>

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
catalog_df = spark.sql(f"show catalogs")
#catalog_df = catalog_df.filter(catalog_df.catalog == "trm_tmbuscalendar_dev")
if dbx_env !='prod':
    catalog_df = catalog_df.filter(catalog_df.catalog.like(f'trm_%_{dbx_env}'))
else:
    catalog_df = catalog_df.filter(catalog_df.catalog.like(f'trm_%')).filter(catalog_df.catalog!= "trm_oracle")
catalog_df.display()

# COMMAND ----------

for row in catalog_df.select("catalog").collect():
    ct = row["catalog"]
    print(f"Running VACUUM Command on catalog: {ct}")
    spark.catalog.setCurrentCatalog(f"{ct}")
    schema_df = spark.sql(f"show schemas in {ct}")
    schema_df = schema_df.filter(schema_df.databaseName != "information_schema")
    #schema_df.display()
    for row in schema_df.select("databaseName").collect():
        db = row["databaseName"]
        #print(db)
        table_df = spark.sql(f"show tables in {ct}.{db}")
        #table_df.display()
        cnt = 0
        for row in table_df.select("database","tableName").collect():
            db, table = row["database"], row["tableName"]
            cmd = ("VACUUM " +db+ "." +table+ ";")
            
            try:
                print(cmd)
                spark.sql(f"{cmd}")
            except Exception as e:
                print("Error:", e.__class__,"occured.")
                print("failed " + cmd )


# COMMAND ----------

dbutils.notebook.exit(f"Completed running vacuum command on tm catalogs")
