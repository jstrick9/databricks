# Databricks notebook source
dbutils.widgets.text("dbx_env", "prod")
dbutils.widgets.text("catalog", "tdet")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")

def optimize_and_vacuum_tables_with_checkpoint(catalog):
    checkpoint_file_dbfs = f"dbfs:/tmp/optimize_vacuum_checkpoint_{catalog}.txt"

    def file_exists(path):
        try:
            dbutils.fs.ls(path)
            return True
        except Exception:
            return False

    # Load already processed tables if checkpoint exists
    processed_tables = set()
    if file_exists(checkpoint_file_dbfs):
        checkpoint_content = dbutils.fs.head(checkpoint_file_dbfs, 1000000)
        processed_tables = set(line.strip() for line in checkpoint_content.split("\n") if line.strip())

    # Get all table names in the specified catalog across all schemas
    tables_df = spark.sql(
        f"""
        SELECT table_schema, table_name
        FROM {catalog}.information_schema.tables
        WHERE table_catalog = '{catalog}'
        """
    )

    tables = [(row.table_schema, row.table_name) for row in tables_df.collect()]

    results = []
    for schema, table in tables:
        table_full_name = f"{catalog}.{schema}.{table}"
        if table_full_name in processed_tables:
            results.append({
                "table": table_full_name,
                "status": "skipped",
                "reason": "already processed"
            })
            continue
        try:
            spark.sql(f"OPTIMIZE {table_full_name}")
            spark.sql(f"VACUUM {table_full_name} RETAIN 360 HOURS")
            results.append({
                "table": table_full_name,
                "status": "completed",
                "reason": None
            })
            # Append to checkpoint file after successful completion
            try:
                new_line = table_full_name + "\n"
                if file_exists(checkpoint_file_dbfs):
                    old_content = dbutils.fs.head(checkpoint_file_dbfs, 1000000)
                    new_content = old_content + new_line
                else:
                    new_content = new_line
                dbutils.fs.put(checkpoint_file_dbfs, new_content, overwrite=True)
            except Exception as log_err:
                results.append({
                    "table": table_full_name,
                    "status": "checkpoint_failed",
                    "reason": f"Checkpoint error: {log_err}"
                })
        except Exception as e:
            results.append({
                "table": table_full_name,
                "status": "failed",
                "reason": str(e)
            })

    #results_df = spark.createDataFrame(results)
    #display(results_df)

# Example usage:
optimize_and_vacuum_tables_with_checkpoint(catalog)

# COMMAND ----------

checkpoint_file_dbfs = f"dbfs:/tmp/optimize_vacuum_checkpoint_{catalog}_{schema}.txt"
checkpoint_content = dbutils.fs.head(checkpoint_file_dbfs, 1000000)
lines = [line.strip() for line in checkpoint_content.split("\n") if line.strip()]
df = spark.createDataFrame([(line,) for line in lines], ["table_full_name"])
display(df)

# COMMAND ----------

dbutils.notebook.exit(f"Completed running optimize and vacuum command for tdet")