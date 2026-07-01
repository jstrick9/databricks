# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
print(f"{trgt_catalog=}")
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)


# COMMAND ----------

# MAGIC %md
# MAGIC ##tri_lookup_status

# COMMAND ----------

try:
    df_tri_lookup_status_files = spark.read.option('multiline', 'true').format('json').load(f"s3://{cdc_bucket}/eds/trademark/nice/tm5/tri_lookup_status/tri_lookup_status.json")
    df_tri_lookup_status_files = df_tri_lookup_status_files.withColumn("create_ts", current_timestamp()).withColumn("create_user_id", lit("etl"))
except:
    print("No new files available")

df_tri_lookup_status_files.write.mode('overwrite').format("delta").insertInto(f"{trgt_catalog}.bronze.tm5_tri_lookup_status")

# COMMAND ----------

# MAGIC %md
# MAGIC ##tri_partners

# COMMAND ----------

try:
    df_tri_partners_files = spark.read.option('multiline', 'true').format('json').load(f"s3://{cdc_bucket}/eds/trademark/nice/tm5/tri_partners/tri_partners.json")

    df_tri_partners_files_col = df_tri_partners_files.withColumn("create_ts", current_timestamp()).withColumn("create_user_id", lit("etl")).withColumn("dt_Joined", col("dt_Joined").cast("date")).withColumn("u_last_expired_email_sent", col("u_last_expired_email_sent").cast("timestamp"))
except:
    print("No new files available")

df_tri_partners_files_col.write.mode('overwrite').format("delta").insertInto(f"{trgt_catalog}.bronze.tm5_tri_partners")

# COMMAND ----------

# MAGIC %md
# MAGIC ##tri_vote_types

# COMMAND ----------

try:
    df_tri_vote_types_files = spark.read.option('multiline', 'true').format('json').load(f"s3://{cdc_bucket}/eds/trademark/nice/tm5/tri_vote_types/tri_vote_types.json")

    df_tri_vote_types_files = df_tri_vote_types_files.withColumn("create_ts", current_timestamp()).withColumn("create_user_id", lit("etl"))
except:
    print("No new files available")
#df_tri_vote_types_files.display()

df_tri_vote_types_files.write.mode('overwrite').format("delta").insertInto(f"{trgt_catalog}.bronze.tm5_tri_vote_types")

# COMMAND ----------

# MAGIC %md
# MAGIC ##tri_items

# COMMAND ----------

# MAGIC %md 
# MAGIC df_tri_items_files = spark.read.option('multiline', 'true').format('json').load(f"s3://{cdc_bucket}/eds/trademark/nice/tm5/tri_items/*.json")
# MAGIC
# MAGIC df_tri_items_files = df_tri_items_files.withColumn("create_ts", current_timestamp()) \
# MAGIC             .withColumn("create_user_id", lit("etl")) \
# MAGIC             .withColumn("dt_Created", f.to_date("dt_Created", "yyyyMMdd")) \
# MAGIC             .withColumn("dt_Accepted", f.to_date("dt_Accepted", "yyyyMMdd")) \
# MAGIC             .withColumn("dt_Rejected", f.to_date("dt_Rejected", "yyyyMMdd")) \
# MAGIC             .withColumn("dt_Removed", f.to_date("dt_Removed", "yyyyMMdd")) \
# MAGIC             .withColumn("dt_Withdrawn", f.to_date("dt_Withdrawn", "yyyyMMdd")) \
# MAGIC             .withColumn("dt_Released", f.to_date("dt_Released", "yyyyMMdd")) \
# MAGIC
# MAGIC #df_tri_items_files.display()
# MAGIC
# MAGIC df_tri_items_files.write.mode('overwrite').format("delta").insertInto(f"{trgt_catalog}.bronze.tm5_tri_items")

# COMMAND ----------

import requests
import json


url = "https://tmidlist.org:80/service.asmx/GetAllItems"

try:
    response = requests.get(url, verify=True )
except ConnectionResetError as exc:
    print("Error Connecting")
    raise

if response.status_code == 200:
    print(response)
    data = response.json()
    df_tri_items_files = spark.createDataFrame(data)
    #display(df_tri_items_files)
else:
    print(f"Error: {response.status_code}")
print("Reached EOF")


# COMMAND ----------

#df_tri_items_files = spark.read.option('multiline', 'true').format('json').load(f"s3://{cdc_bucket}/eds/trademark/nice/tm5/tri_items/*.json")

df_tri_items_files = df_tri_items_files.withColumn("create_ts", current_timestamp()) \
            .withColumn("create_user_id", lit("etl")) \
            .withColumn("dt_Created", f.to_date("dt_Created", "yyyyMMdd")) \
            .withColumn("dt_Accepted", f.to_date("dt_Accepted", "yyyyMMdd")) \
            .withColumn("dt_Released", f.to_date("dt_Released", "yyyyMMdd")) \
            .withColumn("dt_Rejected", lit(None)) \
            .withColumn("dt_Removed", f.to_date("dt_Removed", "yyyyMMdd")) \
            .withColumn("dt_Withdrawn", f.to_date("dt_Withdrawn", "yyyyMMdd")) \
            


df_tri_items_files = df_tri_items_files.select("dt_Accepted","dt_Created","dt_Rejected","dt_Released","dt_Removed","dt_Withdrawn","f_Released","i_Class_ID","i_Item_ID","i_Resubmittal","i_Status","i_User_ID_Created_By","i_User_ID_Released_By","i_User_ID_Resubmitted_By","u_Item_Name","create_ts","create_user_id")
#df_tri_items_files.display()
df_tri_items_files.write.mode('overwrite').format("delta").insertInto(f"{trgt_catalog}.bronze.tm5_tri_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ##tri_votes

# COMMAND ----------

import requests
import json


#url = "https://tmidlist.org:80/service.asmx/GetAllItemVotes"
url = "https://tmidlist.org:80/service.asmx/GetAllVotes"

try:
    response = requests.get(url, verify=True )
except ConnectionResetError as exc:
    print("Error Connecting")
    raise

if response.status_code == 200:
    print(response)
    data = response.json()
    df_tri_votes_files = spark.createDataFrame(data)
    #display(df_tri_votes_files)
else:
    print(f"Error: {response.status_code}")
print("Reached EOF")


# COMMAND ----------

try:
    #df_tri_votes_files = spark.read.option('multiline', 'true').format('json').load(f"s3://{cdc_bucket}/eds/trademark/nice/tm5/tri_votes/*.json")

    df_tri_votes_files = df_tri_votes_files.withColumn("create_ts", current_timestamp()) \
                .withColumn("create_user_id", lit("etl")) \
                .withColumn("dt_Created", f.to_timestamp("dt_Created", "yyyyMMdd"))
    
    df_tri_votes_files.write.mode('overwrite').format("delta").insertInto(f"{trgt_catalog}.bronze.tm5_tri_votes")

except:
    print("No new files available")
#df_tri_votes_files.display()


#get entire data set
# Calculate KPIs
#read data from link
#https://tmidlist.org:80/service.asmx/GetAllItemsByDate?p1=2021-01-01&p2=2021-12-31
#https://tmidlist.org:80/service.asmx/GetAllItemVotesByDate?p1=2024-06-01&p2=2024-10-30


# COMMAND ----------

dbutils.notebook.exit(f"Completed loading tm5_json_file_data Table ")
