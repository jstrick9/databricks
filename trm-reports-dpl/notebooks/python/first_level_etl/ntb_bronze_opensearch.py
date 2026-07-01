# Databricks notebook source
# MAGIC %md
# MAGIC This notebook initializes a connection to TM-PEA OpenSearch, queries and writes a json file to S3 for trademark_applications and tqr indexes. The json files are then pulled from S3 and parsed to create a DF. The DF is then written into the opensearch catalog

# COMMAND ----------

# MAGIC %pip install opensearch-py

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Time Control
# set current time for filename
from datetime import date, datetime, timedelta
import pytz
create_ts = datetime.now().astimezone(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S')  
today = datetime.now().astimezone(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d')
yesterday = (datetime.now().astimezone(pytz.timezone('US/Eastern'))-timedelta(1)).strftime('%Y-%m-%d')
                                                                                 

# COMMAND ----------

# DBTITLE 1,Add Parameters
# add text parameter default to dev
dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.dropdown("index", "trademark_applications", ["trademark_applications", "tqr", "tm_center_ai_reporting"])
dbutils.widgets.dropdown("full_load", "N", ["Y", "N"])

# dates added if needed to add a custom file
dbutils.widgets.text("start_date", yesterday, label="yyyy-mm-dd")
dbutils.widgets.text("end_date", today, label="yyyy-mm-dd")
idx = dbutils.widgets.get("index")
load = dbutils.widgets.get("full_load")
# dates are used within the opensearch json query
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

# COMMAND ----------

# DBTITLE 1,Configuration
import yaml
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
if dbx_env =='qa':
    dbx_env = 'test'
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC
# MAGIC %run  ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
opensearch_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{opensearch_catalog=}, {data_quality_catalog=} ')

# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'

spark.conf.set('conf.catalog',  opensearch_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)
spark.conf.set('conf.dbx_env', dbx_env)
spark.conf.set('conf.idx', idx)
spark.conf.set('conf.load', load)

# COMMAND ----------

# DBTITLE 1,OpenSearch Connection and Query
from opensearchpy import OpenSearch, helpers
from botocore.exceptions import ClientError
#math
import math
#to write to AWS S3
import boto3
import logging
import os
import json

class OpenSearchPipeline:
    ###########################CLASS INIT SECTION############################
    def __init__(self, **kwargs):
        self.__open_search_link = kwargs["open_search_link"]
        self.__open_search_username = kwargs["open_search_username"]
        self.__open_search_password = kwargs["open_search_password"]

    @property
    def openSearchLink(self):
        return self.__open_search_link
    @openSearchLink.setter
    def openSearchLink(self, newVal):
        if newVal:
            self.__open_search_link = newVal
            if self.__open_search_username and self.__open_search_password:
                self.createOpenSearchInstance()
    @property
    def openSearchUsername(self):
        return self.__open_search_username
    @openSearchUsername.setter
    def esUsername(self, newVal):
        if newVal:
            self.__opben_search_username = newVal
            if self.__open_search_link and self.__open_search_password:
                self.createOpenSearchInstance()

    @property
    def openSearchPassword(self):
        return self.__open_search_password
    @openSearchPassword.setter
    def openSearchPassword(self, newVal):
        if newVal:
            self.__open_search_password = newVal
            if self.__open_search_link and self.__open_search_username and self.__open_search_mappings:
                self.createOpenSearchInstance()

    @property
    def openSearchInstance(self):
        return self.__open_search_instance
    @openSearchInstance.setter
    def openSearchInstance(self, newVal):
        if newVal:
            self.__open_search_instance = newVal

    def createOpenSearchInstance(self):
        try:
            self.__open_search_instance = OpenSearch(
                    hosts = [f"https://{self.__open_search_link}"],
                    http_auth = (self.__open_search_username, self.__open_search_password),
                    http_compress = True,
                    ssl_show_warn = False,
                    verify_certs = False
                )
        except Exception as e:
            print(f"failed to create OpenSearch instance: {e}")

    def getDocuments(self, index_name, query):
        '''
            Method returns documents from corresponding ES index based a formulated query parameter coming from the service layer.
            returns result in ES formatted json

            Params: query
        '''
        print(f"Getting documents from index {index_name}.")
        print(f"query body: {query}")
        try:
            es_return_size_limit = 10000
           
            #Initial search to retrieve total hits
            init_resp = self.openSearchInstance.search(
                    index = index_name,
                    body = query,
                    _source = False,
                    seq_no_primary_term = True,
                    track_total_hits = True
                )

            total_hits = init_resp["hits"]["total"]["value"]

            final_output = init_resp
            final_output["hits"]["hits"] = []

            #exporting everything, so these variables stay
            size = total_hits
            if total_hits == 0:
                return []
            max_num_page = math.ceil(total_hits/size)
            page = 1
            request_size = page * size

            resp = None
            if request_size > es_return_size_limit:
                search_after = []
                while request_size > es_return_size_limit:
                    print(request_size)
                    resp = self.openSearchInstance.search(
                            index = index_name,
                            body = query,
                            _source = True,
                            seq_no_primary_term = True,
                            track_total_hits = True,
                            size = es_return_size_limit
                        )

                    search_after = resp["hits"]["hits"][-1]["sort"]
                    request_size -= es_return_size_limit
                    final_output["hits"]["hits"].extend(resp["hits"]["hits"])
                    query["search_after"] = search_after

            if request_size > size:
                print(f"request size after lower than size limit: {request_size}")
                resp = self.openSearchInstance.search(
                    index = index_name,
                    body = query,
                    _source = True,
                    seq_no_primary_term = True,
                    track_total_hits = True,
                    size = request_size - size
                )

                search_after = resp["hits"]["hits"][-1]["sort"]
                final_output["hits"]["hits"].extend(resp["hits"]["hits"])
                query["search_after"] = search_after
            
            resp = self.openSearchInstance.search(
                    index = index_name,
                    body = query,
                    _source = True,
                    seq_no_primary_term = True,
                    track_total_hits = True,
                    size = request_size
                )

            final_output["hits"]["hits"].extend(resp["hits"]["hits"])

            return final_output["hits"]["hits"]
            
        except Exception as e:
            print(f"error when trying to retrieve documents: {e}")
            return str(e)
    

# COMMAND ----------

#upload
def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True



# COMMAND ----------

# DBTITLE 1,Cell 12
def read_OpenSearch_TA(index_name=idx, scope_name='opensearch'):
    """Reads data from Pre-Exam OpenSearch
    :param index_name: the OpenSearch index to query, currently tqr or trademark_applications
    :param os_query: a dictionary formatted query that lists the keys to query, see OS docs for details
    :param scope_name: scope_name to receive secrets
    :return: a json file
    """
    open_search_link = dbutils.secrets.get(scope=scope_name, key="open_search_link")
    open_search_username = dbutils.secrets.get(scope=scope_name, key="open_search_username")
    open_search_password = dbutils.secrets.get(scope=scope_name, key="open_search_password")

    #instantiate the class
    open_search_pipeline = OpenSearchPipeline(
                    open_search_link = open_search_link,
                    open_search_username = open_search_username, 
                    open_search_password = open_search_password
                )
    open_search_pipeline.createOpenSearchInstance()
    open_search_instance = open_search_pipeline.openSearchInstance #reference to the actual instance

    if index_name == "trademark_applications":
        if load == "Y":
            # Retrieves all completed apps
            os_query = {"sort": "serial_number",
                        "query": {
                            "bool": {
                                "must": [
                                    #{
                                        #"match": {
                                            #"pre_exam_status": "103"
                                        #}
                                    #}
                                ]
                            }
                        }
                    }
        else:
            # Retrieves all completed apps within a specified timeframe
            # status 100 - unassigned, 101 - assigned, 103 - completed
            os_query = {"sort": "serial_number",
                        "query": {
                            "bool": {
                                "must": [
                                    #{
                                        #"match": {
                                            #"pre_exam_status": "103" 
                                        #}
                                    #},
                                    {
                                        "range": {
                                            #last_updated indicates when any attribute within the application was changed within our system
                                            "last_updated": {
                                                "gte": start_date,
                                                "lte": end_date
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    }

    elif index_name == "tqr":
        if load == "Y":
            os_query = {
                    "query": {
                            "bool": {
                                    "must": [
                                            ]
                                    }
                            }
                        }
        else:
            os_query = {
                     "query": {
                            "bool": {
                                "must": [
                                    {
                                        "range": {
                                            "date_uploaded": {
                                                "gte": start_date,
                                                "lte": end_date
                                            }
                                        }
                                    }
                            ]
                        }
                    }
                }

    elif index_name == "tm_center_ai_reporting":
        if load == "Y":
            os_query = {"sort": ["_doc"],
                    "query": {
                            "bool": {
                                    "must": []
                                    }
                            }
                        }
        else:
            os_query = {"sort": ["_doc"],
                     "query": {
                            "bool": {
                                "must": [
                                    {
                                        "range": {
                                            "last_updated": {
                                                "gte": start_date,
                                                "lte": end_date
                                            }
                                        }
                                    }
                            ]
                        }
                    }
                }

    try:
        index_json = open_search_pipeline.getDocuments(index_name, os_query)

        if type(index_json) == str:
            return None
        else:
            return index_json
    except Exception as e:
        print("Exception message: {}".format(e))
        return None



# COMMAND ----------

# DBTITLE 1,Initiate Opensearch Query
json_results = read_OpenSearch_TA()

# COMMAND ----------

if json_results == None:
    raise Exception("File did not successfully generate. The workflow has failed!")

# COMMAND ----------

# DBTITLE 1,Remove unnecessary json keys
import json
if idx == "trademark_applications":

    # Remove unnecessary inner keys
    nested_keys = set(
        [
            "classes",
            "owners",
            "color_claimed_statement",
            "prior_owners",
            "trademark_statements",
            "mark_foreign_transliteration_statement",
            "mark_lining_stippling_statement",
            "trademark_location",
            "mark_drawing_code",
            "is_color_mark",
            "prior_registration",
            "mark_description",
            "color_location_statement",
            "domestic_representative",
            "suggestion",
            "mark_suggestion",
            "design_search_code",
            "mark_foreign_translation_statement",
            "mark_type",
            "section_2f",
            "docket_number",
            "pseudomarks",
            "case_status",
            "attorney",
            "application_edit_history",
            "correspondent",
            "is_3d_mark",
            "mark_disclaimer",
            "is_standard_character_mark",
            "trademark_filing_basis",
            "mark_evaluation",
            "trademark_foreign_basis",
            "first_use_anywhere_date",
            "note_history",
            "mark_name_portrait_usage_statement",
        ]
    )

    data = [{k: v for k, v in metadata.items() if k not in nested_keys} for metadata in [record["_source"] for record in json_results]]

    df_schema = StructType([
	StructField('assignee', StringType(), True),
 	StructField('case_internal_status', StringType(), True),
 	StructField('case_status_code', StringType(), True),
 	StructField('date_last_uploaded', StringType(), True),
 	StructField('date_pre_exam_received', StringType(), True),
	StructField('filing_date', StringType(), True),
 	StructField('last_updated', StringType(), True),
 	StructField('mark', StringType(), True),
 	StructField('pre_exam_history', StructType([
		StructField('history', ArrayType(StructType([
			StructField('action', StringType(), True),
			StructField('by', StringType(), True),
			StructField('date_time', StringType(), True),
			StructField('from', StringType(), True),
			StructField('order', LongType(), True), 
			StructField('to', StringType(), True)]), True), True),
		StructField('latest_order_no', LongType(), True)]), True),
	StructField('pre_exam_status', StringType(), True),
	StructField('serial_number', StringType(), True),
	StructField('trademark_track_type', StringType(), True)])

    df = spark.createDataFrame(data, schema=df_schema)

# so tqr doesn't fail with new file path
elif idx == "tqr":
    data = [{k: v for k, v in metadata.items()} for metadata in [record["_source"] for record in json_results]]
    df_schema = StructType([StructField('assignee', StringType(), True),
    StructField('case_internal_status', StringType(), True),
    StructField('class', ArrayType(StructType([
	    StructField('class_number', StringType(), True),
	    StructField('comments', ArrayType(StructType([
		    StructField('action', StringType(), True),
		    StructField('by', StringType(), True),
		    StructField('date_time', StringType(), True),
		    StructField('message', StringType(), True),
		    StructField('order', LongType(), True)]), True), True),
	StructField('goods_services_text', StringType(), True),
	StructField('latest_order_no', LongType(), True),
	StructField('status', StringType(), True)]), True), True),
    StructField('date_uploaded', StringType(), True),
    StructField('design_search_code', StructType([
	    StructField('comments', ArrayType(StructType([
		    StructField('action', StringType(), True),
		    StructField('by', StringType(), True),
		    StructField('date_time', StringType(), True),
		    StructField('message', StringType(), True),
		    StructField('order', LongType(), True)]), True), True),
	    StructField('latest_order_no', LongType(), True),
	    StructField('status', StringType(), True), 
	    StructField('value', ArrayType(
		        StringType(), True), True)]), True),
    StructField('mark_drawing_code', StructType([
	    StructField('comments', ArrayType(StructType([
		    StructField('action', StringType(), True),
		    StructField('by', StringType(), True),
		    StructField('date_time', StringType(), True),
		    StructField('message', StringType(), True),
		    StructField('order', LongType(), True)]), True), True), 
	    StructField('latest_order_no', LongType(), True),
	    StructField('status', StringType(), True), 
	    StructField('value', StringType(), True)]), True),
        StructField('pre_exam_status', StringType(), True),
        StructField('pseudomarks', StructType([
	    StructField('comments', ArrayType(StructType([
		    StructField('action', StringType(), True), 
		    StructField('by', StringType(), True),
		    StructField('date_time', StringType(), True), 
		    StructField('message', StringType(), True),
		    StructField('order', LongType(), True)]), True), True),
	    StructField('latest_order_no', LongType(), True),
	    StructField('status', StringType(), True),
	    StructField('value', ArrayType(StringType(), True), True)]), True),
    StructField('review_manager', StringType(), True),
    StructField('review_started', StringType(), True),
    StructField('reviewer', StringType(), True),
    StructField('serial_number', StringType(), True),
    StructField('word_mark', StructType([
	    StructField('comments', ArrayType(StructType([
		    StructField('action', StringType(), True),
		    StructField('by', StringType(), True),
		    StructField('date_time', StringType(), True),
		    StructField('message', StringType(), True),
		    StructField('order', LongType(), True)]), True), True),
		    StructField('latest_order_no', LongType(), True),
		    StructField('status', StringType(), True),
		    StructField('value', StringType(), True)]), True)])
    
    df = spark.createDataFrame(data, schema=df_schema)

elif idx == "tm_center_ai_reporting":
    data = [json.dumps(record["_source"]) for record in json_results]
    rdd = spark.sparkContext.parallelize(data, numSlices=100)
    df = spark.read.json(rdd)

# COMMAND ----------

# DBTITLE 1,Parse trademark applications json into dataframe
from pyspark.context import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import explode_outer,col

def flatten(df):
   # compute Complex Fields (Lists and Structs) in Schema   
   complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   while len(complex_fields)!=0:
      col_name=list(complex_fields.keys())[0]
      print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))
    
      # if StructType then convert all sub element to columns.
      # i.e. flatten structs
      if (type(complex_fields[col_name]) == StructType):
         expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
         df=df.select("*", *expanded).drop(col_name)
    
      # if ArrayType then add the Array Elements as Rows using the explode function
      # i.e. explode Arrays
      elif (type(complex_fields[col_name]) == ArrayType):    
         df=df.withColumn(col_name,explode_outer(col_name))
    
      # recompute remaining Complex Fields in Schema       
      complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   return df

df_flatten = flatten(df)

# COMMAND ----------

# DBTITLE 1,Count Records
rec_counts = df_flatten.count()

# COMMAND ----------

rec_counts

# COMMAND ----------

# DBTITLE 1,Troubleshooting Column Check
dataColumns = df_flatten.columns
if idx == "trademark_applications":
    table_name = "pea_trademark_applications"
    full_table_name = f"{opensearch_catalog}.{database}.{table_name}"
elif idx == "tqr":
    table_name = "pea_tqr"
    full_table_name = f"{opensearch_catalog}.{database}.{table_name}"
elif idx == "tm_center_ai_reporting":
    table_name = "pea_opensearch_ai_center"
    full_table_name = f"{opensearch_catalog}.{database}.{table_name}"

tableColumns = spark.sql(f"""SELECT * FROM {full_table_name} LIMIT 10""").columns
list(set(dataColumns).difference(tableColumns))

# COMMAND ----------

df_flatten.createOrReplaceTempView("os_temp")

# COMMAND ----------

# DBTITLE 1,Overwrite Table With New Data
 if rec_counts > 0:
    try:
        spark.sql(f"""INSERT OVERWRITE {full_table_name} BY NAME
              SELECT * FROM os_temp        
              """)
    
        exception = None
        status = "complete"

    except Exception as e:
        status = "error"
        exception = str(e)

    finally: 
        dbutils.notebook.exit(f"status: {status}, exception: {exception}")

else:
    exception = None
    status = "There are no records"
    dbutils.notebook.exit(f"status: {status}, exception: {exception}")