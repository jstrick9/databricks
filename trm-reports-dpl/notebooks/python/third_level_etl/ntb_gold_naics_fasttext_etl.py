# Databricks notebook source
# DBTITLE 1,Import Libraries
import pandas as pd
import numpy as np

import os

from sklearn.metrics.pairwise import cosine_similarity
import nltk
from nltk.tokenize import word_tokenize
from gensim.models import FastText
import gensim.downloader as api
from gensim.parsing.preprocessing import remove_stopwords
from gensim import utils
from gensim.models import KeyedVectors

from pyspark.sql.functions import concat, col, lit, udf, pandas_udf, PandasUDFType
from pyspark.sql.types import ArrayType, StringType


# COMMAND ----------

# DBTITLE 1,Set Widgets
dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("start_dt","2022-10-01")
dbutils.widgets.text("end_dt","2023-04-30")

# COMMAND ----------

# DBTITLE 1,Set Config
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
start_dt = dbutils.widgets.get("start_dt").rstrip()
end_dt = dbutils.widgets.get("end_dt").rstrip()
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Load Common Functions
# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

# DBTITLE 1,Read Configs
common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
tmngpdb_catalog = common_configs['schema']['tmngpdb_src_catalog']

# COMMAND ----------

# DBTITLE 1,Start Job Control
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_gold_naics_fasttext_etl'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

# DBTITLE 1,Define Functions
def generateVector(sentence):
    return fasttext_model.wv.get_sentence_vector(sentence)

# UDF
@pandas_udf("array<double>", PandasUDFType.SCALAR)
def generate_vector_udf(text_series):
    return text_series.apply(generateVector)

# COMMAND ----------

# DBTITLE 1,Load Inputs
## Load Industry Title Vector Reference File
industry_title_vectors = pd.read_pickle("/dbfs/FileStore/NAICS/IndustryTitles_vectorRef.pkl")

## Load FastText Model
fasttext_model = FastText.load("/dbfs/FileStore/NAICS/FastText_model")

## Load NAICS Reference Data and Vector Array
naics_napcs_df = pd.read_pickle("/dbfs/FileStore/NAICS/naics_napcs_processed.pkl")
NAICS_vector_array = np.load("/dbfs/FileStore/NAICS/NAICS_vector_array.npy", allow_pickle=True)

## Load NAICS Keyed Vectors
NAICS_KeyedVectors = KeyedVectors.load('/dbfs/FileStore/NAICS/NAICS_KeyedVectors.kv')

## Query for Input Data (Goods and Services)
goods_services_data = spark.sql(f"""
SELECT distinct
ser_num, class, coordinated_class, goods_and_services_desc as goods_services_desc from(
SELECT
  gsd.*,
  cl.goods_and_services_desc
FROM
  {reporting_catalog}.gold.goods_services_dashboard gsd
    LEFT OUTER JOIN {reporting_catalog}.silver.class cl
      ON gsd.ser_num = cl.ser_num
      AND gsd.class = cl.vt_class) a
where pendency_cal_start_dt between '{start_dt}' AND '{end_dt}'
""")

# COMMAND ----------

# DBTITLE 1,Preprocessing data for FastText
goods_services_data = goods_services_data.withColumn('text', concat(col('coordinated_class'), lit(', '), col('goods_services_desc')))

## TM Class Corpus Detailed
corpus = goods_services_data.select('ser_num', 'class', 'coordinated_class','text').orderBy('ser_num')

## Drop NAs from Corpus
corpus = corpus.dropna()

# COMMAND ----------

# DBTITLE 1,Remove Stop Words and Tokenize
# UDFs
remove_stopwords_udf = udf(remove_stopwords, StringType())
simple_preprocess_udf = udf(lambda text: utils.simple_preprocess(text.lower()), ArrayType(StringType()))

# Apply UDFs
corpus = corpus.withColumn('text', remove_stopwords_udf(col('text')))
corpus = corpus.withColumn('Text_Tokenized', simple_preprocess_udf(col('text')))

# COMMAND ----------

# DBTITLE 1,Calculate Vectors
corpus = corpus.withColumn('vector', generate_vector_udf(corpus['Text_Tokenized']))

## Create array of vectors
vector_list = corpus.select('vector').rdd.flatMap(lambda x: x).collect()

## Convert  list to NumPy array
corpus_vector_array = np.array(vector_list)

# COMMAND ----------

# DBTITLE 1,Create NAICS Reference
NAICS_ref = naics_napcs_df[['NAICS2017','NAICS2017_LABEL']]
NAICS_ref.shape

# COMMAND ----------

# DBTITLE 1,NAICS Assignment Loop (top 10) (alternate)
# Pre-process steps outside the loop
NAICS_reference = NAICS_ref.drop_duplicates()
ser_nums = corpus.select('ser_num').toPandas()['ser_num'].tolist()
input_texts = corpus.select('text').toPandas()['text'].tolist()
classes = corpus.select('class').toPandas()['class'].tolist()

# Initialize a list to collect DataFrames
dfs = []

for i, vector in enumerate(corpus_vector_array):
    top10 = NAICS_KeyedVectors.similar_by_vector(vector, topn=10)
    top10_df = pd.DataFrame(top10, columns=['NAICS2017', 'Similarity'])
    top10_df['NAICS2017'] = top10_df['NAICS2017'].astype(int)
    top10_df['ser_num'] = ser_nums[i]
    top10_df['class'] = classes[i]
    top10_df['input_text'] = input_texts[i]
    dfs.append(top10_df)

# Concatenate all DataFrames at once
results = pd.concat(dfs, ignore_index=True)

# COMMAND ----------

# DBTITLE 1,Merge in NAICS Labels
results = pd.merge(results, industry_title_vectors, on='NAICS2017', how='left')
results.shape

# COMMAND ----------

# DBTITLE 1,Group by SerNum+G/S+Label --> drop duplicates
results = results.drop_duplicates(subset=['ser_num', 'class', 'input_text', 'NAICS2017_LABEL'])

# COMMAND ----------

# DBTITLE 1,Create Rank Column
# Group by 'ser_num' and 'input_text', then apply ranking within each group
results['rank'] = results.groupby(['ser_num',  'class', 'input_text'])['Similarity']\
                           .rank(method='first', ascending=False)

# Sort the DataFrame by 'ser_num', 'input_text', and 'Similarity' in descending order
results = results.sort_values(by=['ser_num',  'class', 'input_text', 'Similarity'], ascending=[True, True, True, False])

# COMMAND ----------

# DBTITLE 1,Keep only top 3 matches
# Filter the 'results' DataFrame to keep only rows where 'rank' is less than or equal to 3
results = results[results['rank'] <= 3]

# COMMAND ----------

# DBTITLE 1,Transform Results
# Create results_pivot for NAICS2017 codes
results_pivot_codes = results.pivot_table(
     index=['ser_num',  'class', 'input_text'], 
     columns='rank', 
     values='NAICS2017', 
     aggfunc='first'
 )

# # Rename the columns for NAICS2017 codes
results_pivot_codes.columns = ['NAICS_Code_' + str(col) for col in results_pivot_codes.columns]

# # Create results_pivot for Similarity values
results_pivot_similarity = results.pivot_table(
     index=['ser_num', 'class', 'input_text'], 
     columns='rank', 
     values='Similarity', 
     aggfunc='first'
 )

# # Rename the columns for Similarity values
results_pivot_similarity.columns = ['Similarity_' + str(col) for col in results_pivot_similarity.columns]

# # Create results_pivot for NAICS2017_LABEL
results_pivot_labels = results.pivot_table(
     index=['ser_num', 'class', 'input_text'], 
     columns='rank', 
     values='NAICS2017_LABEL', 
     aggfunc='first'
 )

# # Rename the columns for NAICS2017_LABEL
results_pivot_labels.columns = ['NAICS_Label_' + str(col) for col in results_pivot_labels.columns]

# # Merge the pivot tables
results_pivot = pd.concat([results_pivot_codes, results_pivot_similarity, results_pivot_labels], axis=1)

# # Reset index to flatten the DataFrame
results_pivot.reset_index(inplace=True)

# COMMAND ----------

df_out = spark.createDataFrame(results_pivot).select(
    'ser_num',
    'class',
    'input_text',
    col("`NAICS_Code_1.0`").alias('NAICS_Code_1'),
    col('`NAICS_Code_2.0`').alias('NAICS_Code_2'),
    col('`NAICS_Code_3.0`').alias('NAICS_Code_3'),
    col('`NAICS_Label_1.0`').alias('NAICS_Label_1'),
    col('`NAICS_Label_2.0`').alias('NAICS_Label_2'),
    col('`NAICS_Label_3.0`').alias('NAICS_Label_3'),
    col('`Similarity_1.0`').alias('Similarity_1'),
    col('`Similarity_2.0`').alias('Similarity_2'),
    col('`Similarity_3.0`').alias('Similarity_3')
)

# COMMAND ----------

# insert to delta table
df_out.write.mode("append").format("delta").insertInto(f"{reporting_catalog}.gold.naics_fasttext")

# COMMAND ----------

# DBTITLE 1,End Job Control
# end job control
recs_count = df_out.count()
end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")