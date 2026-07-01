# Databricks notebook source
#from itertools import chain
from pyspark.sql.functions import col, create_map, lit, regexp_extract, concat_ws, collect_list, when, expr,regexp_replace,encode,length, split,trim,filter,to_date,date_format,last,lit,concat,size,min,max,first,last,month,year,current_timestamp,lit, upper,datediff, initcap, lag, add_months, date_add,countDistinct,lead,row_number,substring,avg,current_date
from pyspark.sql.types import ShortType, IntegerType, LongType, StringType, DateType, TimestampType, StructField, StructType, DoubleType
from delta.tables import *
import yaml
import os
from pyspark.sql import Window
from datetime import datetime
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import count as _count
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import col, expr, posexplode, sequence

# COMMAND ----------

#Date validation
def replace_null_with_condition(df,field_name):
    return df.withColumn(field_name,when(col(field_name)<="1400-01-01",None).otherwise(col(field_name)))

# COMMAND ----------

def read_yaml(file_path):
    with open(file_path, "r") as f:
        return yaml.safe_load(f)  # or full_load(f)
