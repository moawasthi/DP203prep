# Databricks notebook source
#importing libraries
from pyspark.sql.functions import col, concat_ws, substring, when

# COMMAND ----------

# read a file uploaded in DBFS

def readfiles(file_format, first_row_as_headers, infer_schema, sep, path):
    df = spark.read.format(file_format)\
        .option("header", first_row_as_headers)\
        .option("inferSchema", infer_schema)\
        .option("sep", sep)\
        .load(path)
    return df

# COMMAND ----------


def writedataframetoDBFS(df, file_format, header, sep, location):
    df.write.format(file_format).mode("overwrite")\
    .option("header", header)\
    .option("sep", sep).save(location)

