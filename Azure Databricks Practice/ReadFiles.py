# Databricks notebook source
#importing libraries
from pyspark.sql.functions import col, concat_ws, substring

# COMMAND ----------

# read a file uploaded in DBFS

def readfiles(file_format, first_row_as_headers, infer_schema, sep, path):
    df = spark.read.format(file_format)\
        .option("header", first_row_as_headers)\
        .option("inferSchema", infer_schema)\
        .option("sep", sep)\
        .load(path)
    return df
