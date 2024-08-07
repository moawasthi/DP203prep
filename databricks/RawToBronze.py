# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS contosodev

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS contosodev.sales

# COMMAND ----------

import os
import pyspark.pandas as ps
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import current_date, date_format
import pyspark.sql.functions as f

# COMMAND ----------

def check_data_existence(df):
    rowcount = df.count()
    if rowcount == 0:        raise ValueError("Data is empty")
    else:                    print("Check Data Existence OK!!")
    

# COMMAND ----------

def check_not_null(df, primary_key):
    null_values = df[primary_key].isNull().any()
    if null_values:        raise ValueError("Null values found")    
    else:                    print("Check Not Null OK!!")
    

# COMMAND ----------

def check_no_duplicates(df, primary_key):
    duplicates = df.select(primary_key).dropDuplicates().count()    
    if duplicates > 0:     raise ValueError("Duplicates found")    
    else:                    print("Check No Duplicates OK!!")


# COMMAND ----------

def check_columns_exist(df, col_list):
    col_not_found = [col for col in col_list if col not in df.columns]
    if col_not_found:
        raise ValueError(f"Columns not found: {', '.join(col_not_found)}")
    else:
        print("Check Columns Exist OK!!")

# COMMAND ----------

file_list = [
    f for f in dbutils.fs.ls("abfss://tsqlv6@sabicontosodev.dfs.core.windows.net/raw") 
    if "/archive/" not in f.path
]

bronze_path = "abfss://tsqlv6@sabicontosodev.dfs.core.windows.net/bronze/"

if file_list:
    file_path = file_list[0].path
    file_name = file_list[0].name
    primary_key = "ASIN"
    salesdata = spark.read.csv(file_path, header=True, inferSchema=True)
    columns_list = ["ASIN", "Product Title", "Ordered Revenue", "Ordered Unit", "Dispatched Revenue", "Dispatched COGS", "Dispatched Unit"]
    #data quality checks
    check_data_existence(salesdata)
   # check_not_null(salesdata, primary_key)
    #check_no_duplicates(salesdata, primary_key)
    check_columns_exist(salesdata, columns_list)
    schema_defined = StructType( [StructField ("ASIN", StringType(), True ), 
                          StructField ("Product_Title", StringType(), True ), 
                          StructField ("Ordered_Revenue", StringType(), True ), 
                          StructField ("Ordered_Unit", StringType(), True ), 
                          StructField ("Dispatched_Revenue", StringType(), True ), 
                          StructField("Dispatched_COGS", StringType(), True),
                          StructField("Dispatched_Unit", StringType(), True)]
                          )
    sales_bronze = spark.read.csv(file_path, header=True, schema=schema_defined)
    sales_bronze.write.mode("overwrite").format("delta").option("MergeSchema", True).saveAsTable("contosodev.sales.Sales_Retail")

        #bronze promotion
    dbutils.fs.mv(file_path, bronze_path + file_name )
    #display(file_path + file_name)



# COMMAND ----------

# MAGIC %environment
# MAGIC "client": "1"
# MAGIC "base_environment": ""
