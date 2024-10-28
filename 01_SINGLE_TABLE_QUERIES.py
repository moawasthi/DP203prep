# Databricks notebook source
# MAGIC %md
# MAGIC How to Use This Notebook:
# MAGIC 1) Upload Sample files in dbks folder
# MAGIC 2) Read the questions, prepare the queries
# MAGIC 3) compare the result

# COMMAND ----------

# select all data from SalesOrders
df_sales_order = spark.read.option("header",True).format("csv").load("dbfs:/FileStore/Orders.csv")
df_sales_order.show()

# COMMAND ----------

# select data from SalesOrders where custid = 71
df_sales_orders= spark.read.option("header", True).format("csv").load("dbfs:/FileStore/Orders.csv")
df_sales_orders_filtered = df_sales_orders.where(df_sales_orders["custid"] == 71)
display(df_sales_orders_filtered)

# COMMAND ----------

from pyspark.sql.functions import col, year, max
from pyspark.sql.types import DateType
from pyspark.sql import functions as F

# select for custid 71 from sales orders the orderyear

df_sales_order = spark.read.option("header", True).format("csv").load("dbfs:/FileStore/Orders.csv")
df_sales_orders_addingYear = df_sales_order.withColumn("orderyear", year(col("orderdate").cast(DateType() ) ) )

df_sales_orders_addingYear_filtered = df_sales_orders_addingYear.filter(col("custid") == 71)

df_sales_orders_groupBy_v2 = df_sales_orders_addingYear_filtered.groupBy("empid").agg(max("orderyear").alias("maxyear"))
display(df_sales_orders_groupBy_v2)
