# Databricks notebook source
# MAGIC %md
# MAGIC How to Use This Notebook:
# MAGIC 1) Upload Sample files in dbks folder
# MAGIC 2) Read the questions, prepare the queries
# MAGIC 3) compare the result

# COMMAND ----------

# select all data from SalesOrders
df_sales_order = spark.read.option("header",True)\
                           .format("csv").load("dbfs:/FileStore/Orders.csv")
display(df_sales_order)

# COMMAND ----------

# select data from SalesOrders where custid = 71
df_sales_orders= spark.read.option("header", True)\
                           .format("csv")\
                           .load("dbfs:/FileStore/Orders.csv")
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

# COMMAND ----------

# select for custid 71 from sales orders the orderyear (another approach)

from pyspark.sql.functions import col, year, max

df_sales_order = spark.read.option("header", True)\
                           .option("inferSchema", True)\
                           .format("csv")\
                           .load("dbfs:/FileStore/Orders.csv")

df_sales_order_agg = df_sales_order.filter(col("custid") == 71)\
                                 .withColumn("orderyear", year("orderdate"))\
                                 .groupBy("empid", "orderyear").agg(max("orderyear").alias("maxorderyear"))\
                                 .select("empid", "orderyear")
                                 

display(df_sales_order_agg)

# COMMAND ----------

from pyspark.sql.functions  import *
from pyspark.sql.types import DateType
from pyspark.sql import functions as F

# in sales order, return empid, totalfreight, count of orders for custid 71
sales_orders = spark.read.option("header", True).format("csv").load("dbfs:/FileStore/Orders.csv")
sales_orders_filtered = sales_orders.where(sales_orders["custid"] ==  71)
sales_orders_filtered_addYear = sales_orders_filtered.withColumn("orderyear", year(col("orderdate").cast(DateType())) )
sales_orders_agg = sales_orders_filtered_addYear.groupBy("empid", "orderyear").agg(sum("freight").alias("totalfreight"), count("orderid").alias("numorders") )
display(sales_orders_agg)


# COMMAND ----------

# from sales orders fetch employee id, orderyear, distinct number of customers
from pyspark.sql.functions import col, year, max, countDistinct

sales_orders = spark.read.option("header", True)\
                         .option("inferSchema", True)\
                         .format("csv").load("dbfs:/FileStore/Orders.csv")

sales_orders_orderyear = sales_orders.withColumn("orderyear", year("orderdate"))

sales_orders_orderyear_filtered = sales_orders_orderyear.filter(col(custid) == 71)

sales_agg_orderyear_num = sales_orders_orderyear_filtered.groupBy("empid", "orderyear").agg(countDistinct("custid").alias("numcust"))

display(sales_agg_orderyear_num)

# COMMAND ----------

#for custid 71, provide all the empids, orderyear which are greater than 1
from pyspark.sql.functions import col, year, count
sales_orders = spark.read.option("header",True)\
                         .option("inferSchema", True)\
                         .format("csv")\
                         .load("dbfs:/FileStore/Orders.csv")
sales_orders_filtered = sales_orders.filter( col("custid") == 71)
sales_orders_addColumn = sales_orders_filtered.withColumn("orderyear", year("orderdate"))

sales_orders_agg = sales_orders_addColumn.groupBy("empid", "orderyear")\
                                         .agg(count("empid").alias("countEmpId"))\
                                         .filter(col("countEmpId") > 0)
display(sales_orders_agg)

# COMMAND ----------

# give distinct empId and orderyear from Sales Orders table for custid = 71
from pyspark.sql.functions import col, year
sales_orders = spark.read.option("header", True)\
                         .option("inferSchema", True)\
                         .format("csv")\
                         .load("dbfs:/FileStore/Orders.csv")

sales_orders_distinct =  sales_orders.filter(col("custid")==71)\
                         .withColumn("orderyear", year("orderdate"))\
                         .select("empid", "orderyear")\
                         .distinct()

display(sales_orders_distinct)


