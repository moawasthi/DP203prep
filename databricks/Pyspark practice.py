# Databricks notebook source
# MAGIC %md
# MAGIC #Single Table Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query the Sales.Orders table and give only orders placed in June 2021

# COMMAND ----------


df_orders = spark.read.csv("dbfs:/FileStore/Sales_Orders.csv", header=True)
#display(df_orders)
df_orders_filtered = df_orders.filter((df_orders.orderdate >= "2021-06-01") & (df_orders.orderdate < "2021-07-01") )
display(df_orders_filtered["orderid","orderdate", "custid","empid"])
 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query the Sales.Orders table and give only orders placed on last day of the montha

# COMMAND ----------

df_orders = spark.read.csv("dbfs:/FileStore/Sales_Orders.csv", header=True)")
df_orders_lastday = df_orders.filter("orderdate" = last_day("orderdate"))
display(df_orders_lastday["orderid","orderdate", "custid","empid"])

# COMMAND ----------


