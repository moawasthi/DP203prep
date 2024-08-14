# Databricks notebook source
# MAGIC %md
# MAGIC #Single Table Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query the Sales.Orders table and give only orders placed in June 2021

# COMMAND ----------

from pyspark.sql.functions import col

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

# MAGIC %md
# MAGIC ## Query the HR.Employees and give lastname with a letter multiple times

# COMMAND ----------

df_employee = spark.read\
              .option("multiline", True)\
              .json("dbfs:/FileStore/HR_Employees.json")
df_employee_filtered = df_employee.filter(col('lastname').like('%a%') )
#display(df_employee)
display(df_employee_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC ##query Sales.Orders returning three shipped to countries with highest average freight

# COMMAND ----------

df_orders = spark.read.csv("dbfs:/FileStore/Sales_Orders.csv", header=True)
schema_orders = StructType([StructField("shipcountry", StringType(),True), \
                            StructField("freight", DoubleType(), True)])
df_schemabound_orders = df_orders.select(col("shipcountry"), col("freight")).schema(schema_orders)
df_grouped= df_schemabound_orders.groupBy("shipcountry").sum("freight").alias("TotalFreight")
display(df_grouped)
