# Databricks notebook source
# MAGIC %md
# MAGIC #Single Table Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query the Sales.Orders table and give only orders placed in June 2021

# COMMAND ----------

from pyspark.sql.functions import col, last_day, countDistinct, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# COMMAND ----------


df_orders = spark.read.csv("dbfs:/FileStore/Sales_Orders.csv", header=True)
#display(df_orders)
df_orders_filtered = df_orders.filter((df_orders.orderdate >= "2021-06-01") & (df_orders.orderdate < "2021-07-01") )
#display(df_orders_filtered["orderid","orderdate", "custid","empid"])

display(df_orders_filtered)
 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query the Sales.Orders table and give only orders placed on last day of the months

# COMMAND ----------

df_orders = spark.read.csv("dbfs:/FileStore/Sales_Orders.csv", header=True)
df_orders_lastday = df_orders.filter("orderdate" == last_day("orderdate"))
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
df_schemabound_orders = df_orders.select(
    col("shipcountry").cast(StringType()),
    col("freight").cast(DoubleType())
)
df_grouped= df_schemabound_orders.groupBy("shipcountry").sum("freight").alias("TotalFreight")
display(df_grouped)
df_grouped_orderby = df_grouped.orderBy(col("sum(freight)").desc())
df_grouped_top3 = df_grouped_orderby.limit(3)
#display(df_grouped_top3('shipcountry', col("sum(freight)")


display(df_grouped_top3.select("shipcountry", col("sum(freight)").alias("TotalFreight")))

# COMMAND ----------

# MAGIC %md
# MAGIC # Joins

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate five copies of Employee table

# COMMAND ----------

df_employee = spark.read.option("multiline", True).json("dbfs:/FileStore/HR_Employees.json")
#df_employees = df_employee.crossJoin(df_employee).crossJoin(df_employee).crossJoin(df_employee)

df_nums = spark.read.option("header",True).csv("dbfs:/FileStore/Nums.csv")
df_employees = df_employee.crossJoin(df_nums.filter(col("n")<=5))
display(df_employees)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Return US customers along with number of orders and total quantity

# COMMAND ----------

df_customers = spark.read.csv("dbfs:/FileStore/Sales_Customers.csv", header=True)
df_salesOrders = spark.read.csv("dbfs:/FileStore/Sales_Orders.csv", header=True)
df_salesOrderDetails = spark.read.csv("dbfs:/FileStore/Sales_OrderDetails.csv", header=True)

df_final = df_customers.filter(col('country') == "USA")\
                       .join(df_salesOrders, \
                             df_customers.custid == df_salesOrders.custid,"inner")\
                       .join(df_salesOrderDetails, \
                              df_salesOrders.orderid == df_salesOrderDetails.orderid,"inner")\
                        .groupBy(df_customers.custid).agg(\
                                    countDistinct(df_salesOrders.orderid).alias("TotalOrders") , sum(df_salesOrderDetails.qty).alias("TotalQuantity") )
                                   
display(df_final)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Return customers who placed no orders

# COMMAND ----------

df_customers = spark.read.csv("dbfs:/FileStore/Sales_Customers.csv", header=True)
df_orders = spark.read.csv("dbfs:/FileStore/Sales_Orders.csv", header=True)

df_final = df_customers.join(df_orders, df_customers.custid == df_orders.custid, "left").filter(df_orders.orderid.isNull() == True).select(df_customers.custid, df_customers.companyname)
display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC #Subqueries alternatives and Ranking functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## max value in orderdate column for each employee

# COMMAND ----------

from pyspark.sql.functions import max

df_orders = spark.read.csv("dbfs:/FileStore/Sales_Orders.csv", header=True)
df_final = df_orders.groupBy("empid")\
                    .agg(max("orderdate").alias("maxorderdate") )\
                    .select("empid", "maxorderdate")
display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add orderid and custid in the above resultset

# COMMAND ----------

from pyspark.sql.functions import max
df_orders = spark.read.csv("dbfs:/FileStore/Sales_Orders.csv", header=True)
df_maxdateorders = df_orders.groupBy("empid")\
                            .agg(max("orderdate").alias("maxorderdate") )\
                            .select("empid", "maxorderdate")
df_orders_alias = df_orders.alias("orders")
df_maxdateorders_alias = df_maxdateorders.alias("maxdateorders")
df_orders_final = df_orders_alias.join(df_maxdateorders_alias, df_orders_alias.empid == df_maxdateorders_alias.empid, "left").select(df_orders_alias.empid, df_orders_alias.orderdate, df_orders_alias.orderid, df_orders_alias.custid)
display(df_orders_final)                        

# COMMAND ----------

# MAGIC %md
# MAGIC ## RowNumber for each order based on Orderdate,orderid

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


windowSpec  = Window.orderBy("orderid", "orderdate")

df_orders = spark.read.csv("dbfs:/FileStore/Sales_Orders.csv", header= True)
df_final = df_orders.withColumn("rnk", row_number().over(windowSpec))
display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ## filter row_numbers 1 to 10 from above resultset

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

df_orders = spark.read.csv("dbfs:/FileStore/Sales_Orders.csv", header= True)
df_rnk_no = df_orders.withColumn("rnk", row_number().over(windowSpec))
df_final = df_rnk_no.filter((df_rnk_no.rnk >=1) & (df_rnk_no.rnk <= 10))
display(df_final)
