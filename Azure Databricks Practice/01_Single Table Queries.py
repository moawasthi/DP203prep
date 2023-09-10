# Databricks notebook source
#source code to connect to ADLS

def connectToAdLS(storageAccountname) :
    spark.conf.set("fs.azure.account.auth.type.{}.dfs.core.windows.net".format(storageAccountname), "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type.{}.dfs.core.windows.net".format(storageAccountname), "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id.{}.dfs.core.windows.net".format(storageAccountname), "c8d09621-a324-48f9-9ffb-e57d032f5282")
    spark.conf.set("fs.azure.account.oauth2.client.secret.{}.dfs.core.windows.net".format(storageAccountname), "mAI8Q~RR5Rikw3wfYt2oNC0zfBx1HJaD2yVZnbnc")
    spark.conf.set("fs.azure.account.oauth2.client.endpoint.{}.dfs.core.windows.net".format(storageAccountname), "https://login.microsoftonline.com/d4b3ee6c-57d2-4a98-9614-17c24476d63b/oauth2/token")


# COMMAND ----------

connectToAdLS("sabicontosodev")

# COMMAND ----------

from pyspark.sql.functions import *

df = spark.read.option("header",True).csv("abfss://source@sabicontosodev.dfs.core.windows.net/Orders.csv")

df_addyear = df.withColumn("year", year("orderdate") ).filter( df.custid == 76)
df_agg = df_addyear.groupBy("year", "empid").agg( count(col("empid")).alias("numcounts") ).where( col("numcounts") > 1)
#display(df_agg)

#numbers of unique customers handled by each employee in each order year
df_orders = spark.read.option("header", True).csv("abfss://source@sabicontosodev.dfs.core.windows.net/Orders.csv")
df_orders_year = df_orders.withColumn("year", year("orderdate"))
df_orders_agg = df_orders_year.groupBy("empid", "year").agg( countDistinct("custid").alias("numcustdistinct")).orderBy("empid")
#display(df_orders_agg) 

# select all the rows from the shippers table

df_shippers = spark.read.option("header",True).csv("abfss://source@sabicontosodev.dfs.core.windows.net/Shippers.csv")
#display(df_shippers)
# select orderid, current year and next year 

df_orders = spark.read.option("header",True).csv("abfss://source@sabicontosodev.dfs.core.windows.net/Orders.csv")
df_orders_select = df_orders.withColumn("year", year("orderdate")).withColumn("nextyear" , year("orderdate") +1 ).select("orderid", "year", "nextyear")
display(df_orders_select)



