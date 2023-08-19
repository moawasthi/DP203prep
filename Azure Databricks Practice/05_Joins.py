# Databricks notebook source
# generate 5 copies
df_emp = spark.read.option("header", "true").csv("/FileStore/Customers.csv")
#df_emp.printSchema()
#df_num = spark.read.option("header", "true").csv("/FileStore/Num.csv")
#display(df_num)
#df_num = df_num.filter( df_num.Num <=5 )
#display(df_num)
#df_copies = df_emp.crossJoin(df_num) 
#display(df_copies)

df_cust = spark.read.option("header","true").csv("/FileStore/Customers.csv")
df_sales = spark.read.option("header","true").option("inferSchema", "true").csv("/FileStore/Orders.csv")
df_salesorder = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/OrderDetails.csv")

#display(df_cust)
#display(df_sales)
#display(df_salesorder)

df_orderqty = df_sales.join(df_salesorder, df_sales.orderid == df_salesorder.orderid, "inner")
df_orderqty.printSchema()
df_orderqty = df_orderqty.groupBy("custid").agg( sum("qty"))
display(df_orderqty)

