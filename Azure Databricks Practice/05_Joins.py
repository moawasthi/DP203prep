# Databricks notebook source
# generate 5 copies
df_emp = spark.read.option("header", "true").csv("/FileStore/demo/Employee.csv")
df_emp.printSchema()
df_num = spark.read.option("header", "true").csv("/FileStore/demo/Num.csv")
#display(df_num)
df_num = df_num.filter( df_num.Num <=5 )
display(df_num)
df_copies = df_emp.crossJoin(df_num) 
display(df_copies)
