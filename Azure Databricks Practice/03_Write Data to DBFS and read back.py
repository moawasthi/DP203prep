# Databricks notebook source
# MAGIC %run "../Azure Databricks Practice/GenericUtils"

# COMMAND ----------

dfnew = readfiles("csv", "true", "true", ",", "dbfs:/FileStore/demo/MOCK_DATA.csv")
dfnew.printSchema()
# Grouping data
dfnew = dfnew.groupBy(substring(dfnew.first_name,1,1).alias("StartsWith")).count()
writedataframetoDBFS(dfnew, "csv", "true", ",", "dbfs:/FileStore/demo_out/Employee_agg.csv")

df = readfiles("csv", "true", "true", "," , "/FileStore/demo_out/Employee_agg.csv")
display(df)
