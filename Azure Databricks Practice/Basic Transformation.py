# Databricks notebook source
# MAGIC %run "../Azure Databricks Practice/ReadFiles"

# COMMAND ----------

# select columns with Alias
df = readfiles("csv", "true", "true",",", "dbfs:/FileStore/demo/MOCK_DATA.csv" )
df.printSchema()
df = df.select(col("id").alias("Id"), col("first_name"), col("last_name") )
display(df)

dfSorted = df.select(col("id").alias("Id"), concat_ws(", ", col("first_name"), col("last_name")).alias("fullName") )
dfSorted = dfSorted.sort(dfSorted.fullName)
display(dfSorted)


# add columns
df = df.select(concat_ws(", ", col("first_name"), col("last_name")).alias("fullName"))
display(df)



# sort columns
df = df.sort(df.fullName.asc())
display(df)
 

#group by
df = df.groupBy(substring(df.fullName, 1,1).alias("StartsWith")).count()
display(df)



# filter columns
df = df.filter(df.StartsWith == "A")
display(df)

# merge two dataframes together
display(df)
display(dfSorted)
df = df.union(dfSorted)

display(df)




