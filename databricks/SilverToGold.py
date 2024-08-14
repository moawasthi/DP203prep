# Databricks notebook source
import os
import pyspark.pandas as ps
import pyspark.sql.functions as f

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.functions import monotonically_increasing_id  

silver_path = "contosodev.sales.fact_sales"
df_silver = spark.table("contosodev.sales.Sales_Retail_Silver")
df_product = df_silver.select("ASIN", "Product_Title").dropDuplicates()
df_product = df_product.withColumn("ProductId", monotonically_increasing_id())

df_product.write.mode("overwrite")\
    .format("delta")\
    .option("overwriteSchema", "true")\
    .saveAsTable("contosodev.sales.Product")

df_product.write \
  .format("com.databricks.spark.sqldw") \
  .option("url", "jdbc:sqlserver://contoso-synapse-dev.sql.azuresynapse.net:1433;Initial Catalog=dsql1108dev;Persist Security Info=False;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;") \
  .option("user", "sqladminuser") \
  .option("password", "adminuser@123") \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", "dbo.Product") \
  .option("tempDir", "abfss://contosometastore@sabicontosodev.dfs.core.windows.net/temp/product") \
  .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from contosodev.sales.product

# COMMAND ----------

from delta.tables import DeltaTable
  
df_fact =  df_silver["ASIN","Ordered_Revenue", "Ordered_Unit", "Dispatched_Revenue", "Dispatched_COGS", "Dispatched_Unit"]
df_fact = df_fact.withColumn("fact_sales_Id", monotonically_increasing_id())
df_fact = df_fact.withColumn("Sales_Date", f.current_date())

df_fact = df_fact.join(df_product, df_fact.ASIN ==  df_product.ASIN, "inner")

df_fact = df_fact["fact_sales_Id","Sales_Date","ProductId","Ordered_Revenue", "Ordered_Unit", "Dispatched_Revenue", "Dispatched_COGS", "Dispatched_Unit"]

df_fact.write.mode("overwrite")\
    .format("delta")\
    .option("overwriteSchema", "true")\
    .saveAsTable("contosodev.sales.Fact_Retail")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from contosodev.sales.fact_retail

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from contosodev.sales.fact_retail
