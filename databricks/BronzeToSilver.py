# Databricks notebook source
import os
import pyspark.pandas as ps
import pyspark.sql.functions as f

from pyspark.sql.functions import col,lit

# COMMAND ----------

# MAGIC %sql
# MAGIC --do additional data cleansing
# MAGIC UPDATE contosodev.sales.Sales_Retail
# MAGIC   SET Ordered_Revenue = replace(Ordered_Revenue, "€", ""),
# MAGIC    Dispatched_Revenue = replace(Dispatched_Revenue, "€", "")
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE contosodev.sales.Sales_Retail
# MAGIC   SET Ordered_Revenue = replace(Ordered_Revenue,",",""),
# MAGIC    Dispatched_Revenue = replace(Dispatched_Revenue,",","")

# COMMAND ----------

from delta.tables import DeltaTable
import pyspark.sql.functions as f

silver_path = "contosodev.sales.Sales_Retail_Silver"

#read file from raw
df_bronze = spark.table("contosodev.sales.Sales_Retail")

#same cleansing can be done in pyspark as well
df_bronze = df_bronze.withColumn("Ordered_Revenue", f.regexp_replace('Ordered_Revenue', '€', '').cast("double"))\
                     .withColumn("Dispatched_Revenue", f.regexp_replace('Dispatched_Revenue', '€', '').cast("double"))

#create a silver dataframe
df_silver = DeltaTable.forName(spark, "contosodev.sales.Sales_Retail")

df_silver.alias("silver").merge(df_bronze.alias("bronze"), "silver.ASIN= bronze.ASIN")\
    .whenMatchedUpdate(set={"Product_Title": "bronze.Product_Title" ,
                            "Ordered_Revenue": "bronze.Ordered_Revenue",
                            "Ordered_Unit": "bronze.Ordered_Unit",
                            "Dispatched_Revenue": "bronze.Dispatched_Revenue",
                            "Dispatched_COGS": "bronze.Dispatched_COGS",
                            "Dispatched_Unit": "bronze.Dispatched_Unit"
                            })\
    .whenNotMatchedInsert(values={"Product_Title": "bronze.Product_Title" ,
                            "Ordered_Revenue": "bronze.Ordered_Revenue",
                            "Ordered_Unit": "bronze.Ordered_Unit",
                            "Dispatched_Revenue": "bronze.Dispatched_Revenue",
                            "Dispatched_COGS": "bronze.Dispatched_COGS",
                            "Dispatched_Unit": "bronze.Dispatched_Unit"}).execute()
df_silver = df_silver.toDF()

# COMMAND ----------

# Silver promotion
df_silver.write.mode("overwrite")\
    .format("delta")\
    .option("overwriteSchema", "true")\
    .saveAsTable("contosodev.sales.Sales_Retail_Silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from contosodev.sales.Sales_Retail_Silver
