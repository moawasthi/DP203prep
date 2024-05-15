# Databricks notebook source
import os
from pyspark.sql.functions import lit, col
import logging
import pyspark.sql.functions as F

# COMMAND ----------

dbutils.widgets.text("FunctionalDomain","")
dbutils.widgets.text("FunctionalSubDomain","")
dbutils.widgets.text("FlowId","")
dbutils.widgets.text("DataSource","")
dbutils.widgets.text("Entity","")
dbutils.widgets.text("IngestionType","")
dbutils.widgets.text("SubEntity","")
dbutils.widgets.text("BusinessDate", "")
business_date = dbutils.widgets.get("BusinessDate") 

# COMMAND ----------

col_dict_Silver_Name_Datatype_PRODUCT = \
    {
        "category" : "StringType",
        "CategoryDesc" : "StringType",
        "Brand" : "StringType",
        "BrandDesc" : "StringType",
        "Weight" : "StringType",
        "Volume" : "StringType"
    }


# COMMAND ----------

col_dict_primary_key = \
    {
        "PRODUCT" : ["category", "CategoryDesc", "Brand", "BrandDesc"]
    }

# COMMAND ----------

def getfilebytime_in_subentityfolder(directory, expected_name, search_method = "last"):
    files_in_dir = dbutils.fs.ls(directory)
    selected_files_in_dir = list(filter(lambda x : expected_name in x.name, files_in_dir))
    if search_method == 'last':
        reverse = True
    if search_method =='first':
        reverse = False
    selected_files_in_dir.sort(key = lambda file: file.modificationTime, reverse = reverse)
    return selected_files_in_dir[0].name

# COMMAND ----------

functional_domain = dbutils.widgets.get("FunctionalDomain")
functional_sub_domain = dbutils.widgets.get("FunctionalSubDomain")
flow_id = dbutils.widgets.get("FlowId")
data_source = dbutils.widgets.get("DataSource")
entity = dbutils.widgets.get("Entity")
ingestion_type = dbutils.widgets.get("IngestionType")
sub_entity = dbutils.widgets.get("SubEntity")

# COMMAND ----------

path_raw = 'abfss://' + functional_domain + '@sabicontosodev.dfs.core.windows.net/{0}/raw/{1}/{2}/{3}/{4}/'.format(flow_id, data_source, entity, ingestion_type, sub_entity)
path_bronze_input = 'abfss://' + functional_domain + '@sabicontosodev.dfs.core.windows.net/{0}/bronze/{1}/{2}/{3}/input/{4}/'.format(flow_id, data_source, entity, ingestion_type, sub_entity)
path_bronze_output = 'abfss://' + functional_domain + '@sabicontosodev.dfs.core.windows.net/{0}/bronze/{1}/{2}/{3}/output/{4}/'.format(flow_id, data_source, entity, ingestion_type, sub_entity)
path_bronze_error = 'abfss://' + functional_domain + '@sabicontosodev.dfs.core.windows.net/{0}/bronze/{1}/{2}/{3}/error/{4}/'.format(flow_id, data_source, entity, ingestion_type, sub_entity)


# COMMAND ----------

display(path_bronze_input)
#dbutils.fs.ls()

# COMMAND ----------

try:
    dbutils.fs.ls(path_raw)
except:
    dbutils.notebook.exit('no files in raw path')

# COMMAND ----------

try:
    search_method = 'first'
    raw_file_name = getfilebytime_in_subentityfolder(path_raw, sub_entity, search_method)
except:
    raise Exception (f"raw path {path_raw} does not contain files with name {sub_entity}")

# COMMAND ----------

bronze_input_file_name = business_date + '_' + raw_file_name
dbutils.fs.cp(path_raw + raw_file_name, path_bronze_input + bronze_input_file_name)

# COMMAND ----------

df_raw = spark.read.format("csv").option("header",True).load(path_bronze_input+bronze_input_file_name)
num_rows = df_raw.count()
display(df_raw)
if num_rows == 0:
    dbutils.notebook.exit("There are no rows in the file {raw_file_name} at path {path_raw}")


# COMMAND ----------

col_dict_Silver = eval('col_dict_Silver_Name_Datatype_' + sub_entity)
col_required_in_Silver = set(col_dict_Silver.keys())
col_in_raw = set(df_raw.columns)
if col_required_in_Silver.difference(col_in_raw):
    raise Exception(f'There are missing attributes in {raw_file_name} at {path_raw}. Missing attributes are {col_required_in_Silver.difference(col_in_raw)}')

# COMMAND ----------

primary_key = col_dict_primary_key[sub_entity]
#display(primary_key)
df_error = None
if primary_key:
    count_distinct_primary_key = df_raw.select(primary_key).count() - (df_raw.select(primary_key).distinct().count())
    if count_distinct_primary_key > 0:
       df_error = df_raw.groupBy(primary_key).count().filter(col('count') > 1).join(df_raw, primary_key, 'left').select(df_raw.columns).withColumn('CORRUPTED_DATA', lit('Duplicate in Primary Key') )
       df_raw = df_raw.distinct()
display(df_error)
display(df_raw)

# COMMAND ----------

isnull_condition = "AND ".join(f"(({col} IS NULL OR {col} = ' ' ))" for col in primary_key)
display(isnull_condition)
argument = "case when " + isnull_condition + "then 'NULL PK' else NULL END"
df_raw = df_raw.withColumn('CORRUPTED_DATA', F.expr( argument) )
display(df_raw) 

# COMMAND ----------

df_bronze_output = df_raw.filter(df_raw.CORRUPTED_DATA.isNull()).drop('CORRUPTED_DATA')
df_bronze_error = df_raw.filter(df_raw.CORRUPTED_DATA.isNotNull()).withColumn("Processing_Date",F.lit(business_date))

if (df_error is not None):
    df_bronze_error = df_bronze_error.union(df_error.withColumn("Processing_Date",F.lit(business_date)) )

df_bronze_error.write.mode('append').format('delta').option("mergeSchema","true").option("path", path_bronze_error).save()



# COMMAND ----------

display(df_bronze_output)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS dev.dbo.{sub_entity}")
spark.sql(f"CREATE EXTERNAL TABLE IF NOT EXISTS dev.dbo.{sub_entity} LOCATION '{path_bronze_output}'")


# COMMAND ----------

df_bronze_output.write.mode('overwrite').format('delta').option('mergeSchema',"true").option('path', path_bronze_output).saveAsTable('dev.dbo.' + sub_entity)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * FROM dev.dbo.product
