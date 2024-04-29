# Databricks notebook source
dbutils.widgets.text("FunctionalDomain","")
dbutils.widgets.text("FunctionalSubDomain","")
dbutils.widgets.text("FlowId","")
dbutils.widgets.text("DataSource","")
dbutils.widgets.text("Entity","")
dbutils.widgets.text("IngestionType","")
dbutils.widgets.text("SubEntity","")



# COMMAND ----------

functional_domain = dbutils.widgets.get("FunctionalDomain")
functional_sub_domain = dbutils.widgets.get("FunctionalSubDomain")
flow_id = dbutils.widgets.get("FlowId")
data_source = dbutils.widgets.get("DataSource")
entity = dbutils.widgets.get("Entity")
ingestion_type = dbutils.widgets.get("IngestionType")
sub_entity = dbutils.widgets.get("SubEntity")
display(functional_domain)

# COMMAND ----------

path_raw = 'abfss://' + functional_domain + '@sabicontosodev.dfs.core.windows.net/raw/{0}/{1}/{2}/{3}/{4}/'.format(flow_id, data_source, entity, ingestion_type, sub_entity)
display(path_raw)

# COMMAND ----------

dbutils.fs.ls(path_raw)

# COMMAND ----------

try:
    dbutils.fs.ls(path_raw)
except:
    dbutils.notebook.exit('no files in raw path')
