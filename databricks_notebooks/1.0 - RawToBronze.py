# Databricks notebook source
dbutils.widgets.text("FunctionalDomain","", "Functional Domain")
dbutils.widgets.text("FunctionalSubDomain","","Functional Sub Domain") 
dbutils.widgets.text("flowId","","flow Id")
dbutils.widgets.text("Datasource","","Data Source")
dbutils.widgets.text("Entity","")
dbutils.widgets.text("IngestionType","")
dbutils.widgets.text("SubEntity","")

# COMMAND ----------

functional_domain = dbutils.widgets.get("FunctionalDomain")
functional_sub_domain = dbutils.widgets.get("FunctionalSubDomain")
flow_id = dbutils.widgets.get("flowId")
data_source = dbutils.widgets.get("Datasource")
entity = dbutils.widgets.get("Entity")
ingestion_type = dbutils.widgets.get("IngestionType")
sub_entity = dbutils.widgets.get("SubEntity")


# COMMAND ----------




# COMMAND ----------

path_raw = 'abfss://' + functional_domain + '@sabiiac2602dev' + '.dfs.core.windows.net/{0}/raw/{1}/{2}/{3}/{4}/{5}'.format(functional_sub_domain, flow_id, data_source, entity, ingestion_type, sub_entity)
path_input = 'abfss://' + functional_domain + '@sabiiac2602dev' + '.dfs.core.windows.net/{0}/input/{1}/{2}/{3}/{4}/{5}'.format(functional_sub_domain, flow_id, data_source, entity, ingestion_type, sub_entity)
path_output = 'abfss://' + functional_domain + '@sabiiac2602dev' + '.dfs.core.windows.net/{0}/output/{1}/{2}/{3}/{4}/{5}'.format(functional_sub_domain, flow_id, data_source, entity, ingestion_type, sub_entity)
path_error = 'abfss://' + functional_domain + '@sabiiac2602dev' + '.dfs.core.windows.net/{0}/output/{1}/{2}/{3}/{4}/{5}'.format(functional_sub_domain, flow_id, data_source, entity, ingestion_type, sub_entity)

display(path_raw)

# COMMAND ----------

try:
  dbutils.fs.ls(path_raw)[0]
except:
  dbutils.notebook.exit(f"No files in {path_raw} or the Path doesn't exist")
