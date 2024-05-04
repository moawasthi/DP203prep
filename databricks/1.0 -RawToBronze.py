# Databricks notebook source
dbutils.widgets.text("FunctionalDomain","")
dbutils.widgets.text("FunctionalSubDomain","")
dbutils.widgets.text("FlowId","")
dbutils.widgets.text("DataSource","")
dbutils.widgets.text("Entity","")
dbutils.widgets.text("IngestionType","")
dbutils.widgets.text("SubEntity","")
business_date = dbutils.widgets.get("BusinessDate") 

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
display(functional_domain)

# COMMAND ----------

path_raw = 'abfss://' + functional_domain + '@sabicontosodev.dfs.core.windows.net/raw/{0}/{1}/{2}/{3}/{4}/'.format(flow_id, data_source, entity, ingestion_type, sub_entity)
path_bronze_input = 'abfss://' + functional_domain + '@sabicontosodev.dfs.core.windows.net/bronze/{0}/{1}/{2}/{3}/input/{4}/'.format(flow_id, data_source, entity, ingestion_type, sub_entity)
path_bronze_output = 'abfss://' + functional_domain + '@sabicontosodev.dfs.core.windows.net/bronze/{0}/{1}/{2}/{3}/output/{4}/'.format(flow_id, data_source, entity, ingestion_type, sub_entity)
path_bronze_error = 'abfss://' + functional_domain + '@sabicontosodev.dfs.core.windows.net/bronze/{0}/{1}/{2}/{3}/error/{4}/'.format(flow_id, data_source, entity, ingestion_type, sub_entity)
display(path_raw)

# COMMAND ----------

dbutils.fs.ls(path_raw)

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
    raise Exception ("raw path {path_raw} does not contain files with name {sub_entity}")

# COMMAND ----------

bronze_input_file_name = business_date + raw_file_name
dbutils.fs.cp(path_raw + raw_file_name, path_bronze_input + bronze_input_file_name)
