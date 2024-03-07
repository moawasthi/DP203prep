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

path_raw = 'abfss://' + functional_domain + '@sabiiacdev' + '.dfs.core.windows.net/raw/input'
path_input = 'abfss://' + functional_domain + '@sabiiacdev' + '.dfs.core.windows.net/{0}/input/{1}/{2}/{3}/{4}/{5}'.format(functional_sub_domain, flow_id, data_source, entity, ingestion_type, sub_entity)
path_output = 'abfss://' + functional_domain + '@sabiiacdev' + '.dfs.core.windows.net/{0}/output/{1}/{2}/{3}/{4}/{5}'.format(functional_sub_domain, flow_id, data_source, entity, ingestion_type, sub_entity)
path_error = 'abfss://' + functional_domain + '@sabiiacdev' + '.dfs.core.windows.net/{0}/output/{1}/{2}/{3}/{4}/{5}'.format(functional_sub_domain, flow_id, data_source, entity, ingestion_type, sub_entity)

display(path_raw)

# COMMAND ----------

dbutils.fs.ls(path_raw)

# COMMAND ----------

try:
  dbutils.fs.ls(path_raw)[0]
except:
  dbutils.notebook.exit(f"No files in {path_raw} or the Path doesn't exist")

# COMMAND ----------

def getFilebyTime_in_subentityFolder(directory, expectedname, searchmethod = 'last'):
    filesindir = dbutils.fs.ls(directory)
    selectedfiles = list(filter(lambda x: expectedname in x.name, filesindir) )
    selectedfiles.sort(key=lambda file: file.modificationTime, reverse = True)
    return selectedfiles[0].name


# COMMAND ----------

try:
    filename = getFilebyTime_in_subentityFolder('abfss://masterdata@sabiiacdev.dfs.core.windows.net/raw/input', 'dailyTripData')
except:
    raise Exception(f'None of the filenames in path {path_raw} contains {sub_entity}')

# COMMAND ----------

dbutils.fs.cp('abfss://masterdata@sabiiacdev.dfs.core.windows.net/raw/input/'+ filename , 'abfss://masterdata@sabiiacdev.dfs.core.windows.net/raw/input/staging/' + filename )
