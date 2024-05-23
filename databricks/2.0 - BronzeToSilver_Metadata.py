# Databricks notebook source
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

dbutils.widgets.text("FunctionalDomain", "","Functional Domain")
dbutils.widgets.text("FlowId","", "Flow Id")
dbutils.widgets.text("DataSource","", "Data Source")
dbutils.widgets.text("Entity", "", "Entity")
dbutils.widgets.text("SubEntity", "" , "Sub Entity")

# COMMAND ----------

data_source = dbutils.widgets.get("DataSource")
functional_domain = dbutils.widgets.get("FunctionalDomain")
flow_id = dbutils.widgets.get("FlowId")
entity = dbutils.widgets.get("Entity")
table_name = dbutils.widgets.get("SubEntity")

# COMMAND ----------

col_dict_Silver_Name_Datatype = eval('col_dict_Silver_Name_Datatype_' + table_name)
col_dict_Silver_Name_Datatype['DATE_INSERT'] = 'TimeStampType'
col_dict_Silver_Name_Datatype['SOURCE'] = 'StringType'
col_names = list(col_dict_Silver_Name_Datatype.keys())
attributename_datatypes = ", ".join([f"{i} {getattr(types, col_dict_Silver_Name_Datatype[i])().simpleString()}" for i in col_names])
