# Databricks notebook source
dbutils.widgets.text("FunctionalDomain","", "Functional Domain")
dbutils.widgets.text("FunctionalSubDomain","","Functional Sub Domain") 
dbutils.widgets.text("flowId","","flow Id")
dbutils.widgets.text("Datasource","","Data Source")
dbutils.widgets.text("Entity","")
dbutils.widgets.text("Ingestion Type","")
dbutils.widgets.text("SubEntity","")

# COMMAND ----------

functional_domain = dbutils.widgets.get("FunctionalDomain")
functional_sub_domain = dbutils.widgets.get("FunctionalSubDomain")
flow_id = dbutils.widgets.get("flowId")
