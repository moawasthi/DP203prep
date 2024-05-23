# Databricks notebook source
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
