# Databricks notebook source
dbutils.fs.help()

# COMMAND ----------

dbutils.fs.cp('dbfs:/FileStore/tables/EmpDetails__diffdata.csv','dbfs:/FileStore/sink_file/EmpDetails__diffdata.csv',True)

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/sink_file')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/tables')

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/tables/EmpDetails-1.csv',True)

# COMMAND ----------

dbutils.fs.mv('dbfs:/FileStore/tables/EmpDetails1.csv','dbfs:/FileStore/sink_file',True)

# COMMAND ----------

dbutils.fs.mkdirs('dbfs:/FileStore/source_file')

# COMMAND ----------

dbutils.fs.put('dbfs:/FileStore/source_file/abc.txt',str('12345Remo Desuza'),True)

# COMMAND ----------

dbutils.fs.head('dbfs:/FileStore/source_file/abc.txt',9)

# COMMAND ----------

