# Databricks notebook source
spark.sql('CREATE DATABASE IF NOT EXISTS db2') 

# COMMAND ----------

spark.sql('USE db2')

# COMMAND ----------

spark.sql("""CREATE TABLE IF NOT EXISTS student1(Id int, Name string)""")

# COMMAND ----------

spark.sql("""INSERT INTO student1
VALUES(1,'Rahul')""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM student1