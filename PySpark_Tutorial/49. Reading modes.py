# Databricks notebook source
# MAGIC %md
# MAGIC Permissive, DropMalformed, FailFast
# MAGIC

# COMMAND ----------

df = spark.read.format("csv").option("header","true").option("inferschema","true").load('dbfs:/FileStore/tables/EmpDetails__diffdata.csv')
display(df)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

sch = StructType([StructField('Emp_Id', IntegerType(),True),\
    StructField('Emp_Name', StringType(),True),\
    StructField('Age', IntegerType(),True),\
    StructField('city', StringType(),True),\
    StructField('corruptRecord', StringType(),True)])

# COMMAND ----------

df = spark.read.format("csv").option("mode","PERMISSIVE").option("header","true").schema(sch).load('dbfs:/FileStore/tables/EmpDetails__diffdata.csv')
display(df)

# COMMAND ----------

df1 = spark.read.format("csv").option("mode","DROPMALFORMED").option("header","true").schema(sch).load('dbfs:/FileStore/tables/EmpDetails__diffdata.csv')
display(df1)

# COMMAND ----------

df2 = spark.read.format("csv").option("mode","FAILFAST").option("header","true").schema(sch).load('dbfs:/FileStore/tables/EmpDetails__diffdata.csv')
display(df2)