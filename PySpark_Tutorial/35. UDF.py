# Databricks notebook source
data = [(1,'Rahul',75,92),(2,'Raj',86,79),(3,'Priya',56,67),(3,'Murti',87,92 )]
schema = "Id int, Name string, Math int, Physics int"

df = spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

def SumOfSub(m,p):
    return m+p

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
SumOfSubjects = udf(lambda x,y: SumOfSub(x,y), IntegerType())

df.withColumn('TotalMarks',SumOfSubjects(df.Math,df.Physics)).show()

# COMMAND ----------

data = [(1,'Rahul',75,92),(2,'Raj',86,79),(3,'Priya',56,67),(3,'Murti',87,92 )]
schema = "Id int, Name string, Math int, Physics int"

df = spark.createDataFrame(data,schema)
df.createOrReplaceTempView('student')


spark.udf.register(name='TotalMarks',f=SumOfSub,returnType=IntegerType())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, TotalMarks(Math,Physics) as TotalMarks FROM student

# COMMAND ----------

