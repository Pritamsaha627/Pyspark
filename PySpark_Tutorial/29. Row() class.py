# Databricks notebook source
from pyspark.sql import Row

r1 = Row(name='Liza',roll=20)
r2 = Row(name='Riya',roll=25)
data = [r1,r2]
df = spark.createDataFrame(data)
df.show()
df.printSchema()

# COMMAND ----------

student = Row('name','roll')
s1 = student('Priyam',12)
s2 = student('Amit',34)

df1 = spark.createDataFrame([s1,s2])
df1.show()

# COMMAND ----------

data = [Row(name='Riya',sub=Row(Eng=85,Math=92)),
        Row(name='Virat',sub=Row(Eng=75,Math=77)),]

df3 = spark.createDataFrame(data)
df3.show()

# COMMAND ----------

help(Row)