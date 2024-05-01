# Databricks notebook source
data = [(1,'Rahul',33,'Kolkata'),(2,'Raj',12,'Delhi'),(3,'Priya',56, 'Mumbai'),(3,'Murti',37, 'Indore')]
schema = "Id int, Name string, Roll int, City string"

df = spark.createDataFrame(data,schema)
display(df)

df.createOrReplaceTempView('student_tempview')

df1 = spark.sql("SELECT ID,Name FROM student_tempview")
df1.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ID, Name from student_tempview