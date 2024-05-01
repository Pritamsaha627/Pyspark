# Databricks notebook source
data = [(1,'Rahul',33,'Kolkata'),(2,'Raj',12,'Delhi'),(3,'Priya',56, 'Mumbai'),(3,'Murti',37, 'Indore')]
schema = "Id int, Name string, Marks int, City string"

df = spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

def ConvertMarksDividation(df):
    return df.withColumn('Marks', df.Marks/2)

def ConvertMarksSum(df):
    return df.withColumn('Marks', df.Marks+10)

# COMMAND ----------

df1 = df.transform(ConvertMarksDividation)\
        .transform(ConvertMarksSum)
df1.show()