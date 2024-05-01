# Databricks notebook source
# MAGIC %md
# MAGIC row_number, rank, dense_rank

# COMMAND ----------

from pyspark.sql.functions import row_number, rank, dense_rank
from pyspark.sql.window import Window
data = [(1,'Ram', 'HR',25000),(2,'Liza', 'IT',30000),(3,'Margarita', 'HR',15000),(4,'Rita', 'IT',35000),(5,'Riya','Finance',30000),(6,'Rahul', 'IT',40000),(7,'Priti','Finance',20000),(8,'Gill', 'IT',35000),(9,'Virat', 'IT',35000)]
schema = 'Id int, Name String, Dept String , Salary int'
df = spark.createDataFrame(data,schema)
df = df.sort('Dept','Salary')
df.show()
w_df = Window.partitionBy('Dept').orderBy(df.Salary)
df.withColumn('row_number',row_number().over(w_df)).\
withColumn('ranking',rank().over(w_df)).\
withColumn('denseRank',dense_rank().over(w_df)).show()

# COMMAND ----------

