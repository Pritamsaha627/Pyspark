# Databricks notebook source
names = ['Virat', 'Rohit', 'Gill']
ages = [35, 36, 25]
cities = ['New York', 'Los Angeles', 'Chicago']

# COMMAND ----------

Output :
+-----+---+-----------+
| Name|Age|       City|
+-----+---+-----------+
|Virat| 35|   New York|
|Rohit| 36|Los Angeles|
| Gill| 25|    Chicago|
+-----+---+-----------+

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data = zip(names,ages,cities)

schema = StructType([StructField('name',StringType(),True),StructField('age',IntegerType(),True),StructField('city',StringType(),True)])

df = spark.createDataFrame(data,schema)
df.show()