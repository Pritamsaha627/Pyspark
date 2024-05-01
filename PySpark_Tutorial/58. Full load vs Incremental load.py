# Databricks notebook source
df = spark.read.csv('dbfs:/FileStore/source_file/emp_nullvalues.csv',header=True)
df.createOrReplaceTempView('df_view')
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM df1_view
# MAGIC WHERE UPDATED_DATE BETWEEN "04-01-2022" and "06-01-2022"

# COMMAND ----------

from pyspark.sql.functions import col
df1 = df.select("*").drop(col("hiredate"),col("updated_date"))
df.createOrReplaceTempView('df1_view')
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from df1_view

# COMMAND ----------

