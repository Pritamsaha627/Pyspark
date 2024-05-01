# Databricks notebook source
# dbutils.fs.rm('dbfs:/FileStore/_delta_log',True)

# COMMAND ----------

sch = 'Emp_Id int, Emp_Name string, Age int, city string'
df=spark.readStream.option('header',True).option('maxfilespertrigger',1).schema(sch).csv('dbfs:/FileStore/tables')
display(df)


# COMMAND ----------

df.writeStream.format('console').trigger(processingTime='5 seconds').outputMode('append').start()

# COMMAND ----------

df.writeStream.format('delta').option("checkpointLocation","dbfs:/FileStore/deltaFile").trigger(processingTime='5 seconds').outputMode('append').start('dbfs:/FileStore/deltaFile')

# COMMAND ----------

df1 = spark.read.format('delta').load('dbfs:/FileStore/deltaFile')
df1.show()