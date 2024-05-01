# Databricks notebook source
data = [(1,'Rahul',93,'Kolkata','WB'),(2,'Raj',82,'Mumbai','Maharashtra'),(3,'Priya',66,'Chennai','Tamil Nadu'),(4,'Riya',77,'Thiruvananthapuram','Kerala'),(5,'Ali',93,'Siliguri','WB'),(6,'Fazal',82,'Mumbai','Maharashtra'),(7,'Virat',66,'Chennai','Tamil Nadu'),(8,'Dhoni',77,'Kollam','Kerala'),(9,'Rahul',93,'Kolkata','WB'),(10,'Raj',82,'Mumbai','Maharashtra'),(11,'Priya',66,'Chennai','Tamil Nadu'),(12,'Riya',77,'Thiruvananthapuram','Kerala')]

schema = "Id int, Name string, Marks int, City string, State string"

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df.write.option("header",True) \
        .partitionBy("State","City") \
        .mode("overwrite") \
        .csv("/tmp/zipcodes-state5")

# COMMAND ----------

df.repartition(2) \
        .write.option("header",True) \
        .partitionBy("State","City") \
        .mode("overwrite") \
        .csv("/tmp/zipcodes-state7")