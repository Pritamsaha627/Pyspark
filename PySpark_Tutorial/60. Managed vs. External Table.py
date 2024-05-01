# Databricks notebook source
cust_data = [(1,'Virat','Delhi'),(2,'Rohit','Mumbai'),(3,'Shami','Kolkata'),(4,'Bumrah','Gujrat'),(5,'Bhuvi','Pune')]
cust_schema = ['cust_id','name','city']
df = spark.createDataFrame(cust_data,cust_schema)
display(df)

# COMMAND ----------

df.write.format('delta').saveAsTable('player_detail_managed')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED player_detail_managed

# COMMAND ----------

df.write.format('delta').option('path','/dbfs/tmp/unmanageTable').saveAsTable('player_detail_unanaged')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/dbfs/tmp/unmanageTable'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED player_detail_unanaged

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM player_detail_unanaged

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE player_detail_unanaged

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DELTA.`/dbfs/tmp/player_detail_unanaged`

# COMMAND ----------

