# Databricks notebook source
# MAGIC %md
# MAGIC current_date()
# MAGIC date_format()
# MAGIC to_date()
# MAGIC date_diff()
# MAGIC months_between()
# MAGIC add_months()
# MAGIC date_add()
# MAGIC year()
# MAGIC month()
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_date, date_format, to_date
data = [(1,'Ram','India'),(2,'Liza','US'),(3,'Margarita','Germany'),(4,'Rita','UK'),(5,'Rita','India')]
schema = 'Id int, Name String, Country String'
df = spark.createDataFrame(data,schema)
df = df.withColumn('JoinDate',current_date())
df1 = df.withColumn('newdate',date_format(df.JoinDate,'MM.dd.yyyy'))
df2 = df1.withColumn('todatecol',to_date(df1.newdate,'MM.dd.yyyy'))
df2.show()

# COMMAND ----------

from pyspark.sql.functions import date_diff,months_between,add_months,date_add,year,month
data = [(1,'Ram','India','1998-05-24','2018-10-14'),(2,'Liza','US','1998-06-24','2020-03-27')]
schema = ['Id' , 'Name', 'Country','JoinDate','ReleaseDate']
df3 = spark.createDataFrame(data,schema)

df3 = df3.withColumn('diff',date_diff(df3.ReleaseDate,df3.JoinDate))
df3 = df3.withColumn('monthsbetween',months_between(df3.ReleaseDate,df3.JoinDate))
df3 = df3.withColumn('newreleasedate',add_months(df3.ReleaseDate,-5))
df3 = df3.withColumn('newreleaseDate',date_add(df3.ReleaseDate,5))
df3 = df3.withColumn('Releaseyear',year(df3.ReleaseDate))
df3 = df3.withColumn('ReleaseMonth',month(df3.ReleaseDate))
df3.show()