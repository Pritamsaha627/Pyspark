# Databricks notebook source
pip install farsante

# COMMAND ----------

import farsante as f

df =  f.quick_pyspark_df(['first_name','last_name','age'],10)
display(df)

# COMMAND ----------

from mimesis import Address

b = Address('en')
df1 = f.pyspark_df([b.address,b.calling_code,b.city],5)
df1.show()

# COMMAND ----------

help('farsante')

# COMMAND ----------

help('mimesis')