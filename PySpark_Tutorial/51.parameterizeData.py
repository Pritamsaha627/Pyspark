# Databricks notebook source
# dbutils.widgets.text("A","")
# dbutils.widgets.text("B","")

# COMMAND ----------

a = int(dbutils.widgets.get("A"))
b = int(dbutils.widgets.get("B"))

# COMMAND ----------

def sum1(x,y):
    return x+y

# COMMAND ----------

s = sum1(a,b)
print(s)