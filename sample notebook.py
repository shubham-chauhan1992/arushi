# Databricks notebook source
# MAGIC %md
# MAGIC # This cis a sample code for reading a dfile
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables'))


# COMMAND ----------

df=spark.read.format('csv').options(header="true",inferSchema="true").load('dbfs:/FileStore/tables/customers_20240710.csv')

display(df)
display(df2)

# COMMAND ----------

df2=df.select("customer Id")
display(df2)

# COMMAND ----------

df.createOrReplaceTempView("customer")
df.createOrReplaceTempView("customer_copy")



# COMMAND ----------

df3=spark.sql("select a.*,b.* from customer a inner join customer b on a.Index=b.Index") 

# COMMAND ----------

display(df3)
