# Databricks notebook source
from pyspark.sql.window import *
from pyspark.sql.functions import *

# COMMAND ----------

df_product_cat = spark.read.table(
    "datawarehouse.bronze.product_cat_data_raw"
)

df_product_cat.show(10)

# COMMAND ----------

df_product_cat.filter(col("MAINTENANCE").isNull()).show()

# COMMAND ----------

df_product_cat.groupBy("ID").count().filter("count > 1").show()

# COMMAND ----------

df_product_cat.filter(col("ID").isNull()).show()

# COMMAND ----------

display(df_product_cat)