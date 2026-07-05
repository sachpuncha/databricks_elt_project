# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

#read the data
df_product_cat = spark.read.table(
    "datawarehouse.bronze.product_cat_data_raw"
)



# COMMAND ----------

#rename
df_prodcut_cat = (
    df_product_cat
    .withColumnRenamed("ID", "id")
    .withColumnRenamed("CAT", "category")
    .withColumnRenamed("SUBCAT", "sub_category" )
    .withColumnRenamed("MAINTENANCE", "maintenance")
)

# COMMAND ----------

#save table
df_product_cat = df_prodcut_cat.write.mode("overwrite").saveAsTable("datawarehouse.silver.product_category_cleaned")