# Databricks notebook source
from pyspark.sql.types import * 
from pyspark.sql.functions import *
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.fs.ls("/Volumes/datawarehouse/bronze/raw_files")

# COMMAND ----------

df_customers = spark.read.format("csv").option("inferschema", "true").option("header", "true").load("/Volumes/datawarehouse/bronze/raw_files/cust_info.csv")

# COMMAND ----------

df_customers.printSchema()

# COMMAND ----------

df_customers.show(10)

# COMMAND ----------

df_products= spark.read.format("csv").option("inferschema", "true").option("header", "true").load("/Volumes/datawarehouse/bronze/raw_files/prd_info.csv")
df_sales_details = spark.read.format("csv").option("inferschema", "true").option("header", "true").load("/Volumes/datawarehouse/bronze/raw_files/sales_details.csv")
df_cust = spark.read.format("csv").option("inferschema", "true").option("header", "true").load("/Volumes/datawarehouse/bronze/raw_files/CUST_AZ12.csv")
df_location = spark.read.format("csv").option("inferschema", "true").option("header", "true").load("/Volumes/datawarehouse/bronze/raw_files/LOC_A101.csv")
df_px_cat = spark.read.format("csv").option("inferschema", "true").option("header", "true").load("/Volumes/datawarehouse/bronze/raw_files/PX_CAT_G1V2.csv")


# COMMAND ----------

df_cust.show(10)
df_location.show(10)
df_products.show(10)
df_sales_details.show(10)
df_px_cat.show(10)

# COMMAND ----------

df_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("datawarehouse.bronze.customers_info_raw")

# COMMAND ----------

df_sales_details.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("datawarehouse.bronze.sales_details_raw")

df_cust.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("datawarehouse.bronze.customers_data_raw")

df_products.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("datawarehouse.bronze.products_info_raw")

df_location.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("datawarehouse.bronze.location_data_raw")

df_px_cat.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("datawarehouse.bronze.product_cat_data_raw")