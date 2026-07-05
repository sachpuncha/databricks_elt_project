# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

#read 
df_sales = spark.read.table(
    "datawarehouse.bronze.sales_details_raw"
)

# COMMAND ----------

#make positive the sales price
df_sales = df_sales.withColumn(
    "sls_price",
    abs(col("sls_price"))
)

# COMMAND ----------

#new column for total sales
df_sales=df_sales.withColumn(
    "total_sales", col("sls_price")*col("sls_quantity")
)

# COMMAND ----------

df_sales = df_sales.withColumn(
    "sls_order_dt",
    when(length(col("sls_order_dt")) < 8, lit(0))
    .otherwise(col("sls_order_dt"))
)

# COMMAND ----------

#change datatype to string
df_sales = df_sales.withColumn(
    "order_date",
    col("sls_order_dt").cast("string")
)
df_sales = df_sales.withColumn(
    "ship_date",
    col("sls_ship_dt").cast("string")
)
df_sales = df_sales.withColumn(
    "due_date",
    col("sls_due_dt").cast("string")
)

# COMMAND ----------

#change the dates to correct type
df_sales = df_sales.withColumn(
    "order_date",
    when(col("sls_order_dt") == "0", lit(None))
    .otherwise(to_date(col("sls_order_dt"), "yyyyMMdd"))
)

df_sales = df_sales.withColumn(
    "due_date",
    when(col("sls_due_dt") == "0", lit(None))
    .otherwise(to_date(col("sls_due_dt"), "yyyyMMdd"))
)

df_sales = df_sales.withColumn(
    "ship_date",
    when(col("sls_ship_dt") == "0", lit(None))
    .otherwise(to_date(col("sls_ship_dt"), "yyyyMMdd"))
)

# COMMAND ----------

#drop columns
df_sales = df_sales.drop(
    "sls_order_dt",
    "sls_ship_dt",
    "sls_due_dt",
    "sls_sales"
)



# COMMAND ----------

#rename columns
df_sales = (
    df_sales
    .withColumnRenamed("sls_ord_num", "order_number")
    .withColumnRenamed("sls_prd_key", "product_key")
    .withColumnRenamed("sls_cust_id", "customer_id")
    .withColumnRenamed("sls_quantity", "quantity")
    .withColumnRenamed("sls_price", "unit_price")
    .withColumnRenamed("total_sales", "total_sales")
    .withColumnRenamed("order_date", "order_date")
    .withColumnRenamed("ship_date", "ship_date")
    .withColumnRenamed("due_date","due_date")
)

# COMMAND ----------

#save the table
df_sales = df_sales.write.mode("overwrite").saveAsTable("datawarehouse.silver.sales_details_cleaned")
