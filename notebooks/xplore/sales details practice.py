# Databricks notebook source
from pyspark.sql.window import *
from pyspark.sql.functions import *

# COMMAND ----------

df_sales = spark.read.table(
    "datawarehouse.bronze.sales_details_raw"
)

display(df_sales)

# COMMAND ----------

df_sales.filter(col("sls_ord_num").isNull()).show()

# COMMAND ----------

df_sales.filter(col("sls_order_dt").isNull()).show()

# COMMAND ----------

display(df_sales.filter(col("sls_quantity")>2))

# COMMAND ----------

df_sales = df_sales.withColumn(
    "sls_price",
    abs(col("sls_price"))
)

# COMMAND ----------

df_sales=df_sales.withColumn(
    "total_sales", col("sls_price")*col("sls_quantity")
)

# COMMAND ----------

display(df_sales)

# COMMAND ----------

df_sales = df_sales.withColumn(
    "order_date",
    to_date(col("sls_order_dt").cast("string"), "yyyyMMdd")
)

df_sales = df_sales.withColumn(
    "ship_date",
    to_date(col("sls_ship_dt").cast("string"), "yyyyMMdd")
)

df_sales = df_sales.withColumn(
    "due_date",
    to_date(col("sls_due_dt").cast("string"), "yyyyMMdd")
)

# COMMAND ----------

display(df_sales)

# COMMAND ----------



# COMMAND ----------

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