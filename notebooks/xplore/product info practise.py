# Databricks notebook source
from pyspark.sql.window import *
from pyspark.sql.functions import *

# COMMAND ----------

df_product_info = spark.read.table(
    "datawarehouse.bronze.products_info_raw"
)

display(df_product_info)

# COMMAND ----------

df_product_info=df_product_info.fillna(value=0,subset=['prd_cost'])

# COMMAND ----------

display(df_product_info)

# COMMAND ----------

display(df_product_info)

# COMMAND ----------

df_product_info = (
    df_product_info
    .withColumn(
        "cat_id",
        regexp_replace(substring(col("prd_key"), 1, 5), "-", "_")
    )
    .withColumn(
        "prd_key_extract",
        substring(col("prd_key"), 7, length(col("prd_key")))
    )
)

# COMMAND ----------

df_product_info = df_product_info.withColumn(
    "prd_line",
    when(trim(col("prd_line")) == "M", "Mountain")
    .when(trim(col("prd_line")) == "S", "Other Sales")
    .when(trim(col("prd_line")) == "R", "Road")
    .when(trim(col("prd_line")) == "T", "Touring")
    .otherwise("unknown")
)

# COMMAND ----------

window_spec = Window.partitionBy("prd_key").orderBy("prd_start_dt")

df_product_info = df_product_info.withColumn(
    "next_start",
    lead("prd_start_dt").over(window_spec)
)

df_product_info = df_product_info.withColumn(
    "prd_end_dt",
    date_sub(col("next_start"), 1)
).drop("next_start")

# COMMAND ----------

display(df_product_info)