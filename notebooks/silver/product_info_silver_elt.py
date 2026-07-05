# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

df_product_info = spark.read.table(
    "datawarehouse.bronze.products_info_raw"
)


# COMMAND ----------

#fill product cost
df_product_info=df_product_info.fillna(value=0,subset=['prd_cost'])

# COMMAND ----------

#extract keys 
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

#trim
df_product_info = df_product_info.withColumn(
    "prd_line",
    when(trim(col("prd_line")) == "M", "Mountain")
    .when(trim(col("prd_line")) == "S", "Other Sales")
    .when(trim(col("prd_line")) == "R", "Road")
    .when(trim(col("prd_line")) == "T", "Touring")
    .otherwise("unknown")
)

# COMMAND ----------

#add end dates with previous start dates
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

df_product_info = df_product_info.drop("prd_key")

# COMMAND ----------

#rename columns
df_product_info = (
    df_product_info
    .withColumnRenamed("prd_id", "product_id")
    .withColumnRenamed("prd_nm", "product_name")
    .withColumnRenamed("prd_cost", "product_cost")
    .withColumnRenamed("prd_line", "product_line")
    .withColumnRenamed("prd_start_dt", "product_startDate")
    .withColumnRenamed("prd_end_dt", "product_endDate")
    .withColumnRenamed("cat_id","category_id")
    .withColumnRenamed("prd_key_extract","product_key")
)

# COMMAND ----------

#rearrange coloumns
df_product_info = df_product_info.select(
    "product_id",
    "product_key",
    "category_id",
    "product_name",
    "product_cost",
    "product_line",
    "product_startDate",
    "product_endDate"
)

# COMMAND ----------

# MAGIC %md
# MAGIC save

# COMMAND ----------

#save the table
df_product_info.write.mode("overwrite").saveAsTable("datawarehouse.silver.products_info_cleaned")