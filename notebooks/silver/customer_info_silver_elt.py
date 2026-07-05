# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

#read table
df_customer_info = spark.read.table(
    "datawarehouse.bronze.customers_info_raw"
)

# COMMAND ----------

#drop all NULL rows
df_customer_info = df_customer_info.dropna("all")

# COMMAND ----------

#drop customer id null values
df_customer_info = df_customer_info.dropna(subset=["cst_id"])

# COMMAND ----------

#drop duplicates
window_spec = Window.partitionBy("cst_id").orderBy(col("cst_create_date").desc())

df_customer_info = (
    df_customer_info
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)



# COMMAND ----------

#fill null values
df_customer_info.fillna("unknown")

# COMMAND ----------

#trim
df_customer_info = df_customer_info \
    .withColumn("cst_firstname", trim(col("cst_firstname"))) \
    .withColumn("cst_lastname", trim(col("cst_lastname"))) \
    .withColumn("cst_marital_status", trim(col("cst_marital_status"))) \
    .withColumn("cst_gndr", trim(col("cst_gndr")))

# COMMAND ----------

#replace gender values
df_customer_info = df_customer_info.withColumn(
    "cst_gndr",
    when(col("cst_gndr").isin("M"), "Male")
    .when(col("cst_gndr").isin("F"), "Female")
    .otherwise("unknown")
)

# COMMAND ----------

#replace marital status values
df_customer_info = df_customer_info.withColumn(
    "cst_marital_status",
    when(col("cst_marital_status").isin("M"), "Married")
    .when(col("cst_marital_status").isin("S"), "Single")
    .otherwise("unknown")
)

# COMMAND ----------

#create new columns for year, month,date
df_customer_info = df_customer_info \
    .withColumn("cst_create_year", year(col("cst_create_date"))) \
    .withColumn("cst_create_month", month(col("cst_create_date"))) \
    .withColumn("cst_create_day", dayofmonth(col("cst_create_date")))

# COMMAND ----------

#rename columns
df_customer_info = (
    df_customer_info
    .withColumnRenamed("cst_id", "customer_id")
    .withColumnRenamed("cst_key", "customer_key")
    .withColumnRenamed("cst_firstname", "customer_firstNname")
    .withColumnRenamed("cst_lastname", "customer_lastName")
    .withColumnRenamed("cst_marital_status", "customer_marital_status")
    .withColumnRenamed("cst_gndr", "customer_gender")
    .withColumnRenamed("cst_create_date", "customer_createFullDate")
    .withColumnRenamed("cst_create_year", "customer_createYear")
    .withColumnRenamed("cst_create_month", "customer_createMonth")
    .withColumnRenamed("cst_create_day", "customer_createDay")
)

# COMMAND ----------

#save cleaned table
df_customer_info.write \
    .mode("overwrite") \
    .saveAsTable("datawarehouse.silver.customer_info_cleaned")