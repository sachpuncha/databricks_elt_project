# Databricks notebook source
from pyspark.sql.window import *
from pyspark.sql.functions import *

# COMMAND ----------

df_customer_info = spark.read.table(
    "datawarehouse.bronze.customers_info_raw"
)

df_customer_info.show(10)

# COMMAND ----------

df_customer_info.filter(col("cst_id").isNull()).show()

# COMMAND ----------

df_customer_info.groupBy("cst_id").count().filter("count > 1").show()

# COMMAND ----------

df_customer_info.filter(col('cst_id')=="29483").show()

# COMMAND ----------

df_customer_info.filter(col('cst_id')=="29433").show()

# COMMAND ----------

df_customer_info = df_customer_info.dropna("all")

df_customer_info.filter(col("cst_id").isNull()).show()


# COMMAND ----------

df_customer_info.groupBy("cst_id").count().filter("count > 1").show()

# COMMAND ----------

df_customer_info.count()

# COMMAND ----------

df_customer_info = df_customer_info.dropna(subset=["cst_id"])

df_customer_info.count()

# COMMAND ----------

df_customer_info.filter(col("cst_firstname").isNull() | col('cst_lastname').isNull()).show()

# COMMAND ----------

window_spec = Window.partitionBy("cst_id").orderBy(col("cst_create_date").desc())

df_customer_info = (
    df_customer_info
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)



# COMMAND ----------

df_customer_info.groupBy("cst_id").count().filter("count > 1").show()

# COMMAND ----------

df_customer_info.show(10)

# COMMAND ----------

df_customer_info.fillna("unknown")

# COMMAND ----------

df_customer_info.filter(col('cst_id')=="29483").show()

# COMMAND ----------

df_customer_info = df_customer_info \
    .withColumn("cst_firstname", trim(col("cst_firstname"))) \
    .withColumn("cst_lastname", trim(col("cst_lastname"))) \
    .withColumn("cst_marital_status", trim(col("cst_marital_status"))) \
    .withColumn("cst_gndr", trim(col("cst_gndr")))

# COMMAND ----------

df_customer_info.show(14)

# COMMAND ----------

df_customer_info = df_customer_info.withColumn(
    "cst_gndr",
    when(col("cst_gndr").isin("M"), "Male")
    .when(col("cst_gndr").isin("F"), "Female")
    .otherwise("unknown")
)

# COMMAND ----------

df_customer_info.groupBy("cst_marital_status").count().show()

# COMMAND ----------

df_customer_info = df_customer_info.withColumn(
    "cst_marital_status",
    when(col("cst_marital_status").isin("M"), "Married")
    .when(col("cst_marital_status").isin("S"), "Single")
    .otherwise("unknown")
)

# COMMAND ----------

df_customer_info.groupBy("cst_gndr").count().show()

# COMMAND ----------

display(df_customer_info)

# COMMAND ----------

df_customer_info.printSchema()

# COMMAND ----------

df_customer_info = df_customer_info \
    .withColumn("cst_create_year", year(col("cst_create_date"))) \
    .withColumn("cst_create_month", month(col("cst_create_date"))) \
    .withColumn("cst_create_day", dayofmonth(col("cst_create_date")))

# COMMAND ----------

