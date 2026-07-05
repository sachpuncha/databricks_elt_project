# Databricks notebook source
from pyspark.sql.window import *
from pyspark.sql.functions import *

# COMMAND ----------

df_customer_data = spark.read.table(
    "datawarehouse.bronze.customers_data_raw"
)

df_customer_data.show(10)

# COMMAND ----------

df_customer_data.printSchema()

# COMMAND ----------

df_customer_data.groupBy("CID").count().filter("count > 1").show()

# COMMAND ----------

df_customer_data.filter(col("CID").isNull()).show()

# COMMAND ----------

df_customer_data = df_customer_data \
    .withColumn("CID", trim(col("CID"))) \
    .withColumn("BDATE", to_date(col("BDATE"))) \
    .withColumn("GEN", trim(col("GEN")))
df_customer_data.show(10)


# COMMAND ----------

df_customer_data.groupby("GEN").count().show()

# COMMAND ----------

df_customer_data = df_customer_data.withColumn(
    "GEN",
    when(col("GEN").isin("M","Male","male"), "Male")
    .when(col("GEN").isin("F","Female","female"), "Female")
    .otherwise("unknown")
)

# COMMAND ----------

df_customer_data.filter(~col("CID").contains("NAS")).show(truncate=False)

# COMMAND ----------

df_customer_data = df_customer_data.withColumn(
    "c_id",
    when(
        col("CID").contains("NAS"),
        split(col("CID"), "NAS")[1]
    ).otherwise(col("CID"))
)

# COMMAND ----------

display(df_customer_data)

# COMMAND ----------

df_customer_data.groupBy("c_id").count().filter("count > 1").show()