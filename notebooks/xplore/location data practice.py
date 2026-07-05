# Databricks notebook source
from pyspark.sql.window import *
from pyspark.sql.functions import *

# COMMAND ----------

df_location = spark.read.table(
    "datawarehouse.bronze.location_data_raw"
)

df_location.show(10)

# COMMAND ----------

df_location.filter(col("CNTRY").isNull()).show()

# COMMAND ----------

df_location.groupBy("CID").count().filter("count > 1").show()

# COMMAND ----------


df_location = df_location.fillna(subset=["CNTRY"], value="Unknown")

df_location.show()

# COMMAND ----------

df_location = df_location \
    .withColumn("CID", trim(col("CID"))) \
    .withColumn("CNTRY", trim(col("CNTRY"))) \
    
df_location.show(10)


# COMMAND ----------

df_location = df_location.withColumn(
    "CID",
    regexp_replace(col("CID"), "-", "")
)

# COMMAND ----------

df_location.show(10)