# Databricks notebook source
from pyspark.sql.window import *
from pyspark.sql.functions import *

# COMMAND ----------

#read data
df_location = spark.read.table(
    "datawarehouse.bronze.location_data_raw"
)

df_location.show(10)

# COMMAND ----------

#trim
df_location = df_location \
    .withColumn("CID", trim(col("CID"))) \
    .withColumn("CNTRY", trim(col("CNTRY")))

# COMMAND ----------

#remove -
df_location = df_location.withColumn(
    "CID",
    regexp_replace(col("CID"), "-", "")
)

# COMMAND ----------

#change the countries names
df_location = df_location.withColumn(
    "CNTRY",
    when(col("CNTRY").isin("US", "USA"), "United States")
    .when(col("CNTRY") == "DE", "Germany")
    .when((col("CNTRY") == "") | (col("CNTRY").isNull()), "Unknown")
    .otherwise(col("CNTRY"))
)

# COMMAND ----------

#rename columns
df_location = (
    df_location
    .withColumnRenamed("CID", "customer_id")
    .withColumnRenamed("CNTRY", "country")
)



# COMMAND ----------

#write to silver
df_location.write.mode("overwrite").saveAsTable("datawarehouse.silver.location_data_cleaned")