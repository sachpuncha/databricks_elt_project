# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

#read the data
df_customer_data = spark.read.table(
    "datawarehouse.bronze.customers_data_raw"
)

df_customer_data.show(10)

# COMMAND ----------

#trim the columns
df_customer_data = df_customer_data \
    .withColumn("CID", trim(col("CID"))) \
    .withColumn("BDATE", to_date(col("BDATE"))) \
    .withColumn("GEN", trim(col("GEN")))
df_customer_data.show(10)


# COMMAND ----------

#replace values
df_customer_data = df_customer_data.withColumn(
    "GEN",
    when(col("GEN").isin("M","Male","male"), "Male")
    .when(col("GEN").isin("F","Female","female"), "Female")
    .otherwise("unknown")
)

# COMMAND ----------

#remove the NAS in CID and dont if not save to new column
df_customer_data = df_customer_data.withColumn(
    "c_id",
    when(
        col("CID").contains("NAS"),
        split(col("CID"), "NAS")[1]
    ).otherwise(col("CID"))
)

# COMMAND ----------

#drop CID
df_customer_data=df_customer_data.drop("CID")



# COMMAND ----------

#rename columns
df_customer_data = (
    df_customer_data
    .withColumnRenamed("BDATE", "birthday")
    .withColumnRenamed("GEN", "gender")
    .withColumnRenamed("c_id", "customer_id")
)



# COMMAND ----------

#rearrange columns

df_customer_data = df_customer_data.select(
    "customer_id",
    "birthday",
    "gender"
)


# COMMAND ----------

#write to silver
df_customer_data.write.mode("overwrite").saveAsTable("datawarehouse.silver.customers_metadata_cleaned")