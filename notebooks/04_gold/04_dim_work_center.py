# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - Work Center Dimension
# MAGIC
# MAGIC Uses manufacturing locations as work centers because the current source has no machine master.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

location_df = spark.table(silver_table("location"))

dim_work_center_df = (
    location_df
    .select(
        F.col("LocationID").alias("WorkCenterKey"),
        F.col("LocationID"),
        F.col("Name").alias("WorkCenterName"),
        F.col("CostRate"),
        F.col("Availability"),
        F.col("ModifiedDate"),
    )
    .withColumn("_gold_processed_timestamp", F.current_timestamp())
)

target_table = merge_to_gold(dim_work_center_df, "dim_work_center", ["WorkCenterKey"])

print(f"Work center rows: {spark.table(target_table).count():,}")
display(spark.table(target_table).limit(20))