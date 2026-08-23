# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - Customer Dimension
# MAGIC
# MAGIC Creates one simple customer dimension row for each Silver customer.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

customer_df = spark.table(silver_table("customer"))

dim_customer_df = (
    customer_df
    .select(
        F.col("CustomerID").alias("CustomerKey"),
        F.col("CustomerID"),
        F.col("PersonID"),
        F.col("StoreID"),
        F.col("TerritoryID"),
        F.col("AccountNumber"),
        F.col("ModifiedDate"),
    )
    .withColumn("CustomerType", F.when(F.col("StoreID").isNotNull(), "Store").otherwise("Individual"))
    .withColumn("_gold_processed_timestamp", F.current_timestamp())
)

target_table = merge_to_gold(dim_customer_df, "dim_customer", ["CustomerKey"])

print(f"Customer rows: {spark.table(target_table).count():,}")
display(spark.table(target_table).limit(20))