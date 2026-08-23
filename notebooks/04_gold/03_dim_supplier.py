# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - Supplier Dimension
# MAGIC
# MAGIC Renames the AdventureWorks vendor entity to the business-friendly supplier dimension.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

vendor_df = spark.table(silver_table("vendor"))

dim_supplier_df = (
    vendor_df
    .select(
        F.col("BusinessEntityID").alias("SupplierKey"),
        F.col("BusinessEntityID").alias("SupplierID"),
        F.col("AccountNumber"),
        F.col("Name").alias("SupplierName"),
        F.col("CreditRating"),
        F.col("PreferredVendorStatus").alias("PreferredSupplierStatus"),
        F.col("ActiveFlag"),
        F.col("PurchasingWebServiceURL"),
        F.col("ModifiedDate"),
    )
    .withColumn("_gold_processed_timestamp", F.current_timestamp())
)

target_table = merge_to_gold(dim_supplier_df, "dim_supplier", ["SupplierKey"])

print(f"Supplier rows: {spark.table(target_table).count():,}")
display(spark.table(target_table).limit(20))