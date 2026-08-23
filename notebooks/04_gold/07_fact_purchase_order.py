# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - Purchase Order Fact
# MAGIC
# MAGIC Grain: one row per `PurchaseOrderID` and `PurchaseOrderDetailID`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

header_df = spark.table(silver_table("purchase_order_header")).alias("h")
detail_df = spark.table(silver_table("purchase_order_detail")).alias("d")
product_dim_df = spark.table(gold_table("dim_product")).alias("p")

purchase_source_df = detail_df.join(
    header_df,
    F.col("d.PurchaseOrderID") == F.col("h.PurchaseOrderID"),
    "inner",
)

purchase_with_product_df = purchase_source_df.join(
    product_dim_df,
    (F.col("d.ProductID") == F.col("p.ProductID"))
    & (F.col("h.OrderDate") >= F.col("p.EffectiveFrom"))
    & (F.col("h.OrderDate") < F.col("p.EffectiveTo")),
    "left",
)

fact_purchase_order_df = (
    purchase_with_product_df
    .select(
        F.col("d.PurchaseOrderID").alias("PurchaseOrderID"),
        F.col("d.PurchaseOrderDetailID").alias("PurchaseOrderDetailID"),
        date_key(F.col("h.OrderDate")).alias("OrderDateKey"),
        date_key(F.col("d.DueDate")).alias("DueDateKey"),
        date_key(F.col("h.ShipDate")).alias("ShipDateKey"),
        F.col("p.ProductKey").alias("ProductKey"),
        F.col("h.VendorID").alias("SupplierKey"),
        F.col("d.ProductID").alias("ProductID"),
        F.col("h.Status").alias("PurchaseStatus"),
        F.col("d.OrderQty").alias("OrderQty"),
        F.col("d.UnitPrice").alias("UnitPrice"),
        F.col("d.LineTotal").alias("PurchaseAmount"),
        F.col("d.ReceivedQty").alias("ReceivedQty"),
        F.col("d.RejectedQty").alias("RejectedQty"),
        F.col("d.StockedQty").alias("StockedQty"),
        F.col("d.ModifiedDate").alias("ModifiedDate"),
    )
    .withColumn("_gold_processed_timestamp", F.current_timestamp())
)

target_table = merge_to_gold(
    fact_purchase_order_df,
    "fact_purchase_order",
    ["PurchaseOrderID", "PurchaseOrderDetailID"],
)

print(f"Purchase fact rows: {spark.table(target_table).count():,}")
display(spark.table(target_table).limit(20))
