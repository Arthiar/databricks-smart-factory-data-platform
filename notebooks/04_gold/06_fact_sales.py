# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - Sales Fact
# MAGIC
# MAGIC Grain: one row per `SalesOrderID` and `SalesOrderDetailID`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

header_df = spark.table(silver_table("sales_order_header")).alias("h")
detail_df = spark.table(silver_table("sales_order_detail")).alias("d")
product_dim_df = spark.table(gold_table("dim_product")).alias("p")

sales_source_df = detail_df.join(
    header_df,
    F.col("d.SalesOrderID") == F.col("h.SalesOrderID"),
    "inner",
)

sales_with_product_df = sales_source_df.join(
    product_dim_df,
    (F.col("d.ProductID") == F.col("p.ProductID"))
    & (F.col("h.OrderDate") >= F.col("p.EffectiveFrom"))
    & (F.col("h.OrderDate") < F.col("p.EffectiveTo")),
    "left",
)

fact_sales_df = (
    sales_with_product_df
    .select(
        F.col("d.SalesOrderID").alias("SalesOrderID"),
        F.col("d.SalesOrderDetailID").alias("SalesOrderDetailID"),
        date_key(F.col("h.OrderDate")).alias("OrderDateKey"),
        date_key(F.col("h.DueDate")).alias("DueDateKey"),
        date_key(F.col("h.ShipDate")).alias("ShipDateKey"),
        F.col("p.ProductKey").alias("ProductKey"),
        F.col("h.CustomerID").alias("CustomerKey"),
        F.col("d.ProductID").alias("ProductID"),
        F.col("h.Status").alias("OrderStatus"),
        F.col("h.OnlineOrderFlag").alias("OnlineOrderFlag"),
        F.col("d.OrderQty").alias("OrderQty"),
        F.col("d.UnitPrice").alias("UnitPrice"),
        F.col("d.UnitPriceDiscount").alias("UnitPriceDiscount"),
        F.col("d.LineTotal").alias("SalesAmount"),
        F.col("d.ModifiedDate").alias("ModifiedDate"),
    )
    .withColumn("_gold_processed_timestamp", F.current_timestamp())
)

target_table = merge_to_gold(
    fact_sales_df,
    "fact_sales",
    ["SalesOrderID", "SalesOrderDetailID"],
)

print(f"Sales fact rows: {spark.table(target_table).count():,}")
display(spark.table(target_table).limit(20))
