# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - Production Fact
# MAGIC
# MAGIC Grain: one row per `WorkOrderID`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

work_order_df = spark.table(silver_table("work_order")).alias("w")
product_dim_df = spark.table(gold_table("dim_product")).alias("p")

production_with_product_df = work_order_df.join(
    product_dim_df,
    (F.col("w.ProductID") == F.col("p.ProductID"))
    & (F.col("w.StartDate") >= F.col("p.EffectiveFrom"))
    & (F.col("w.StartDate") < F.col("p.EffectiveTo")),
    "left",
)

fact_production_df = (
    production_with_product_df
    .select(
        F.col("w.WorkOrderID").alias("WorkOrderID"),
        date_key(F.col("w.StartDate")).alias("StartDateKey"),
        date_key(F.col("w.EndDate")).alias("EndDateKey"),
        date_key(F.col("w.DueDate")).alias("DueDateKey"),
        F.col("p.ProductKey").alias("ProductKey"),
        F.col("w.ProductID").alias("ProductID"),
        F.col("w.ScrapReasonID").alias("ScrapReasonID"),
        F.col("w.OrderQty").alias("OrderQty"),
        F.col("w.StockedQty").alias("StockedQty"),
        F.col("w.ScrappedQty").alias("ScrappedQty"),
        F.when(
            F.col("w.OrderQty") > 0,
            F.round(F.col("w.StockedQty") / F.col("w.OrderQty"), 4),
        ).otherwise(F.lit(0.0)).alias("YieldRate"),
        F.when(
            F.col("w.OrderQty") > 0,
            F.round(F.col("w.ScrappedQty") / F.col("w.OrderQty"), 4),
        ).otherwise(F.lit(0.0)).alias("ScrapRate"),
        F.col("w.ModifiedDate").alias("ModifiedDate"),
    )
    .withColumn("_gold_processed_timestamp", F.current_timestamp())
)

target_table = merge_to_gold(
    fact_production_df,
    "fact_production",
    ["WorkOrderID"],
)

print(f"Production fact rows: {spark.table(target_table).count():,}")
display(spark.table(target_table).limit(20))
