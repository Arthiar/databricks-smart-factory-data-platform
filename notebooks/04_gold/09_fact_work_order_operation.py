# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - Work Order Operation Fact
# MAGIC
# MAGIC Grain: one row per `WorkOrderID`, `ProductID`, and `OperationSequence`.
# MAGIC A true machine fact is not possible yet because the current source has no MachineID.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

routing_df = spark.table(silver_table("work_order_routing")).alias("r")
product_dim_df = spark.table(gold_table("dim_product")).alias("p")

routing_with_product_df = routing_df.join(
    product_dim_df,
    (F.col("r.ProductID") == F.col("p.ProductID"))
    & (F.col("r.ScheduledStartDate") >= F.col("p.EffectiveFrom"))
    & (F.col("r.ScheduledStartDate") < F.col("p.EffectiveTo")),
    "left",
)

fact_operation_df = (
    routing_with_product_df
    .select(
        F.col("r.WorkOrderID").alias("WorkOrderID"),
        F.col("r.ProductID").alias("ProductID"),
        F.col("r.OperationSequence").alias("OperationSequence"),
        F.col("p.ProductKey").alias("ProductKey"),
        F.col("r.LocationID").alias("WorkCenterKey"),
        date_key(F.col("r.ScheduledStartDate")).alias("ScheduledStartDateKey"),
        date_key(F.col("r.ScheduledEndDate")).alias("ScheduledEndDateKey"),
        date_key(F.col("r.ActualStartDate")).alias("ActualStartDateKey"),
        date_key(F.col("r.ActualEndDate")).alias("ActualEndDateKey"),
        F.col("r.ActualResourceHrs").alias("ActualResourceHours"),
        F.col("r.PlannedCost").alias("PlannedCost"),
        F.col("r.ActualCost").alias("ActualCost"),
        (F.col("r.ActualCost") - F.col("r.PlannedCost")).alias("CostVariance"),
        F.when(
            F.col("r.ActualStartDate").isNotNull()
            & F.col("r.ActualEndDate").isNotNull(),
            F.round(
                (
                    F.unix_timestamp("r.ActualEndDate")
                    - F.unix_timestamp("r.ActualStartDate")
                ) / 3600.0,
                2,
            ),
        ).alias("ActualDurationHours"),
        F.col("r.ModifiedDate").alias("ModifiedDate"),
    )
    .withColumn("_gold_processed_timestamp", F.current_timestamp())
)

target_table = merge_to_gold(
    fact_operation_df,
    "fact_work_order_operation",
    ["WorkOrderID", "ProductID", "OperationSequence"],
)

print(f"Operation fact rows: {spark.table(target_table).count():,}")
display(spark.table(target_table).limit(20))
