# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - Work Order Routing
# MAGIC
# MAGIC Cleans, validates, quarantines, deduplicates, and upserts the Bronze `work_order_routing` table.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

entity_name = "work_order_routing"
bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.{entity_name}"
target_table = f"{CATALOG}.{SILVER_SCHEMA}.{entity_name}"
quarantine_table = f"{CATALOG}.{SILVER_SCHEMA}.{entity_name}_quarantine"
business_keys = ['WorkOrderID', 'ProductID', 'OperationSequence']

print(f"Bronze:     {bronze_table}")
print(f"Silver:     {target_table}")
print(f"Quarantine: {quarantine_table}")

# COMMAND ----------

bronze_df = spark.table(bronze_table)

typed_df = (
    bronze_df
    .select(
        try_int("WorkOrderID").alias("WorkOrderID"),
        try_int("ProductID").alias("ProductID"),
        try_int("OperationSequence").alias("OperationSequence"),
        try_int("LocationID").alias("LocationID"),
        try_timestamp("ScheduledStartDate").alias("ScheduledStartDate"),
        try_timestamp("ScheduledEndDate").alias("ScheduledEndDate"),
        try_timestamp("ActualStartDate").alias("ActualStartDate"),
        try_timestamp("ActualEndDate").alias("ActualEndDate"),
        try_decimal("ActualResourceHrs").alias("ActualResourceHrs"),
        try_decimal("PlannedCost").alias("PlannedCost"),
        try_decimal("ActualCost").alias("ActualCost"),
        try_timestamp("ModifiedDate").alias("ModifiedDate"),
        F.col("_source_file"),
        F.col("_source_file_modification_time"),
        F.col("_ingestion_timestamp"),
        F.col("_source_system"),
        F.col("_source_schema"),
        F.col("_source_entity"),
    )
    .withColumn("_silver_processed_timestamp", F.current_timestamp())
)

# COMMAND ----------

checked_df = typed_df

work_order_parent_df = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.work_order")
    .select(F.col("WorkOrderID").alias("_parent_work_order_WorkOrderID"))
    .distinct()
    .withColumn("_parent_work_order_exists", F.lit(True))
)

checked_df = checked_df.join(
    work_order_parent_df,
    checked_df["WorkOrderID"] == work_order_parent_df["_parent_work_order_WorkOrderID"],
    "left",
)

product_parent_df = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.product")
    .select(F.col("ProductID").alias("_parent_product_ProductID"))
    .distinct()
    .withColumn("_parent_product_exists", F.lit(True))
)

checked_df = checked_df.join(
    product_parent_df,
    checked_df["ProductID"] == product_parent_df["_parent_product_ProductID"],
    "left",
)

location_parent_df = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.location")
    .select(F.col("LocationID").alias("_parent_location_LocationID"))
    .distinct()
    .withColumn("_parent_location_exists", F.lit(True))
)

checked_df = checked_df.join(
    location_parent_df,
    checked_df["LocationID"] == location_parent_df["_parent_location_LocationID"],
    "left",
)

# COMMAND ----------

rejection_expression = (
    F.when(F.col("WorkOrderID").isNull(), F.lit("WorkOrderID is null or invalid"))
    .when(F.col("ProductID").isNull(), F.lit("ProductID is null or invalid"))
    .when(F.col("OperationSequence").isNull() | (F.col("OperationSequence") <= 0), F.lit("OperationSequence must be greater than zero"))
    .when(F.col("LocationID").isNull(), F.lit("LocationID is null or invalid"))
    .when(F.col("ScheduledStartDate").isNull(), F.lit("ScheduledStartDate is null or invalid"))
    .when(F.col("ScheduledEndDate").isNull(), F.lit("ScheduledEndDate is null or invalid"))
    .when(F.col("ScheduledEndDate") < F.col("ScheduledStartDate"), F.lit("ScheduledEndDate is before ScheduledStartDate"))
    .when(F.col("ActualEndDate").isNotNull() & F.col("ActualStartDate").isNotNull() & (F.col("ActualEndDate") < F.col("ActualStartDate")), F.lit("ActualEndDate is before ActualStartDate"))
    .when(F.col("ActualResourceHrs").isNotNull() & (F.col("ActualResourceHrs") < 0), F.lit("ActualResourceHrs cannot be negative"))
    .when(F.col("PlannedCost").isNull() | (F.col("PlannedCost") < 0), F.lit("PlannedCost is null, invalid, or negative"))
    .when(F.col("ActualCost").isNotNull() & (F.col("ActualCost") < 0), F.lit("ActualCost cannot be negative"))
    .when(F.col("ModifiedDate").isNull(), F.lit("ModifiedDate is null or invalid"))
    .when(F.col("WorkOrderID").isNotNull() & F.col("_parent_work_order_exists").isNull(), F.lit("WorkOrderID has no matching parent in work_order"))
    .when(F.col("ProductID").isNotNull() & F.col("_parent_product_exists").isNull(), F.lit("ProductID has no matching parent in product"))
    .when(F.col("LocationID").isNotNull() & F.col("_parent_location_exists").isNull(), F.lit("LocationID has no matching parent in location"))
)

checked_df = (
    checked_df
    .withColumn("_rejection_reason", rejection_expression)
    .drop("_parent_work_order_WorkOrderID", "_parent_work_order_exists", "_parent_product_ProductID", "_parent_product_exists", "_parent_location_LocationID", "_parent_location_exists")
)

quarantine_df = checked_df.filter(F.col("_rejection_reason").isNotNull())
valid_df = (
    checked_df
    .filter(F.col("_rejection_reason").isNull())
    .drop("_rejection_reason")
)

# COMMAND ----------

(
    quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(quarantine_table)
)

# COMMAND ----------

latest_record_window = (
    Window.partitionBy(*business_keys)
    .orderBy(
        F.col("ModifiedDate").desc_nulls_last(),
        F.col("_ingestion_timestamp").desc_nulls_last(),
        F.col("_source_file_modification_time").desc_nulls_last(),
    )
)

valid_df = (
    valid_df
    .withColumn("_row_number", F.row_number().over(latest_record_window))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
)

# COMMAND ----------

if spark.catalog.tableExists(target_table):
    merge_condition = " AND ".join(
        [f"t.`{key}` = s.`{key}`" for key in business_keys]
    )

    (
        DeltaTable.forName(spark, target_table)
        .alias("t")
        .merge(valid_df.alias("s"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    (
        valid_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
    )

print(f"Silver rows: {spark.table(target_table).count():,}")
print(f"Quarantine rows: {spark.table(quarantine_table).count():,}")

# COMMAND ----------

display(spark.table(target_table).limit(20))