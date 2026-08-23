# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - Work Order
# MAGIC
# MAGIC Cleans, validates, quarantines, deduplicates, and upserts the Bronze `work_order` table.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

entity_name = "work_order"
bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.{entity_name}"
target_table = f"{CATALOG}.{SILVER_SCHEMA}.{entity_name}"
quarantine_table = f"{CATALOG}.{SILVER_SCHEMA}.{entity_name}_quarantine"
business_keys = ['WorkOrderID']

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
        try_int("OrderQty").alias("OrderQty"),
        try_int("StockedQty").alias("StockedQty"),
        try_int("ScrappedQty").alias("ScrappedQty"),
        try_timestamp("StartDate").alias("StartDate"),
        try_timestamp("EndDate").alias("EndDate"),
        try_timestamp("DueDate").alias("DueDate"),
        try_int("ScrapReasonID").alias("ScrapReasonID"),
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

scrap_reason_parent_df = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.scrap_reason")
    .select(F.col("ScrapReasonID").alias("_parent_scrap_reason_ScrapReasonID"))
    .distinct()
    .withColumn("_parent_scrap_reason_exists", F.lit(True))
)

checked_df = checked_df.join(
    scrap_reason_parent_df,
    checked_df["ScrapReasonID"] == scrap_reason_parent_df["_parent_scrap_reason_ScrapReasonID"],
    "left",
)

# COMMAND ----------

rejection_expression = (
    F.when(F.col("WorkOrderID").isNull(), F.lit("WorkOrderID is null or invalid"))
    .when(F.col("ProductID").isNull(), F.lit("ProductID is null or invalid"))
    .when(F.col("OrderQty").isNull() | (F.col("OrderQty") <= 0), F.lit("OrderQty must be greater than zero"))
    .when(F.col("StockedQty").isNull() | (F.col("StockedQty") < 0), F.lit("StockedQty is null, invalid, or negative"))
    .when(F.col("ScrappedQty").isNull() | (F.col("ScrappedQty") < 0), F.lit("ScrappedQty is null, invalid, or negative"))
    .when(F.col("StartDate").isNull(), F.lit("StartDate is null or invalid"))
    .when(F.col("DueDate").isNull(), F.lit("DueDate is null or invalid"))
    .when(F.col("EndDate").isNotNull() & (F.col("EndDate") < F.col("StartDate")), F.lit("EndDate is before StartDate"))
    .when(F.col("ModifiedDate").isNull(), F.lit("ModifiedDate is null or invalid"))
    .when(F.col("ProductID").isNotNull() & F.col("_parent_product_exists").isNull(), F.lit("ProductID has no matching parent in product"))
    .when(F.col("ScrapReasonID").isNotNull() & F.col("_parent_scrap_reason_exists").isNull(), F.lit("ScrapReasonID has no matching parent in scrap_reason"))
)

checked_df = (
    checked_df
    .withColumn("_rejection_reason", rejection_expression)
    .drop("_parent_product_ProductID", "_parent_product_exists", "_parent_scrap_reason_ScrapReasonID", "_parent_scrap_reason_exists")
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