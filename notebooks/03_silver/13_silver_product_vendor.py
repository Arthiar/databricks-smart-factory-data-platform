# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - Product Vendor
# MAGIC
# MAGIC Cleans, validates, quarantines, deduplicates, and upserts the Bronze `product_vendor` table.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

entity_name = "product_vendor"
bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.{entity_name}"
target_table = f"{CATALOG}.{SILVER_SCHEMA}.{entity_name}"
quarantine_table = f"{CATALOG}.{SILVER_SCHEMA}.{entity_name}_quarantine"
business_keys = ['ProductID', 'BusinessEntityID']

print(f"Bronze:     {bronze_table}")
print(f"Silver:     {target_table}")
print(f"Quarantine: {quarantine_table}")

# COMMAND ----------

bronze_df = spark.table(bronze_table)

typed_df = (
    bronze_df
    .select(
        try_int("ProductID").alias("ProductID"),
        try_int("BusinessEntityID").alias("BusinessEntityID"),
        try_int("AverageLeadTime").alias("AverageLeadTime"),
        try_decimal("StandardPrice").alias("StandardPrice"),
        try_decimal("LastReceiptCost").alias("LastReceiptCost"),
        try_timestamp("LastReceiptDate").alias("LastReceiptDate"),
        try_int("MinOrderQty").alias("MinOrderQty"),
        try_int("MaxOrderQty").alias("MaxOrderQty"),
        try_int("OnOrderQty").alias("OnOrderQty"),
        clean_text("UnitMeasureCode").alias("UnitMeasureCode"),
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

vendor_parent_df = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.vendor")
    .select(F.col("BusinessEntityID").alias("_parent_vendor_BusinessEntityID"))
    .distinct()
    .withColumn("_parent_vendor_exists", F.lit(True))
)

checked_df = checked_df.join(
    vendor_parent_df,
    checked_df["BusinessEntityID"] == vendor_parent_df["_parent_vendor_BusinessEntityID"],
    "left",
)

# COMMAND ----------

rejection_expression = (
    F.when(F.col("ProductID").isNull(), F.lit("ProductID is null or invalid"))
    .when(F.col("BusinessEntityID").isNull(), F.lit("BusinessEntityID is null or invalid"))
    .when(F.col("AverageLeadTime").isNull() | (F.col("AverageLeadTime") < 0), F.lit("AverageLeadTime is null, invalid, or negative"))
    .when(F.col("StandardPrice").isNull() | (F.col("StandardPrice") < 0), F.lit("StandardPrice is null, invalid, or negative"))
    .when(F.col("LastReceiptCost").isNotNull() & (F.col("LastReceiptCost") < 0), F.lit("LastReceiptCost cannot be negative"))
    .when(F.col("MinOrderQty").isNull() | (F.col("MinOrderQty") <= 0), F.lit("MinOrderQty must be greater than zero"))
    .when(F.col("MaxOrderQty").isNull() | (F.col("MaxOrderQty") < F.col("MinOrderQty")), F.lit("MaxOrderQty must be at least MinOrderQty"))
    .when(F.col("OnOrderQty").isNotNull() & (F.col("OnOrderQty") < 0), F.lit("OnOrderQty cannot be negative"))
    .when(F.col("UnitMeasureCode").isNull(), F.lit("UnitMeasureCode is blank"))
    .when(F.col("ModifiedDate").isNull(), F.lit("ModifiedDate is null or invalid"))
    .when(F.col("ProductID").isNotNull() & F.col("_parent_product_exists").isNull(), F.lit("ProductID has no matching parent in product"))
    .when(F.col("BusinessEntityID").isNotNull() & F.col("_parent_vendor_exists").isNull(), F.lit("BusinessEntityID has no matching parent in vendor"))
)

checked_df = (
    checked_df
    .withColumn("_rejection_reason", rejection_expression)
    .drop("_parent_product_ProductID", "_parent_product_exists", "_parent_vendor_BusinessEntityID", "_parent_vendor_exists")
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