# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - Sales Order Detail
# MAGIC
# MAGIC Cleans, validates, quarantines, deduplicates, and upserts the Bronze `sales_order_detail` table.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

entity_name = "sales_order_detail"
bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.{entity_name}"
target_table = f"{CATALOG}.{SILVER_SCHEMA}.{entity_name}"
quarantine_table = f"{CATALOG}.{SILVER_SCHEMA}.{entity_name}_quarantine"
business_keys = ['SalesOrderID', 'SalesOrderDetailID']

print(f"Bronze:     {bronze_table}")
print(f"Silver:     {target_table}")
print(f"Quarantine: {quarantine_table}")

# COMMAND ----------

bronze_df = spark.table(bronze_table)

typed_df = (
    bronze_df
    .select(
        try_int("SalesOrderID").alias("SalesOrderID"),
        try_int("SalesOrderDetailID").alias("SalesOrderDetailID"),
        clean_text("CarrierTrackingNumber").alias("CarrierTrackingNumber"),
        try_int("OrderQty").alias("OrderQty"),
        try_int("ProductID").alias("ProductID"),
        try_int("SpecialOfferID").alias("SpecialOfferID"),
        try_decimal("UnitPrice").alias("UnitPrice"),
        try_decimal("UnitPriceDiscount").alias("UnitPriceDiscount"),
        try_decimal("LineTotal").alias("LineTotal"),
        clean_text("rowguid").alias("rowguid"),
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

sales_order_parent_df = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.sales_order_header")
    .select(F.col("SalesOrderID").alias("_parent_sales_order_SalesOrderID"))
    .distinct()
    .withColumn("_parent_sales_order_exists", F.lit(True))
)

checked_df = checked_df.join(
    sales_order_parent_df,
    checked_df["SalesOrderID"] == sales_order_parent_df["_parent_sales_order_SalesOrderID"],
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

# COMMAND ----------

rejection_expression = (
    F.when(F.col("SalesOrderID").isNull(), F.lit("SalesOrderID is null or invalid"))
    .when(F.col("SalesOrderDetailID").isNull(), F.lit("SalesOrderDetailID is null or invalid"))
    .when(F.col("OrderQty").isNull() | (F.col("OrderQty") <= 0), F.lit("OrderQty must be greater than zero"))
    .when(F.col("ProductID").isNull(), F.lit("ProductID is null or invalid"))
    .when(F.col("SpecialOfferID").isNull(), F.lit("SpecialOfferID is null or invalid"))
    .when(F.col("UnitPrice").isNull() | (F.col("UnitPrice") < 0), F.lit("UnitPrice is null, invalid, or negative"))
    .when(F.col("UnitPriceDiscount").isNull() | ~F.col("UnitPriceDiscount").between(0, 1), F.lit("UnitPriceDiscount must be between 0 and 1"))
    .when(F.col("LineTotal").isNull() | (F.col("LineTotal") < 0), F.lit("LineTotal is null, invalid, or negative"))
    .when(F.col("ModifiedDate").isNull(), F.lit("ModifiedDate is null or invalid"))
    .when(F.col("SalesOrderID").isNotNull() & F.col("_parent_sales_order_exists").isNull(), F.lit("SalesOrderID has no matching parent in sales_order_header"))
    .when(F.col("ProductID").isNotNull() & F.col("_parent_product_exists").isNull(), F.lit("ProductID has no matching parent in product"))
)

checked_df = (
    checked_df
    .withColumn("_rejection_reason", rejection_expression)
    .drop("_parent_sales_order_SalesOrderID", "_parent_sales_order_exists", "_parent_product_ProductID", "_parent_product_exists")
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

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            _rejection_reason,
            COUNT(*) AS rejected_rows
        FROM smart_factory_dev.silver.sales_order_detail_quarantine
        GROUP BY _rejection_reason
        ORDER BY rejected_rows DESC
    """)
)

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            LineTotal,
            OrderQty,
            UnitPrice,
            UnitPriceDiscount,
            rowguid,
            ModifiedDate
        FROM smart_factory_dev.bronze.sales_order_detail
        LIMIT 10
    """)
)