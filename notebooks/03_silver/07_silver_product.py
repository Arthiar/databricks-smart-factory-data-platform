# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - Product
# MAGIC
# MAGIC Cleans, validates, quarantines, deduplicates, and upserts the Bronze `product` table.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

entity_name = "product"
bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.{entity_name}"
target_table = f"{CATALOG}.{SILVER_SCHEMA}.{entity_name}"
quarantine_table = f"{CATALOG}.{SILVER_SCHEMA}.{entity_name}_quarantine"
business_keys = ['ProductID']

print(f"Bronze:     {bronze_table}")
print(f"Silver:     {target_table}")
print(f"Quarantine: {quarantine_table}")

# COMMAND ----------

bronze_df = spark.table(bronze_table)

typed_df = (
    bronze_df
    .select(
        try_int("ProductID").alias("ProductID"),
        clean_text("Name").alias("Name"),
        clean_text("ProductNumber").alias("ProductNumber"),
        try_boolean("MakeFlag").alias("MakeFlag"),
        try_boolean("FinishedGoodsFlag").alias("FinishedGoodsFlag"),
        clean_text("Color").alias("Color"),
        try_int("SafetyStockLevel").alias("SafetyStockLevel"),
        try_int("ReorderPoint").alias("ReorderPoint"),
        try_decimal("StandardCost").alias("StandardCost"),
        try_decimal("ListPrice").alias("ListPrice"),
        clean_text("Size").alias("Size"),
        clean_text("SizeUnitMeasureCode").alias("SizeUnitMeasureCode"),
        clean_text("WeightUnitMeasureCode").alias("WeightUnitMeasureCode"),
        try_decimal("Weight").alias("Weight"),
        try_int("DaysToManufacture").alias("DaysToManufacture"),
        clean_text("ProductLine").alias("ProductLine"),
        clean_text("Class").alias("Class"),
        clean_text("Style").alias("Style"),
        try_int("ProductSubcategoryID").alias("ProductSubcategoryID"),
        try_int("ProductModelID").alias("ProductModelID"),
        try_timestamp("SellStartDate").alias("SellStartDate"),
        try_timestamp("SellEndDate").alias("SellEndDate"),
        try_timestamp("DiscontinuedDate").alias("DiscontinuedDate"),
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

subcategory_parent_df = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.product_subcategory")
    .select(F.col("ProductSubcategoryID").alias("_parent_subcategory_ProductSubcategoryID"))
    .distinct()
    .withColumn("_parent_subcategory_exists", F.lit(True))
)

checked_df = checked_df.join(
    subcategory_parent_df,
    checked_df["ProductSubcategoryID"] == subcategory_parent_df["_parent_subcategory_ProductSubcategoryID"],
    "left",
)

# COMMAND ----------

rejection_expression = (
    F.when(F.col("ProductID").isNull(), F.lit("ProductID is null or invalid"))
    .when(F.col("Name").isNull(), F.lit("Name is blank"))
    .when(F.col("ProductNumber").isNull(), F.lit("ProductNumber is blank"))
    .when(F.col("MakeFlag").isNull(), F.lit("MakeFlag is null or invalid"))
    .when(F.col("FinishedGoodsFlag").isNull(), F.lit("FinishedGoodsFlag is null or invalid"))
    .when(F.col("SafetyStockLevel").isNull() | (F.col("SafetyStockLevel") < 0), F.lit("SafetyStockLevel is null, invalid, or negative"))
    .when(F.col("ReorderPoint").isNull() | (F.col("ReorderPoint") < 0), F.lit("ReorderPoint is null, invalid, or negative"))
    .when(F.col("StandardCost").isNull() | (F.col("StandardCost") < 0), F.lit("StandardCost is null, invalid, or negative"))
    .when(F.col("ListPrice").isNull() | (F.col("ListPrice") < 0), F.lit("ListPrice is null, invalid, or negative"))
    .when(F.col("Weight").isNotNull() & (F.col("Weight") < 0), F.lit("Weight cannot be negative"))
    .when(F.col("DaysToManufacture").isNull() | (F.col("DaysToManufacture") < 0), F.lit("DaysToManufacture is null, invalid, or negative"))
    .when(F.col("SellStartDate").isNull(), F.lit("SellStartDate is null or invalid"))
    .when(F.col("SellEndDate").isNotNull() & (F.col("SellEndDate") < F.col("SellStartDate")), F.lit("SellEndDate is before SellStartDate"))
    .when(F.col("ModifiedDate").isNull(), F.lit("ModifiedDate is null or invalid"))
    .when(F.col("ProductSubcategoryID").isNotNull() & F.col("_parent_subcategory_exists").isNull(), F.lit("ProductSubcategoryID has no matching parent in product_subcategory"))
)

checked_df = (
    checked_df
    .withColumn("_rejection_reason", rejection_expression)
    .drop("_parent_subcategory_ProductSubcategoryID", "_parent_subcategory_exists")
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