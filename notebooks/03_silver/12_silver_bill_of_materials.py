# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - Bill Of Materials
# MAGIC
# MAGIC Cleans, validates, quarantines, deduplicates, and upserts the Bronze `bill_of_materials` table.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

entity_name = "bill_of_materials"
bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.{entity_name}"
target_table = f"{CATALOG}.{SILVER_SCHEMA}.{entity_name}"
quarantine_table = f"{CATALOG}.{SILVER_SCHEMA}.{entity_name}_quarantine"
business_keys = ['BillOfMaterialsID']

print(f"Bronze:     {bronze_table}")
print(f"Silver:     {target_table}")
print(f"Quarantine: {quarantine_table}")

# COMMAND ----------

bronze_df = spark.table(bronze_table)

typed_df = (
    bronze_df
    .select(
        try_int("BillOfMaterialsID").alias("BillOfMaterialsID"),
        try_int("ProductAssemblyID").alias("ProductAssemblyID"),
        try_int("ComponentID").alias("ComponentID"),
        try_timestamp("StartDate").alias("StartDate"),
        try_timestamp("EndDate").alias("EndDate"),
        clean_text("UnitMeasureCode").alias("UnitMeasureCode"),
        try_int("BOMLevel").alias("BOMLevel"),
        try_decimal("PerAssemblyQty").alias("PerAssemblyQty"),
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

assembly_product_parent_df = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.product")
    .select(F.col("ProductID").alias("_parent_assembly_product_ProductID"))
    .distinct()
    .withColumn("_parent_assembly_product_exists", F.lit(True))
)

checked_df = checked_df.join(
    assembly_product_parent_df,
    checked_df["ProductAssemblyID"] == assembly_product_parent_df["_parent_assembly_product_ProductID"],
    "left",
)

component_product_parent_df = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.product")
    .select(F.col("ProductID").alias("_parent_component_product_ProductID"))
    .distinct()
    .withColumn("_parent_component_product_exists", F.lit(True))
)

checked_df = checked_df.join(
    component_product_parent_df,
    checked_df["ComponentID"] == component_product_parent_df["_parent_component_product_ProductID"],
    "left",
)

# COMMAND ----------

rejection_expression = (
    F.when(F.col("BillOfMaterialsID").isNull(), F.lit("BillOfMaterialsID is null or invalid"))
    .when(F.col("ComponentID").isNull(), F.lit("ComponentID is null or invalid"))
    .when(F.col("StartDate").isNull(), F.lit("StartDate is null or invalid"))
    .when(F.col("EndDate").isNotNull() & (F.col("EndDate") < F.col("StartDate")), F.lit("EndDate is before StartDate"))
    .when(F.col("UnitMeasureCode").isNull(), F.lit("UnitMeasureCode is blank"))
    .when(F.col("BOMLevel").isNull() | (F.col("BOMLevel") < 0), F.lit("BOMLevel is null, invalid, or negative"))
    .when(F.col("PerAssemblyQty").isNull() | (F.col("PerAssemblyQty") <= 0), F.lit("PerAssemblyQty must be greater than zero"))
    .when(F.col("ModifiedDate").isNull(), F.lit("ModifiedDate is null or invalid"))
    .when(F.col("ProductAssemblyID").isNotNull() & F.col("_parent_assembly_product_exists").isNull(), F.lit("ProductAssemblyID has no matching parent in product"))
    .when(F.col("ComponentID").isNotNull() & F.col("_parent_component_product_exists").isNull(), F.lit("ComponentID has no matching parent in product"))
)

checked_df = (
    checked_df
    .withColumn("_rejection_reason", rejection_expression)
    .drop("_parent_assembly_product_ProductID", "_parent_assembly_product_exists", "_parent_component_product_ProductID", "_parent_component_product_exists")
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