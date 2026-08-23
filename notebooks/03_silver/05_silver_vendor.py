# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - Vendor
# MAGIC
# MAGIC Cleans, validates, quarantines, deduplicates, and upserts the Bronze `vendor` table.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

entity_name = "vendor"
bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.{entity_name}"
target_table = f"{CATALOG}.{SILVER_SCHEMA}.{entity_name}"
quarantine_table = f"{CATALOG}.{SILVER_SCHEMA}.{entity_name}_quarantine"
business_keys = ['BusinessEntityID']

print(f"Bronze:     {bronze_table}")
print(f"Silver:     {target_table}")
print(f"Quarantine: {quarantine_table}")

# COMMAND ----------

bronze_df = spark.table(bronze_table)

typed_df = (
    bronze_df
    .select(
        try_int("BusinessEntityID").alias("BusinessEntityID"),
        clean_text("AccountNumber").alias("AccountNumber"),
        clean_text("Name").alias("Name"),
        try_int("CreditRating").alias("CreditRating"),
        try_boolean("PreferredVendorStatus").alias("PreferredVendorStatus"),
        try_boolean("ActiveFlag").alias("ActiveFlag"),
        clean_text("PurchasingWebServiceURL").alias("PurchasingWebServiceURL"),
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

# COMMAND ----------

rejection_expression = (
    F.when(F.col("BusinessEntityID").isNull(), F.lit("BusinessEntityID is null or invalid"))
    .when(F.col("AccountNumber").isNull(), F.lit("AccountNumber is blank"))
    .when(F.col("Name").isNull(), F.lit("Name is blank"))
    .when(F.col("CreditRating").isNull() | ~F.col("CreditRating").between(1, 5), F.lit("CreditRating must be between 1 and 5"))
    .when(F.col("PreferredVendorStatus").isNull(), F.lit("PreferredVendorStatus is null or invalid"))
    .when(F.col("ActiveFlag").isNull(), F.lit("ActiveFlag is null or invalid"))
    .when(F.col("ModifiedDate").isNull(), F.lit("ModifiedDate is null or invalid"))
)

checked_df = (
    checked_df
    .withColumn("_rejection_reason", rejection_expression)
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