# Databricks notebook source
# MAGIC %md
# MAGIC # 15_autoload_customer
# MAGIC
# MAGIC Simple Bronze Auto Loader notebook.
# MAGIC No raise checks, no field-count validation, no hashing.
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp, lit, col


# COMMAND ----------

SOURCE_PATH = (
    "/Volumes/smart_factory_dev/raw/landing_files/"
    "batch/erp/sales/customer"
)

CHECKPOINT_PATH = (
    "/Volumes/smart_factory_dev/raw/checkpoint_files/"
    "bronze/customer"
)

TARGET_TABLE = "smart_factory_dev.bronze.customer"


# COMMAND ----------

source_schema = StructType([
    StructField("CustomerID", StringType(), True),
    StructField("PersonID", StringType(), True),
    StructField("StoreID", StringType(), True),
    StructField("TerritoryID", StringType(), True),
    StructField("AccountNumber", StringType(), True),
    StructField("rowguid", StringType(), True),
    StructField("ModifiedDate", StringType(), True)
])


# COMMAND ----------

bronze_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("header", "false")
    .option("sep", "\t")
    .schema(source_schema)
    .load(SOURCE_PATH)
    .select(
        "*",
        col("_metadata.file_path").alias("_source_file"),
        col("_metadata.file_modification_time").alias(
            "_source_file_modification_time"
        )
    )
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_system", lit("adventureworks"))
    .withColumn("_source_schema", lit("Sales"))
    .withColumn("_source_entity", lit("Customer"))
)


# COMMAND ----------

query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

query.awaitTermination()

print(f"Bronze ingestion completed: {TARGET_TABLE}")
print(f"Bronze rows: {spark.table(TARGET_TABLE).count():,}")


# COMMAND ----------

display(
    spark.table(TARGET_TABLE)
    .select(
            "CustomerID",
            "PersonID",
            "StoreID",
            "TerritoryID",
            "AccountNumber",
            "rowguid",
            "ModifiedDate"
    )
    .limit(20)
)
