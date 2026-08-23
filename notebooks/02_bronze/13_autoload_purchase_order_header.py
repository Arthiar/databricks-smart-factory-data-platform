# Databricks notebook source
# MAGIC %md
# MAGIC # 13_autoload_purchase_order_header
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
    "batch/erp/purchasing/purchase_order_header"
)

CHECKPOINT_PATH = (
    "/Volumes/smart_factory_dev/raw/checkpoint_files/"
    "bronze/purchase_order_header"
)

TARGET_TABLE = "smart_factory_dev.bronze.purchase_order_header"


# COMMAND ----------

source_schema = StructType([
    StructField("PurchaseOrderID", StringType(), True),
    StructField("RevisionNumber", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("EmployeeID", StringType(), True),
    StructField("VendorID", StringType(), True),
    StructField("ShipMethodID", StringType(), True),
    StructField("OrderDate", StringType(), True),
    StructField("ShipDate", StringType(), True),
    StructField("SubTotal", StringType(), True),
    StructField("TaxAmt", StringType(), True),
    StructField("Freight", StringType(), True),
    StructField("TotalDue", StringType(), True),
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
    .withColumn("_source_schema", lit("Purchasing"))
    .withColumn("_source_entity", lit("PurchaseOrderHeader"))
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
            "PurchaseOrderID",
            "RevisionNumber",
            "Status",
            "EmployeeID",
            "VendorID",
            "ShipMethodID",
            "OrderDate",
            "ShipDate",
            "SubTotal",
            "TaxAmt",
            "Freight",
            "TotalDue",
            "ModifiedDate"
    )
    .limit(20)
)
