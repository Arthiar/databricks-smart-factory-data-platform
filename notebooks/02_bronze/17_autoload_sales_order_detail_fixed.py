# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Auto Loader - Sales.SalesOrderDetail
# MAGIC
# MAGIC Corrected notebook for the shifted `LineTotal`, `rowguid`, and `ModifiedDate` columns.
# MAGIC
# MAGIC For the first corrective run only, change `RUN_ONE_TIME_RESET` to `True`. After the notebook finishes successfully, change it back to `False`.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructField, StringType, StructType

# COMMAND ----------

SOURCE_PATH = (
    "/Volumes/smart_factory_dev/raw/landing_files/"
    "batch/erp/sales/sales_order_detail"
)

CHECKPOINT_PATH = (
    "/Volumes/smart_factory_dev/raw/checkpoint_files/"
    "bronze/sales_order_detail"
)

TARGET_TABLE = "smart_factory_dev.bronze.sales_order_detail"

print("Source:", SOURCE_PATH)
print("Checkpoint:", CHECKPOINT_PATH)
print("Target:", TARGET_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## One-time correction
# MAGIC
# MAGIC Set the variable below to `True` only for the first run. This removes the incorrectly loaded Bronze table and its stale Auto Loader checkpoint. The landing source files are not deleted.

# COMMAND ----------

RUN_ONE_TIME_RESET = False

if RUN_ONE_TIME_RESET:
    spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    dbutils.fs.rm(CHECKPOINT_PATH, True)
    print("Old Bronze SalesOrderDetail table and checkpoint removed.")
else:
    print("Reset skipped. Change RUN_ONE_TIME_RESET to True only for the first corrective run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explicit Bronze schema
# MAGIC
# MAGIC `LineTotal` must be positioned between `UnitPriceDiscount` and `rowguid`.

# COMMAND ----------

source_schema = StructType([
    StructField("SalesOrderID", StringType(), True),
    StructField("SalesOrderDetailID", StringType(), True),
    StructField("CarrierTrackingNumber", StringType(), True),
    StructField("OrderQty", StringType(), True),
    StructField("ProductID", StringType(), True),
    StructField("SpecialOfferID", StringType(), True),
    StructField("UnitPrice", StringType(), True),
    StructField("UnitPriceDiscount", StringType(), True),
    StructField("LineTotal", StringType(), True),
    StructField("rowguid", StringType(), True),
    StructField("ModifiedDate", StringType(), True)
])

print("Schema column count:", len(source_schema.fields))

# COMMAND ----------

bronze_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("header", "false")
    .option("sep", "\t")
    .option("encoding", "UTF-8")
    .schema(source_schema)
    .load(SOURCE_PATH)
    .select(
        "*",
        col("_metadata.file_path").alias("_source_file"),
        col("_metadata.file_name").alias("_source_file_name"),
        col("_metadata.file_size").alias("_source_file_size"),
        col("_metadata.file_modification_time").alias(
            "_source_file_modification_time"
        )
    )
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_system", lit("adventureworks"))
    .withColumn("_source_schema", lit("Sales"))
    .withColumn("_source_entity", lit("SalesOrderDetail"))
)

bronze_df.printSchema()

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

# MAGIC %md
# MAGIC ## Verification
# MAGIC
# MAGIC Confirm that `LineTotal` is numeric, `rowguid` contains GUID values, and `ModifiedDate` contains timestamps.

# COMMAND ----------

display(
    spark.table(TARGET_TABLE)
    .select(
        "SalesOrderID",
        "SalesOrderDetailID",
        "OrderQty",
        "UnitPrice",
        "UnitPriceDiscount",
        "LineTotal",
        "rowguid",
        "ModifiedDate"
    )
    .limit(20)
)