# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Auto Loader - Production.WorkOrder
# MAGIC
# MAGIC Corrected execution order for the existing WorkOrder ingestion.
# MAGIC The source schema, paths, target table, and metadata remain unchanged.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StringType, StructField, StructType

# COMMAND ----------

CATALOG = "smart_factory_dev"
BRONZE_SCHEMA = "bronze"

TARGET_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.work_order"

SOURCE_PATH = (
    "/Volumes/smart_factory_dev/raw/landing_files/"
    "batch/erp/production/work_order"
)

CHECKPOINT_PATH = (
    "/Volumes/smart_factory_dev/raw/checkpoint_files/"
    "bronze/work_order"
)

SOURCE_SYSTEM = "adventureworks"
SOURCE_SCHEMA = "Production"
SOURCE_ENTITY = "WorkOrder"

print("Source:", SOURCE_PATH)
print("Checkpoint:", CHECKPOINT_PATH)
print("Target:", TARGET_TABLE)

# COMMAND ----------

work_order_schema = StructType([
    StructField("WorkOrderID", StringType(), True),
    StructField("ProductID", StringType(), True),
    StructField("OrderQty", StringType(), True),
    StructField("StockedQty", StringType(), True),
    StructField("ScrappedQty", StringType(), True),
    StructField("StartDate", StringType(), True),
    StructField("EndDate", StringType(), True),
    StructField("DueDate", StringType(), True),
    StructField("ScrapReasonID", StringType(), True),
    StructField("ModifiedDate", StringType(), True),
])

print("Schema column count:", len(work_order_schema.fields))

# COMMAND ----------

bronze_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("header", "false")
    .option("sep", "\t")
    .option("encoding", "UTF-8")
    .schema(work_order_schema)
    .load(SOURCE_PATH)
    .select(
        "*",
        col("_metadata.file_path").alias("_source_file"),
        col("_metadata.file_name").alias("_source_file_name"),
        col("_metadata.file_size").alias("_source_file_size"),
        col("_metadata.file_modification_time").alias(
            "_source_file_modification_time"
        ),
    )
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_system", lit(SOURCE_SYSTEM))
    .withColumn("_source_schema", lit(SOURCE_SCHEMA))
    .withColumn("_source_entity", lit(SOURCE_ENTITY))
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
print(f"Bronze WorkOrder rows: {spark.table(TARGET_TABLE).count():,}")

# COMMAND ----------

display(
    spark.table(TARGET_TABLE)
    .select(
        "WorkOrderID",
        "ProductID",
        "OrderQty",
        "StockedQty",
        "ScrappedQty",
        "StartDate",
        "EndDate",
        "DueDate",
        "ScrapReasonID",
        "ModifiedDate",
    )
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional one-time reset
# MAGIC
# MAGIC Use the commands below only when an older WorkOrder table was loaded with
# MAGIC the wrong schema. Keep them commented during normal pipeline execution.

# COMMAND ----------

# spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
# dbutils.fs.rm(CHECKPOINT_PATH, True)
# print("Old WorkOrder Bronze table and checkpoint removed.")
