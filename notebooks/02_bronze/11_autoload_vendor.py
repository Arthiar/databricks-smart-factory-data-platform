# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Auto Loader - Purchasing.Vendor
# MAGIC
# MAGIC Incrementally ingests the official Microsoft AdventureWorks `Vendor.csv` source file from the governed ADLS landing volume into the Bronze Delta table.
# MAGIC
# MAGIC All source business columns are intentionally retained as strings in Bronze. Datatype conversion, business validation, deduplication and quarantine belong in Silver.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp, lit, col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source-specific configuration

# COMMAND ----------

entity_name = "vendor"
cfg = ENTITY_CONFIG[entity_name]

source_path = cfg["source_path"]
checkpoint_path = cfg["checkpoint_path"]
target_table = cfg["target_table"]

print(f"Source:     {source_path}")
print(f"Checkpoint: {checkpoint_path}")
print(f"Target:     {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explicit Bronze schema

# COMMAND ----------

vendor_schema = StructType([
    StructField("BusinessEntityID", StringType(), True),
    StructField("AccountNumber", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("CreditRating", StringType(), True),
    StructField("PreferredVendorStatus", StringType(), True),
    StructField("ActiveFlag", StringType(), True),
    StructField("PurchasingWebServiceURL", StringType(), True),
    StructField("ModifiedDate", StringType(), True)
])

print(f"Schema contains {len(vendor_schema.fields)} raw source columns.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader stream

# COMMAND ----------

bronze_df = (
    spark.readStream
    .format("cloudFiles")
    .options(**AUTOLOADER_OPTIONS)
    .schema(vendor_schema)
    .load(source_path)
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
    .withColumn("_source_schema", lit(cfg["source_schema"]))
    .withColumn("_source_entity", lit(cfg["source_entity"]))
)

bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Delta

# COMMAND ----------

query = (
    bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(target_table)
)

query.awaitTermination()

print("Bronze ingestion completed: Purchasing.Vendor")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick verification

# COMMAND ----------

display(
    spark.sql(f'''
        SELECT *
        FROM {target_table}
        LIMIT 10
    ''')
)