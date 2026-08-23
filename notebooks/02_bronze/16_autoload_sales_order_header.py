# Databricks notebook source
# MAGIC %md
# MAGIC # 16_autoload_sales_order_header
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
    "batch/erp/sales/sales_order_header"
)

CHECKPOINT_PATH = (
    "/Volumes/smart_factory_dev/raw/checkpoint_files/"
    "bronze/sales_order_header"
)

TARGET_TABLE = "smart_factory_dev.bronze.sales_order_header"


# COMMAND ----------

source_schema = StructType([
    StructField("SalesOrderID", StringType(), True),
    StructField("RevisionNumber", StringType(), True),
    StructField("OrderDate", StringType(), True),
    StructField("DueDate", StringType(), True),
    StructField("ShipDate", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("OnlineOrderFlag", StringType(), True),
    StructField("SalesOrderNumber", StringType(), True),
    StructField("PurchaseOrderNumber", StringType(), True),
    StructField("AccountNumber", StringType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("SalesPersonID", StringType(), True),
    StructField("TerritoryID", StringType(), True),
    StructField("BillToAddressID", StringType(), True),
    StructField("ShipToAddressID", StringType(), True),
    StructField("ShipMethodID", StringType(), True),
    StructField("CreditCardID", StringType(), True),
    StructField("CreditCardApprovalCode", StringType(), True),
    StructField("CurrencyRateID", StringType(), True),
    StructField("SubTotal", StringType(), True),
    StructField("TaxAmt", StringType(), True),
    StructField("Freight", StringType(), True),
    StructField("TotalDue", StringType(), True),
    StructField("Comment", StringType(), True),
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
    .withColumn("_source_entity", lit("SalesOrderHeader"))
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
            "SalesOrderID",
            "RevisionNumber",
            "OrderDate",
            "DueDate",
            "ShipDate",
            "Status",
            "OnlineOrderFlag",
            "SalesOrderNumber",
            "PurchaseOrderNumber",
            "AccountNumber",
            "CustomerID",
            "SalesPersonID",
            "TerritoryID",
            "BillToAddressID",
            "ShipToAddressID",
            "ShipMethodID",
            "CreditCardID",
            "CreditCardApprovalCode",
            "CurrencyRateID",
            "SubTotal",
            "TaxAmt",
            "Freight",
            "TotalDue",
            "Comment",
            "rowguid",
            "ModifiedDate"
    )
    .limit(20)
)
