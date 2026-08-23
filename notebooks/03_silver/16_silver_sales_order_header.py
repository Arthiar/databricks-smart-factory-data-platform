# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - Sales Order Header
# MAGIC
# MAGIC Cleans, validates, quarantines, deduplicates, and upserts the Bronze `sales_order_header` table.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

entity_name = "sales_order_header"
bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.{entity_name}"
target_table = f"{CATALOG}.{SILVER_SCHEMA}.{entity_name}"
quarantine_table = f"{CATALOG}.{SILVER_SCHEMA}.{entity_name}_quarantine"
business_keys = ['SalesOrderID']

print(f"Bronze:     {bronze_table}")
print(f"Silver:     {target_table}")
print(f"Quarantine: {quarantine_table}")

# COMMAND ----------

bronze_df = spark.table(bronze_table)

typed_df = (
    bronze_df
    .select(
        try_int("SalesOrderID").alias("SalesOrderID"),
        try_int("RevisionNumber").alias("RevisionNumber"),
        try_timestamp("OrderDate").alias("OrderDate"),
        try_timestamp("DueDate").alias("DueDate"),
        try_timestamp("ShipDate").alias("ShipDate"),
        try_int("Status").alias("Status"),
        try_boolean("OnlineOrderFlag").alias("OnlineOrderFlag"),
        clean_text("SalesOrderNumber").alias("SalesOrderNumber"),
        clean_text("PurchaseOrderNumber").alias("PurchaseOrderNumber"),
        clean_text("AccountNumber").alias("AccountNumber"),
        try_int("CustomerID").alias("CustomerID"),
        try_int("SalesPersonID").alias("SalesPersonID"),
        try_int("TerritoryID").alias("TerritoryID"),
        try_int("BillToAddressID").alias("BillToAddressID"),
        try_int("ShipToAddressID").alias("ShipToAddressID"),
        try_int("ShipMethodID").alias("ShipMethodID"),
        try_int("CreditCardID").alias("CreditCardID"),
        clean_text("CreditCardApprovalCode").alias("CreditCardApprovalCode"),
        try_int("CurrencyRateID").alias("CurrencyRateID"),
        try_decimal("SubTotal").alias("SubTotal"),
        try_decimal("TaxAmt").alias("TaxAmt"),
        try_decimal("Freight").alias("Freight"),
        try_decimal("TotalDue").alias("TotalDue"),
        clean_text("Comment").alias("Comment"),
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

customer_parent_df = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.customer")
    .select(F.col("CustomerID").alias("_parent_customer_CustomerID"))
    .distinct()
    .withColumn("_parent_customer_exists", F.lit(True))
)

checked_df = checked_df.join(
    customer_parent_df,
    checked_df["CustomerID"] == customer_parent_df["_parent_customer_CustomerID"],
    "left",
)

# COMMAND ----------

rejection_expression = (
    F.when(F.col("SalesOrderID").isNull(), F.lit("SalesOrderID is null or invalid"))
    .when(F.col("OrderDate").isNull(), F.lit("OrderDate is null or invalid"))
    .when(F.col("DueDate").isNull(), F.lit("DueDate is null or invalid"))
    .when(F.col("DueDate") < F.col("OrderDate"), F.lit("DueDate is before OrderDate"))
    .when(F.col("ShipDate").isNotNull() & (F.col("ShipDate") < F.col("OrderDate")), F.lit("ShipDate is before OrderDate"))
    .when(F.col("Status").isNull(), F.lit("Status is null or invalid"))
    .when(F.col("OnlineOrderFlag").isNull(), F.lit("OnlineOrderFlag is null or invalid"))
    .when(F.col("SalesOrderNumber").isNull(), F.lit("SalesOrderNumber is blank"))
    .when(F.col("CustomerID").isNull(), F.lit("CustomerID is null or invalid"))
    .when(F.col("SubTotal").isNull() | (F.col("SubTotal") < 0), F.lit("SubTotal is null, invalid, or negative"))
    .when(F.col("TaxAmt").isNull() | (F.col("TaxAmt") < 0), F.lit("TaxAmt is null, invalid, or negative"))
    .when(F.col("Freight").isNull() | (F.col("Freight") < 0), F.lit("Freight is null, invalid, or negative"))
    .when(F.col("TotalDue").isNull() | (F.col("TotalDue") < 0), F.lit("TotalDue is null, invalid, or negative"))
    .when(F.col("ModifiedDate").isNull(), F.lit("ModifiedDate is null or invalid"))
    .when(F.col("CustomerID").isNotNull() & F.col("_parent_customer_exists").isNull(), F.lit("CustomerID has no matching parent in customer"))
)

checked_df = (
    checked_df
    .withColumn("_rejection_reason", rejection_expression)
    .drop("_parent_customer_CustomerID", "_parent_customer_exists")
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