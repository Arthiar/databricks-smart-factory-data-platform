# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Configuration
# MAGIC
# MAGIC Shared names and small helpers for the AdventureWorks Gold notebooks.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = "smart_factory_dev"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

GOLD_TABLE_KEYS = {
    "dim_date": ["DateKey"],
    "dim_product": ["ProductKey"],
    "dim_customer": ["CustomerKey"],
    "dim_supplier": ["SupplierKey"],
    "dim_work_center": ["WorkCenterKey"],
    "fact_sales": ["SalesOrderID", "SalesOrderDetailID"],
    "fact_purchase_order": ["PurchaseOrderID", "PurchaseOrderDetailID"],
    "fact_production": ["WorkOrderID"],
    "fact_work_order_operation": ["WorkOrderID", "ProductID", "OperationSequence"],
    "kpi_daily_sales": ["DateKey"],
    "kpi_product_sales": ["ProductKey"],
    "kpi_supplier_performance": ["SupplierKey"],
    "kpi_production_efficiency": ["ProductKey"],
    "kpi_operation_cost": ["WorkCenterKey"],
}

# COMMAND ----------

def silver_table(table_name):
    return f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"


def gold_table(table_name):
    return f"{CATALOG}.{GOLD_SCHEMA}.{table_name}"


def date_key(column):
    """Convert a date or timestamp column to an integer yyyyMMdd key."""
    return F.date_format(F.to_date(column), "yyyyMMdd").cast("int")


def merge_to_gold(source_df, table_name, business_keys):
    """Insert new rows and update existing rows by readable business keys."""
    target_table = gold_table(table_name)

    if spark.catalog.tableExists(target_table):
        merge_condition = " AND ".join(
            [f"t.`{key}` = s.`{key}`" for key in business_keys]
        )

        (
            DeltaTable.forName(spark, target_table)
            .alias("t")
            .merge(source_df.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            source_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )

    return target_table


print(f"Gold configuration ready: {CATALOG}.{GOLD_SCHEMA}")