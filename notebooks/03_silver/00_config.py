# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Configuration
# MAGIC
# MAGIC Shared catalog names and small casting helpers for the AdventureWorks Silver notebooks.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = "smart_factory_dev"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

COMMON_METADATA_COLUMNS = ['_source_file', '_source_file_modification_time', '_ingestion_timestamp', '_source_system', '_source_schema', '_source_entity']

ENTITY_CONFIG = {
    "product_category": {"business_keys": ['ProductCategoryID']},
    "product_subcategory": {"business_keys": ['ProductSubcategoryID']},
    "location": {"business_keys": ['LocationID']},
    "scrap_reason": {"business_keys": ['ScrapReasonID']},
    "vendor": {"business_keys": ['BusinessEntityID']},
    "customer": {"business_keys": ['CustomerID']},
    "product": {"business_keys": ['ProductID']},
    "work_order": {"business_keys": ['WorkOrderID']},
    "work_order_routing": {"business_keys": ['WorkOrderID', 'ProductID', 'OperationSequence']},
    "product_inventory": {"business_keys": ['ProductID', 'LocationID']},
    "transaction_history": {"business_keys": ['TransactionID']},
    "bill_of_materials": {"business_keys": ['BillOfMaterialsID']},
    "product_vendor": {"business_keys": ['ProductID', 'BusinessEntityID']},
    "purchase_order_header": {"business_keys": ['PurchaseOrderID']},
    "purchase_order_detail": {"business_keys": ['PurchaseOrderID', 'PurchaseOrderDetailID']},
    "sales_order_header": {"business_keys": ['SalesOrderID']},
    "sales_order_detail": {"business_keys": ['SalesOrderID', 'SalesOrderDetailID']},
}

PARENT_RELATIONSHIPS = [
    {"child_entity": "product_subcategory", "child_column": "ProductCategoryID", "parent_entity": "product_category", "parent_column": "ProductCategoryID", "nullable": False},
    {"child_entity": "product", "child_column": "ProductSubcategoryID", "parent_entity": "product_subcategory", "parent_column": "ProductSubcategoryID", "nullable": True},
    {"child_entity": "work_order", "child_column": "ProductID", "parent_entity": "product", "parent_column": "ProductID", "nullable": False},
    {"child_entity": "work_order", "child_column": "ScrapReasonID", "parent_entity": "scrap_reason", "parent_column": "ScrapReasonID", "nullable": True},
    {"child_entity": "work_order_routing", "child_column": "WorkOrderID", "parent_entity": "work_order", "parent_column": "WorkOrderID", "nullable": False},
    {"child_entity": "work_order_routing", "child_column": "ProductID", "parent_entity": "product", "parent_column": "ProductID", "nullable": False},
    {"child_entity": "work_order_routing", "child_column": "LocationID", "parent_entity": "location", "parent_column": "LocationID", "nullable": False},
    {"child_entity": "product_inventory", "child_column": "ProductID", "parent_entity": "product", "parent_column": "ProductID", "nullable": False},
    {"child_entity": "product_inventory", "child_column": "LocationID", "parent_entity": "location", "parent_column": "LocationID", "nullable": False},
    {"child_entity": "transaction_history", "child_column": "ProductID", "parent_entity": "product", "parent_column": "ProductID", "nullable": False},
    {"child_entity": "bill_of_materials", "child_column": "ProductAssemblyID", "parent_entity": "product", "parent_column": "ProductID", "nullable": True},
    {"child_entity": "bill_of_materials", "child_column": "ComponentID", "parent_entity": "product", "parent_column": "ProductID", "nullable": False},
    {"child_entity": "product_vendor", "child_column": "ProductID", "parent_entity": "product", "parent_column": "ProductID", "nullable": False},
    {"child_entity": "product_vendor", "child_column": "BusinessEntityID", "parent_entity": "vendor", "parent_column": "BusinessEntityID", "nullable": False},
    {"child_entity": "purchase_order_header", "child_column": "VendorID", "parent_entity": "vendor", "parent_column": "BusinessEntityID", "nullable": False},
    {"child_entity": "purchase_order_detail", "child_column": "PurchaseOrderID", "parent_entity": "purchase_order_header", "parent_column": "PurchaseOrderID", "nullable": False},
    {"child_entity": "purchase_order_detail", "child_column": "ProductID", "parent_entity": "product", "parent_column": "ProductID", "nullable": False},
    {"child_entity": "sales_order_header", "child_column": "CustomerID", "parent_entity": "customer", "parent_column": "CustomerID", "nullable": False},
    {"child_entity": "sales_order_detail", "child_column": "SalesOrderID", "parent_entity": "sales_order_header", "parent_column": "SalesOrderID", "nullable": False},
    {"child_entity": "sales_order_detail", "child_column": "ProductID", "parent_entity": "product", "parent_column": "ProductID", "nullable": False},
]

# COMMAND ----------

def clean_text(column_name):
    cleaned = F.trim(F.col(column_name))
    return F.when(cleaned.isNull() | (cleaned == ""), F.lit(None)).otherwise(cleaned)


def try_int(column_name):
    return F.expr(f"try_cast(`{column_name}` AS INT)")


def try_decimal(column_name):
    return F.expr(f"try_cast(`{column_name}` AS DECIMAL(18, 4))")


def try_timestamp(column_name):
    return F.expr(f"try_cast(`{column_name}` AS TIMESTAMP)")


def try_boolean(column_name):
    cleaned = F.lower(F.trim(F.col(column_name)))
    return (
        F.when(cleaned.isin("1", "true", "t", "yes", "y"), F.lit(True))
        .when(cleaned.isin("0", "false", "f", "no", "n"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )

print(f"Silver configuration ready for {len(ENTITY_CONFIG)} entities.")