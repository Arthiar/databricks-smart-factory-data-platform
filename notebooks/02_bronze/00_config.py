# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Configuration
# MAGIC
# MAGIC Shared configuration for all AdventureWorks batch Auto Loader notebooks.
# MAGIC
# MAGIC This notebook does not create infrastructure. It only centralizes paths, target names and source metadata already created for the Smart Factory project.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

CATALOG = "smart_factory_dev"
BRONZE_SCHEMA = "bronze"

LANDING_BASE = (
    "/Volumes/smart_factory_dev/raw/landing_files/"
    "batch/erp"
)

CHECKPOINT_BASE = (
    "/Volumes/smart_factory_dev/raw/checkpoint_files/"
    "bronze"
)

SOURCE_SYSTEM = "adventureworks"

AUTOLOADER_OPTIONS = {
    "cloudFiles.format": "csv",
    "cloudFiles.includeExistingFiles": "true",
    "sep": "\t",
    "header": "false",
    "encoding": "UTF-8",
}

# COMMAND ----------

ENTITY_CONFIG = {
    "product": {
        "source_schema": "Production",
        "source_entity": "Product",
        "source_path": f"{LANDING_BASE}/production/product",
        "checkpoint_path": f"{CHECKPOINT_BASE}/product",
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.product",
        "business_keys": ['ProductID'],
    },
    "product_category": {
        "source_schema": "Production",
        "source_entity": "ProductCategory",
        "source_path": f"{LANDING_BASE}/production/product_category",
        "checkpoint_path": f"{CHECKPOINT_BASE}/product_category",
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.product_category",
        "business_keys": ['ProductCategoryID'],
    },
    "product_subcategory": {
        "source_schema": "Production",
        "source_entity": "ProductSubcategory",
        "source_path": f"{LANDING_BASE}/production/product_subcategory",
        "checkpoint_path": f"{CHECKPOINT_BASE}/product_subcategory",
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.product_subcategory",
        "business_keys": ['ProductSubcategoryID'],
    },
    "work_order": {
        "source_schema": "Production",
        "source_entity": "WorkOrder",
        "source_path": f"{LANDING_BASE}/production/work_order",
        "checkpoint_path": f"{CHECKPOINT_BASE}/work_order",
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.work_order",
        "business_keys": ['WorkOrderID'],
    },
    "work_order_routing": {
        "source_schema": "Production",
        "source_entity": "WorkOrderRouting",
        "source_path": f"{LANDING_BASE}/production/work_order_routing",
        "checkpoint_path": f"{CHECKPOINT_BASE}/work_order_routing",
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.work_order_routing",
        "business_keys": ['WorkOrderID', 'ProductID', 'OperationSequence'],
    },
    "location": {
        "source_schema": "Production",
        "source_entity": "Location",
        "source_path": f"{LANDING_BASE}/production/location",
        "checkpoint_path": f"{CHECKPOINT_BASE}/location",
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.location",
        "business_keys": ['LocationID'],
    },
    "scrap_reason": {
        "source_schema": "Production",
        "source_entity": "ScrapReason",
        "source_path": f"{LANDING_BASE}/production/scrap_reason",
        "checkpoint_path": f"{CHECKPOINT_BASE}/scrap_reason",
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.scrap_reason",
        "business_keys": ['ScrapReasonID'],
    },
    "product_inventory": {
        "source_schema": "Production",
        "source_entity": "ProductInventory",
        "source_path": f"{LANDING_BASE}/production/product_inventory",
        "checkpoint_path": f"{CHECKPOINT_BASE}/product_inventory",
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.product_inventory",
        "business_keys": ['ProductID', 'LocationID'],
    },
    "transaction_history": {
        "source_schema": "Production",
        "source_entity": "TransactionHistory",
        "source_path": f"{LANDING_BASE}/production/transaction_history",
        "checkpoint_path": f"{CHECKPOINT_BASE}/transaction_history",
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.transaction_history",
        "business_keys": ['TransactionID'],
    },
    "bill_of_materials": {
        "source_schema": "Production",
        "source_entity": "BillOfMaterials",
        "source_path": f"{LANDING_BASE}/production/bill_of_materials",
        "checkpoint_path": f"{CHECKPOINT_BASE}/bill_of_materials",
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.bill_of_materials",
        "business_keys": ['BillOfMaterialsID'],
    },
    "vendor": {
        "source_schema": "Purchasing",
        "source_entity": "Vendor",
        "source_path": f"{LANDING_BASE}/purchasing/vendor",
        "checkpoint_path": f"{CHECKPOINT_BASE}/vendor",
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.vendor",
        "business_keys": ['BusinessEntityID'],
    },
    "product_vendor": {
        "source_schema": "Purchasing",
        "source_entity": "ProductVendor",
        "source_path": f"{LANDING_BASE}/purchasing/product_vendor",
        "checkpoint_path": f"{CHECKPOINT_BASE}/product_vendor",
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.product_vendor",
        "business_keys": ['ProductID', 'BusinessEntityID'],
    },
    "purchase_order_header": {
        "source_schema": "Purchasing",
        "source_entity": "PurchaseOrderHeader",
        "source_path": f"{LANDING_BASE}/purchasing/purchase_order_header",
        "checkpoint_path": f"{CHECKPOINT_BASE}/purchase_order_header",
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.purchase_order_header",
        "business_keys": ['PurchaseOrderID'],
    },
    "purchase_order_detail": {
        "source_schema": "Purchasing",
        "source_entity": "PurchaseOrderDetail",
        "source_path": f"{LANDING_BASE}/purchasing/purchase_order_detail",
        "checkpoint_path": f"{CHECKPOINT_BASE}/purchase_order_detail",
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.purchase_order_detail",
        "business_keys": ['PurchaseOrderID', 'PurchaseOrderDetailID'],
    },
    "customer": {
        "source_schema": "Sales",
        "source_entity": "Customer",
        "source_path": f"{LANDING_BASE}/sales/customer",
        "checkpoint_path": f"{CHECKPOINT_BASE}/customer",
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.customer",
        "business_keys": ['CustomerID'],
    },
    "sales_order_header": {
        "source_schema": "Sales",
        "source_entity": "SalesOrderHeader",
        "source_path": f"{LANDING_BASE}/sales/sales_order_header",
        "checkpoint_path": f"{CHECKPOINT_BASE}/sales_order_header",
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.sales_order_header",
        "business_keys": ['SalesOrderID'],
    },
    "sales_order_detail": {
        "source_schema": "Sales",
        "source_entity": "SalesOrderDetail",
        "source_path": f"{LANDING_BASE}/sales/sales_order_detail",
        "checkpoint_path": f"{CHECKPOINT_BASE}/sales_order_detail",
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.sales_order_detail",
        "business_keys": ['SalesOrderID', 'SalesOrderDetailID'],
    }
}

print(f"Configured {len(ENTITY_CONFIG)} Bronze entities.")
for entity_name, cfg in ENTITY_CONFIG.items():
    print(
        f"{entity_name:<24} "
        f"{cfg['source_schema']}.{cfg['source_entity']} "
        f"-> {cfg['target_table']}"
    )