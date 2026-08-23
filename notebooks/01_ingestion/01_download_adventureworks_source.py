# Databricks notebook source
from pathlib import Path
import requests

BASE_URL = (
    "https://raw.githubusercontent.com/microsoft/"
    "sql-server-samples/master/"
    "samples/databases/adventure-works/oltp-install-script"
)

LANDING = Path(
    "/Volumes/smart_factory_dev/raw/landing_files/batch/erp"
)

files = {
    # Production
    "Product.csv": "production/product",
    "ProductSubcategory.csv": "production/product_subcategory",
    "ProductCategory.csv": "production/product_category",
    "WorkOrder.csv": "production/work_order",
    "WorkOrderRouting.csv": "production/work_order_routing",
    "Location.csv": "production/location",
    "ScrapReason.csv": "production/scrap_reason",
    "ProductInventory.csv": "production/product_inventory",
    "TransactionHistory.csv": "production/transaction_history",
    "BillOfMaterials.csv": "production/bill_of_materials",

    # Purchasing
    "Vendor.csv": "purchasing/vendor",
    "ProductVendor.csv": "purchasing/product_vendor",
    "PurchaseOrderHeader.csv": "purchasing/purchase_order_header",
    "PurchaseOrderDetail.csv": "purchasing/purchase_order_detail",

    # Sales
    "Customer.csv": "sales/customer",
    "SalesOrderHeader.csv": "sales/sales_order_header",
    "SalesOrderDetail.csv": "sales/sales_order_detail",
}

for filename, relative_folder in files.items():

    destination_folder = LANDING / relative_folder
    destination_folder.mkdir(parents=True, exist_ok=True)

    destination = destination_folder / filename
    url = f"{BASE_URL}/{filename}"

    print(f"Downloading {filename}...")

    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()

        with open(destination, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    print(f"Saved -> {destination}")

print("AdventureWorks landing download completed.")