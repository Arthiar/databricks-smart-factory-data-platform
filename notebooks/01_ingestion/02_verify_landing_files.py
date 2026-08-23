# Databricks notebook source
base_path = "/Volumes/smart_factory_dev/raw/landing_files/batch/erp"

paths = [
    "production/product/Product.csv",
    "production/work_order/WorkOrder.csv",
    "production/work_order_routing/WorkOrderRouting.csv",
    "purchasing/vendor/Vendor.csv",
    "purchasing/purchase_order_header/PurchaseOrderHeader.csv",
    "sales/customer/Customer.csv",
    "sales/sales_order_header/SalesOrderHeader.csv",
]

for path in paths:
    full_path = f"{base_path}/{path}"

    file_info = dbutils.fs.ls(full_path)[0]

    print(
        f"{path:<70} "
        f"{file_info.size / 1024 / 1024:.2f} MB"
    )