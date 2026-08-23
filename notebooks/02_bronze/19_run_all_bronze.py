# Databricks notebook source
# MAGIC %md
# MAGIC # Run All Bronze Notebooks
# MAGIC
# MAGIC Runs every Bronze Auto Loader notebook in order and finishes with Bronze validation. Keep this notebook inside the same `02_bronze` folder as notebooks `00` to `18`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./01_autoload_product

# COMMAND ----------

# MAGIC %run ./02_autoload_product_category

# COMMAND ----------

# MAGIC %run ./03_autoload_product_subcategory

# COMMAND ----------

# MAGIC %run ./04_autoload_work_order

# COMMAND ----------

# MAGIC %run ./05_autoload_work_order_routing

# COMMAND ----------

# MAGIC %run ./06_autoload_location

# COMMAND ----------

# MAGIC %run ./07_autoload_scrap_reason

# COMMAND ----------

# MAGIC %run ./08_autoload_product_inventory

# COMMAND ----------

# MAGIC %run ./09_autoload_transaction_history

# COMMAND ----------

# MAGIC %run ./10_autoload_bill_of_materials

# COMMAND ----------

# MAGIC %run ./11_autoload_vendor

# COMMAND ----------

# MAGIC %run ./12_autoload_product_vendor

# COMMAND ----------

# MAGIC %run ./13_autoload_purchase_order_header

# COMMAND ----------

# MAGIC %run ./14_autoload_purchase_order_detail

# COMMAND ----------

# MAGIC %run ./15_autoload_customer

# COMMAND ----------

# MAGIC %run ./16_autoload_sales_order_header

# COMMAND ----------

# MAGIC %run ./17_autoload_sales_order_detail_fixed

# COMMAND ----------

# MAGIC %run ./18_validate_bronze

# COMMAND ----------

print("Bronze pipeline and validation completed successfully.")