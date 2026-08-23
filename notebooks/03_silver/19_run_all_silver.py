# Databricks notebook source
# MAGIC %md
# MAGIC # Run Complete AdventureWorks Silver Layer
# MAGIC
# MAGIC Executes all Silver notebooks in dependency order.

# COMMAND ----------

# MAGIC %run ./01_silver_product_category

# COMMAND ----------

# MAGIC %run ./02_silver_product_subcategory

# COMMAND ----------

# MAGIC %run ./03_silver_location

# COMMAND ----------

# MAGIC %run ./04_silver_scrap_reason

# COMMAND ----------

# MAGIC %run ./05_silver_vendor

# COMMAND ----------

# MAGIC %run ./06_silver_customer

# COMMAND ----------

# MAGIC %run ./07_silver_product

# COMMAND ----------

# MAGIC %run ./08_silver_work_order

# COMMAND ----------

# MAGIC %run ./09_silver_work_order_routing

# COMMAND ----------

# MAGIC %run ./10_silver_product_inventory

# COMMAND ----------

# MAGIC %run ./11_silver_transaction_history

# COMMAND ----------

# MAGIC %run ./12_silver_bill_of_materials

# COMMAND ----------

# MAGIC %run ./13_silver_product_vendor

# COMMAND ----------

# MAGIC %run ./14_silver_purchase_order_header

# COMMAND ----------

# MAGIC %run ./15_silver_purchase_order_detail

# COMMAND ----------

# MAGIC %run ./16_silver_sales_order_header

# COMMAND ----------

# MAGIC %run ./17_silver_sales_order_detail

# COMMAND ----------

print("All 17 Silver entity notebooks completed.")
print("Run 18_validate_silver next.")

