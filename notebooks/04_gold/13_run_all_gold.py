# Databricks notebook source
# MAGIC %md
# MAGIC # Run All Gold Notebooks
# MAGIC
# MAGIC Executes the Gold layer in dependency order.

# COMMAND ----------

# MAGIC %run ./01_dim_date

# COMMAND ----------

# MAGIC %run ./02_dim_customer

# COMMAND ----------

# MAGIC %run ./03_dim_supplier

# COMMAND ----------

# MAGIC %run ./04_dim_work_center

# COMMAND ----------

# MAGIC %run ./05_dim_product_scd2

# COMMAND ----------

# MAGIC %run ./06_fact_sales

# COMMAND ----------

# MAGIC %run ./07_fact_purchase_order

# COMMAND ----------

# MAGIC %run ./08_fact_production

# COMMAND ----------

# MAGIC %run ./09_fact_work_order_operation

# COMMAND ----------

# MAGIC %run ./10_build_kpi_tables

# COMMAND ----------

# MAGIC %run ./12_validate_gold

# COMMAND ----------

print("All Gold notebooks completed successfully.")