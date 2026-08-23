# Databricks notebook source
# MAGIC %md
# MAGIC # Create Pipeline Audit Table
# MAGIC
# MAGIC Run this notebook once before adding the audit tasks to Lakeflow Jobs.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

print(f"Audit table created successfully: {AUDIT_TABLE}")
display(spark.sql(f"DESCRIBE TABLE {AUDIT_TABLE}"))