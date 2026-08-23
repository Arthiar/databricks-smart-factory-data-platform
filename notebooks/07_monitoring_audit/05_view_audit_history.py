# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # Pipeline Audit History
# MAGIC
# MAGIC Displays recent pipeline runs and a small status summary.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

audit_df = spark.table(AUDIT_TABLE)

display(
    audit_df
    .orderBy(F.col("StartTime").desc())
    .limit(100)
)

# COMMAND ----------



# COMMAND ----------

status_summary_df = (
    audit_df
    .groupBy("Status")
    .agg(F.count("RunID").alias("RunCount"))
    .orderBy("Status")
)

display(status_summary_df)