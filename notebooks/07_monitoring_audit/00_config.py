# Databricks notebook source
# MAGIC %md
# MAGIC # Monitoring Configuration
# MAGIC
# MAGIC Creates the monitoring schema and the pipeline audit Delta table.

# COMMAND ----------

CATALOG = "smart_factory_dev"
MONITORING_SCHEMA = "monitoring"
AUDIT_TABLE = f"{CATALOG}.{MONITORING_SCHEMA}.pipeline_audit"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{MONITORING_SCHEMA}")

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
        RunID STRING,
        JobName STRING,
        TriggerType STRING,
        StartTime TIMESTAMP,
        EndTime TIMESTAMP,
        Status STRING,
        ErrorMessage STRING,
        CreatedTimestamp TIMESTAMP,
        UpdatedTimestamp TIMESTAMP
    )
    USING DELTA
    """
)

print(f"Audit table ready: {AUDIT_TABLE}")