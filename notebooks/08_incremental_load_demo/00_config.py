# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental Demo Configuration
# MAGIC
# MAGIC Shared table names and one small control table for the incremental-load demo.

# COMMAND ----------

CATALOG = "smart_factory_dev"
MONITORING_SCHEMA = "monitoring"

BRONZE_CUSTOMER = f"{CATALOG}.bronze.customer"
SILVER_CUSTOMER = f"{CATALOG}.silver.customer"
GOLD_CUSTOMER = f"{CATALOG}.gold.dim_customer"

CONTROL_TABLE = f"{CATALOG}.{MONITORING_SCHEMA}.incremental_demo_control"

CUSTOMER_SOURCE_PATH = (
    "/Volumes/smart_factory_dev/raw/landing_files/"
    "batch/erp/sales/customer"
)

DEMO_ID = "customer_999999"
DEMO_CUSTOMER_ID = 999999
DEMO_FILE = f"{CUSTOMER_SOURCE_PATH}/incremental_customer_999999.csv"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{MONITORING_SCHEMA}")

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_TABLE} (
        DemoID STRING,
        CustomerID INT,
        SourceFile STRING,
        BronzeBefore BIGINT,
        SilverBefore BIGINT,
        GoldBefore BIGINT,
        BronzeAfter BIGINT,
        SilverAfter BIGINT,
        GoldAfter BIGINT,
        PreparedTimestamp TIMESTAMP,
        VerifiedTimestamp TIMESTAMP,
        Status STRING
    )
    USING DELTA
    """
)

print(f"Incremental demo control table ready: {CONTROL_TABLE}")