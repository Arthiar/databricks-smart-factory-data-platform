-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Creation of Volumes

-- COMMAND ----------

USE CATALOG smart_factory_dev;
USE SCHEMA raw;

CREATE EXTERNAL VOLUME IF NOT EXISTS landing_files
LOCATION 'abfss://smart-factory-dev@stsfactorydev2026.dfs.core.windows.net/landing/'
COMMENT 'Governed landing area for incoming Smart Factory source files';

-- COMMAND ----------

SHOW VOLUMES IN smart_factory_dev.raw;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC base_path = "/Volumes/smart_factory_dev/raw/landing_files"
-- MAGIC
-- MAGIC folders = [
-- MAGIC     "batch/erp/plant",
-- MAGIC     "batch/erp/machine",
-- MAGIC     "batch/erp/product",
-- MAGIC     "batch/erp/material",
-- MAGIC     "batch/erp/supplier",
-- MAGIC     "batch/erp/customer",
-- MAGIC     "batch/erp/production_order",
-- MAGIC     "batch/erp/purchase_order",
-- MAGIC     "batch/erp/quality_inspection",
-- MAGIC     "batch/erp/maintenance_order",
-- MAGIC     "batch/erp/customer_shipment",
-- MAGIC     "reference",
-- MAGIC     "streaming/machine_telemetry"
-- MAGIC ]
-- MAGIC
-- MAGIC for folder in folders:
-- MAGIC     dbutils.fs.mkdirs(f"{base_path}/{folder}")
-- MAGIC
-- MAGIC print("Landing-zone folder structure created successfully.")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/Volumes/smart_factory_dev/raw/landing_files"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/Volumes/smart_factory_dev/raw/landing_files/batch/erp"))

-- COMMAND ----------

USE CATALOG smart_factory_dev;
USE SCHEMA raw;

CREATE EXTERNAL VOLUME IF NOT EXISTS checkpoint_files
LOCATION 'abfss://smart-factory-dev@stsfactorydev2026.dfs.core.windows.net/checkpoints/'
COMMENT 'Auto Loader and Structured Streaming checkpoint storage';

CREATE EXTERNAL VOLUME IF NOT EXISTS archive_files
LOCATION 'abfss://smart-factory-dev@stsfactorydev2026.dfs.core.windows.net/archive/'
COMMENT 'Archive storage for successfully processed source files';

CREATE EXTERNAL VOLUME IF NOT EXISTS quarantine_files
LOCATION 'abfss://smart-factory-dev@stsfactorydev2026.dfs.core.windows.net/quarantine/'
COMMENT 'File-level quarantine area for malformed or rejected source data';

-- COMMAND ----------

SHOW VOLUMES IN smart_factory_dev.raw;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC old_folders = [
-- MAGIC     "customer",
-- MAGIC     "customer_shipment",
-- MAGIC     "machine",
-- MAGIC     "maintenance_order",
-- MAGIC     "material",
-- MAGIC     "plant",
-- MAGIC     "product",
-- MAGIC     "production_order",
-- MAGIC     "purchase_order",
-- MAGIC     "quality_inspection",
-- MAGIC     "supplier",
-- MAGIC ]
-- MAGIC
-- MAGIC base_path = "/Volumes/smart_factory_dev/raw/landing_files/batch/erp"
-- MAGIC
-- MAGIC for folder in old_folders:
-- MAGIC     path = f"{base_path}/{folder}"
-- MAGIC     dbutils.fs.rm(path, recurse=True)
-- MAGIC     print(f"Deleted: {path}")

-- COMMAND ----------

SHOW EXTERNAL LOCATIONS;

-- COMMAND ----------

DESCRIBE EXTERNAL LOCATION ext_smart_factory_dev;