# Databricks notebook source
# MAGIC %md
# MAGIC ##Smart Factory Data Platform
# MAGIC ###Unity Catalog External Locations - Development

# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC %sql
# MAGIC DROP EXTERNAL LOCATION IF EXISTS ext_smart_factory_dev;
# MAGIC
# MAGIC CREATE EXTERNAL LOCATION ext_smart_factory_dev
# MAGIC URL 'abfss://smart-factory-dev@stsfactorydev2026.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `sc-smart-factory-dev`)
# MAGIC COMMENT 'Root external location for the Smart Factory development data platform';