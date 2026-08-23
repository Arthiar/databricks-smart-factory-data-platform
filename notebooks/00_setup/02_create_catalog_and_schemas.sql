-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Smart Factory Data Platform
-- MAGIC #### Development Catalog and Schemas

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS smart_factory_dev
MANAGED LOCATION 'abfss://smart-factory-dev@stsfactorydev2026.dfs.core.windows.net/managed/'
COMMENT 'Development catalog for the Smart Factory Data Platform';

USE CATALOG smart_factory_dev;

CREATE SCHEMA IF NOT EXISTS raw
COMMENT 'Governed access to landing and source files before table ingestion';

CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw source-aligned Delta tables with ingestion metadata';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Cleaned, validated, deduplicated and conformed datasets';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Business-ready dimensional models, fact tables and KPI datasets';

CREATE SCHEMA IF NOT EXISTS quarantine
COMMENT 'Rejected records that fail technical or business data quality rules';

CREATE SCHEMA IF NOT EXISTS monitoring
COMMENT 'Pipeline audit logs, data quality results, execution metrics and observability data';