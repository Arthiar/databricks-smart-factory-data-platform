-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Dashboard - Data Checks
-- MAGIC
-- MAGIC Run these checks before creating dashboard datasets.

-- COMMAND ----------

-- 1. Gold table row counts

SELECT 'dim_date' AS TableName, COUNT(*) AS RowCount
FROM smart_factory_dev.gold.dim_date
UNION ALL
SELECT 'dim_product', COUNT(*)
FROM smart_factory_dev.gold.dim_product
UNION ALL
SELECT 'dim_customer', COUNT(*)
FROM smart_factory_dev.gold.dim_customer
UNION ALL
SELECT 'dim_supplier', COUNT(*)
FROM smart_factory_dev.gold.dim_supplier
UNION ALL
SELECT 'dim_work_center', COUNT(*)
FROM smart_factory_dev.gold.dim_work_center
UNION ALL
SELECT 'fact_sales', COUNT(*)
FROM smart_factory_dev.gold.fact_sales
UNION ALL
SELECT 'fact_purchase_order', COUNT(*)
FROM smart_factory_dev.gold.fact_purchase_order
UNION ALL
SELECT 'fact_production', COUNT(*)
FROM smart_factory_dev.gold.fact_production
UNION ALL
SELECT 'fact_work_order_operation', COUNT(*)
FROM smart_factory_dev.gold.fact_work_order_operation
ORDER BY TableName;

-- COMMAND ----------

-- 2. Gold table refresh timestamps

SELECT 'dim_product' AS TableName,
       MAX(_gold_processed_timestamp) AS LastProcessedTimestamp
FROM smart_factory_dev.gold.dim_product
UNION ALL
SELECT 'fact_sales', MAX(_gold_processed_timestamp)
FROM smart_factory_dev.gold.fact_sales
UNION ALL
SELECT 'fact_purchase_order', MAX(_gold_processed_timestamp)
FROM smart_factory_dev.gold.fact_purchase_order
UNION ALL
SELECT 'fact_production', MAX(_gold_processed_timestamp)
FROM smart_factory_dev.gold.fact_production
UNION ALL
SELECT 'fact_work_order_operation', MAX(_gold_processed_timestamp)
FROM smart_factory_dev.gold.fact_work_order_operation
ORDER BY TableName;

-- COMMAND ----------

-- 3. Available dashboard date ranges

SELECT
    'Sales' AS BusinessArea,
    MIN(d.FullDate) AS MinimumDate,
    MAX(d.FullDate) AS MaximumDate
FROM smart_factory_dev.gold.fact_sales f
INNER JOIN smart_factory_dev.gold.dim_date d
    ON f.OrderDateKey = d.DateKey
UNION ALL
SELECT
    'Procurement',
    MIN(d.FullDate),
    MAX(d.FullDate)
FROM smart_factory_dev.gold.fact_purchase_order f
INNER JOIN smart_factory_dev.gold.dim_date d
    ON f.OrderDateKey = d.DateKey
UNION ALL
SELECT
    'Production',
    MIN(d.FullDate),
    MAX(d.FullDate)
FROM smart_factory_dev.gold.fact_production f
INNER JOIN smart_factory_dev.gold.dim_date d
    ON f.StartDateKey = d.DateKey;