-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Simple Dashboard - Customer Overview
-- MAGIC
-- MAGIC The source contains customer IDs and account numbers, but not customer names.

-- COMMAND ----------

-- 1. Customer KPI cards
-- Visualizations: three counters

SELECT
    COUNT(*) AS TotalCustomers,
    SUM(CASE WHEN CustomerType = 'Individual' THEN 1 ELSE 0 END) AS IndividualCustomers,
    SUM(CASE WHEN CustomerType = 'Store' THEN 1 ELSE 0 END) AS StoreCustomers
FROM smart_factory_dev.gold.dim_customer;

-- COMMAND ----------

-- DBTITLE 1,Cell 3
-- 2. Customer type sales performance
-- Visualization: grouped bar chart

SELECT
    c.CustomerType,
    COUNT(DISTINCT c.CustomerKey) AS ActiveCustomers,
    COUNT(DISTINCT f.SalesOrderID) AS SalesOrders,
    ROUND(SUM(f.SalesAmount), 2) AS CustomerRevenue,
    ROUND(
        SUM(f.SalesAmount) / NULLIF(COUNT(DISTINCT f.SalesOrderID), 0),
        2
    ) AS AverageOrderValue
FROM smart_factory_dev.gold.fact_sales f
INNER JOIN smart_factory_dev.gold.dim_date d
    ON f.OrderDateKey = d.DateKey
LEFT JOIN smart_factory_dev.gold.dim_customer c
    ON f.CustomerKey = c.CustomerKey
WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
GROUP BY c.CustomerType
ORDER BY CustomerRevenue DESC;

-- COMMAND ----------

-- DBTITLE 1,Cell 4
-- 3. Top customer accounts by revenue
-- Visualization: table

SELECT
    c.CustomerKey,
    c.AccountNumber,
    c.CustomerType,
    c.TerritoryID,
    COUNT(DISTINCT f.SalesOrderID) AS SalesOrders,
    ROUND(SUM(f.SalesAmount), 2) AS CustomerRevenue,
    SUM(f.OrderQty) AS UnitsPurchased
FROM smart_factory_dev.gold.fact_sales f
INNER JOIN smart_factory_dev.gold.dim_date d
    ON f.OrderDateKey = d.DateKey
LEFT JOIN smart_factory_dev.gold.dim_customer c
    ON f.CustomerKey = c.CustomerKey
WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
GROUP BY
    c.CustomerKey,
    c.AccountNumber,
    c.CustomerType,
    c.TerritoryID
ORDER BY CustomerRevenue DESC
LIMIT 20;

-- COMMAND ----------

-- 4. Customers without sales orders
-- Visualization: counter and detail table

SELECT
    COUNT(*) AS CustomersWithoutOrders
FROM smart_factory_dev.gold.dim_customer c
LEFT JOIN smart_factory_dev.gold.fact_sales f
    ON c.CustomerKey = f.CustomerKey
WHERE f.CustomerKey IS NULL;