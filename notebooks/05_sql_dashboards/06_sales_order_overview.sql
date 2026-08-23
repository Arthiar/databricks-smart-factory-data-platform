-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Simple Dashboard - Sales Order Overview
-- MAGIC
-- MAGIC A compact view of sales-order volume, value, status and delivery performance.

-- COMMAND ----------

-- DBTITLE 1,Cell 2
-- 1. Sales order KPI cards
-- Visualizations: four counters

SELECT
    COUNT(DISTINCT f.SalesOrderID) AS TotalSalesOrders,
    ROUND(SUM(f.SalesAmount), 2) AS TotalRevenue,
    SUM(f.OrderQty) AS UnitsSold,
    ROUND(
        SUM(f.SalesAmount) / NULLIF(COUNT(DISTINCT f.SalesOrderID), 0),
        2
    ) AS AverageOrderValue
FROM smart_factory_dev.gold.fact_sales f
INNER JOIN smart_factory_dev.gold.dim_date d
    ON f.OrderDateKey = d.DateKey
WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31';

-- COMMAND ----------

-- DBTITLE 1,Cell 3
-- 2. Sales order status
-- Visualization: donut chart

WITH sales_orders AS (
    SELECT DISTINCT
        f.SalesOrderID,
        f.OrderStatus
    FROM smart_factory_dev.gold.fact_sales f
    INNER JOIN smart_factory_dev.gold.dim_date d
        ON f.OrderDateKey = d.DateKey
    WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
)
SELECT
    CASE OrderStatus
        WHEN 1 THEN 'In Process'
        WHEN 2 THEN 'Approved'
        WHEN 3 THEN 'Backordered'
        WHEN 4 THEN 'Rejected'
        WHEN 5 THEN 'Shipped'
        WHEN 6 THEN 'Cancelled'
        ELSE 'Unknown'
    END AS OrderStatusName,
    COUNT(*) AS SalesOrders
FROM sales_orders
GROUP BY
    CASE OrderStatus
        WHEN 1 THEN 'In Process'
        WHEN 2 THEN 'Approved'
        WHEN 3 THEN 'Backordered'
        WHEN 4 THEN 'Rejected'
        WHEN 5 THEN 'Shipped'
        WHEN 6 THEN 'Cancelled'
        ELSE 'Unknown'
    END
ORDER BY SalesOrders DESC;

-- COMMAND ----------

-- DBTITLE 1,Cell 4
-- 3. Monthly order trend
-- Visualization: line and column chart

SELECT
    DATE_TRUNC('MONTH', d.FullDate) AS OrderMonth,
    COUNT(DISTINCT f.SalesOrderID) AS SalesOrders,
    ROUND(SUM(f.SalesAmount), 2) AS TotalRevenue,
    SUM(f.OrderQty) AS UnitsSold
FROM smart_factory_dev.gold.fact_sales f
INNER JOIN smart_factory_dev.gold.dim_date d
    ON f.OrderDateKey = d.DateKey
WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
GROUP BY DATE_TRUNC('MONTH', d.FullDate)
ORDER BY OrderMonth;

-- COMMAND ----------

-- DBTITLE 1,Cell 5
-- 4. Late shipment KPI
-- Visualization: counter

WITH sales_orders AS (
    SELECT
        f.SalesOrderID,
        MAX(f.DueDateKey) AS DueDateKey,
        MAX(f.ShipDateKey) AS ShipDateKey
    FROM smart_factory_dev.gold.fact_sales f
    INNER JOIN smart_factory_dev.gold.dim_date d
        ON f.OrderDateKey = d.DateKey
    WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
    GROUP BY f.SalesOrderID
)
SELECT
    COUNT(CASE WHEN ShipDateKey IS NOT NULL THEN 1 END) AS ShippedOrders,
    SUM(CASE WHEN ShipDateKey > DueDateKey THEN 1 ELSE 0 END) AS LateOrders,
    ROUND(
        SUM(CASE WHEN ShipDateKey > DueDateKey THEN 1 ELSE 0 END)
        / NULLIF(COUNT(CASE WHEN ShipDateKey IS NOT NULL THEN 1 END), 0) * 100,
        2
    ) AS LateShipmentRatePercent
FROM sales_orders;