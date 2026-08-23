-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Dashboard - Facts and Figures
-- MAGIC
-- MAGIC A short landing page with the most important numbers across the platform.

-- COMMAND ----------

-- DBTITLE 1,Cell 2
-- 1. Company-wide facts and figures
-- Visualizations: counters

WITH sales AS (
    SELECT
        ROUND(SUM(f.SalesAmount), 2) AS TotalRevenue,
        COUNT(DISTINCT f.SalesOrderID) AS TotalSalesOrders,
        SUM(f.OrderQty) AS UnitsSold
    FROM smart_factory_dev.gold.fact_sales f
    INNER JOIN smart_factory_dev.gold.dim_date d
        ON f.OrderDateKey = d.DateKey
    WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
),
customers AS (
    SELECT COUNT(*) AS TotalCustomers
    FROM smart_factory_dev.gold.dim_customer
),
products AS (
    SELECT COUNT(DISTINCT ProductID) AS TotalProducts
    FROM smart_factory_dev.gold.dim_product
    WHERE IsCurrent = true
),
vendors AS (
    SELECT COUNT(*) AS TotalVendors
    FROM smart_factory_dev.gold.dim_supplier
),
purchasing AS (
    SELECT
        ROUND(SUM(f.PurchaseAmount), 2) AS PurchaseSpend,
        COUNT(DISTINCT f.PurchaseOrderID) AS TotalPurchaseOrders
    FROM smart_factory_dev.gold.fact_purchase_order f
    INNER JOIN smart_factory_dev.gold.dim_date d
        ON f.OrderDateKey = d.DateKey
    WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
),
production AS (
    SELECT
        COUNT(DISTINCT f.WorkOrderID) AS TotalWorkOrders,
        ROUND(
            SUM(f.StockedQty) / NULLIF(SUM(f.OrderQty), 0) * 100,
            2
        ) AS ProductionYieldPercent,
        ROUND(
            SUM(f.ScrappedQty) / NULLIF(SUM(f.OrderQty), 0) * 100,
            2
        ) AS ScrapRatePercent
    FROM smart_factory_dev.gold.fact_production f
    INNER JOIN smart_factory_dev.gold.dim_date d
        ON f.StartDateKey = d.DateKey
    WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
)
SELECT
    sales.TotalRevenue,
    sales.TotalSalesOrders,
    sales.UnitsSold,
    customers.TotalCustomers,
    products.TotalProducts,
    vendors.TotalVendors,
    purchasing.PurchaseSpend,
    purchasing.TotalPurchaseOrders,
    production.TotalWorkOrders,
    production.ProductionYieldPercent,
    production.ScrapRatePercent
FROM sales
CROSS JOIN customers
CROSS JOIN products
CROSS JOIN vendors
CROSS JOIN purchasing
CROSS JOIN production;

-- COMMAND ----------

-- 2. Financial figures by business area
-- Visualization: bar chart

SELECT
    'Sales Revenue' AS FigureName,
    ROUND(SUM(SalesAmount), 2) AS FigureValue
FROM smart_factory_dev.gold.fact_sales
UNION ALL
SELECT
    'Purchase Spend',
    ROUND(SUM(PurchaseAmount), 2)
FROM smart_factory_dev.gold.fact_purchase_order
UNION ALL
SELECT
    'Planned Operation Cost',
    ROUND(SUM(PlannedCost), 2)
FROM smart_factory_dev.gold.fact_work_order_operation
UNION ALL
SELECT
    'Actual Operation Cost',
    ROUND(SUM(ActualCost), 2)
FROM smart_factory_dev.gold.fact_work_order_operation
ORDER BY FigureValue DESC;

-- COMMAND ----------

-- 3. Platform record volumes
-- Visualization: compact table

SELECT 'Sales Lines' AS DatasetName, COUNT(*) AS Records
FROM smart_factory_dev.gold.fact_sales
UNION ALL
SELECT 'Purchase Lines', COUNT(*)
FROM smart_factory_dev.gold.fact_purchase_order
UNION ALL
SELECT 'Work Orders', COUNT(*)
FROM smart_factory_dev.gold.fact_production
UNION ALL
SELECT 'Work Order Operations', COUNT(*)
FROM smart_factory_dev.gold.fact_work_order_operation
ORDER BY Records DESC;