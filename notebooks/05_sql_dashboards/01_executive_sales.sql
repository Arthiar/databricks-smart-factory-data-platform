-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Dashboard Page 1 - Executive Sales
-- MAGIC
-- MAGIC Uses the full AdventureWorks date range. Change the two DATE values if needed.

-- COMMAND ----------

-- 1. KPI cards
-- Visualizations: four counters

SELECT
    ROUND(SUM(f.SalesAmount), 2) AS TotalRevenue,
    COUNT(DISTINCT SalesOrderID) AS TotalOrders,
    SUM(f.OrderQty) AS UnitsSold,
    ROUND(
        SUM(f.SalesAmount) / NULLIF(COUNT(DISTINCT SalesOrderID), 0),
        2
    ) AS AverageOrderValue
FROM smart_factory_dev.gold.fact_sales f
INNER JOIN smart_factory_dev.gold.dim_date d
    ON f.OrderDateKey = d.DateKey
WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31';

-- COMMAND ----------

-- 2. Monthly revenue growth
-- Visualization: combination chart or table

WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('MONTH', d.FullDate) AS SalesMonth,
        ROUND(SUM(f.SalesAmount), 2) AS TotalRevenue,
        COUNT(DISTINCT SalesOrderID) AS TotalOrders
    FROM smart_factory_dev.gold.fact_sales f
    INNER JOIN smart_factory_dev.gold.dim_date d
        ON f.OrderDateKey = d.DateKey
    WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
    GROUP BY DATE_TRUNC('MONTH', d.FullDate)
),
monthly_comparison AS (
    SELECT
        SalesMonth,
        TotalRevenue,
        TotalOrders,
        LAG(TotalRevenue) OVER (ORDER BY SalesMonth) AS PreviousMonthRevenue
    FROM monthly_sales
)
SELECT
    SalesMonth,
    TotalRevenue,
    TotalOrders,
    PreviousMonthRevenue,
    ROUND(
        (TotalRevenue - PreviousMonthRevenue)
        / NULLIF(PreviousMonthRevenue, 0) * 100,
        2
    ) AS MonthOverMonthGrowthPercent
FROM monthly_comparison
ORDER BY SalesMonth;

-- COMMAND ----------

-- 3. Monthly sales trend
-- Visualization: line chart

SELECT
    DATE_TRUNC('MONTH', d.FullDate) AS SalesMonth,
    ROUND(SUM(f.SalesAmount), 2) AS TotalRevenue,
    COUNT(DISTINCT SalesOrderID) AS TotalOrders,
    SUM(f.OrderQty) AS UnitsSold
FROM smart_factory_dev.gold.fact_sales f
INNER JOIN smart_factory_dev.gold.dim_date d
    ON f.OrderDateKey = d.DateKey
WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
GROUP BY DATE_TRUNC('MONTH', d.FullDate)
ORDER BY SalesMonth;

-- COMMAND ----------

-- 4. Revenue contribution by product category
-- Visualization: horizontal bar chart

WITH category_sales AS (
    SELECT
        COALESCE(p.CategoryName, 'Uncategorized') AS CategoryName,
        ROUND(SUM(f.SalesAmount), 2) AS CategoryRevenue
    FROM smart_factory_dev.gold.fact_sales f
    INNER JOIN smart_factory_dev.gold.dim_date d
        ON f.OrderDateKey = d.DateKey
    LEFT JOIN smart_factory_dev.gold.dim_product p
        ON f.ProductKey = p.ProductKey
    WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
    GROUP BY COALESCE(p.CategoryName, 'Uncategorized')
)
SELECT
    CategoryName,
    CategoryRevenue,
    ROUND(
        CategoryRevenue / NULLIF(SUM(CategoryRevenue) OVER (), 0) * 100,
        2
    ) AS RevenueContributionPercent
FROM category_sales
ORDER BY CategoryRevenue DESC;

-- COMMAND ----------

-- 5. Top 10 products by revenue
-- Visualization: bar chart

SELECT
    p.ProductName,
    COALESCE(p.CategoryName, 'Uncategorized') AS CategoryName,
    ROUND(SUM(f.SalesAmount), 2) AS TotalRevenue,
    SUM(f.OrderQty) AS UnitsSold,
    COUNT(DISTINCT SalesOrderID) AS TotalOrders
FROM smart_factory_dev.gold.fact_sales f
INNER JOIN smart_factory_dev.gold.dim_date d
    ON f.OrderDateKey = d.DateKey
LEFT JOIN smart_factory_dev.gold.dim_product p
    ON f.ProductKey = p.ProductKey
WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
GROUP BY p.ProductName, COALESCE(p.CategoryName, 'Uncategorized')
ORDER BY TotalRevenue DESC
LIMIT 10;

-- COMMAND ----------

-- 6. Online versus offline performance
-- Visualization: grouped bar chart

SELECT
    CASE
        WHEN f.OnlineOrderFlag = true THEN 'Online'
        ELSE 'Offline'
    END AS SalesChannel,
    ROUND(SUM(f.SalesAmount), 2) AS TotalRevenue,
    COUNT(DISTINCT SalesOrderID) AS TotalOrders,
    SUM(f.OrderQty) AS UnitsSold,
    ROUND(
        SUM(f.SalesAmount) / NULLIF(COUNT(DISTINCT SalesOrderID), 0),
        2
    ) AS AverageOrderValue
FROM smart_factory_dev.gold.fact_sales f
INNER JOIN smart_factory_dev.gold.dim_date d
    ON f.OrderDateKey = d.DateKey
WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
GROUP BY
    CASE
        WHEN f.OnlineOrderFlag = true THEN 'Online'
        ELSE 'Offline'
    END
ORDER BY TotalRevenue DESC;

-- COMMAND ----------

-- 7. Late shipment KPI
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
),
shipment_status AS (
    SELECT
        SalesOrderID,
        CASE
            WHEN ShipDateKey IS NULL THEN 'Not Shipped'
            WHEN ShipDateKey > DueDateKey THEN 'Late'
            ELSE 'On Time'
        END AS ShipmentStatus
    FROM sales_orders
)
SELECT
    COUNT(*) AS ShippedOrders,
    SUM(CASE WHEN ShipmentStatus = 'Late' THEN 1 ELSE 0 END) AS LateOrders,
    ROUND(
        SUM(CASE WHEN ShipmentStatus = 'Late' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0) * 100,
        2
    ) AS LateShipmentRatePercent
FROM shipment_status
WHERE ShipmentStatus IN ('Late', 'On Time');

-- COMMAND ----------

-- 8. Shipment status breakdown
-- Visualization: donut chart

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
    CASE
        WHEN ShipDateKey IS NULL THEN 'Not Shipped'
        WHEN ShipDateKey > DueDateKey THEN 'Late'
        ELSE 'On Time'
    END AS ShipmentStatus,
    COUNT(*) AS Orders
FROM sales_orders
GROUP BY
    CASE
        WHEN ShipDateKey IS NULL THEN 'Not Shipped'
        WHEN ShipDateKey > DueDateKey THEN 'Late'
        ELSE 'On Time'
    END
ORDER BY Orders DESC;

-- COMMAND ----------

-- 9. Customer type performance
-- Visualization: bar chart

SELECT
    c.CustomerType,
    ROUND(SUM(f.SalesAmount), 2) AS TotalRevenue,
    COUNT(DISTINCT SalesOrderID) AS TotalOrders,
    SUM(f.OrderQty) AS UnitsSold,
    ROUND(
        SUM(f.SalesAmount) / NULLIF(COUNT(DISTINCT SalesOrderID), 0),
        2
    ) AS AverageOrderValue
FROM smart_factory_dev.gold.fact_sales f
INNER JOIN smart_factory_dev.gold.dim_date d
    ON f.OrderDateKey = d.DateKey
LEFT JOIN smart_factory_dev.gold.dim_customer c
    ON f.CustomerKey = c.CustomerKey
WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
GROUP BY c.CustomerType
ORDER BY TotalRevenue DESC;