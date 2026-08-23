-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Simple Dashboard - Product Overview
-- MAGIC
-- MAGIC A compact product dashboard using the current SCD Type 2 product records.

-- COMMAND ----------

-- 1. Product KPI cards
-- Visualizations: four counters

SELECT
    COUNT(DISTINCT ProductID) AS TotalProducts,
    SUM(CASE WHEN FinishedGoodsFlag = true THEN 1 ELSE 0 END) AS FinishedGoodsProducts,
    ROUND(AVG(ListPrice), 2) AS AverageListPrice,
    ROUND(
        (SUM(ListPrice) - SUM(StandardCost))
        / NULLIF(SUM(ListPrice), 0) * 100,
        2
    ) AS EstimatedMarginPercent
FROM smart_factory_dev.gold.dim_product
WHERE IsCurrent = true;

-- COMMAND ----------

-- 2. Product portfolio by category
-- Visualization: horizontal bar chart

SELECT
    COALESCE(CategoryName, 'Uncategorized') AS CategoryName,
    COUNT(DISTINCT ProductID) AS TotalProducts,
    SUM(CASE WHEN FinishedGoodsFlag = true THEN 1 ELSE 0 END) AS FinishedGoodsProducts,
    ROUND(AVG(ListPrice), 2) AS AverageListPrice,
    ROUND(AVG(StandardCost), 2) AS AverageStandardCost
FROM smart_factory_dev.gold.dim_product
WHERE IsCurrent = true
GROUP BY COALESCE(CategoryName, 'Uncategorized')
ORDER BY TotalProducts DESC;

-- COMMAND ----------

-- DBTITLE 1,Cell 4
-- 3. Top products by sales
-- Visualization: bar chart

SELECT
    p.ProductName,
    COALESCE(p.CategoryName, 'Uncategorized') AS CategoryName,
    ROUND(SUM(f.SalesAmount), 2) AS ProductRevenue,
    SUM(f.OrderQty) AS UnitsSold,
    COUNT(DISTINCT f.SalesOrderID) AS SalesOrders
FROM smart_factory_dev.gold.fact_sales f
INNER JOIN smart_factory_dev.gold.dim_date d
    ON f.OrderDateKey = d.DateKey
LEFT JOIN smart_factory_dev.gold.dim_product p
    ON f.ProductKey = p.ProductKey
WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
GROUP BY p.ProductName, COALESCE(p.CategoryName, 'Uncategorized')
ORDER BY ProductRevenue DESC
LIMIT 15;

-- COMMAND ----------

-- DBTITLE 1,Cell 5
-- 4. Current products with low or no sales
-- Visualization: table

WITH product_sales AS (
    SELECT
        p.ProductID,
        ROUND(SUM(f.SalesAmount), 2) AS ProductRevenue,
        SUM(f.OrderQty) AS UnitsSold
    FROM smart_factory_dev.gold.fact_sales f
    INNER JOIN smart_factory_dev.gold.dim_date d
        ON f.OrderDateKey = d.DateKey
    INNER JOIN smart_factory_dev.gold.dim_product p
        ON f.ProductKey = p.ProductKey
    WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
    GROUP BY p.ProductID
)
SELECT
    p.ProductID,
    p.ProductNumber,
    p.ProductName,
    COALESCE(p.CategoryName, 'Uncategorized') AS CategoryName,
    p.ListPrice,
    COALESCE(s.ProductRevenue, 0) AS ProductRevenue,
    COALESCE(s.UnitsSold, 0) AS UnitsSold
FROM smart_factory_dev.gold.dim_product p
LEFT JOIN product_sales s
    ON p.ProductID = s.ProductID
WHERE p.IsCurrent = true
ORDER BY ProductRevenue ASC, p.ProductName
LIMIT 20;