-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Dashboard Page 2 - Procurement and Suppliers
-- MAGIC
-- MAGIC Uses the full AdventureWorks date range. Change the two DATE values if needed.

-- COMMAND ----------

-- 1. Procurement KPI cards
-- Visualizations: seven counters

SELECT
    ROUND(SUM(f.PurchaseAmount), 2) AS PurchaseSpend,
    COUNT(DISTINCT PurchaseOrderID) AS PurchaseOrders,
    SUM(f.OrderQty) AS OrderedQuantity,
    ROUND(SUM(f.ReceivedQty), 2) AS ReceivedQuantity,
    ROUND(SUM(f.RejectedQty), 2) AS RejectedQuantity,
    ROUND(
        SUM(f.ReceivedQty) / NULLIF(SUM(f.OrderQty), 0) * 100,
        2
    ) AS ReceiptRatePercent,
    ROUND(
        SUM(f.RejectedQty) / NULLIF(SUM(f.ReceivedQty), 0) * 100,
        2
    ) AS RejectionRatePercent
FROM smart_factory_dev.gold.fact_purchase_order f
INNER JOIN smart_factory_dev.gold.dim_date d
    ON f.OrderDateKey = d.DateKey
WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31';

-- COMMAND ----------

-- 2. Monthly purchase-spend trend
-- Visualization: line chart

WITH monthly_purchasing AS (
    SELECT
        DATE_TRUNC('MONTH', d.FullDate) AS PurchaseMonth,
        ROUND(SUM(f.PurchaseAmount), 2) AS PurchaseSpend,
        COUNT(DISTINCT PurchaseOrderID) AS PurchaseOrders
    FROM smart_factory_dev.gold.fact_purchase_order f
    INNER JOIN smart_factory_dev.gold.dim_date d
        ON f.OrderDateKey = d.DateKey
    WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
    GROUP BY DATE_TRUNC('MONTH', d.FullDate)
)
SELECT
    PurchaseMonth,
    PurchaseSpend,
    PurchaseOrders,
    LAG(PurchaseSpend) OVER (ORDER BY PurchaseMonth) AS PreviousMonthSpend
FROM monthly_purchasing
ORDER BY PurchaseMonth;

-- COMMAND ----------

-- 3. Supplier spend and quality performance
-- Visualization: table with conditional formatting

SELECT
    s.SupplierName,
    ROUND(SUM(f.PurchaseAmount), 2) AS PurchaseSpend,
    COUNT(DISTINCT PurchaseOrderID) AS PurchaseOrders,
    ROUND(SUM(f.ReceivedQty), 2) AS ReceivedQuantity,
    ROUND(SUM(f.RejectedQty), 2) AS RejectedQuantity,
    ROUND(
        SUM(f.RejectedQty) / NULLIF(SUM(f.ReceivedQty), 0) * 100,
        2
    ) AS RejectionRatePercent
FROM smart_factory_dev.gold.fact_purchase_order f
INNER JOIN smart_factory_dev.gold.dim_date d
    ON f.OrderDateKey = d.DateKey
LEFT JOIN smart_factory_dev.gold.dim_supplier s
    ON f.SupplierKey = s.SupplierKey
WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
GROUP BY s.SupplierName
ORDER BY PurchaseSpend DESC;

-- COMMAND ----------

-- 4. Purchase-order delivery status
-- Visualization: donut chart

WITH purchase_orders AS (
    SELECT
        f.PurchaseOrderID,
        MIN(f.DueDateKey) AS EarliestDueDateKey,
        MAX(f.ShipDateKey) AS ShipDateKey
    FROM smart_factory_dev.gold.fact_purchase_order f
    INNER JOIN smart_factory_dev.gold.dim_date d
        ON f.OrderDateKey = d.DateKey
    WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
    GROUP BY f.PurchaseOrderID
),
delivery_status AS (
    SELECT
        PurchaseOrderID,
        CASE
            WHEN ShipDateKey IS NULL THEN 'Not Shipped'
            WHEN ShipDateKey > EarliestDueDateKey THEN 'Late'
            ELSE 'On Time'
        END AS DeliveryStatus
    FROM purchase_orders
)
SELECT
    DeliveryStatus,
    COUNT(*) AS PurchaseOrders,
    ROUND(
        COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0) * 100,
        2
    ) AS OrderSharePercent
FROM delivery_status
GROUP BY DeliveryStatus
ORDER BY PurchaseOrders DESC;

-- COMMAND ----------

-- 5. Most-purchased products
-- Visualization: horizontal bar chart

SELECT
    p.ProductName,
    COALESCE(p.CategoryName, 'Uncategorized') AS CategoryName,
    ROUND(SUM(f.PurchaseAmount), 2) AS PurchaseSpend,
    SUM(f.OrderQty) AS OrderedQuantity,
    ROUND(SUM(f.ReceivedQty), 2) AS ReceivedQuantity
FROM smart_factory_dev.gold.fact_purchase_order f
INNER JOIN smart_factory_dev.gold.dim_date d
    ON f.OrderDateKey = d.DateKey
LEFT JOIN smart_factory_dev.gold.dim_product p
    ON f.ProductKey = p.ProductKey
WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
GROUP BY p.ProductName, COALESCE(p.CategoryName, 'Uncategorized')
ORDER BY PurchaseSpend DESC
LIMIT 10;

-- COMMAND ----------

-- 6. Purchase status distribution
-- Visualization: column chart

WITH order_status AS (
    SELECT DISTINCT
        f.PurchaseOrderID,
        f.PurchaseStatus
    FROM smart_factory_dev.gold.fact_purchase_order f
    INNER JOIN smart_factory_dev.gold.dim_date d
        ON f.OrderDateKey = d.DateKey
    WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
)
SELECT
    CASE PurchaseStatus
        WHEN 1 THEN 'Pending'
        WHEN 2 THEN 'Approved'
        WHEN 3 THEN 'Rejected'
        WHEN 4 THEN 'Complete'
        ELSE 'Unknown'
    END AS PurchaseStatusName,
    COUNT(*) AS PurchaseOrders
FROM order_status
GROUP BY
    CASE PurchaseStatus
        WHEN 1 THEN 'Pending'
        WHEN 2 THEN 'Approved'
        WHEN 3 THEN 'Rejected'
        WHEN 4 THEN 'Complete'
        ELSE 'Unknown'
    END
ORDER BY PurchaseOrders DESC;