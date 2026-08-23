-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Simple Dashboard - Vendor Overview
-- MAGIC
-- MAGIC AdventureWorks calls these records vendors. The Gold dimension uses the name supplier.

-- COMMAND ----------

-- 1. Vendor KPI cards
-- Visualizations: three counters

SELECT
    COUNT(*) AS TotalVendors,
    SUM(CASE WHEN ActiveFlag = true THEN 1 ELSE 0 END) AS ActiveVendors,
    SUM(CASE WHEN PreferredSupplierStatus = true THEN 1 ELSE 0 END) AS PreferredVendors
FROM smart_factory_dev.gold.dim_supplier;

-- COMMAND ----------

-- 2. Vendors by credit rating
-- Visualization: column chart

SELECT
    CreditRating,
    COUNT(*) AS Vendors,
    SUM(CASE WHEN ActiveFlag = true THEN 1 ELSE 0 END) AS ActiveVendors
FROM smart_factory_dev.gold.dim_supplier
GROUP BY CreditRating
ORDER BY CreditRating;

-- COMMAND ----------

-- DBTITLE 1,Cell 4
-- 3. Vendor spend and quality
-- Visualization: table with conditional formatting

SELECT
    s.SupplierKey AS VendorKey,
    s.SupplierName AS VendorName,
    s.CreditRating,
    s.PreferredSupplierStatus,
    COUNT(DISTINCT f.PurchaseOrderID) AS PurchaseOrders,
    ROUND(SUM(f.PurchaseAmount), 2) AS VendorPurchaseSpend,
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
GROUP BY
    s.SupplierKey,
    s.SupplierName,
    s.CreditRating,
    s.PreferredSupplierStatus
ORDER BY VendorPurchaseSpend DESC;

-- COMMAND ----------

-- 4. Vendors without purchase orders
-- Visualization: counter

SELECT
    COUNT(*) AS VendorsWithoutPurchaseOrders
FROM smart_factory_dev.gold.dim_supplier s
LEFT JOIN smart_factory_dev.gold.fact_purchase_order f
    ON s.SupplierKey = f.SupplierKey
WHERE f.SupplierKey IS NULL;