-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Dashboard Validation
-- MAGIC
-- MAGIC These queries verify the grains, dimension relationships and KPI totals used by the dashboard.

-- COMMAND ----------

-- 1. Duplicate fact-grain checks. Every DuplicateGroups value should be zero.

SELECT
    'fact_sales' AS TableName,
    COUNT(*) AS DuplicateGroups
FROM (
    SELECT SalesOrderID, SalesOrderDetailID
    FROM smart_factory_dev.gold.fact_sales
    GROUP BY SalesOrderID, SalesOrderDetailID
    HAVING COUNT(*) > 1
)
UNION ALL
SELECT
    'fact_purchase_order',
    COUNT(*)
FROM (
    SELECT PurchaseOrderID, PurchaseOrderDetailID
    FROM smart_factory_dev.gold.fact_purchase_order
    GROUP BY PurchaseOrderID, PurchaseOrderDetailID
    HAVING COUNT(*) > 1
)
UNION ALL
SELECT
    'fact_production',
    COUNT(*)
FROM (
    SELECT WorkOrderID
    FROM smart_factory_dev.gold.fact_production
    GROUP BY WorkOrderID
    HAVING COUNT(*) > 1
)
UNION ALL
SELECT
    'fact_work_order_operation',
    COUNT(*)
FROM (
    SELECT WorkOrderID, ProductID, OperationSequence
    FROM smart_factory_dev.gold.fact_work_order_operation
    GROUP BY WorkOrderID, ProductID, OperationSequence
    HAVING COUNT(*) > 1
)
ORDER BY TableName;

-- COMMAND ----------

-- 2. Fact-to-dimension relationship checks. Every OrphanRows value should be zero.

SELECT
    'fact_sales_to_dim_product' AS RelationshipName,
    COUNT(*) AS OrphanRows
FROM smart_factory_dev.gold.fact_sales f
LEFT JOIN smart_factory_dev.gold.dim_product d
    ON f.ProductKey = d.ProductKey
WHERE d.ProductKey IS NULL
UNION ALL
SELECT
    'fact_sales_to_dim_customer',
    COUNT(*)
FROM smart_factory_dev.gold.fact_sales f
LEFT JOIN smart_factory_dev.gold.dim_customer d
    ON f.CustomerKey = d.CustomerKey
WHERE d.CustomerKey IS NULL
UNION ALL
SELECT
    'fact_purchase_to_dim_supplier',
    COUNT(*)
FROM smart_factory_dev.gold.fact_purchase_order f
LEFT JOIN smart_factory_dev.gold.dim_supplier d
    ON f.SupplierKey = d.SupplierKey
WHERE d.SupplierKey IS NULL
UNION ALL
SELECT
    'fact_production_to_dim_product',
    COUNT(*)
FROM smart_factory_dev.gold.fact_production f
LEFT JOIN smart_factory_dev.gold.dim_product d
    ON f.ProductKey = d.ProductKey
WHERE d.ProductKey IS NULL
UNION ALL
SELECT
    'fact_operation_to_dim_work_center',
    COUNT(*)
FROM smart_factory_dev.gold.fact_work_order_operation f
LEFT JOIN smart_factory_dev.gold.dim_work_center d
    ON f.WorkCenterKey = d.WorkCenterKey
WHERE d.WorkCenterKey IS NULL
ORDER BY RelationshipName;

-- COMMAND ----------

-- 3. Sales KPI reconciliation. RevenueDifference should be zero.

WITH fact_total AS (
    SELECT ROUND(SUM(SalesAmount), 2) AS FactRevenue
    FROM smart_factory_dev.gold.fact_sales
),
kpi_total AS (
    SELECT ROUND(SUM(TotalRevenue), 2) AS KpiRevenue
    FROM smart_factory_dev.gold.kpi_daily_sales
)
SELECT
    FactRevenue,
    KpiRevenue,
    ROUND(FactRevenue - KpiRevenue, 2) AS RevenueDifference
FROM fact_total
CROSS JOIN kpi_total;

-- COMMAND ----------

-- 4. Procurement KPI reconciliation. SpendDifference should be zero.

WITH fact_total AS (
    SELECT ROUND(SUM(PurchaseAmount), 2) AS FactPurchaseSpend
    FROM smart_factory_dev.gold.fact_purchase_order
),
kpi_total AS (
    SELECT ROUND(SUM(PurchaseSpend), 2) AS KpiPurchaseSpend
    FROM smart_factory_dev.gold.kpi_supplier_performance
)
SELECT
    FactPurchaseSpend,
    KpiPurchaseSpend,
    ROUND(FactPurchaseSpend - KpiPurchaseSpend, 2) AS SpendDifference
FROM fact_total
CROSS JOIN kpi_total;

-- COMMAND ----------

-- 5. Product SCD Type 2 current-row check. InvalidProducts should be zero.

WITH product_current_rows AS (
    SELECT
        ProductID,
        SUM(CASE WHEN IsCurrent = true THEN 1 ELSE 0 END) AS CurrentRows
    FROM smart_factory_dev.gold.dim_product
    GROUP BY ProductID
)
SELECT COUNT(*) AS InvalidProducts
FROM product_current_rows
WHERE CurrentRows <> 1;