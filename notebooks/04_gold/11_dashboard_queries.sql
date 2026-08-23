-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Databricks SQL Dashboard Queries
-- MAGIC
-- MAGIC Run each query in Databricks SQL and add its result to a dashboard.

-- COMMAND ----------
-- KPI cards: revenue, orders, units, and average order value

SELECT
    ROUND(SUM(TotalRevenue), 2) AS TotalRevenue,
    SUM(TotalOrders) AS TotalOrders,
    SUM(UnitsSold) AS UnitsSold,
    ROUND(SUM(TotalRevenue) / NULLIF(SUM(TotalOrders), 0), 2) AS AverageOrderValue
FROM smart_factory_dev.gold.kpi_daily_sales;

-- COMMAND ----------
-- Line chart: monthly sales trend

SELECT
    DATE_TRUNC('MONTH', FullDate) AS SalesMonth,
    ROUND(SUM(TotalRevenue), 2) AS Revenue,
    SUM(TotalOrders) AS Orders,
    SUM(UnitsSold) AS UnitsSold
FROM smart_factory_dev.gold.kpi_daily_sales
GROUP BY DATE_TRUNC('MONTH', FullDate)
ORDER BY SalesMonth;

-- COMMAND ----------
-- Bar chart: top 10 products by revenue

SELECT
    ProductName,
    CategoryName,
    TotalRevenue,
    UnitsSold,
    OrderCount
FROM smart_factory_dev.gold.kpi_product_sales
ORDER BY TotalRevenue DESC
LIMIT 10;

-- COMMAND ----------
-- Table: supplier performance

SELECT
    SupplierName,
    PurchaseSpend,
    OrderedQty,
    ReceivedQty,
    RejectedQty,
    ROUND(RejectionRate * 100, 2) AS RejectionRatePercent
FROM smart_factory_dev.gold.kpi_supplier_performance
ORDER BY PurchaseSpend DESC;

-- COMMAND ----------
-- Bar chart: production efficiency by product

SELECT
    ProductName,
    WorkOrders,
    PlannedQty,
    StockedQty,
    ScrappedQty,
    ROUND(ProductionYieldRate * 100, 2) AS YieldPercent,
    ROUND(ScrapRate * 100, 2) AS ScrapPercent
FROM smart_factory_dev.gold.kpi_production_efficiency
WHERE PlannedQty > 0
ORDER BY YieldPercent ASC
LIMIT 20;

-- COMMAND ----------
-- Bar chart: work-center cost variance

SELECT
    WorkCenterName,
    Operations,
    PlannedCost,
    ActualCost,
    CostVariance,
    AverageResourceHours
FROM smart_factory_dev.gold.kpi_operation_cost
ORDER BY ABS(CostVariance) DESC;
