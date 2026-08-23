-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Dashboard Page 3 - Production and Operations
-- MAGIC
-- MAGIC Uses the full AdventureWorks date range. Change the two DATE values if needed.

-- COMMAND ----------

-- DBTITLE 1,Cell 2
-- 1. Production KPI cards
-- Visualizations: six counters

SELECT
    COUNT(DISTINCT f.WorkOrderID) AS WorkOrders,
    SUM(f.OrderQty) AS PlannedQuantity,
    SUM(f.StockedQty) AS StockedQuantity,
    SUM(f.ScrappedQty) AS ScrappedQuantity,
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
WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31';

-- COMMAND ----------

-- DBTITLE 1,Cell 3
-- 2. Monthly production trend
-- Visualization: line and column combination chart

SELECT
    DATE_TRUNC('MONTH', d.FullDate) AS ProductionMonth,
    COUNT(DISTINCT f.WorkOrderID) AS WorkOrders,
    SUM(f.OrderQty) AS PlannedQuantity,
    SUM(f.StockedQty) AS StockedQuantity,
    SUM(f.ScrappedQty) AS ScrappedQuantity
FROM smart_factory_dev.gold.fact_production f
INNER JOIN smart_factory_dev.gold.dim_date d
    ON f.StartDateKey = d.DateKey
WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
GROUP BY DATE_TRUNC('MONTH', d.FullDate)
ORDER BY ProductionMonth;

-- COMMAND ----------

-- DBTITLE 1,Cell 4
-- 3. Product production efficiency
-- Visualization: table with conditional formatting

SELECT
    p.ProductName,
    COALESCE(p.CategoryName, 'Uncategorized') AS CategoryName,
    COUNT(DISTINCT f.WorkOrderID) AS WorkOrders,
    SUM(f.OrderQty) AS PlannedQuantity,
    SUM(f.StockedQty) AS StockedQuantity,
    SUM(f.ScrappedQty) AS ScrappedQuantity,
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
LEFT JOIN smart_factory_dev.gold.dim_product p
    ON f.ProductKey = p.ProductKey
WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
GROUP BY p.ProductName, COALESCE(p.CategoryName, 'Uncategorized')
ORDER BY ProductionYieldPercent ASC, PlannedQuantity DESC;

-- COMMAND ----------

-- DBTITLE 1,Cell 5
-- 4. Planned versus actual work-center cost
-- Visualization: grouped bar chart

SELECT
    w.WorkCenterName,
    COUNT(*) AS Operations,
    ROUND(SUM(f.PlannedCost), 2) AS PlannedCost,
    ROUND(SUM(f.ActualCost), 2) AS ActualCost,
    ROUND(SUM(f.CostVariance), 2) AS CostVariance,
    ROUND(
        SUM(f.CostVariance) / NULLIF(SUM(f.PlannedCost), 0) * 100,
        2
    ) AS CostVariancePercent
FROM smart_factory_dev.gold.fact_work_order_operation f
INNER JOIN smart_factory_dev.gold.dim_date d
    ON f.ScheduledStartDateKey = d.DateKey
LEFT JOIN smart_factory_dev.gold.dim_work_center w
    ON f.WorkCenterKey = w.WorkCenterKey
WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
GROUP BY w.WorkCenterName
ORDER BY ABS(CostVariance) DESC;

-- COMMAND ----------

-- DBTITLE 1,Cell 6
-- 5. Highest operation cost variances
-- Visualization: table

SELECT
    f.WorkOrderID,
    f.OperationSequence,
    p.ProductName,
    w.WorkCenterName,
    f.PlannedCost,
    f.ActualCost,
    f.CostVariance,
    f.ActualResourceHours,
    f.ActualDurationHours
FROM smart_factory_dev.gold.fact_work_order_operation f
INNER JOIN smart_factory_dev.gold.dim_date d
    ON f.ScheduledStartDateKey = d.DateKey
LEFT JOIN smart_factory_dev.gold.dim_product p
    ON f.ProductKey = p.ProductKey
LEFT JOIN smart_factory_dev.gold.dim_work_center w
    ON f.WorkCenterKey = w.WorkCenterKey
WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
ORDER BY ABS(f.CostVariance) DESC
LIMIT 20;

-- COMMAND ----------

-- DBTITLE 1,Cell 7
-- 6. Work-center resource usage
-- Visualization: bubble chart or table

SELECT
    w.WorkCenterName,
    COUNT(*) AS Operations,
    ROUND(SUM(f.ActualResourceHours), 2) AS TotalResourceHours,
    ROUND(AVG(f.ActualResourceHours), 2) AS AverageResourceHours,
    ROUND(AVG(f.ActualDurationHours), 2) AS AverageDurationHours
FROM smart_factory_dev.gold.fact_work_order_operation f
INNER JOIN smart_factory_dev.gold.dim_date d
    ON f.ScheduledStartDateKey = d.DateKey
LEFT JOIN smart_factory_dev.gold.dim_work_center w
    ON f.WorkCenterKey = w.WorkCenterKey
WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
GROUP BY w.WorkCenterName
ORDER BY TotalResourceHours DESC;

-- COMMAND ----------

-- DBTITLE 1,Cell 8
-- 7. Operation schedule adherence
-- Visualization: counter

WITH operation_status AS (
    SELECT
        f.WorkOrderID,
        f.ProductID,
        f.OperationSequence,
        CASE
            WHEN f.ActualEndDateKey IS NULL THEN 'Not Completed'
            WHEN f.ActualEndDateKey <= f.ScheduledEndDateKey THEN 'On Schedule'
            ELSE 'Delayed'
        END AS ScheduleStatus
    FROM smart_factory_dev.gold.fact_work_order_operation f
    INNER JOIN smart_factory_dev.gold.dim_date d
        ON f.ScheduledStartDateKey = d.DateKey
    WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
)
SELECT
    COUNT(*) AS CompletedOperations,
    SUM(CASE WHEN ScheduleStatus = 'On Schedule' THEN 1 ELSE 0 END) AS OnScheduleOperations,
    ROUND(
        SUM(CASE WHEN ScheduleStatus = 'On Schedule' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0) * 100,
        2
    ) AS ScheduleAdherencePercent
FROM operation_status
WHERE ScheduleStatus IN ('On Schedule', 'Delayed');

-- COMMAND ----------

-- DBTITLE 1,Cell 9
-- 8. Operation schedule status
-- Visualization: donut chart

WITH operation_status AS (
    SELECT
        CASE
            WHEN f.ActualEndDateKey IS NULL THEN 'Not Completed'
            WHEN f.ActualEndDateKey <= f.ScheduledEndDateKey THEN 'On Schedule'
            ELSE 'Delayed'
        END AS ScheduleStatus
    FROM smart_factory_dev.gold.fact_work_order_operation f
    INNER JOIN smart_factory_dev.gold.dim_date d
        ON f.ScheduledStartDateKey = d.DateKey
    WHERE d.FullDate BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
)
SELECT
    ScheduleStatus,
    COUNT(*) AS Operations
FROM operation_status
GROUP BY ScheduleStatus
ORDER BY Operations DESC;