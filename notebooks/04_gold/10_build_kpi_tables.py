# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - Business KPI Tables
# MAGIC
# MAGIC Builds small reporting tables from the Gold facts and dimensions.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

def save_kpi(dataframe, table_name):
    target_table = gold_table(table_name)
    (
        dataframe
        .withColumn("_gold_processed_timestamp", F.current_timestamp())
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
    print(f"{table_name}: {spark.table(target_table).count():,} rows")


# COMMAND ----------
# Daily sales KPIs

sales_df = spark.table(gold_table("fact_sales"))
date_df = spark.table(gold_table("dim_date")).select("DateKey", "FullDate")

kpi_daily_sales_df = (
    sales_df
    .groupBy("OrderDateKey")
    .agg(
        F.round(F.sum("SalesAmount"), 2).alias("TotalRevenue"),
        F.countDistinct("SalesOrderID").alias("TotalOrders"),
        F.sum("OrderQty").alias("UnitsSold"),
    )
    .withColumn(
        "AverageOrderValue",
        F.when(
            F.col("TotalOrders") > 0,
            F.round(F.col("TotalRevenue") / F.col("TotalOrders"), 2),
        ).otherwise(F.lit(0.0)),
    )
    .join(date_df, F.col("OrderDateKey") == F.col("DateKey"), "left")
    .select(
        "DateKey",
        "FullDate",
        "TotalRevenue",
        "TotalOrders",
        "UnitsSold",
        "AverageOrderValue",
    )
)

save_kpi(kpi_daily_sales_df, "kpi_daily_sales")

# COMMAND ----------
# Product sales KPIs

product_df = spark.table(gold_table("dim_product")).select(
    "ProductKey", "ProductName", "CategoryName", "SubcategoryName"
)

kpi_product_sales_df = (
    sales_df
    .groupBy("ProductKey")
    .agg(
        F.round(F.sum("SalesAmount"), 2).alias("TotalRevenue"),
        F.sum("OrderQty").alias("UnitsSold"),
        F.countDistinct("SalesOrderID").alias("OrderCount"),
    )
    .join(product_df, "ProductKey", "left")
    .select(
        "ProductKey",
        "ProductName",
        "CategoryName",
        "SubcategoryName",
        "TotalRevenue",
        "UnitsSold",
        "OrderCount",
    )
)

save_kpi(kpi_product_sales_df, "kpi_product_sales")

# COMMAND ----------
# Supplier performance KPIs

purchase_df = spark.table(gold_table("fact_purchase_order"))
supplier_df = spark.table(gold_table("dim_supplier")).select(
    "SupplierKey", "SupplierName"
)

kpi_supplier_performance_df = (
    purchase_df
    .groupBy("SupplierKey")
    .agg(
        F.round(F.sum("PurchaseAmount"), 2).alias("PurchaseSpend"),
        F.sum("OrderQty").alias("OrderedQty"),
        F.sum("ReceivedQty").alias("ReceivedQty"),
        F.sum("RejectedQty").alias("RejectedQty"),
    )
    .withColumn(
        "RejectionRate",
        F.when(
            F.col("ReceivedQty") > 0,
            F.round(F.col("RejectedQty") / F.col("ReceivedQty"), 4),
        ).otherwise(F.lit(0.0)),
    )
    .join(supplier_df, "SupplierKey", "left")
    .select(
        "SupplierKey",
        "SupplierName",
        "PurchaseSpend",
        "OrderedQty",
        "ReceivedQty",
        "RejectedQty",
        "RejectionRate",
    )
)

save_kpi(kpi_supplier_performance_df, "kpi_supplier_performance")

# COMMAND ----------
# Production efficiency KPIs

production_df = spark.table(gold_table("fact_production"))

kpi_production_efficiency_df = (
    production_df
    .groupBy("ProductKey")
    .agg(
        F.countDistinct("WorkOrderID").alias("WorkOrders"),
        F.sum("OrderQty").alias("PlannedQty"),
        F.sum("StockedQty").alias("StockedQty"),
        F.sum("ScrappedQty").alias("ScrappedQty"),
    )
    .withColumn(
        "ProductionYieldRate",
        F.when(
            F.col("PlannedQty") > 0,
            F.round(F.col("StockedQty") / F.col("PlannedQty"), 4),
        ).otherwise(F.lit(0.0)),
    )
    .withColumn(
        "ScrapRate",
        F.when(
            F.col("PlannedQty") > 0,
            F.round(F.col("ScrappedQty") / F.col("PlannedQty"), 4),
        ).otherwise(F.lit(0.0)),
    )
    .join(product_df.select("ProductKey", "ProductName"), "ProductKey", "left")
    .select(
        "ProductKey",
        "ProductName",
        "WorkOrders",
        "PlannedQty",
        "StockedQty",
        "ScrappedQty",
        "ProductionYieldRate",
        "ScrapRate",
    )
)

save_kpi(kpi_production_efficiency_df, "kpi_production_efficiency")

# COMMAND ----------
# Work-center cost KPIs

operation_df = spark.table(gold_table("fact_work_order_operation"))
work_center_df = spark.table(gold_table("dim_work_center")).select(
    "WorkCenterKey", "WorkCenterName"
)

kpi_operation_cost_df = (
    operation_df
    .groupBy("WorkCenterKey")
    .agg(
        F.count("OperationSequence").alias("Operations"),
        F.round(F.sum("PlannedCost"), 2).alias("PlannedCost"),
        F.round(F.sum("ActualCost"), 2).alias("ActualCost"),
        F.round(F.sum("CostVariance"), 2).alias("CostVariance"),
        F.round(F.avg("ActualResourceHours"), 2).alias("AverageResourceHours"),
    )
    .join(work_center_df, "WorkCenterKey", "left")
    .select(
        "WorkCenterKey",
        "WorkCenterName",
        "Operations",
        "PlannedCost",
        "ActualCost",
        "CostVariance",
        "AverageResourceHours",
    )
)

save_kpi(kpi_operation_cost_df, "kpi_operation_cost")

# COMMAND ----------

display(spark.table(gold_table("kpi_daily_sales")).orderBy(F.col("FullDate").desc()).limit(20))
