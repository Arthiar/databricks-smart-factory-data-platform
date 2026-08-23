from pyspark.sql import functions as F


TABLE_KEYS = {
    "dim_date": ["DateKey"],
    "dim_product": ["ProductKey"],
    "dim_customer": ["CustomerKey"],
    "dim_supplier": ["SupplierKey"],
    "dim_work_center": ["WorkCenterKey"],
    "fact_sales": ["SalesOrderID", "SalesOrderDetailID"],
    "fact_purchase_order": ["PurchaseOrderID", "PurchaseOrderDetailID"],
    "fact_production": ["WorkOrderID"],
    "fact_work_order_operation": [
        "WorkOrderID",
        "ProductID",
        "OperationSequence",
    ],
    "kpi_daily_sales": ["DateKey"],
    "kpi_product_sales": ["ProductKey"],
    "kpi_supplier_performance": ["SupplierKey"],
    "kpi_production_efficiency": ["ProductKey"],
    "kpi_operation_cost": ["WorkCenterKey"],
}


REQUIRED_COLUMNS = {
    "dim_date": ["DateKey", "FullDate", "MonthNumber", "YearNumber"],
    "dim_product": [
        "ProductKey",
        "ProductID",
        "ProductName",
        "ProductVersion",
        "EffectiveFrom",
        "EffectiveTo",
        "IsCurrent",
    ],
    "dim_customer": ["CustomerKey", "CustomerType"],
    "dim_supplier": ["SupplierKey", "SupplierName"],
    "dim_work_center": ["WorkCenterKey", "WorkCenterName"],
    "fact_sales": [
        "SalesOrderID",
        "SalesOrderDetailID",
        "OrderDateKey",
        "ProductKey",
        "CustomerKey",
        "OrderQty",
        "SalesAmount",
    ],
    "fact_purchase_order": [
        "PurchaseOrderID",
        "PurchaseOrderDetailID",
        "OrderDateKey",
        "ProductKey",
        "SupplierKey",
        "OrderQty",
        "PurchaseAmount",
    ],
    "fact_production": [
        "WorkOrderID",
        "ProductKey",
        "OrderQty",
        "StockedQty",
        "ScrappedQty",
        "YieldRate",
        "ScrapRate",
    ],
    "fact_work_order_operation": [
        "WorkOrderID",
        "ProductID",
        "OperationSequence",
        "ProductKey",
        "WorkCenterKey",
    ],
    "kpi_daily_sales": [
        "DateKey",
        "FullDate",
        "TotalRevenue",
        "TotalOrders",
        "UnitsSold",
        "AverageOrderValue",
    ],
    "kpi_product_sales": [
        "ProductKey",
        "ProductName",
        "TotalRevenue",
        "UnitsSold",
        "OrderCount",
    ],
    "kpi_supplier_performance": [
        "SupplierKey",
        "SupplierName",
        "PurchaseSpend",
        "RejectionRate",
    ],
    "kpi_production_efficiency": [
        "ProductKey",
        "ProductName",
        "ProductionYieldRate",
        "ScrapRate",
    ],
    "kpi_operation_cost": [
        "WorkCenterKey",
        "WorkCenterName",
        "PlannedCost",
        "ActualCost",
        "CostVariance",
    ],
}


def test_gold_table_exists(spark, gold_table):
    missing_tables = []

    for table_name in TABLE_KEYS:
        if not spark.catalog.tableExists(gold_table(table_name)):
            missing_tables.append(table_name)

    assert not missing_tables, f"Missing Gold tables: {missing_tables}"


def test_gold_tables_have_required_columns(spark, gold_table):
    problems = []

    for table_name, required_columns in REQUIRED_COLUMNS.items():
        actual_columns = spark.table(gold_table(table_name)).columns
        missing_columns = [
            column for column in required_columns
            if column not in actual_columns
        ]

        if missing_columns:
            problems.append(f"{table_name}: {missing_columns}")

    assert not problems, f"Missing required columns: {problems}"


def test_gold_tables_are_not_empty(spark, gold_table):
    empty_tables = []

    for table_name in TABLE_KEYS:
        if spark.table(gold_table(table_name)).limit(1).count() == 0:
            empty_tables.append(table_name)

    assert not empty_tables, f"Empty Gold tables: {empty_tables}"


def test_primary_keys_are_not_null(spark, gold_table):
    problems = []

    for table_name, key_columns in TABLE_KEYS.items():
        table_df = spark.table(gold_table(table_name))
        null_condition = F.lit(False)

        for key_column in key_columns:
            null_condition = null_condition | F.col(key_column).isNull()

        null_count = table_df.filter(null_condition).count()

        if null_count > 0:
            problems.append(f"{table_name}: {null_count}")

    assert not problems, f"Tables with null primary keys: {problems}"


def test_primary_keys_are_unique(spark, gold_table):
    problems = []

    for table_name, key_columns in TABLE_KEYS.items():
        duplicate_count = (
            spark.table(gold_table(table_name))
            .groupBy(*key_columns)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if duplicate_count > 0:
            problems.append(f"{table_name}: {duplicate_count}")

    assert not problems, f"Tables with duplicate primary keys: {problems}"
