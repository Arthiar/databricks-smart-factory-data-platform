from pyspark.sql import functions as F


RELATIONSHIPS = [
    ("fact_sales", "OrderDateKey", "dim_date", "DateKey"),
    ("fact_sales", "ProductKey", "dim_product", "ProductKey"),
    ("fact_sales", "CustomerKey", "dim_customer", "CustomerKey"),
    ("fact_purchase_order", "OrderDateKey", "dim_date", "DateKey"),
    ("fact_purchase_order", "ProductKey", "dim_product", "ProductKey"),
    ("fact_purchase_order", "SupplierKey", "dim_supplier", "SupplierKey"),
    ("fact_production", "StartDateKey", "dim_date", "DateKey"),
    ("fact_production", "ProductKey", "dim_product", "ProductKey"),
    (
        "fact_work_order_operation",
        "ProductKey",
        "dim_product",
        "ProductKey",
    ),
    (
        "fact_work_order_operation",
        "WorkCenterKey",
        "dim_work_center",
        "WorkCenterKey",
    ),
]


def test_foreign_keys_have_matching_dimension_rows(spark, gold_table):
    problems = []

    for fact_name, fact_key, dimension_name, dimension_key in RELATIONSHIPS:
        fact_keys = (
            spark.table(gold_table(fact_name))
            .select(fact_key)
            .filter(F.col(fact_key).isNotNull())
        )

        dimension_keys = (
            spark.table(gold_table(dimension_name))
            .select(F.col(dimension_key).alias("DimensionKey"))
            .distinct()
        )

        orphan_count = (
            fact_keys
            .join(
                dimension_keys,
                F.col(fact_key) == F.col("DimensionKey"),
                "left_anti",
            )
            .count()
        )

        if orphan_count > 0:
            problems.append(f"{fact_name}.{fact_key}: {orphan_count}")

    assert not problems, f"Fact rows without matching dimensions: {problems}"
