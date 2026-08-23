from pyspark.sql import functions as F


def test_sales_values_are_not_negative(spark, gold_table):
    sales_df = spark.table(gold_table("fact_sales"))

    invalid_count = sales_df.filter(
        (F.col("OrderQty") <= 0)
        | (F.col("UnitPrice") < 0)
        | (F.col("SalesAmount") < 0)
    ).count()

    assert invalid_count == 0, f"Invalid sales rows: {invalid_count}"


def test_purchase_values_are_not_negative(spark, gold_table):
    purchase_df = spark.table(gold_table("fact_purchase_order"))

    invalid_count = purchase_df.filter(
        (F.col("OrderQty") <= 0)
        | (F.col("UnitPrice") < 0)
        | (F.col("PurchaseAmount") < 0)
        | (F.col("ReceivedQty") < 0)
        | (F.col("RejectedQty") < 0)
        | (F.col("StockedQty") < 0)
    ).count()

    assert invalid_count == 0, f"Invalid purchase rows: {invalid_count}"


def test_production_rates_are_valid(spark, gold_table):
    production_df = spark.table(gold_table("fact_production"))

    invalid_count = production_df.filter(
        F.col("YieldRate").isNull()
        | F.col("ScrapRate").isNull()
        | ~F.col("YieldRate").between(0.0, 1.0)
        | ~F.col("ScrapRate").between(0.0, 1.0)
    ).count()

    assert invalid_count == 0, f"Invalid production rate rows: {invalid_count}"


def test_one_current_row_per_product(spark, gold_table):
    product_df = spark.table(gold_table("dim_product"))

    invalid_count = (
        product_df
        .groupBy("ProductID")
        .agg(
            F.sum(
                F.when(F.col("IsCurrent") == True, 1).otherwise(0)
            ).alias("CurrentRowCount")
        )
        .filter(F.col("CurrentRowCount") != 1)
        .count()
    )

    assert invalid_count == 0, (
        f"Products without exactly one current row: {invalid_count}"
    )


def test_product_effective_dates_are_valid(spark, gold_table):
    product_df = spark.table(gold_table("dim_product"))

    invalid_count = product_df.filter(
        F.col("EffectiveFrom").isNull()
        | F.col("EffectiveTo").isNull()
        | (F.col("EffectiveFrom") >= F.col("EffectiveTo"))
    ).count()

    assert invalid_count == 0, f"Invalid product date ranges: {invalid_count}"
