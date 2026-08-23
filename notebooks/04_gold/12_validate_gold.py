# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Validation
# MAGIC
# MAGIC Checks table existence, duplicate grains, null keys, SCD2 rules, and fact-to-dimension links.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

validation_results = []


def add_result(check_name, failed_rows, details):
    validation_results.append(
        {
            "CheckName": check_name,
            "Status": "PASS" if failed_rows == 0 else "FAIL",
            "FailedRows": int(failed_rows),
            "Details": details,
        }
    )


# COMMAND ----------

# Check that every expected table exists.

for table_name in GOLD_TABLE_KEYS:
    full_table_name = gold_table(table_name)
    missing_count = 0 if spark.catalog.tableExists(full_table_name) else 1
    add_result(
        f"table_exists_{table_name}",
        missing_count,
        full_table_name,
    )

missing_tables = [
    table_name
    for table_name in GOLD_TABLE_KEYS
    if not spark.catalog.tableExists(gold_table(table_name))
]

if missing_tables:
    results_df = spark.createDataFrame(validation_results)
    display(results_df)
    raise ValueError(f"Missing Gold tables: {missing_tables}")

# COMMAND ----------

# Check table grains and required keys.

for table_name, key_columns in GOLD_TABLE_KEYS.items():
    table_df = spark.table(gold_table(table_name))

    duplicate_count = (
        table_df
        .groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    add_result(
        f"unique_key_{table_name}",
        duplicate_count,
        f"Key: {', '.join(key_columns)}",
    )

    null_condition = F.lit(False)
    for key_column in key_columns:
        null_condition = null_condition | F.col(key_column).isNull()

    null_key_count = table_df.filter(null_condition).count()
    add_result(
        f"not_null_key_{table_name}",
        null_key_count,
        f"Key: {', '.join(key_columns)}",
    )

# COMMAND ----------

# Check the product SCD Type 2 rules.

product_dim_df = spark.table(gold_table("dim_product"))

multiple_current_products = (
    product_dim_df
    .groupBy("ProductID")
    .agg(
        F.sum(
            F.when(F.col("IsCurrent") == True, F.lit(1)).otherwise(F.lit(0))
        ).alias("CurrentRowCount")
    )
    .filter(F.col("CurrentRowCount") != 1)
    .count()
)
add_result(
    "one_current_row_per_product",
    multiple_current_products,
    "Each ProductID must have exactly one current row.",
)

invalid_effective_ranges = product_dim_df.filter(
    F.col("EffectiveFrom") >= F.col("EffectiveTo")
).count()
add_result(
    "valid_product_effective_range",
    invalid_effective_ranges,
    "EffectiveFrom must be earlier than EffectiveTo.",
)

# COMMAND ----------

# Check required fact-to-dimension relationships.

relationship_checks = [
    ("fact_sales", "ProductKey", "dim_product", "ProductKey"),
    ("fact_sales", "CustomerKey", "dim_customer", "CustomerKey"),
    ("fact_purchase_order", "ProductKey", "dim_product", "ProductKey"),
    ("fact_purchase_order", "SupplierKey", "dim_supplier", "SupplierKey"),
    ("fact_production", "ProductKey", "dim_product", "ProductKey"),
    ("fact_work_order_operation", "ProductKey", "dim_product", "ProductKey"),
    ("fact_work_order_operation", "WorkCenterKey", "dim_work_center", "WorkCenterKey"),
]

for fact_name, fact_key, dimension_name, dimension_key in relationship_checks:
    fact_df = spark.table(gold_table(fact_name)).select(fact_key)
    dimension_df = (
        spark.table(gold_table(dimension_name))
        .select(F.col(dimension_key).alias("_dimension_key"))
        .distinct()
    )

    orphan_count = (
        fact_df
        .join(
            dimension_df,
            F.col(fact_key) == F.col("_dimension_key"),
            "left_anti",
        )
        .count()
    )

    add_result(
        f"relationship_{fact_name}_{fact_key}",
        orphan_count,
        f"{fact_name}.{fact_key} -> {dimension_name}.{dimension_key}",
    )

# COMMAND ----------

results_df = spark.createDataFrame(validation_results).select(
    "CheckName", "Status", "FailedRows", "Details"
)
display(results_df.orderBy("Status", "CheckName"))

failed_check_count = results_df.filter(F.col("Status") == "FAIL").count()
if failed_check_count > 0:
    raise ValueError(f"Gold validation failed: {failed_check_count} check(s) failed.")

print(f"Gold validation passed: {results_df.count()} checks.")