# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - Product Dimension (SCD Type 2)
# MAGIC
# MAGIC Keeps product history with readable column-by-column comparisons. No hash is used.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# COMMAND ----------

target_table = gold_table("dim_product")

product_df = spark.table(silver_table("product")).alias("p")
subcategory_df = spark.table(silver_table("product_subcategory")).alias("s")
category_df = spark.table(silver_table("product_category")).alias("c")

source_df = (
    product_df
    .join(
        subcategory_df,
        F.col("p.ProductSubcategoryID") == F.col("s.ProductSubcategoryID"),
        "left",
    )
    .join(
        category_df,
        F.col("s.ProductCategoryID") == F.col("c.ProductCategoryID"),
        "left",
    )
    .select(
        F.col("p.ProductID").alias("ProductID"),
        F.col("p.Name").alias("ProductName"),
        F.col("p.ProductNumber").alias("ProductNumber"),
        F.col("c.Name").alias("CategoryName"),
        F.col("s.Name").alias("SubcategoryName"),
        F.col("p.Color").alias("Color"),
        F.col("p.Size").alias("Size"),
        F.col("p.StandardCost").alias("StandardCost"),
        F.col("p.ListPrice").alias("ListPrice"),
        F.col("p.MakeFlag").alias("MakeFlag"),
        F.col("p.FinishedGoodsFlag").alias("FinishedGoodsFlag"),
        F.col("p.ProductLine").alias("ProductLine"),
        F.col("p.Class").alias("Class"),
        F.col("p.Style").alias("Style"),
        F.col("p.ModifiedDate").alias("ModifiedDate"),
    )
)

tracked_columns = [
    "ProductName",
    "ProductNumber",
    "CategoryName",
    "SubcategoryName",
    "Color",
    "Size",
    "StandardCost",
    "ListPrice",
    "MakeFlag",
    "FinishedGoodsFlag",
    "ProductLine",
    "Class",
    "Style",
]

# Use one timestamp for the complete load so expired and inserted records match exactly.
load_timestamp = spark.sql("SELECT current_timestamp() AS load_timestamp").first()[0]

# COMMAND ----------

if not spark.catalog.tableExists(target_table):
    initial_df = (
        source_df
        .withColumn("ProductVersion", F.lit(1))
        .withColumn(
            "ProductKey",
            F.concat(F.col("ProductID").cast("string"), F.lit("-1")),
        )
        .withColumn("EffectiveFrom", F.lit("1900-01-01").cast("timestamp"))
        .withColumn("EffectiveTo", F.lit("9999-12-31").cast("timestamp"))
        .withColumn("IsCurrent", F.lit(True))
        .withColumn("_gold_processed_timestamp", F.lit(load_timestamp))
        .select(
            "ProductKey",
            "ProductVersion",
            "ProductID",
            "ProductName",
            "ProductNumber",
            "CategoryName",
            "SubcategoryName",
            "Color",
            "Size",
            "StandardCost",
            "ListPrice",
            "MakeFlag",
            "FinishedGoodsFlag",
            "ProductLine",
            "Class",
            "Style",
            "ModifiedDate",
            "EffectiveFrom",
            "EffectiveTo",
            "IsCurrent",
            "_gold_processed_timestamp",
        )
    )

    (
        initial_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
    )
else:
    current_df = (
        spark.table(target_table)
        .filter(F.col("IsCurrent") == True)
        .select("ProductID", "ProductVersion", *tracked_columns)
    )

    joined_df = source_df.alias("s").join(
        current_df.alias("t"),
        F.col("s.ProductID") == F.col("t.ProductID"),
        "left",
    )

    all_values_same = F.lit(True)
    for column_name in tracked_columns:
        all_values_same = all_values_same & F.col(f"s.{column_name}").eqNullSafe(
            F.col(f"t.{column_name}")
        )

    new_products_df = (
        joined_df
        .filter(F.col("t.ProductID").isNull())
        .select("s.*")
        .withColumn("ProductVersion", F.lit(1))
    )

    changed_products_df = (
        joined_df
        .filter(F.col("t.ProductID").isNotNull() & ~all_values_same)
        .select(
            "s.*",
            (F.col("t.ProductVersion") + 1).alias("ProductVersion"),
        )
        .cache()
    )

    changed_product_count = changed_products_df.count()

    new_versions_df = (
        new_products_df
        .unionByName(changed_products_df)
        .withColumn(
            "ProductKey",
            F.concat_ws(
                "-",
                F.col("ProductID").cast("string"),
                F.col("ProductVersion").cast("string"),
            ),
        )
        .withColumn(
            "EffectiveFrom",
            F.when(
                F.col("ProductVersion") == 1,
                F.lit("1900-01-01").cast("timestamp"),
            ).otherwise(F.lit(load_timestamp)),
        )
        .withColumn("EffectiveTo", F.lit("9999-12-31").cast("timestamp"))
        .withColumn("IsCurrent", F.lit(True))
        .withColumn("_gold_processed_timestamp", F.lit(load_timestamp))
        .select(
            "ProductKey",
            "ProductVersion",
            "ProductID",
            "ProductName",
            "ProductNumber",
            "CategoryName",
            "SubcategoryName",
            "Color",
            "Size",
            "StandardCost",
            "ListPrice",
            "MakeFlag",
            "FinishedGoodsFlag",
            "ProductLine",
            "Class",
            "Style",
            "ModifiedDate",
            "EffectiveFrom",
            "EffectiveTo",
            "IsCurrent",
            "_gold_processed_timestamp",
        )
        .cache()
    )

    new_version_count = new_versions_df.count()

    if changed_product_count > 0:
        changed_ids_df = (
            changed_products_df
            .select("ProductID")
            .distinct()
            .withColumn("ChangeTimestamp", F.lit(load_timestamp))
        )

        (
            DeltaTable.forName(spark, target_table)
            .alias("t")
            .merge(
                changed_ids_df.alias("s"),
                "t.ProductID = s.ProductID AND t.IsCurrent = true",
            )
            .whenMatchedUpdate(
                set={
                    "IsCurrent": "false",
                    "EffectiveTo": "s.ChangeTimestamp",
                }
            )
            .execute()
        )

    if new_version_count > 0:
        new_versions_df.write.format("delta").mode("append").saveAsTable(target_table)

    changed_products_df.unpersist()
    new_versions_df.unpersist()

print(f"Product dimension rows: {spark.table(target_table).count():,}")
display(spark.table(target_table).orderBy("ProductID", "ProductVersion").limit(30))
