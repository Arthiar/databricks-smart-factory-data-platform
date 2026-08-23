# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Validation
# MAGIC
# MAGIC Runs one consolidated validation after all AdventureWorks Auto Loader notebooks have completed.
# MAGIC
# MAGIC This avoids repeating the same row-count and checkpoint validation logic inside every entity notebook.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from functools import reduce

# COMMAND ----------

results = []

for entity_name, cfg in ENTITY_CONFIG.items():
    target_table = cfg["target_table"]

    if not spark.catalog.tableExists(target_table):
        results.append(
            (entity_name, target_table, "MISSING", None, None, None, None)
        )
        continue

    summary = (
        spark.table(target_table)
        .agg(
            F.count("*").alias("row_count"),
            F.countDistinct("_source_file").alias("source_file_count"),
            F.min("_ingestion_timestamp").alias("first_ingestion"),
            F.max("_ingestion_timestamp").alias("last_ingestion"),
        )
        .first()
    )

    results.append(
        (
            entity_name,
            target_table,
            "OK",
            summary["row_count"],
            summary["source_file_count"],
            summary["first_ingestion"],
            summary["last_ingestion"],
        )
    )

validation_df = spark.createDataFrame(
    results,
    [
        "entity",
        "target_table",
        "status",
        "row_count",
        "source_file_count",
        "first_ingestion",
        "last_ingestion",
    ],
)

display(validation_df.orderBy("entity"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Duplicate business-key check

# COMMAND ----------

duplicate_results = []

for entity_name, cfg in ENTITY_CONFIG.items():
    target_table = cfg["target_table"]
    keys = cfg["business_keys"]

    if not spark.catalog.tableExists(target_table):
        continue

    duplicate_count = (
        spark.table(target_table)
        .groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    duplicate_results.append(
        (entity_name, ", ".join(keys), duplicate_count)
    )

display(
    spark.createDataFrame(
        duplicate_results,
        ["entity", "business_keys", "duplicate_key_groups"],
    ).orderBy("entity")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected result
# MAGIC
# MAGIC - Every configured entity should have `status = OK`.
# MAGIC - `row_count` should be greater than zero.
# MAGIC - `source_file_count` should be at least one.
# MAGIC - Duplicate business-key groups should be zero for the initial official AdventureWorks load.
# MAGIC
# MAGIC Auto Loader checkpoints ensure that rerunning an entity notebook without a new source file does not append the same file again.