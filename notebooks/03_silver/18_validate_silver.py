# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Validation
# MAGIC
# MAGIC Run this notebook after all 17 Silver entity notebooks.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

table_results = []

for entity_name, cfg in ENTITY_CONFIG.items():
    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.{entity_name}"
    quarantine_table = f"{CATALOG}.{SILVER_SCHEMA}.{entity_name}_quarantine"
    keys = cfg["business_keys"]

    if not spark.catalog.tableExists(silver_table):
        table_results.append(
            (entity_name, "MISSING", None, None, None, None)
        )
        continue

    silver_df = spark.table(silver_table)
    silver_count = silver_df.count()

    duplicate_key_groups = (
        silver_df
        .groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    null_key_condition = None
    for key in keys:
        current_condition = F.col(key).isNull()
        null_key_condition = (
            current_condition
            if null_key_condition is None
            else null_key_condition | current_condition
        )

    null_key_rows = silver_df.filter(null_key_condition).count()

    quarantine_count = (
        spark.table(quarantine_table).count()
        if spark.catalog.tableExists(quarantine_table)
        else None
    )

    table_results.append(
        (
            entity_name,
            "OK",
            silver_count,
            quarantine_count,
            duplicate_key_groups,
            null_key_rows,
        )
    )

table_validation_df = spark.createDataFrame(
    table_results,
    [
        "entity",
        "status",
        "silver_rows",
        "quarantine_rows",
        "duplicate_key_groups",
        "null_key_rows",
    ],
)

display(table_validation_df.orderBy("entity"))

# COMMAND ----------

relationship_results = []

for relationship in PARENT_RELATIONSHIPS:
    child_entity = relationship["child_entity"]
    child_column = relationship["child_column"]
    parent_entity = relationship["parent_entity"]
    parent_column = relationship["parent_column"]

    child_table = f"{CATALOG}.{SILVER_SCHEMA}.{child_entity}"
    parent_table = f"{CATALOG}.{SILVER_SCHEMA}.{parent_entity}"

    if not spark.catalog.tableExists(child_table) or not spark.catalog.tableExists(parent_table):
        relationship_results.append(
            (
                child_entity,
                child_column,
                parent_entity,
                parent_column,
                "MISSING_TABLE",
                None,
            )
        )
        continue

    child_keys_df = (
        spark.table(child_table)
        .select(F.col(child_column).alias("child_key"))
        .filter(F.col("child_key").isNotNull())
        .distinct()
    )

    parent_keys_df = (
        spark.table(parent_table)
        .select(F.col(parent_column).alias("parent_key"))
        .filter(F.col("parent_key").isNotNull())
        .distinct()
    )

    orphan_count = (
        child_keys_df
        .join(
            parent_keys_df,
            child_keys_df["child_key"] == parent_keys_df["parent_key"],
            "left_anti",
        )
        .count()
    )

    relationship_results.append(
        (
            child_entity,
            child_column,
            parent_entity,
            parent_column,
            "OK",
            orphan_count,
        )
    )

relationship_validation_df = spark.createDataFrame(
    relationship_results,
    [
        "child_entity",
        "child_column",
        "parent_entity",
        "parent_column",
        "status",
        "orphan_key_count",
    ],
)

display(
    relationship_validation_df.orderBy(
        "child_entity",
        "child_column",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected result
# MAGIC
# MAGIC - Every entity has `status = OK`.
# MAGIC - `silver_rows` is greater than zero for populated source tables.
# MAGIC - `duplicate_key_groups = 0`.
# MAGIC - `null_key_rows = 0`.
# MAGIC - Every supported relationship has `orphan_key_count = 0`.
# MAGIC - Quarantine counts may be greater than zero when invalid source rows exist.
# MAGIC