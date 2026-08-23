# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - Date Dimension
# MAGIC
# MAGIC Creates one row per date from 2000-01-01 through 2035-12-31.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = gold_table("dim_date")

date_df = spark.sql(
    """
    SELECT explode(
        sequence(
            to_date('2000-01-01'),
            to_date('2035-12-31'),
            interval 1 day
        )
    ) AS FullDate
    """
)

dim_date_df = (
    date_df
    .select(
        date_key(F.col("FullDate")).alias("DateKey"),
        F.col("FullDate"),
        F.dayofmonth("FullDate").alias("DayOfMonth"),
        F.date_format("FullDate", "EEEE").alias("DayName"),
        F.weekofyear("FullDate").alias("WeekOfYear"),
        F.month("FullDate").alias("MonthNumber"),
        F.date_format("FullDate", "MMMM").alias("MonthName"),
        F.quarter("FullDate").alias("QuarterNumber"),
        F.year("FullDate").alias("YearNumber"),
        F.dayofweek("FullDate").isin(1, 7).alias("IsWeekend"),
    )
    .withColumn("_gold_processed_timestamp", F.current_timestamp())
)

(
    dim_date_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Date rows: {spark.table(target_table).count():,}")
display(spark.table(target_table).orderBy("FullDate").limit(20))