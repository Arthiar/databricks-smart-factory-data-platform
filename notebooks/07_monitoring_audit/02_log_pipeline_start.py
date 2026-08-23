# Databricks notebook source
# MAGIC %md
# MAGIC # Log Pipeline Start
# MAGIC
# MAGIC Inserts one RUNNING record when the Lakeflow Job begins.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.text("job_run_id", "manual_run")
dbutils.widgets.text("job_name", "Smart Factory Medallion Pipeline")
dbutils.widgets.text("trigger_type", "manual")

job_run_id = dbutils.widgets.get("job_run_id")
job_name = dbutils.widgets.get("job_name")
trigger_type = dbutils.widgets.get("trigger_type")
status = "RUNNING"

# COMMAND ----------

start_df = spark.createDataFrame(
    [(job_run_id, job_name, trigger_type, status)],
    ["RunID", "JobName", "TriggerType", "Status"],
)

start_df.createOrReplaceTempView("audit_start_input")

spark.sql(
    f"""
    MERGE INTO {AUDIT_TABLE} AS target
    USING audit_start_input AS source
        ON target.RunID = source.RunID
    WHEN MATCHED THEN UPDATE SET
        target.JobName = source.JobName,
        target.TriggerType = source.TriggerType,
        target.StartTime = current_timestamp(),
        target.EndTime = NULL,
        target.Status = source.Status,
        target.ErrorMessage = NULL,
        target.UpdatedTimestamp = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (
        RunID,
        JobName,
        TriggerType,
        StartTime,
        EndTime,
        Status,
        ErrorMessage,
        CreatedTimestamp,
        UpdatedTimestamp
    ) VALUES (
        source.RunID,
        source.JobName,
        source.TriggerType,
        current_timestamp(),
        NULL,
        source.Status,
        NULL,
        current_timestamp(),
        current_timestamp()
    )
    """
)

print(f"Pipeline start logged for run: {job_run_id}")