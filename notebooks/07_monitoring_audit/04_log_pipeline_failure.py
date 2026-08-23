# Databricks notebook source
# MAGIC %md
# MAGIC # Log Pipeline Failure
# MAGIC
# MAGIC Updates the audit record when at least one pipeline task fails.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.text("job_run_id", "manual_run")
dbutils.widgets.text("job_name", "Smart Factory Medallion Pipeline")
dbutils.widgets.text("trigger_type", "manual")
dbutils.widgets.text(
    "error_message",
    "One or more pipeline tasks failed. Check the Lakeflow run details.",
)

job_run_id = dbutils.widgets.get("job_run_id")
job_name = dbutils.widgets.get("job_name")
trigger_type = dbutils.widgets.get("trigger_type")
error_message = dbutils.widgets.get("error_message")
status = "FAILED"

# COMMAND ----------

failure_df = spark.createDataFrame(
    [(job_run_id, job_name, trigger_type, status, error_message)],
    ["RunID", "JobName", "TriggerType", "Status", "ErrorMessage"],
)

failure_df.createOrReplaceTempView("audit_failure_input")

spark.sql(
    f"""
    MERGE INTO {AUDIT_TABLE} AS target
    USING audit_failure_input AS source
        ON target.RunID = source.RunID
    WHEN MATCHED THEN UPDATE SET
        target.EndTime = current_timestamp(),
        target.Status = source.Status,
        target.ErrorMessage = source.ErrorMessage,
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
        current_timestamp(),
        source.Status,
        source.ErrorMessage,
        current_timestamp(),
        current_timestamp()
    )
    """
)

print(f"Pipeline failure logged for run: {job_run_id}")

# Keep the Lakeflow Job status as failed after writing the audit record.
raise RuntimeError(error_message)