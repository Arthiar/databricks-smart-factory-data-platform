# Monitoring and Audit

This folder adds simple job-level monitoring to the Smart Factory Lakeflow Job. It
creates one Delta record for every end-to-end pipeline run.

## Audit table

```text
smart_factory_dev.monitoring.pipeline_audit
```

The table records:

- Run ID
- Job name
- Trigger type
- Start and end timestamps
- Status: RUNNING, SUCCESS, or FAILED
- Error message
- Created and updated timestamps

Lakeflow already provides detailed task-level history. This custom table intentionally
stores only the overall pipeline status and does not invent row-processing counts.

## Initial setup

1. Import the complete `07_monitoring_audit` folder into Databricks.
2. Attach the same compute used by the pipeline.
3. Run `01_create_audit_table` once.
4. Confirm that the audit table is created.

## Update the Lakeflow Job

### 1. Add audit_start

Add a notebook task before Bronze:

```text
Task name: audit_start
Notebook: 07_monitoring_audit/02_log_pipeline_start
Depends on: None
```

Add these task parameters:

```text
job_run_id   = {{job.run_id}}
job_name     = {{job.name}}
trigger_type = {{job.trigger.type}}
```

Change `bronze_pipeline` so it depends on `audit_start`.

### 2. Add audit_success

```text
Task name: audit_success
Notebook: 07_monitoring_audit/03_log_pipeline_success
Depends on: gold_pytest
Run if dependencies: All succeeded
```

Add the same three task parameters:

```text
job_run_id   = {{job.run_id}}
job_name     = {{job.name}}
trigger_type = {{job.trigger.type}}
```

### 3. Add audit_failure

```text
Task name: audit_failure
Notebook: 07_monitoring_audit/04_log_pipeline_failure
Depends on: bronze_pipeline, silver_pipeline, gold_pipeline, gold_pytest
Run if dependencies: At least one failed
```

Add these task parameters:

```text
job_run_id   = {{job.run_id}}
job_name     = {{job.name}}
trigger_type = {{job.trigger.type}}
error_message = One or more pipeline tasks failed. Check the Lakeflow run details.
```

The failure notebook writes the FAILED audit record and then raises an error. This keeps
the overall Lakeflow Job status failed instead of falsely showing success.

## Final task graph

```text
audit_start
    |
bronze_pipeline
    |
silver_pipeline
    |
gold_pipeline
    |
gold_pytest
   / \
audit_success   audit_failure
```

Only one final audit task runs:

- `audit_success` runs when the pipeline succeeds.
- `audit_failure` runs when at least one pipeline task fails.

After testing the job, run `05_view_audit_history` to view the saved audit records.
