# 07_monitoring_audit

## Recruiter overview

This folder shows the monitoring and audit side of the project.

It tracks whether a full pipeline run started, succeeded, or failed, which helps make the workflow easier to operate and review.

## What this folder demonstrates

This stage shows practical operational work such as:

* creating an audit table
* logging pipeline start and finish events
* recording success and failure outcomes
* making pipeline runs easier to track over time

## Main contents

* `00_config` stores shared settings
* `01_create_audit_table` creates the audit table
* `02_log_pipeline_start` records when a pipeline run begins
* `03_log_pipeline_success` records a successful pipeline run
* `04_log_pipeline_failure` records a failed pipeline run
* `05_view_audit_history` shows the saved audit history

## Why this stage matters

This stage matters because a good pipeline should be monitored, not just executed. Audit logging makes the project easier to support and troubleshoot.

## Summary

In short, this folder shows how the project tracks pipeline runs in a clean and professional way.
