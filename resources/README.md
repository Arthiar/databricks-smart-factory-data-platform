# Resources Folder

This folder contains the deployment resources for the Smart Factory Data Platform.

The files here describe what Databricks should create or run when the project is deployed.

## What is in this folder

### `smart_factory_job.yml`
This file defines the main Lakeflow Job for the project.

The job runs the pipeline in this order:

1. Start audit logging
2. Run the Bronze notebooks
3. Run the Silver notebooks
4. Run the Gold notebooks
5. Run the Pytest checks
6. Write success or failure audit records

## Why this folder matters

This folder helps turn the project from a set of notebooks into a repeatable deployment.

Instead of creating jobs by hand every time, the YAML file keeps the job definition in source control so it is easier to review, reuse, and maintain.

## Related files

* `../databricks.yml` contains the main bundle settings
* `../notebooks/` contains the notebooks used by the job

## Summary

In short, this folder stores the deployment-ready job configuration for the full Smart Factory pipeline.
