# Notebooks Folder

This folder contains the main notebook-based workflow for the Smart Factory Data Platform.

The notebooks are organized by stage so it is easy to follow the full project from setup to testing and monitoring.

## Folder guide

### `00_setup`
Prepares the Databricks environment.

This stage creates the external locations, catalog, schemas, and volumes that the project needs.

### `01_ingestion`
Brings the source files into the landing area.

This stage downloads the sample data and checks that the required files are available before loading starts.

### `02_bronze`
Loads raw source files into Bronze Delta tables.

This stage keeps the source data close to its original form and adds ingestion metadata.

### `03_silver`
Cleans and standardizes the Bronze data.

This stage applies business-friendly structure so the data is ready for reporting and analytics.

### `04_gold`
Builds the final reporting tables.

This stage creates dimensions, facts, and KPI-style outputs for analytics and dashboards.

### `05_sql_dashboards`
Contains SQL notebooks used for analysis and dashboard-style reporting.

### `06_pytest`
Contains automated tests for the Gold layer.

These tests help confirm that important tables, relationships, and business rules are correct.

### `07_monitoring_audit`
Contains notebooks for logging pipeline start, success, and failure events.

### `08_incremental_load_demo`
Contains a small demo that shows how a newly arrived file moves through the pipeline.

## Recommended order

If you are new to the project, use this order:

1. `00_setup`
2. `01_ingestion`
3. `02_bronze`
4. `03_silver`
5. `04_gold`
6. `05_sql_dashboards`
7. `06_pytest`
8. `07_monitoring_audit`
9. `08_incremental_load_demo`

## Summary

In short, this folder is the heart of the project. It contains the notebooks that build, test, and monitor the Smart Factory data platform.
