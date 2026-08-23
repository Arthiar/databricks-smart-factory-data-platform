# Notebooks Folder

## Recruiter overview

This folder contains the main project work.

If the main README explains the project at a high level, this folder shows how the work is actually organized from setup to final validation.

## What this folder demonstrates

A recruiter can read this folder as a step-by-step view of the pipeline lifecycle:

* environment setup
* source data ingestion
* raw-to-clean transformation
* business-ready modeling
* reporting
* testing
* monitoring
* incremental load demonstration

## Folder guide

### `00_setup`
Shows how the project environment is prepared before any data is processed.

### `01_ingestion`
Shows how source files are brought into the platform and checked.

### `02_bronze`
Shows how raw files are loaded into structured Bronze tables.

### `03_silver`
Shows how raw data is cleaned and standardized.

### `04_gold`
Shows how final reporting tables are built for business use.

### `05_sql_dashboards`
Shows reporting-focused SQL work built on top of the Gold layer.

### `06_pytest`
Shows automated testing for important Gold-layer outputs.

### `07_monitoring_audit`
Shows how pipeline run status is tracked and logged.

### `08_incremental_load_demo`
Shows a simple example of how a new file can move through the pipeline after the initial load.

## Suggested reading order

For a quick understanding of the project, review the folders in this order:

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

In short, this folder is the main proof of the engineering work. It shows the full journey from raw data to trusted reporting outputs in a clear, reviewable structure.
