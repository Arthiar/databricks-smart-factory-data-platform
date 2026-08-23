# Smart Factory Data Platform

This project is a simple and professional Smart Factory data platform built on Azure Databricks.

It shows how raw manufacturing and business data can move through a clear medallion design:

* Landing and ingestion
* Bronze tables for raw data
* Silver tables for cleaned data
* Gold tables for reporting data
* SQL dashboards for business users
* Pytest checks for data quality
* Audit logging for job monitoring
* A Declarative Automation Bundle for deployment

The project uses the AdventureWorks sample data to demonstrate a realistic factory and supply-chain reporting flow.

## What this project does

This project helps you understand how to build an end-to-end data platform in Databricks.

It includes:

* Environment setup for storage, catalog objects, schemas, and volumes
* File ingestion into a governed landing area
* Bronze notebooks that load source files with Auto Loader
* Silver notebooks that standardize and clean the data
* Gold notebooks that build dimensions, facts, and KPI tables
* SQL notebooks for dashboard-style analysis
* Automated tests for important Gold-layer checks
* Audit notebooks that log pipeline status
* A job definition that runs the full pipeline in order

## Project flow

The project follows this order:

1. Set up the Databricks environment
2. Download and verify the source files
3. Load raw source data into Bronze
4. Clean and standardize data in Silver
5. Build reporting tables in Gold
6. Run business queries and dashboards
7. Run automated tests
8. Save pipeline audit records

## Main folders

### `notebooks/`
Contains the main project logic.

* `00_setup` prepares storage, catalog objects, schemas, and volumes
* `01_ingestion` downloads and checks the source files
* `02_bronze` loads raw files into Bronze Delta tables
* `03_silver` cleans and standardizes the Bronze data
* `04_gold` builds reporting tables and KPI outputs
* `05_sql_dashboards` contains SQL notebooks for analysis and dashboards
* `06_pytest` contains automated tests for the Gold layer
* `07_monitoring_audit` records pipeline run status in an audit table
* `08_incremental_load_demo` demonstrates an incremental customer load

### `resources/`
Contains deployment resources for the project.

* `smart_factory_job.yml` defines the Lakeflow Job that runs the pipeline tasks in sequence

### Root files

* `databricks.yml` is the main Declarative Automation Bundle configuration
* `README.md` is the main project guide
* `LICENSE` contains the project license
* `.gitignore` lists ignored files

## Key technologies

* Azure Databricks
* PySpark
* Delta Lake
* Unity Catalog
* Auto Loader
* Lakeflow Jobs
* Pytest
* YAML-based deployment configuration

## How to use this project

A simple way to work with the project is:

1. Review the README files in each folder
2. Run the setup notebooks first
3. Load the source files
4. Run Bronze, Silver, and Gold in order
5. Run the tests in `06_pytest`
6. Review monitoring results in `07_monitoring_audit`
7. Deploy or run the job from the bundle configuration if needed

## Who this project is for

This project is useful for:

* Data engineers learning Databricks
* Teams that want a clean example of a medallion architecture
* Anyone who wants a simple project structure with clear folder-level documentation

## Summary

In short, this repository shows a full Smart Factory data platform in a clean and practical way. It covers setup, ingestion, transformation, testing, monitoring, and deployment in one project.
