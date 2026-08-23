# 04_gold

## Recruiter overview

This folder shows the final business-ready modeling stage of the project.

The Gold layer is where cleaned data is turned into reporting tables that are useful for dashboards, analysis, and business decisions.

## What this folder demonstrates

This stage shows practical analytics engineering work such as:

* building dimension tables
* building fact tables
* creating KPI-style outputs
* preparing data for reporting and dashboard use
* validating final outputs

## Main contents

* `00_config` stores shared Gold settings
* `01_dim_date` to `09_fact_work_order_operation` build the main Gold dimensions and facts
* `10_build_kpi_tables` creates KPI reporting tables
* `11_dashboard_queries` contains reporting-focused queries
* `12_validate_gold` checks that the Gold outputs are correct
* `13_run_all_gold` runs the full Gold stage

## Why this stage matters

This stage matters because it creates the final tables that business users, dashboards, and reporting teams can work with directly.

## Summary

In short, this folder shows how trusted data is shaped into final reporting models for business use.
