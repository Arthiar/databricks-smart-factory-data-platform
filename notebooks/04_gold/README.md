# 04_gold

This folder contains the Gold-layer notebooks for the Smart Factory Data Platform.

The Gold layer turns trusted Silver data into final reporting tables that are ready for analytics, dashboards, and business checks.

## What this folder builds

This folder creates:

* Dimension tables such as date, customer, supplier, work center, and product
* Fact tables for sales, purchase orders, production, and work-order operations
* KPI tables for reporting and business summaries

## What is in this folder

* `00_config` stores shared settings for the Gold notebooks
* `01_dim_date` to `09_fact_work_order_operation` build the main Gold dimensions and facts
* `10_build_kpi_tables` creates KPI-style reporting tables
* `11_dashboard_queries` contains queries used for dashboard-style analysis
* `12_validate_gold` checks that the Gold outputs are correct
* `13_run_all_gold` runs the full Gold stage in sequence

## Why this layer matters

The Gold layer is the business-ready layer of the project.

It is the part of the platform that is easiest for reporting users, dashboard builders, and reviewers to understand.

## When to use this folder

Use this folder after the Silver notebooks have finished successfully.

## Summary

In short, this folder creates the final reporting tables that business users and dashboards can use with confidence.
