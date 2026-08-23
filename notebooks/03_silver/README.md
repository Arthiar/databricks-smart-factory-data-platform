# 03_silver

## Recruiter overview

This folder shows the data cleaning and standardization stage of the project.

The Silver layer takes raw Bronze data and turns it into cleaner, more reliable tables that are easier to use in downstream reporting.

## What this folder demonstrates

This stage shows practical transformation work such as:

* cleaning raw data
* standardizing structures and columns
* preparing trusted tables for business reporting
* validating transformation outputs

## Main contents

* `00_config` stores shared Silver settings
* `01_silver_product_category` to `17_silver_sales_order_detail` build the main Silver tables
* `18_validate_silver` checks that the Silver outputs were created correctly
* `19_run_all_silver` runs the full Silver stage

## Why this stage matters

This stage matters because business-ready reporting cannot be built directly on raw data. The Silver layer creates a cleaner and more dependable middle layer.

## Summary

In short, this folder shows how raw tables are cleaned and organized into trusted Silver tables for later business use.
