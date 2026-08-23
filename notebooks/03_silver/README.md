# 03_silver

This folder contains the Silver-layer notebooks for the Smart Factory Data Platform.

The Silver layer takes the raw Bronze data and turns it into cleaner, more consistent tables that are easier to use.

## What this folder does

The notebooks in this folder:

* Read data from the Bronze layer
* Standardize important columns and data types
* Apply cleaner table structures
* Prepare trusted data for the Gold layer

## What is in this folder

### `00_config`
Stores shared settings used by the Silver notebooks.

### `01_silver_product_category` to `17_silver_sales_order_detail`
Each notebook builds or updates one Silver table.

### `18_validate_silver`
Checks that the Silver tables were created correctly.

### `19_run_all_silver`
Runs the full Silver stage in sequence.

## How to think about this layer

The Silver layer is the clean and trusted middle layer of the project.

It sits between:

* Bronze, where data is still close to the source
* Gold, where data is shaped for reporting and business use

## When to use this folder

Use this folder after the Bronze notebooks have finished successfully and before the Gold notebooks begin.

## Summary

In short, this folder turns raw Bronze data into clean Silver tables that are ready for reporting, testing, and analytics.
