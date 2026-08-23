# 02_bronze

This folder contains the Bronze-layer notebooks for the Smart Factory Data Platform.

The Bronze layer loads raw source files into Delta tables and keeps the data close to the original source.

## What is in this folder

* `00_config` stores the shared settings used by the Bronze notebooks
* `01_autoload_product` to `17_autoload_sales_order_detail_fixed` load source files into Bronze tables
* `18_validate_bronze` checks that the Bronze outputs were created correctly
* `19_run_all_bronze` runs the full Bronze stage in sequence

## Why this layer matters

The Bronze layer is the raw foundation of the project.

It keeps ingestion simple and reliable so later layers can focus on cleaning, business rules, and reporting.

## When to use this folder

Use this folder after setup and ingestion are complete and before the Silver notebooks begin.

## Summary

In short, this folder brings raw source files into managed Bronze tables that can be trusted as the starting point for the rest of the pipeline.
