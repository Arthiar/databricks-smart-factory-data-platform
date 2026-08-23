# 02_bronze

## Recruiter overview

This folder shows the raw data loading stage of the project.

The Bronze layer is where source files are loaded into structured tables while still staying close to their original form.

## What this folder demonstrates

This stage shows practical data engineering work such as:

* loading source files into Delta tables
* organizing raw data by entity
* using shared configuration for repeatable processing
* validating that raw loads completed correctly

## Main contents

* `00_config` stores shared Bronze settings
* `01_autoload_product` to `17_autoload_sales_order_detail_fixed` load individual source entities into Bronze tables
* `18_validate_bronze` checks the Bronze outputs
* `19_run_all_bronze` runs the full Bronze stage

## Why this stage matters

This stage matters because it creates the reliable raw data base that later cleaning and business modeling depend on.

## Summary

In short, this folder shows how raw source data is turned into a stable Bronze layer for the rest of the project.
