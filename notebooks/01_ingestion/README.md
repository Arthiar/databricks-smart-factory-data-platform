# 01_ingestion

This folder prepares the source files for the Smart Factory pipeline.

The notebooks here bring the sample files into the landing area and confirm that the expected files are available before Bronze loading begins.

## What is in this folder

* `01_download_adventureworks_source` downloads the AdventureWorks source files and places them in the project landing location
* `02_verify_landing_files` checks that the important landing files are present and readable

## When to use this folder

Use this folder after `00_setup` and before `02_bronze`.

## Summary

In short, this folder makes sure the source data is in the right place before the raw Bronze tables are created.
