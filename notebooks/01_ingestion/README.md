# 01_ingestion

## Recruiter overview

This folder shows how source data enters the project.

It focuses on bringing files into the landing area and checking that the pipeline has the inputs it needs before transformation begins.

## What this folder demonstrates

This stage shows practical ingestion work, including:

* collecting source files
* placing them in the correct project location
* validating that the files are ready for processing

## Main notebooks

* `01_download_adventureworks_source` downloads the source files into the project landing area
* `02_verify_landing_files` checks that the expected files are present and readable

## Why this stage matters

This stage matters because a pipeline cannot produce reliable results if the input data is missing or incomplete.

## Summary

In short, this folder shows the first data movement step in the project: bringing source files into the platform in a controlled way.
