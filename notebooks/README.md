# Notebooks Overview

This folder contains the notebooks used to set up and load the Smart Factory data platform.

The notebooks are grouped by stage so it is easier to understand what happens first and what happens next.

* `00_setup` prepares the Databricks environment. It creates the storage connection, catalog, schemas, and volumes that the project needs before any data can be loaded.
* `01_ingestion` brings source files into the landing area and checks that the files are available.
* `02_bronze` loads the raw source files into Bronze Delta tables with Auto Loader and keeps the original source values for later processing.

Recommended reading and run order:

1. Start with `00_setup`.
2. Move to `01_ingestion`.
3. Finish with `02_bronze`.

In short, this folder shows the early part of the data platform journey: prepare the environment, bring in the files, and store the raw data in Bronze tables.
