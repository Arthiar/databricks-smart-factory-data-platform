# 00_setup

This folder is the starting point for the project.

These notebooks prepare the Databricks environment so the rest of the pipeline has a safe place to store and read data.

What the notebooks do:

* `01_create_external_locations` connects Databricks to the Azure storage location used by this project.
* `02_create_catalog_and_schemas` creates the Unity Catalog catalog and the main schemas such as raw, bronze, silver, gold, quarantine, and monitoring.
* `03_create_volumes` creates the volume used for landing files and builds the basic folder structure for incoming data.

Run this folder first. If setup is not done, the later notebooks will not have the storage paths, catalog objects, or folders they expect.
