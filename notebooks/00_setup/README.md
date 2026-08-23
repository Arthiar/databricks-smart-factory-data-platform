# 00_setup

## Recruiter overview

This folder shows the environment preparation work for the project.

Before a data pipeline can run, the platform needs storage locations, schemas, and folders. This stage handles that foundation.

## What this folder demonstrates

A recruiter can read this folder as proof that the project was set up in a structured way, not just built with isolated notebooks.

It shows work related to:

* storage setup
* catalog and schema creation
* governed data locations
* project-ready environment preparation

## Main notebooks

* `01_create_external_locations` connects the project to the storage location
* `02_create_catalog_and_schemas` creates the main catalog and schemas
* `03_create_volumes` creates the landing volume and folder structure

## Why this stage matters

This stage matters because every later part of the pipeline depends on it. Without setup, the project has nowhere reliable to store or organize data.

## Summary

In short, this folder shows the platform setup work that makes the rest of the Smart Factory pipeline possible.
