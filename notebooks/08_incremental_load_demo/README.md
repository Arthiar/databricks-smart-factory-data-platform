# 08_incremental_load_demo

## Recruiter overview

This folder shows that the project can handle more than a one-time batch load.

It demonstrates how a new file can be introduced after the initial setup and then move through the pipeline.

## What this folder demonstrates

This stage shows practical pipeline behavior such as:

* handling new incoming data
* processing incremental files
* confirming that new data reaches later layers

## Main contents

* `01_incremental_customer_999998` prepares and checks a simple incremental load example

## Why this stage matters

This stage matters because real projects usually receive new data over time, not only in one initial load.

## Summary

In short, this folder shows that the pipeline can process new incoming data after the first run.
