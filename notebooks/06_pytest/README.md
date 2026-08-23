# 06_pytest

This folder contains automated tests for the Gold layer of the Smart Factory Data Platform.

The goal of this folder is to make sure the final reporting tables are reliable.

## What is in this folder

* `00_run_pytest` runs the test suite
* `conftest.py` stores shared test setup and fixtures
* `pytest.ini` stores Pytest settings
* `test_gold_tables.py` checks that important Gold tables exist and can be queried
* `test_gold_relationships.py` checks important table relationships
* `test_gold_business_rules.py` checks key business rules in the Gold layer

## Why this folder matters

Testing is important because it helps catch data issues before business users rely on the results.

This folder adds a simple quality gate after the Gold notebooks run.

## When to use this folder

Use this folder after the Gold layer has been built.

It can be run by itself during development or as part of the full Lakeflow Job.

## Summary

In short, this folder helps confirm that the final reporting layer is correct, stable, and ready to use.
