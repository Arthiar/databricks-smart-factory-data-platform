# 06_pytest

## Recruiter overview

This folder shows the testing part of the project.

It contains automated checks for the Gold layer so the final reporting tables are not only built, but also validated.

## What this folder demonstrates

This stage shows practical quality-focused work such as:

* automated test execution
* validation of key tables
* validation of important relationships
* validation of business rules in final outputs

## Main contents

* `00_run_pytest` runs the test suite
* `conftest.py` stores shared test setup
* `pytest.ini` stores test settings
* `test_gold_tables.py` checks important Gold tables
* `test_gold_relationships.py` checks important relationships
* `test_gold_business_rules.py` checks business rules in the Gold layer

## Why this stage matters

This stage matters because it shows that the project includes quality checks, not just transformations. That makes the final outputs more trustworthy.

## Summary

In short, this folder shows how automated testing is used to protect the quality of the final reporting layer.
