# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Run Gold Pytest Tests
# MAGIC
# MAGIC Runs the automated Gold data-quality tests in the current Databricks session.

# COMMAND ----------

# DBTITLE 1,Cell 2
import pytest

assert pytest.__version__ == "8.3.5", f"Expected pytest 8.3.5, found {pytest.__version__}"
print(f"pytest {pytest.__version__} is available.")

# COMMAND ----------

# DBTITLE 1,Cell 3
import os
import sys

import pytest


sys.dont_write_bytecode = True

test_folder = "/Workspace/Users/aravindveerakumar21@gmail.com/databricks-smart-factory-data-platform/notebooks/06_pytest"

print(f"Running tests from: {test_folder}")

exit_code = pytest.main(
    [
        test_folder,
        "-v",
        "--tb=short",
    ]
)

if exit_code != 0:
    raise ValueError(f"Pytest failed with exit code {exit_code}")

print("All Gold Pytest tests passed.")