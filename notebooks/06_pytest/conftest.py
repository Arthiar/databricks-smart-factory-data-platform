import os

import pytest
from pyspark.sql import SparkSession


CATALOG = os.getenv("TEST_CATALOG", "smart_factory_dev")
GOLD_SCHEMA = os.getenv("TEST_GOLD_SCHEMA", "gold")


@pytest.fixture(scope="session")
def spark():
    """Use the Spark session from the running Databricks notebook."""
    active_spark = SparkSession.getActiveSession()

    if active_spark is None:
        raise RuntimeError(
            "No active Spark session. Run 00_run_pytest on a Databricks cluster."
        )

    return active_spark


@pytest.fixture(scope="session")
def gold_table():
    """Return a complete Unity Catalog table name."""
    def table_name(name):
        return f"{CATALOG}.{GOLD_SCHEMA}.{name}"

    return table_name
