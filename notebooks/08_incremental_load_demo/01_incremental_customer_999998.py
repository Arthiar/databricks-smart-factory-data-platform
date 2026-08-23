# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental Customer Demo - Customer 999998
# MAGIC
# MAGIC Use **PREPARE** to create the new source file. Wait for the file-arrival job to finish, then change the widget to **VERIFY** and run the notebook again.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.dropdown("ACTION", "PREPARE", ["PREPARE", "VERIFY"])
ACTION = dbutils.widgets.get("ACTION")
print(f"Selected action: {ACTION}")

# COMMAND ----------

CATALOG = "smart_factory_dev"
BRONZE_CUSTOMER = f"{CATALOG}.bronze.customer"
SILVER_CUSTOMER = f"{CATALOG}.silver.customer"
GOLD_CUSTOMER = f"{CATALOG}.gold.dim_customer"
CONTROL_TABLE = "smart_factory_dev.monitoring.incremental_demo_control"

CUSTOMER_SOURCE_PATH = (
    "/Volumes/smart_factory_dev/raw/landing_files/"
    "batch/erp/sales/customer"
)

DEMO_ID = "customer_999998"
DEMO_CUSTOMER_ID = 999998
DEMO_FILE = f"{CUSTOMER_SOURCE_PATH}/incremental_customer_{DEMO_CUSTOMER_ID}.csv"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.monitoring")
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_TABLE} (
        DemoID STRING,
        CustomerID INT,
        SourceFile STRING,
        BronzeBefore BIGINT,
        SilverBefore BIGINT,
        GoldBefore BIGINT,
        BronzeAfter BIGINT,
        SilverAfter BIGINT,
        GoldAfter BIGINT,
        PreparedTimestamp TIMESTAMP,
        VerifiedTimestamp TIMESTAMP,
        Status STRING
    ) USING DELTA
    """
)

# COMMAND ----------

if ACTION == "PREPARE":
    required_tables = [BRONZE_CUSTOMER, SILVER_CUSTOMER, GOLD_CUSTOMER]
    missing_tables = [
        table_name
        for table_name in required_tables
        if not spark.catalog.tableExists(table_name)
    ]

    if missing_tables:
        raise ValueError(f"Required tables are missing: {missing_tables}")

    existing_demo = (
        spark.table(CONTROL_TABLE)
        .filter(F.col("DemoID") == DEMO_ID)
        .count()
    )

    if existing_demo > 0:
        raise ValueError(
            "This demo was already prepared. Select VERIFY instead."
        )

    existing_customer = (
        spark.table(BRONZE_CUSTOMER)
        .filter(F.col("CustomerID").cast("int") == DEMO_CUSTOMER_ID)
        .count()
    )

    if existing_customer > 0:
        raise ValueError(f"Customer {DEMO_CUSTOMER_ID} already exists in Bronze.")

    BronzeBefore = spark.table(BRONZE_CUSTOMER).count()
    SilverBefore = spark.table(SILVER_CUSTOMER).count()
    GoldBefore = spark.table(GOLD_CUSTOMER).count()

    customer_values = [
        str(DEMO_CUSTOMER_ID),
        "",
        "",
        "1",
        f"AW{DEMO_CUSTOMER_ID}99",
        "99999998-9998-9998-9998-999999999998",
        "2026-08-23 00:00:00.000",
    ]

    customer_line = "\t".join(customer_values) + "\n"
    dbutils.fs.put(DEMO_FILE, customer_line, overwrite=False)

    control_df = spark.createDataFrame(
        [(
            DEMO_ID, DEMO_CUSTOMER_ID, DEMO_FILE,
            BronzeBefore, SilverBefore, GoldBefore, "PREPARED"
        )],
        [
            "DemoID", "CustomerID", "SourceFile",
            "BronzeBefore", "SilverBefore", "GoldBefore", "Status"
        ],
    )
    control_df.createOrReplaceTempView("incremental_demo_input")

    spark.sql(
        f"""
        INSERT INTO {CONTROL_TABLE}
        SELECT
            DemoID, CustomerID, SourceFile,
            BronzeBefore, SilverBefore, GoldBefore,
            NULL, NULL, NULL,
            current_timestamp(), NULL, Status
        FROM incremental_demo_input
        """
    )

    display(spark.createDataFrame(
        [("Bronze", BronzeBefore), ("Silver", SilverBefore), ("Gold", GoldBefore)],
        ["Layer", "BeforeCount"],
    ))
    print(f"New file created: {DEMO_FILE}")
    print("Wait for the automatic Lakeflow Job to finish. Then select VERIFY.")

elif ACTION == "VERIFY":
    control_rows = (
        spark.table(CONTROL_TABLE)
        .filter(F.col("DemoID") == DEMO_ID)
        .orderBy(F.col("PreparedTimestamp").desc())
        .limit(1)
        .collect()
    )

    if not control_rows:
        raise ValueError("No prepared demo found. Run PREPARE first.")

    control = control_rows[0]
    BronzeAfter = spark.table(BRONZE_CUSTOMER).count()
    SilverAfter = spark.table(SILVER_CUSTOMER).count()
    GoldAfter = spark.table(GOLD_CUSTOMER).count()

    bronze_found = (
        spark.table(BRONZE_CUSTOMER)
        .filter(F.col("CustomerID").cast("int") == DEMO_CUSTOMER_ID)
        .count() == 1
    )
    silver_found = (
        spark.table(SILVER_CUSTOMER)
        .filter(F.col("CustomerID") == DEMO_CUSTOMER_ID)
        .count() == 1
    )
    gold_found = (
        spark.table(GOLD_CUSTOMER)
        .filter(F.col("CustomerKey") == DEMO_CUSTOMER_ID)
        .count() == 1
    )

    verification_rows = [
        ("Bronze", control.BronzeBefore, BronzeAfter, bronze_found),
        ("Silver", control.SilverBefore, SilverAfter, silver_found),
        ("Gold", control.GoldBefore, GoldAfter, gold_found),
    ]

    results = []
    for Layer, BeforeCount, AfterCount, CustomerFound in verification_rows:
        Difference = AfterCount - BeforeCount
        Status = "PASS" if Difference >= 1 and CustomerFound else "FAIL"
        results.append((Layer, BeforeCount, AfterCount, Difference, CustomerFound, Status))

    results_df = spark.createDataFrame(
        results,
        ["Layer", "BeforeCount", "AfterCount", "Difference", "CustomerFound", "Status"],
    )
    display(results_df)

    failed_checks = results_df.filter(F.col("Status") == "FAIL").count()
    final_status = "VERIFIED" if failed_checks == 0 else "FAILED"

    spark.sql(
        f"""
        UPDATE {CONTROL_TABLE}
        SET BronzeAfter = {BronzeAfter},
            SilverAfter = {SilverAfter},
            GoldAfter = {GoldAfter},
            VerifiedTimestamp = current_timestamp(),
            Status = '{final_status}'
        WHERE DemoID = '{DEMO_ID}'
        """
    )

    if failed_checks > 0:
        raise ValueError(f"Verification failed for {failed_checks} layer(s).")

    print("Incremental load passed in Bronze, Silver and Gold.")