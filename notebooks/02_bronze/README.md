# 02_bronze

This folder loads the raw source files into Bronze Delta tables.

The goal of the Bronze layer is simple: keep the data close to the original source while adding basic ingestion metadata such as file information and load timestamps.

What is in this folder:

* `00_config` stores the shared settings used by all Bronze notebooks, such as source paths, checkpoint paths, target tables, and business keys.
* `01_autoload_product` to `17_autoload_sales_order_detail` each load one source file into one Bronze table using Auto Loader.
* `18_validate_bronze` runs a final check across all Bronze tables to confirm the loads completed and to check for duplicate business keys.

How to think about this folder:

* One notebook usually equals one source entity.
* Each notebook reads from the landing area.
* Each notebook writes to a Bronze table in `smart_factory_dev.bronze`.
* The data is kept mostly as-is so cleaning and business rules can happen later in Silver.

Use this folder after the setup and ingestion steps are complete.
