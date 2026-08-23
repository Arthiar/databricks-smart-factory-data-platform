# 01_ingestion

This folder is about getting source data ready for loading.

In simple terms, these notebooks bring files into the landing area and confirm that the files are there before the raw tables are created.

What the notebooks do:

* `01_download_adventureworks_source` downloads the AdventureWorks source files and places them in the governed landing folders.
* `02_verify_landing_files` checks that important files arrived and shows their sizes so you can confirm the download worked.
* `12_autoload_product_vendor` looks like an early Bronze-style ingestion notebook for one source table. It helps show how a single file can be loaded with Auto Loader.

Use this folder after setup and before the full Bronze load. Its main job is to make sure the raw source files are available in the right place.
