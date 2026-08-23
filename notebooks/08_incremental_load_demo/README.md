# Incremental-Load Demo

This folder demonstrates that Databricks Auto Loader processes one newly arrived file
and that the new Customer record reaches Bronze, Silver, and Gold.

The demo uses:

```text
CustomerID: 999999
AccountNumber: AW99999999
```

The customer has no sales order. Therefore, the demo does not change sales revenue or
other sales KPIs.

## Important

Run this demo only once. Do not run the preparation notebook while another Lakeflow Job
run is active.

## Execution order

### 1. Prepare the incremental file

Run:

```text
01_prepare_incremental_customer
```

The notebook:

- Confirms the demo customer is new.
- Saves the current Bronze, Silver, and Gold customer counts.
- Writes one new tab-separated Customer file to the raw landing volume.

### 2. Run the complete Lakeflow Job

Open `Smart Factory Medallion Pipeline` and click **Run now**.

The Job processes the new file through:

```text
Auto Loader -> Bronze -> Silver -> Gold -> Pytest -> Audit
```

Wait until the complete Job succeeds.

### 3. Verify the incremental result

Run:

```text
02_verify_incremental_customer
```

Expected result:

| Layer | Difference | CustomerFound | Status |
|---|---:|---|---|
| Bronze | 1 | true | PASS |
| Silver | 1 | true | PASS |
| Gold | 1 | true | PASS |

Capture screenshots of:

- The new source file path printed by the preparation notebook.
- The successful Lakeflow Job run.
- The three PASS verification rows.
- The SUCCESS record in `05_view_audit_history`.

The package intentionally contains no automated cleanup. This prevents accidental
removal of source files or project tables.
