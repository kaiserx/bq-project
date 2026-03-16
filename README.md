# Olist E-Commerce Data Pipeline
### Practical Assessment – Data Engineer (DE-M) | elaniin Tech Company

---

## Overview

This project implements a batch data pipeline for the **Brazilian E-Commerce Public Dataset by Olist**, following the 6-part assessment specification. The pipeline ingests raw CSV data into **Google BigQuery Sandbox**, applies transformations and quality controls, and produces a clean curated layer with full observability.

**Stack:** Python 3.10+ · SQL (BigQuery Standard SQL) · Google Cloud BigQuery

---

## Project Structure

```
olist-pipeline/
├── setup/
│   └── 01_create_datasets_and_upload_raw.py   # Part A – create BQ datasets, upload CSVs
├── pipeline/
│   ├── 02_batch1_full_load.py                 # Part C – full batch load
│   ├── 03_batch2_incremental.py               # Part D – incremental / delta load
│   └── sql/
│       ├── ops_tables_ddl.sql                 # DDL for ops layer tables
│       ├── curated_orders.sql                 # Transform: orders + customers
│       ├── curated_order_items.sql            # Transform: order items
│       ├── curated_order_payments.sql         # Transform: payments
│       ├── curated_order_reviews.sql          # Transform: reviews
│       ├── curated_products.sql               # Transform: products + translations
│       ├── curated_sellers.sql                # Transform: sellers
│       ├── incremental_orders.sql             # MERGE: orders delta
│       ├── incremental_order_items.sql        # MERGE: items delta
│       ├── incremental_order_payments.sql     # MERGE: payments delta
│       └── incremental_order_reviews.sql      # MERGE: reviews delta
├── quality/
│   └── 04_data_quality_checks.py             # Part E – 6 DQ validations
├── docs/
│   ├── architecture_diagram.png              # Part 3 – production diagram
│   └── recommendations.md                   # Part F – production improvements
└── README.md
```

---

## Prerequisites

```bash
pip install google-cloud-bigquery db-dtypes
```

Authenticate with GCP:
```bash
gcloud auth application-default login
```

You need a **BigQuery Sandbox** project (free, no billing required).

---

## Execution Steps

### Step 1 – Setup: Create datasets and upload raw data (Part A)

```bash
python setup/01_create_datasets_and_upload_raw.py \
  --project YOUR_PROJECT_ID \
  --data_dir /path/to/csv/folder
```

**What it does:**
- Creates 3 BigQuery datasets: `olist_raw`, `olist_curated`, `olist_ops`
- Uploads all 8 CSV files to `olist_raw` as-is (raw tables, no transformation)
- Idempotent: re-running truncates and reloads raw tables safely

---

### Step 2 – Batch 1: Full Load (Part C)

```bash
python pipeline/02_batch1_full_load.py --project YOUR_PROJECT_ID
```

**What it does:**
- Creates ops tracking tables (`pipeline_run_log`, `dq_results`) if not present
- Runs 6 transformation SQL scripts → writes to `olist_curated`
- Logs per-table metrics (rows read, inserted, skipped, duration) to `olist_ops.pipeline_run_log`
- Generates a unique `batch_id` (UUID) for the run

---

### Step 3 – Batch 2: Incremental Load (Part D)

```bash
python pipeline/03_batch2_incremental.py --project YOUR_PROJECT_ID
```

**What it does:**
- Calculates watermark from `MAX(order_purchase_timestamp)` in `olist_curated.orders`
- Runs MERGE statements — inserts new records, updates changed ones
- Dimension tables (sellers, products) are fully refreshed (small, low-churn)
- Logs new vs updated counts to `olist_ops.pipeline_run_log`

---

### Step 4 – Data Quality Checks (Part E)

```bash
python quality/04_data_quality_checks.py --project YOUR_PROJECT_ID
```

**What it does:**
- Runs 6 DQ validations (see below)
- Writes all results to `olist_ops.dq_results`
- Exits with code 1 if any CRITICAL check fails

---

## Dataset Architecture (BigQuery Layers)

| Dataset | Purpose |
|---|---|
| `olist_raw` | Raw CSVs loaded as-is. Source of truth. Never modified after load. |
| `olist_curated` | Cleaned, typed, deduplicated, joined tables. Ready for consumers. |
| `olist_ops` | Run logs and DQ results. Operational observability. |

### Curated Tables

| Table | Primary Key | Notes |
|---|---|---|
| `orders` | `order_id` | Joined with customers; derived fields (days_to_delivery, on_time flag) |
| `order_items` | `order_id + order_item_id` | Includes total_item_value derived column |
| `order_payments` | `order_id + payment_sequential` | payment_type standardized to lowercase |
| `order_reviews` | `review_id` | Empty strings → NULL; out-of-range scores excluded |
| `products` | `product_id` | Category name joined from translation table (PT→EN) |
| `sellers` | `seller_id` | city lowercase, state uppercase |

All curated tables include traceability fields: `_source_file`, `_load_timestamp`, `_batch_id`.

---

## Transformations Applied

| Transformation | Tables Affected |
|---|---|
| Normalize all date fields to `TIMESTAMP` | All |
| Lowercase + trim `order_status` | orders |
| Lowercase + trim `payment_type` | order_payments |
| Lowercase city / uppercase state | orders, sellers |
| Deduplicate by PK using `ROW_NUMBER()` window | All |
| Replace empty strings with `NULL` | reviews |
| Filter review_score outside 1–5 range | reviews |
| Null-safe cast for numeric fields | order_items, order_payments |
| Join PT→EN product category translation | products |
| Derived: `days_to_delivery`, `delivered_on_time` | orders |
| Derived: `total_item_value`, `product_volume_cm3` | order_items, products |

---

## Incremental Strategy (Batch 2)

**Incremental key:** `order_purchase_timestamp` on the `orders` table.

**Logic:**
- Watermark = `MAX(order_purchase_timestamp)` from `olist_curated.orders`
- Only orders with `order_purchase_timestamp > watermark` are processed
- Fact tables (order_items, payments, reviews) piggyback on order MERGE via `_batch_id` join
- MERGE strategy: `WHEN MATCHED AND <key field changed> THEN UPDATE` / `WHEN NOT MATCHED THEN INSERT`
- Dimension tables (products, sellers) use full `CREATE OR REPLACE` (small, bounded size)

**Why this key?** `order_purchase_timestamp` is the natural creation timestamp for orders and is always present. It is immutable once set, making it a reliable watermark.

---

## Data Quality Checks

| # | Check Name | Severity | Table |
|---|---|---|---|
| 1 | `null_mandatory_fields_orders` | CRITICAL | raw_orders |
| 2 | `invalid_order_status` | WARNING | orders |
| 3 | `orphan_order_items` | ERROR | order_items |
| 4 | `duplicate_order_ids_raw` | ERROR | raw_orders |
| 5 | `invalid_date_range_delivery` | WARNING | orders |
| 6 | `payments_without_items` | WARNING | order_payments / order_items |

All results stored in `olist_ops.dq_results` with: `batch_id`, `check_name`, `severity`, `status`, `affected_count`, `total_count`, `sample_ids`, `check_timestamp`.

---

## Ops Queries (sample)

```sql
-- View last pipeline run summary
SELECT table_name, run_type, records_inserted, records_updated, status, duration_seconds
FROM `YOUR_PROJECT.olist_ops.pipeline_run_log`
ORDER BY run_timestamp DESC;

-- View DQ results for latest batch
SELECT check_name, severity, status, affected_count, total_count, sample_ids
FROM `YOUR_PROJECT.olist_ops.dq_results`
ORDER BY check_timestamp DESC;

-- All FAIL checks
SELECT * FROM `YOUR_PROJECT.olist_ops.dq_results`
WHERE status = 'FAIL'
ORDER BY severity, check_timestamp DESC;
```

---

## Technical Decisions

1. **No geolocation file** – Excluded from scope. High row count (~1M), no direct FK in main tables, adds no value to the operational model without a geospatial use case.
2. **`CREATE OR REPLACE TABLE` for full loads** – Simpler and idempotent. MERGE reserved for incremental.
3. **`ROW_NUMBER()` deduplication** – Preferred over `DISTINCT` or `GROUP BY` because it allows documenting which row is kept (most recent / highest price) and is transparent.
4. **Dimension tables full-refresh in Batch 2** – Products and sellers are small (3K/33K rows) and have no reliable change-detection timestamp, so full refresh is safer and cheaper than hash-based SCD logic.
5. **Watermark on orders only** – All fact tables (items, payments, reviews) are linked to orders; processing only facts tied to new orders is sufficient for correctness and avoids duplicate scans.
6. **Empty string → NULL normalization** – Review comment fields contain `""` as a standing proxy for NULL; normalizing ensures consistent `IS NULL` behavior in downstream queries.
