"""
PART D: Batch Pipeline 2 – Incremental / Delta Load
Uses MERGE statements to upsert only new or changed records into olist_curated.
Watermark is derived from MAX(order_purchase_timestamp) in curated.orders.

Usage:
    python pipeline/03_batch2_incremental.py --project YOUR_PROJECT_ID
"""

import argparse
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

SQL_DIR = Path(__file__).parent / "sql"

# Incremental tables and their SQL files
INCREMENTAL_TABLES = [
    ("incremental_orders",         "orders"),
    ("incremental_order_items",    "order_items"),
    ("incremental_order_payments", "order_payments"),
    ("incremental_order_reviews",  "order_reviews"),
]

# Dimension tables use full-replace (small, low-churn)
DIMENSION_TABLES = [
    ("curated_sellers",  "sellers"),
    ("curated_products", "products"),
]


def load_sql(filename: str, project: str, batch_id: str, watermark: str = "") -> str:
    path = SQL_DIR / f"{filename}.sql"
    sql = path.read_text()
    return (
        sql
        .replace("{project}", project)
        .replace("{batch_id}", batch_id)
        .replace("{watermark}", watermark)
    )


def get_watermark(client: bigquery.Client, project: str) -> str:
    """Get the max order_purchase_timestamp from curated.orders."""
    query = f"""
        SELECT CAST(MAX(order_purchase_timestamp) AS STRING) AS wm
        FROM `{project}.olist_curated.orders`
    """
    rows = list(client.query(query).result())
    wm = rows[0].wm if rows else "2000-01-01 00:00:00 UTC"
    print(f"  Watermark (max order_purchase_timestamp): {wm}")
    return wm


def count_rows(client: bigquery.Client, project: str, dataset: str, table: str) -> int:
    result = client.query(
        f"SELECT COUNT(*) AS n FROM `{project}.{dataset}.{table}`"
    ).result()
    return list(result)[0].n


def get_merge_stats(client: bigquery.Client, project: str, table: str, batch_id: str) -> dict:
    """Count inserted and updated rows in this batch."""
    query = f"""
        SELECT
          COUNTIF(_batch_id = '{batch_id}') AS batch_rows
        FROM `{project}.olist_curated.{table}`
    """
    rows = list(client.query(query).result())
    return {"batch_rows": rows[0].batch_rows}


def log_run_metric(
    client: bigquery.Client,
    project: str,
    batch_id: str,
    run_type: str,
    table_name: str,
    records_read: int,
    records_inserted: int,
    records_updated: int,
    records_skipped: int,
    status: str,
    run_timestamp: datetime,
    duration_seconds: float,
    error_message: str = None,
) -> None:
    rows = [{
        "batch_id":          batch_id,
        "run_type":          run_type,
        "table_name":        table_name,
        "records_read":      records_read,
        "records_inserted":  records_inserted,
        "records_updated":   records_updated,
        "records_skipped":   records_skipped,
        "status":            status,
        "error_message":     error_message,
        "run_timestamp":     run_timestamp.isoformat(),
        "duration_seconds":  duration_seconds,
    }]
    errors = client.insert_rows_json(
        f"{project}.olist_ops.pipeline_run_log", rows
    )
    if errors:
        print(f"  ⚠ Metric log error: {errors}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Olist – Batch 2 incremental load")
    parser.add_argument("--project", required=True, help="GCP project ID")
    args = parser.parse_args()

    project = args.project
    client = bigquery.Client(project=project)
    batch_id = str(uuid.uuid4())
    run_timestamp = datetime.now(timezone.utc)

    print(f"\n{'='*60}")
    print(f"  Olist Batch Pipeline 2 – INCREMENTAL LOAD")
    print(f"  batch_id : {batch_id}")
    print(f"  started  : {run_timestamp.isoformat()}")
    print(f"{'='*60}\n")

    # ── Get watermark ─────────────────────────────────────────────────────────
    print("[0] Calculating watermark…")
    watermark = get_watermark(client, project)

    results = []

    # ── Dimension tables: full refresh (small, schema may change) ─────────────
    print("\n[1] Refreshing dimension tables (full replace)…")
    for sql_key, table_name in DIMENSION_TABLES:
        print(f"\n  [→] {table_name}")
        t0 = time.time()
        try:
            sql = load_sql(sql_key, project, batch_id)
            client.query(sql).result()
            count = count_rows(client, project, "olist_curated", table_name)
            duration = round(time.time() - t0, 2)
            print(f"    rows : {count:,} | duration: {duration}s | ✓ SUCCESS")
            log_run_metric(
                client, project, batch_id, "INCREMENTAL", table_name,
                count, count, 0, 0, "SUCCESS", run_timestamp, duration
            )
            results.append((table_name, "SUCCESS", count, 0))
        except Exception as exc:
            duration = round(time.time() - t0, 2)
            print(f"    ✗ FAILED – {exc}")
            log_run_metric(
                client, project, batch_id, "INCREMENTAL", table_name,
                0, 0, 0, 0, "FAILED", run_timestamp, duration, str(exc)
            )
            results.append((table_name, "FAILED", 0, 0))

    # ── Fact tables: MERGE (insert new + update changed) ─────────────────────
    print("\n[2] Running incremental MERGE on fact tables…")
    for sql_key, table_name in INCREMENTAL_TABLES:
        print(f"\n  [→] {table_name}")
        t0 = time.time()
        try:
            count_before = count_rows(client, project, "olist_curated", table_name)
            sql = load_sql(sql_key, project, batch_id, watermark)
            client.query(sql).result()
            count_after = count_rows(client, project, "olist_curated", table_name)
            stats = get_merge_stats(client, project, table_name, batch_id)

            new_rows = count_after - count_before
            updated = stats["batch_rows"] - new_rows
            duration = round(time.time() - t0, 2)

            print(f"    before : {count_before:,}")
            print(f"    after  : {count_after:,}")
            print(f"    new    : {new_rows:,}")
            print(f"    updated: {max(updated,0):,}")
            print(f"    duration: {duration}s | ✓ SUCCESS")

            log_run_metric(
                client, project, batch_id, "INCREMENTAL", table_name,
                count_after, new_rows, max(updated, 0), 0,
                "SUCCESS", run_timestamp, duration
            )
            results.append((table_name, "SUCCESS", new_rows, max(updated, 0)))

        except Exception as exc:
            duration = round(time.time() - t0, 2)
            print(f"    ✗ FAILED – {exc}")
            log_run_metric(
                client, project, batch_id, "INCREMENTAL", table_name,
                0, 0, 0, 0, "FAILED", run_timestamp, duration, str(exc)
            )
            results.append((table_name, "FAILED", 0, 0))

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("  BATCH 2 SUMMARY")
    print(f"{'='*60}")
    for tbl, status, new, upd in results:
        icon = "✓" if status == "SUCCESS" else "✗"
        print(f"  {icon} {tbl:<30} new: {new:>8,}  updated: {upd:>6,}  [{status}]")
    print(f"\n  batch_id : {batch_id}")
    print(
        f"\nNext step:\n"
        f"  python quality/04_data_quality_checks.py --project {project}\n"
    )


if __name__ == "__main__":
    main()
