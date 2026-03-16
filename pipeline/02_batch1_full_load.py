"""
PART C: Batch Pipeline 1 – Full Load
Reads from olist_raw, applies transformations via SQL, loads into olist_curated,
and writes run metrics to olist_ops.pipeline_run_log.

Usage:
    python pipeline/02_batch1_full_load.py --project YOUR_PROJECT_ID
"""

import argparse
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

# ─── Config ──────────────────────────────────────────────────────────────────

SQL_DIR = Path(__file__).parent / "sql"

# Order matters: sellers/products/customers before orders (FK deps for validation)
TABLES = [
    "curated_sellers",
    "curated_products",
    "curated_orders",
    "curated_order_items",
    "curated_order_payments",
    "curated_order_reviews",
]

# Map sql filename → curated table name
SQL_FILE_MAP = {
    "curated_sellers":        "sellers",
    "curated_products":       "products",
    "curated_orders":         "orders",
    "curated_order_items":    "order_items",
    "curated_order_payments": "order_payments",
    "curated_order_reviews":  "order_reviews",
}


# ─── Helpers ─────────────────────────────────────────────────────────────────

def load_sql(filename: str, project: str, batch_id: str) -> str:
    """Load a SQL file and substitute template variables."""
    path = SQL_DIR / f"{filename}.sql"
    sql = path.read_text()
    return sql.replace("{project}", project).replace("{batch_id}", batch_id)


def count_rows(client: bigquery.Client, project: str, dataset: str, table: str) -> int:
    """Return the current row count of a BQ table."""
    query = f"SELECT COUNT(*) AS n FROM `{project}.{dataset}.{table}`"
    result = client.query(query).result()
    return list(result)[0].n


def create_ops_tables(client: bigquery.Client, project: str) -> None:
    """Create ops tables if they don't exist yet."""
    ddl = load_sql("ops_tables_ddl", project, "")
    # Execute each statement separately (BigQuery doesn't support multi-statement DDL via query())
    for statement in ddl.split(";"):
        stmt = statement.strip()
        if stmt:
            client.query(stmt).result()
    print("  ✓ Ops tables ready")


def log_run_metric(
    client: bigquery.Client,
    project: str,
    batch_id: str,
    table_name: str,
    records_read: int,
    records_inserted: int,
    records_skipped: int,
    status: str,
    run_timestamp: datetime,
    duration_seconds: float,
    error_message: str = None,
) -> None:
    """Append one row to olist_ops.pipeline_run_log."""
    rows = [{
        "batch_id":          batch_id,
        "run_type":          "FULL",
        "table_name":        table_name,
        "records_read":      records_read,
        "records_inserted":  records_inserted,
        "records_updated":   0,
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
        print(f"  ⚠ Warning: failed to log metrics: {errors}")


def get_raw_count(client: bigquery.Client, project: str, sql_key: str) -> int:
    """Get the row count from the corresponding raw table."""
    raw_table_map = {
        "curated_sellers":        "raw_sellers",
        "curated_products":       "raw_products",
        "curated_orders":         "raw_orders",
        "curated_order_items":    "raw_order_items",
        "curated_order_payments": "raw_order_payments",
        "curated_order_reviews":  "raw_order_reviews",
    }
    raw_table = raw_table_map[sql_key]
    return count_rows(client, project, "olist_raw", raw_table)


# ─── Main ────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Olist – Batch 1 full load")
    parser.add_argument("--project", required=True, help="GCP project ID")
    args = parser.parse_args()

    project = args.project
    client = bigquery.Client(project=project)
    batch_id = str(uuid.uuid4())
    run_timestamp = datetime.now(timezone.utc)

    print(f"\n{'='*60}")
    print(f"  Olist Batch Pipeline 1 – FULL LOAD")
    print(f"  batch_id : {batch_id}")
    print(f"  started  : {run_timestamp.isoformat()}")
    print(f"{'='*60}\n")

    # Ensure ops tables exist
    print("[0] Initialising ops tables…")
    create_ops_tables(client, project)

    total_inserted = 0
    results = []

    for sql_key in TABLES:
        curated_table = SQL_FILE_MAP[sql_key]
        print(f"\n[→] Processing: {curated_table}")
        t0 = time.time()

        try:
            # Count raw rows before transform
            raw_count = get_raw_count(client, project, sql_key)

            # Execute transformation SQL
            sql = load_sql(sql_key, project, batch_id)
            job = client.query(sql)
            job.result()

            # Count curated rows after load
            curated_count = count_rows(client, project, "olist_curated", curated_table)
            skipped = raw_count - curated_count
            duration = round(time.time() - t0, 2)

            print(f"    raw rows   : {raw_count:,}")
            print(f"    curated    : {curated_count:,}")
            print(f"    skipped    : {skipped:,}  (dupes / nulls filtered)")
            print(f"    duration   : {duration}s")
            print(f"    status     : ✓ SUCCESS")

            log_run_metric(
                client, project, batch_id, curated_table,
                raw_count, curated_count, max(skipped, 0),
                "SUCCESS", run_timestamp, duration
            )
            total_inserted += curated_count
            results.append((curated_table, "SUCCESS", curated_count))

        except Exception as exc:
            duration = round(time.time() - t0, 2)
            print(f"    status     : ✗ FAILED – {exc}")
            log_run_metric(
                client, project, batch_id, curated_table,
                0, 0, 0, "FAILED", run_timestamp, duration,
                error_message=str(exc)
            )
            results.append((curated_table, "FAILED", 0))

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("  BATCH 1 SUMMARY")
    print(f"{'='*60}")
    for tbl, status, count in results:
        icon = "✓" if status == "SUCCESS" else "✗"
        print(f"  {icon} {tbl:<30} {count:>10,} rows  [{status}]")
    print(f"\n  Total rows in curated : {total_inserted:,}")
    print(f"  batch_id              : {batch_id}")
    print(
        f"\nNext step:\n"
        f"  python pipeline/03_batch2_incremental.py --project {project}\n"
    )


if __name__ == "__main__":
    main()
