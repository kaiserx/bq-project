"""
PART A: Initial Setup
Creates BigQuery datasets and uploads raw CSVs to olist_raw.

Usage:
    python setup/01_create_datasets_and_upload_raw.py \
        --project YOUR_PROJECT_ID \
        --data_dir /path/to/csv/folder
"""

import argparse
import io
import sys
from pathlib import Path

from google.cloud import bigquery
from google.api_core.exceptions import Conflict

# ─── Config ──────────────────────────────────────────────────────────────────

DATASETS = {
    "olist_raw":     "Raw layer – CSVs loaded as-is from source files",
    "olist_curated": "Curated layer – clean, transformed, traceable data",
    "olist_ops":     "Ops layer – pipeline run logs, metrics, DQ results",
}

CSV_TABLE_MAP = {
    "olist_orders_dataset.csv":               "raw_orders",
    "olist_order_items_dataset.csv":          "raw_order_items",
    "olist_order_payments_dataset.csv":       "raw_order_payments",
    "olist_order_reviews_dataset.csv":        "raw_order_reviews",
    "olist_customers_dataset.csv":            "raw_customers",
    "olist_products_dataset.csv":             "raw_products",
    "olist_sellers_dataset.csv":              "raw_sellers",
    "product_category_name_translation.csv":  "raw_product_category_translation",
}

# Files with messy free-text: allow up to N bad records instead of failing the load
LENIENT_FILES = {
    "raw_order_reviews": 1000,
}

# Files that must have their BOM stripped before upload
# (BOM causes BigQuery autodetect to name the first column '﻿column_name')
BOM_FILES = {
    "raw_product_category_translation",
}

LOCATION = "US"


# ─── Helpers ─────────────────────────────────────────────────────────────────

def create_datasets(client: bigquery.Client, project: str) -> None:
    for name, description in DATASETS.items():
        dataset_id = f"{project}.{name}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = LOCATION
        dataset.description = description
        try:
            client.create_dataset(dataset, timeout=30)
            print(f"  ✓ Created dataset: {dataset_id}")
        except Conflict:
            print(f"  ~ Dataset already exists: {dataset_id}")


def read_file_bytes(csv_path: Path, table_name: str) -> bytes:
    """Read file bytes, stripping UTF-8 BOM if present."""
    raw = csv_path.read_bytes()
    if table_name in BOM_FILES:
        # Strip UTF-8 BOM (EF BB BF) if present so column names are clean
        if raw.startswith(b"\xef\xbb\xbf"):
            raw = raw[3:]
            print(f"    (BOM stripped from {csv_path.name})")
    return raw


def upload_csv_to_raw(
    client: bigquery.Client,
    project: str,
    csv_path: Path,
    table_name: str,
) -> None:
    """Upload a single CSV file to olist_raw with auto-detected schema."""
    table_id = f"{project}.olist_raw.{table_name}"
    max_bad = LENIENT_FILES.get(table_name, 0)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        null_marker="",
        allow_quoted_newlines=True,
        max_bad_records=max_bad,
    )

    file_bytes = read_file_bytes(csv_path, table_name)

    job = client.load_table_from_file(
        io.BytesIO(file_bytes), table_id, job_config=job_config
    )
    job.result()

    table = client.get_table(table_id)
    note = f"  ⚠ up to {max_bad} malformed rows may have been skipped" if max_bad else ""
    print(f"  ✓ Loaded {table.num_rows:,} rows → {table_id}{note}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Olist pipeline – Part A setup")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument(
        "--data_dir",
        required=True,
        help="Directory containing the Olist CSV files",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.is_dir():
        print(f"ERROR: data_dir '{data_dir}' does not exist.")
        sys.exit(1)

    client = bigquery.Client(project=args.project)

    print("\n[1/2] Creating BigQuery datasets…")
    create_datasets(client, args.project)

    print("\n[2/2] Uploading CSVs to olist_raw…")
    missing = []
    for filename, table_name in CSV_TABLE_MAP.items():
        csv_path = data_dir / filename
        if not csv_path.exists():
            print(f"  ✗ File not found, skipping: {filename}")
            missing.append(filename)
            continue
        print(f"  → Loading {filename}…")
        upload_csv_to_raw(client, args.project, csv_path, table_name)

    print("\n─── Setup complete ───────────────────────────────────────────────")
    print(f"  Tables loaded : {len(CSV_TABLE_MAP) - len(missing)}/{len(CSV_TABLE_MAP)}")
    if missing:
        print(f"  Files missing : {', '.join(missing)}")
    print(
        "\nNext step:\n"
        "  python pipeline/02_batch1_full_load.py "
        "--project YOUR_PROJECT_ID\n"
    )


if __name__ == "__main__":
    main()
