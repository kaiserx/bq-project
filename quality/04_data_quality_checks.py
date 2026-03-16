"""
PART E: Data Quality Checks
Runs 6 DQ validations against olist_curated tables and writes results to
olist_ops.dq_results.

Usage:
    python quality/04_data_quality_checks.py --project YOUR_PROJECT_ID
"""

import argparse
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from google.cloud import bigquery

# ─── Check definitions ────────────────────────────────────────────────────────
# Each check is a dict with:
#   name, description, table, severity, sql (returns: affected_count, total_count, sample_ids)

def build_checks(project: str) -> list[dict]:
    return [
        # ── Check 1: Null mandatory fields in orders ──────────────────────────
        {
            "name": "null_mandatory_fields_orders",
            "description": "Orders with NULL order_id, customer_id, or order_status",
            "table": "orders",
            "severity": "CRITICAL",
            "sql": f"""
                SELECT
                  COUNTIF(order_id IS NULL OR customer_id IS NULL OR order_status IS NULL)
                    AS affected_count,
                  COUNT(*) AS total_count,
                  TO_JSON_STRING(
                    ARRAY(
                      SELECT order_id FROM `{project}.olist_raw.raw_orders`
                      WHERE order_id IS NULL OR customer_id IS NULL OR order_status IS NULL
                      LIMIT 5
                    )
                  ) AS sample_ids
                FROM `{project}.olist_raw.raw_orders`
            """,
        },

        # ── Check 2: order_status values outside valid catalog ────────────────
        {
            "name": "invalid_order_status",
            "description": "Orders with order_status not in approved catalog values",
            "table": "orders",
            "severity": "WARNING",
            "sql": f"""
                WITH valid_statuses AS (
                  SELECT status FROM UNNEST([
                    'delivered','shipped','canceled','invoiced',
                    'processing','approved','unavailable','created'
                  ]) AS status
                )
                SELECT
                  COUNTIF(o.order_status NOT IN (SELECT status FROM valid_statuses))
                    AS affected_count,
                  COUNT(*) AS total_count,
                  TO_JSON_STRING(
                    ARRAY(
                      SELECT order_id
                      FROM `{project}.olist_curated.orders`
                      WHERE order_status NOT IN (SELECT status FROM valid_statuses)
                      LIMIT 5
                    )
                  ) AS sample_ids
                FROM `{project}.olist_curated.orders` o
            """,
        },

        # ── Check 3: Orphan order_items (order_id not in orders) ─────────────
        {
            "name": "orphan_order_items",
            "description": "order_items rows whose order_id has no matching order in curated.orders",
            "table": "order_items",
            "severity": "ERROR",
            "sql": f"""
                SELECT
                  COUNT(*) AS affected_count,
                  (SELECT COUNT(*) FROM `{project}.olist_curated.order_items`) AS total_count,
                  TO_JSON_STRING(
                    ARRAY(
                      SELECT i.order_id
                      FROM `{project}.olist_curated.order_items` i
                      LEFT JOIN `{project}.olist_curated.orders` o USING (order_id)
                      WHERE o.order_id IS NULL
                      LIMIT 5
                    )
                  ) AS sample_ids
                FROM `{project}.olist_curated.order_items` i
                LEFT JOIN `{project}.olist_curated.orders` o USING (order_id)
                WHERE o.order_id IS NULL
            """,
        },

        # ── Check 4: Logical duplicates in raw orders ─────────────────────────
        {
            "name": "duplicate_order_ids_raw",
            "description": "order_id values that appear more than once in raw_orders",
            "table": "raw_orders",
            "severity": "ERROR",
            "sql": f"""
                WITH dup_counts AS (
                  SELECT order_id, COUNT(*) AS cnt
                  FROM `{project}.olist_raw.raw_orders`
                  GROUP BY order_id
                  HAVING cnt > 1
                )
                SELECT
                  SUM(cnt - 1)  AS affected_count,
                  (SELECT COUNT(*) FROM `{project}.olist_raw.raw_orders`) AS total_count,
                  TO_JSON_STRING(
                    ARRAY(SELECT order_id FROM dup_counts LIMIT 5)
                  ) AS sample_ids
                FROM dup_counts
            """,
        },

        # ── Check 5: Invalid date ranges (delivered before purchased) ─────────
        {
            "name": "invalid_date_range_delivery",
            "description": "Orders where delivered_customer_date < purchase_timestamp",
            "table": "orders",
            "severity": "WARNING",
            "sql": f"""
                SELECT
                  COUNTIF(
                    order_delivered_customer_date IS NOT NULL
                    AND order_delivered_customer_date < order_purchase_timestamp
                  ) AS affected_count,
                  COUNT(*) AS total_count,
                  TO_JSON_STRING(
                    ARRAY(
                      SELECT order_id
                      FROM `{project}.olist_curated.orders`
                      WHERE order_delivered_customer_date IS NOT NULL
                        AND order_delivered_customer_date < order_purchase_timestamp
                      LIMIT 5
                    )
                  ) AS sample_ids
                FROM `{project}.olist_curated.orders`
            """,
        },

        # ── Check 6: Orders with payments but no items (or vice versa) ────────
        {
            "name": "payments_without_items",
            "description": "Orders that have payment records but no item records in curated",
            "table": "order_payments / order_items",
            "severity": "WARNING",
            "sql": f"""
                WITH orders_with_payments AS (
                  SELECT DISTINCT order_id FROM `{project}.olist_curated.order_payments`
                ),
                orders_with_items AS (
                  SELECT DISTINCT order_id FROM `{project}.olist_curated.order_items`
                )
                SELECT
                  COUNT(*) AS affected_count,
                  (SELECT COUNT(DISTINCT order_id) FROM `{project}.olist_curated.order_payments`)
                    AS total_count,
                  TO_JSON_STRING(
                    ARRAY(
                      SELECT p.order_id
                      FROM orders_with_payments p
                      LEFT JOIN orders_with_items i USING (order_id)
                      WHERE i.order_id IS NULL
                      LIMIT 5
                    )
                  ) AS sample_ids
                FROM orders_with_payments p
                LEFT JOIN orders_with_items i USING (order_id)
                WHERE i.order_id IS NULL
            """,
        },
    ]


# ─── Runner ──────────────────────────────────────────────────────────────────

def run_check(
    client: bigquery.Client,
    project: str,
    batch_id: str,
    check: dict,
    check_timestamp: datetime,
) -> dict:
    """Execute a single DQ check and return a result dict."""
    try:
        rows = list(client.query(check["sql"]).result())
        row = rows[0] if rows else None

        affected = int(row.affected_count) if row and row.affected_count else 0
        total    = int(row.total_count)    if row and row.total_count    else 0
        samples  = row.sample_ids          if row                         else "[]"
        status   = "PASS" if affected == 0 else "FAIL"

    except Exception as exc:
        affected = -1
        total    = 0
        samples  = "[]"
        status   = "ERROR"
        print(f"  ⚠ Check '{check['name']}' raised an exception: {exc}")

    return {
        "batch_id":          batch_id,
        "check_name":        check["name"],
        "check_description": check["description"],
        "table_name":        check["table"],
        "severity":          check["severity"],
        "status":            status,
        "affected_count":    affected,
        "total_count":       total,
        "sample_ids":        samples,
        "check_timestamp":   check_timestamp.isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Olist – DQ checks")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument(
        "--batch_id",
        default=None,
        help="Optional: link DQ run to an existing batch_id (defaults to new UUID)",
    )
    args = parser.parse_args()

    project = args.project
    client  = bigquery.Client(project=project)
    batch_id = args.batch_id or str(uuid.uuid4())
    check_timestamp = datetime.now(timezone.utc)

    print(f"\n{'='*60}")
    print(f"  Olist Data Quality Checks")
    print(f"  batch_id : {batch_id}")
    print(f"  started  : {check_timestamp.isoformat()}")
    print(f"{'='*60}\n")

    checks = build_checks(project)
    results = []

    for i, check in enumerate(checks, 1):
        print(f"[{i}/{len(checks)}] {check['name']}  ({check['severity']})")
        result = run_check(client, project, batch_id, check, check_timestamp)
        results.append(result)

        icon = "✓" if result["status"] == "PASS" else ("✗" if result["status"] == "FAIL" else "⚠")
        pct  = f"{result['affected_count']/result['total_count']*100:.2f}%" if result["total_count"] else "n/a"
        print(f"    {icon} {result['status']}  |  affected: {result['affected_count']:,} / {result['total_count']:,}  ({pct})")
        if result["status"] == "FAIL" and result["sample_ids"] not in ("[]", ""):
            print(f"    sample IDs: {result['sample_ids']}")

    # ── Write to ops ──────────────────────────────────────────────────────────
    print("\n[→] Writing results to olist_ops.dq_results…")
    errors = client.insert_rows_json(
        f"{project}.olist_ops.dq_results", results
    )
    if errors:
        print(f"  ⚠ Insert errors: {errors}")
    else:
        print(f"  ✓ {len(results)} check results written.")

    # ── Summary ───────────────────────────────────────────────────────────────
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    print(f"\n{'='*60}")
    print(f"  DQ SUMMARY:  {passed} PASS  |  {failed} FAIL  |  {len(results)} total")
    print(f"{'='*60}\n")

    # Exit with non-zero if any CRITICAL check failed
    critical_fails = [
        r for r in results
        if r["status"] == "FAIL" and r["severity"] == "CRITICAL"
    ]
    if critical_fails:
        print("  ✗ CRITICAL checks failed:")
        for r in critical_fails:
            print(f"    - {r['check_name']}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
