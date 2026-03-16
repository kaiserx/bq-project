# Part F: Production Improvements & Recommendations

## Overview

The pipeline built in this assessment is functional and reproducible. However, moving it to a
production environment requires additional engineering to ensure sustainability, scalability,
observability, and governance. The following recommendations are grouped by concern.

---

## 1. Table Partitioning & Clustering

**Problem:** As order volume grows, full table scans become expensive and slow.

**Recommendation:**
- Partition `olist_curated.orders` by `order_purchase_timestamp` (monthly granularity).
  This limits scan cost to the relevant time window per query.
- Cluster `orders` by `order_status` — the most common filter in operational queries.
- Partition `order_reviews` by `review_creation_date`.
- Cluster `order_items` by `seller_id` and `product_id` to accelerate seller/product analytics.

```sql
-- Example: partitioned + clustered orders table
CREATE TABLE `project.olist_curated.orders`
PARTITION BY DATE_TRUNC(order_purchase_timestamp, MONTH)
CLUSTER BY order_status, customer_state
(
  order_id STRING NOT NULL,
  ...
);
```

**Impact:** Reduces query cost by 60–90% for time-bounded queries; mandatory for BigQuery cost control at scale.

---

## 2. Automated Quality Alerts

**Problem:** DQ checks currently only report results — they don't alert anyone when something breaks.

**Recommendation:**
- Schedule DQ checks via **BigQuery Scheduled Queries** or **Cloud Composer (Airflow)** after each pipeline run.
- Route CRITICAL/ERROR failures to **Cloud Monitoring** custom metrics + alerting policies.
- Send Slack or email notifications using **Cloud Pub/Sub → Cloud Functions → Webhook**.
- Define acceptable thresholds per check (e.g., "orphan items < 0.1% is acceptable").

**Example alert rule:** If `affected_count / total_count > 0.001` for `orphan_order_items`, trigger PagerDuty incident.

---

## 3. Re-processing & Failure Handling

**Problem:** If a pipeline run fails mid-way, the curated layer may be partially updated with no clean way to retry.

**Recommendation:**
- Make every pipeline run **fully idempotent** using `batch_id`:
  - Before writing to curated, delete any rows from the current `batch_id` (if re-running).
  - Use `MERGE` with `batch_id` tracking so partial runs can be safely replayed.
- Implement a **pipeline state machine** in `olist_ops.pipeline_run_log`:
  - States: `STARTED → IN_PROGRESS → SUCCESS / FAILED`
  - On re-run: detect `FAILED` batches and resume from the last successful table.
- Store raw files in **GCS** (not local), so the raw source is always available for replay.
- Implement **dead-letter queues** for records that fail transformation — store them in `olist_ops.rejected_records` with rejection reason and batch_id.

---

## 4. Schema Versioning

**Problem:** Source schemas change over time (new columns, renamed fields, type changes). Currently there is no mechanism to detect or handle this.

**Recommendation:**
- Track schema snapshots in `olist_ops.schema_versions` after each load:
  - Store: `table_name`, `batch_id`, `column_name`, `data_type`, `is_nullable`, `captured_at`
- Compare incoming schema against the last known version before running transformations.
- If a **breaking change** is detected (dropped column, type change), halt the pipeline and alert.
- If a **non-breaking change** is detected (new nullable column), log a warning and continue.
- Use **BigQuery `INFORMATION_SCHEMA.COLUMNS`** to automate schema capture.

```sql
-- Capture schema snapshot
INSERT INTO `project.olist_ops.schema_versions`
SELECT
  table_name, column_name, data_type, is_nullable,
  '{batch_id}' AS batch_id, CURRENT_TIMESTAMP() AS captured_at
FROM `project.olist_raw.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name IN ('raw_orders', 'raw_order_items', ...);
```

---

## 5. Data Contracts

**Problem:** Downstream consumers (analysts, dashboards, ML models) have no formal guarantee about what data they will receive from the curated layer.

**Recommendation:**
- Define **data contracts** as YAML files per curated table, specifying:
  - Expected columns and types
  - Null rate thresholds (e.g., `order_status` must be non-null 100%)
  - Value domain constraints (e.g., `review_score` ∈ [1,5])
  - Row count SLAs (e.g., daily order count should be > 500)
  - Freshness guarantees (e.g., orders data must be updated within 24h)
- Validate contracts as part of the DQ check pipeline.
- Publish contracts to an internal data catalog so consumers can self-serve.

```yaml
# Example: contracts/orders.yaml
table: olist_curated.orders
owner: data-engineering
sla_freshness_hours: 24
columns:
  - name: order_id
    type: STRING
    nullable: false
  - name: order_status
    type: STRING
    nullable: false
    allowed_values: [delivered, shipped, canceled, invoiced, processing, approved, unavailable, created]
  - name: order_purchase_timestamp
    type: TIMESTAMP
    nullable: false
row_count_min_daily: 500
```

---

## 6. Security & Governance

**Problem:** The current pipeline has no access controls, PII handling, or audit trail beyond pipeline logs.

**Recommendation:**

### Access Control
- Use **BigQuery column-level security** to mask PII fields:
  - `customer_zip_code_prefix`, `customer_city` in `orders` should only be visible to authorized roles.
  - Use **Policy Tags** in BigQuery Data Catalog to enforce this automatically.
- Apply **row-level security** if multi-tenant access is needed (e.g., sellers can only see their own data).
- Separate service accounts: one for pipeline writes, one read-only for analysts.

### Data Lineage
- Register all datasets and transformations in **Google Cloud Data Catalog** or **Dataplex**.
- Tag curated tables with: `source_system`, `data_classification` (PII / non-PII), `owner`, `refresh_frequency`.
- Use **BigQuery Audit Logs** (via Cloud Logging) to track who queried what and when.

### Audit Trail
- Retain `olist_ops.pipeline_run_log` and `olist_ops.dq_results` indefinitely (cheap columnar storage).
- Set **table expiration policies** on raw layer: e.g., auto-expire raw tables after 90 days once curated is validated.
- Enable **BigQuery time travel** (default 7 days) for point-in-time recovery of curated tables.

---

## 7. Orchestration

**Problem:** Scripts are currently run manually in sequence. In production, this must be automated, monitored, and retried on failure.

**Recommendation:**
- Use **Cloud Composer (managed Airflow)** or **Prefect** to orchestrate the pipeline:
  - DAG: `upload_raw → batch_transform → dq_checks → alert_if_failed`
  - Schedule: daily at a fixed time (e.g., 02:00 UTC)
  - SLA monitoring: alert if DAG hasn't completed by 04:00 UTC
- Alternatively, **Cloud Workflows** for simpler serverless orchestration without a persistent Airflow cluster.

---

## Summary Table

| Area | Priority | Effort | Impact |
|---|---|---|---|
| Partitioning & clustering | High | Low | Immediate cost reduction |
| Automated DQ alerts | High | Medium | Proactive issue detection |
| Idempotent re-processing | High | Medium | Reliability & recoverability |
| Schema versioning | Medium | Medium | Stability for downstream |
| Data contracts | Medium | Medium | Consumer trust & SLA clarity |
| Column-level security (PII) | High | Low | Compliance & data governance |
| Orchestration (Airflow) | High | High | Full production readiness |
