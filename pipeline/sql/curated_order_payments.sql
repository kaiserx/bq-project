-- curated_order_payments.sql
-- Reads from olist_raw.raw_order_payments
-- Transformations:
--   - Standardize payment_type (lowercase, trim)
--   - Cast numeric fields, null → 0
--   - Deduplicate by (order_id, payment_sequential)

CREATE OR REPLACE TABLE `{project}.olist_curated.order_payments`
AS
WITH deduped AS (
  SELECT
    order_id,
    CAST(payment_sequential   AS INT64)                AS payment_sequential,
    LOWER(TRIM(payment_type))                          AS payment_type,
    COALESCE(CAST(payment_installments AS INT64), 0)   AS payment_installments,
    COALESCE(CAST(payment_value        AS FLOAT64), 0.0) AS payment_value,
    ROW_NUMBER() OVER (
      PARTITION BY order_id, payment_sequential
      ORDER BY CAST(payment_value AS FLOAT64) DESC
    ) AS _row_num
  FROM `{project}.olist_raw.raw_order_payments`
  WHERE order_id IS NOT NULL
)

SELECT
  order_id,
  payment_sequential,
  payment_type,
  payment_installments,
  payment_value,

  -- Traceability
  'olist_raw.raw_order_payments'  AS _source_file,
  CURRENT_TIMESTAMP()             AS _load_timestamp,
  '{batch_id}'                    AS _batch_id

FROM deduped
WHERE _row_num = 1
