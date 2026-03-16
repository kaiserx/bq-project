-- curated_order_items.sql
-- Reads from olist_raw.raw_order_items
-- Transformations:
--   - Normalize shipping_limit_date to TIMESTAMP
--   - Cast price & freight_value to FLOAT64, default nulls to 0.0
--   - Deduplicate by (order_id, order_item_id) – keep highest price (data anomaly guard)
--   - Traceability fields

CREATE OR REPLACE TABLE `{project}.olist_curated.order_items`
AS
WITH deduped AS (
  SELECT
    order_id,
    CAST(order_item_id AS INT64)              AS order_item_id,
    product_id,
    seller_id,
    CAST(shipping_limit_date AS TIMESTAMP)    AS shipping_limit_date,
    COALESCE(CAST(price         AS FLOAT64), 0.0) AS price,
    COALESCE(CAST(freight_value AS FLOAT64), 0.0) AS freight_value,
    ROW_NUMBER() OVER (
      PARTITION BY order_id, order_item_id
      ORDER BY CAST(price AS FLOAT64) DESC
    ) AS _row_num
  FROM `{project}.olist_raw.raw_order_items`
  WHERE order_id IS NOT NULL
    AND order_item_id IS NOT NULL
)

SELECT
  order_id,
  order_item_id,
  product_id,
  seller_id,
  shipping_limit_date,
  price,
  freight_value,
  price + freight_value                 AS total_item_value,

  -- Traceability
  'olist_raw.raw_order_items'           AS _source_file,
  CURRENT_TIMESTAMP()                   AS _load_timestamp,
  '{batch_id}'                          AS _batch_id

FROM deduped
WHERE _row_num = 1
