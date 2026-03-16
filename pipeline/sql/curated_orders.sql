-- curated_orders.sql
-- Reads from olist_raw.raw_orders + raw_customers
-- Transformations:
--   - Normalize all timestamps to TIMESTAMP type
--   - Lowercase & trim order_status
--   - Explicit null handling for optional date fields
--   - Deduplicate by order_id (keep row with latest order_purchase_timestamp)
--   - Attach traceability fields

CREATE OR REPLACE TABLE `{project}.olist_curated.orders`
AS
WITH
-- Step 1: Deduplicate raw orders (keep latest per order_id)
deduped_orders AS (
  SELECT
    order_id,
    customer_id,
    LOWER(TRIM(order_status))           AS order_status,
    CAST(order_purchase_timestamp  AS TIMESTAMP)  AS order_purchase_timestamp,
    CAST(order_approved_at         AS TIMESTAMP)  AS order_approved_at,
    CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_date,
    CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
    CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY CAST(order_purchase_timestamp AS TIMESTAMP) DESC
    ) AS _row_num
  FROM `{project}.olist_raw.raw_orders`
  WHERE order_id IS NOT NULL
),

clean_orders AS (
  SELECT * EXCEPT(_row_num)
  FROM deduped_orders
  WHERE _row_num = 1
),

-- Step 2: Join customer info
customers AS (
  SELECT
    customer_id,
    customer_unique_id,
    CAST(customer_zip_code_prefix AS STRING) AS customer_zip_code_prefix,
    LOWER(TRIM(customer_city))  AS customer_city,
    UPPER(TRIM(customer_state)) AS customer_state
  FROM `{project}.olist_raw.raw_customers`
  WHERE customer_id IS NOT NULL
)

SELECT
  o.order_id,
  o.customer_id,
  c.customer_unique_id,
  c.customer_zip_code_prefix,
  c.customer_city,
  c.customer_state,
  o.order_status,
  o.order_purchase_timestamp,
  o.order_approved_at,
  o.order_delivered_carrier_date,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date,

  -- Derived: days from purchase to delivery
  CASE
    WHEN o.order_delivered_customer_date IS NOT NULL
    THEN DATE_DIFF(
      DATE(o.order_delivered_customer_date),
      DATE(o.order_purchase_timestamp),
      DAY
    )
    ELSE NULL
  END AS days_to_delivery,

  -- Derived: was delivery on time?
  CASE
    WHEN o.order_delivered_customer_date IS NOT NULL
      AND o.order_estimated_delivery_date IS NOT NULL
    THEN o.order_delivered_customer_date <= o.order_estimated_delivery_date
    ELSE NULL
  END AS delivered_on_time,

  -- Traceability
  'olist_raw.raw_orders'        AS _source_file,
  CURRENT_TIMESTAMP()           AS _load_timestamp,
  '{batch_id}'                  AS _batch_id

FROM clean_orders o
LEFT JOIN customers c USING (customer_id)
