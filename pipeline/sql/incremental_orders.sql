-- incremental_orders.sql
-- MERGE strategy for orders (Part D).
-- Incremental key: order_purchase_timestamp
-- Logic:
--   NEW   → order_id not present in curated
--   UPDATED → order_id exists but order_status has changed
--
-- Only processes raw rows with order_purchase_timestamp > watermark

MERGE `{project}.olist_curated.orders` AS target
USING (
  WITH raw_deduped AS (
    SELECT
      order_id,
      customer_id,
      LOWER(TRIM(order_status))                          AS order_status,
      CAST(order_purchase_timestamp       AS TIMESTAMP)  AS order_purchase_timestamp,
      CAST(order_approved_at              AS TIMESTAMP)  AS order_approved_at,
      CAST(order_delivered_carrier_date   AS TIMESTAMP)  AS order_delivered_carrier_date,
      CAST(order_delivered_customer_date  AS TIMESTAMP)  AS order_delivered_customer_date,
      CAST(order_estimated_delivery_date  AS TIMESTAMP)  AS order_estimated_delivery_date,
      ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY CAST(order_purchase_timestamp AS TIMESTAMP) DESC
      ) AS _rn
    FROM `{project}.olist_raw.raw_orders`
    WHERE order_id IS NOT NULL
  ),
  customers AS (
    SELECT
      customer_id,
      customer_unique_id,
      CAST(customer_zip_code_prefix AS STRING) AS customer_zip_code_prefix,
      LOWER(TRIM(customer_city))  AS customer_city,
      UPPER(TRIM(customer_state)) AS customer_state
    FROM `{project}.olist_raw.raw_customers`
    WHERE customer_id IS NOT NULL
  ),
  incoming AS (
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
      CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
        THEN DATE_DIFF(DATE(o.order_delivered_customer_date), DATE(o.order_purchase_timestamp), DAY)
        ELSE NULL
      END AS days_to_delivery,
      CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
          AND o.order_estimated_delivery_date IS NOT NULL
        THEN o.order_delivered_customer_date <= o.order_estimated_delivery_date
        ELSE NULL
      END AS delivered_on_time
    FROM raw_deduped o
    LEFT JOIN customers c USING (customer_id)
    WHERE o._rn = 1
      -- Only rows newer than the current watermark
      AND CAST(o.order_purchase_timestamp AS TIMESTAMP) > TIMESTAMP('{watermark}')
  )
  SELECT * FROM incoming
) AS source
ON target.order_id = source.order_id

-- Update existing orders whose status has changed
WHEN MATCHED AND target.order_status != source.order_status THEN
  UPDATE SET
    order_status                  = source.order_status,
    order_approved_at             = source.order_approved_at,
    order_delivered_carrier_date  = source.order_delivered_carrier_date,
    order_delivered_customer_date = source.order_delivered_customer_date,
    days_to_delivery              = source.days_to_delivery,
    delivered_on_time             = source.delivered_on_time,
    _load_timestamp               = CURRENT_TIMESTAMP(),
    _batch_id                     = '{batch_id}'

-- Insert brand new orders
WHEN NOT MATCHED THEN
  INSERT (
    order_id, customer_id, customer_unique_id,
    customer_zip_code_prefix, customer_city, customer_state,
    order_status, order_purchase_timestamp, order_approved_at,
    order_delivered_carrier_date, order_delivered_customer_date,
    order_estimated_delivery_date, days_to_delivery, delivered_on_time,
    _source_file, _load_timestamp, _batch_id
  )
  VALUES (
    source.order_id, source.customer_id, source.customer_unique_id,
    source.customer_zip_code_prefix, source.customer_city, source.customer_state,
    source.order_status, source.order_purchase_timestamp, source.order_approved_at,
    source.order_delivered_carrier_date, source.order_delivered_customer_date,
    source.order_estimated_delivery_date, source.days_to_delivery, source.delivered_on_time,
    'olist_raw.raw_orders', CURRENT_TIMESTAMP(), '{batch_id}'
  )
