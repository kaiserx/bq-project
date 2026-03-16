-- incremental_order_items.sql
-- MERGE for order_items based on orders watermark.
-- Only processes items belonging to orders newer than watermark.

MERGE `{project}.olist_curated.order_items` AS target
USING (
  SELECT
    i.order_id,
    CAST(i.order_item_id AS INT64)               AS order_item_id,
    i.product_id,
    i.seller_id,
    CAST(i.shipping_limit_date AS TIMESTAMP)     AS shipping_limit_date,
    COALESCE(CAST(i.price          AS FLOAT64), 0.0) AS price,
    COALESCE(CAST(i.freight_value  AS FLOAT64), 0.0) AS freight_value,
    COALESCE(CAST(i.price AS FLOAT64), 0.0)
      + COALESCE(CAST(i.freight_value AS FLOAT64), 0.0) AS total_item_value
  FROM `{project}.olist_raw.raw_order_items` i
  INNER JOIN `{project}.olist_curated.orders` o USING (order_id)
  WHERE o._batch_id = '{batch_id}'   -- only items linked to newly processed orders
    AND i.order_id IS NOT NULL
    AND i.order_item_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY i.order_id, CAST(i.order_item_id AS INT64)
    ORDER BY CAST(i.price AS FLOAT64) DESC
  ) = 1
) AS source
ON target.order_id = source.order_id
AND target.order_item_id = source.order_item_id

WHEN MATCHED AND target.price != source.price THEN
  UPDATE SET
    price            = source.price,
    freight_value    = source.freight_value,
    total_item_value = source.total_item_value,
    _load_timestamp  = CURRENT_TIMESTAMP(),
    _batch_id        = '{batch_id}'

WHEN NOT MATCHED THEN
  INSERT (
    order_id, order_item_id, product_id, seller_id,
    shipping_limit_date, price, freight_value, total_item_value,
    _source_file, _load_timestamp, _batch_id
  )
  VALUES (
    source.order_id, source.order_item_id, source.product_id, source.seller_id,
    source.shipping_limit_date, source.price, source.freight_value, source.total_item_value,
    'olist_raw.raw_order_items', CURRENT_TIMESTAMP(), '{batch_id}'
  )
