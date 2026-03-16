-- incremental_order_payments.sql
MERGE `{project}.olist_curated.order_payments` AS target
USING (
  SELECT
    p.order_id,
    CAST(p.payment_sequential   AS INT64)               AS payment_sequential,
    LOWER(TRIM(p.payment_type))                         AS payment_type,
    COALESCE(CAST(p.payment_installments AS INT64), 0)  AS payment_installments,
    COALESCE(CAST(p.payment_value        AS FLOAT64), 0.0) AS payment_value
  FROM `{project}.olist_raw.raw_order_payments` p
  INNER JOIN `{project}.olist_curated.orders` o USING (order_id)
  WHERE o._batch_id = '{batch_id}'
    AND p.order_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY p.order_id, CAST(p.payment_sequential AS INT64)
    ORDER BY CAST(p.payment_value AS FLOAT64) DESC
  ) = 1
) AS source
ON target.order_id = source.order_id
AND target.payment_sequential = source.payment_sequential

WHEN MATCHED AND target.payment_value != source.payment_value THEN
  UPDATE SET
    payment_value   = source.payment_value,
    payment_type    = source.payment_type,
    _load_timestamp = CURRENT_TIMESTAMP(),
    _batch_id       = '{batch_id}'

WHEN NOT MATCHED THEN
  INSERT (
    order_id, payment_sequential, payment_type,
    payment_installments, payment_value,
    _source_file, _load_timestamp, _batch_id
  )
  VALUES (
    source.order_id, source.payment_sequential, source.payment_type,
    source.payment_installments, source.payment_value,
    'olist_raw.raw_order_payments', CURRENT_TIMESTAMP(), '{batch_id}'
  )
