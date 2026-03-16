-- incremental_order_reviews.sql
MERGE `{project}.olist_curated.order_reviews` AS target
USING (
  SELECT
    r.review_id,
    r.order_id,
    CAST(r.review_score AS INT64)              AS review_score,
    NULLIF(TRIM(r.review_comment_title),   '') AS review_comment_title,
    NULLIF(TRIM(r.review_comment_message), '') AS review_comment_message,
    CAST(r.review_creation_date    AS TIMESTAMP) AS review_creation_date,
    CAST(r.review_answer_timestamp AS TIMESTAMP) AS review_answer_timestamp
  FROM `{project}.olist_raw.raw_order_reviews` r
  INNER JOIN `{project}.olist_curated.orders` o USING (order_id)
  WHERE o._batch_id = '{batch_id}'
    AND r.review_id IS NOT NULL
    AND r.order_id  IS NOT NULL
    AND CAST(r.review_score AS INT64) BETWEEN 1 AND 5
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY r.review_id
    ORDER BY CAST(r.review_answer_timestamp AS TIMESTAMP) DESC
  ) = 1
) AS source
ON target.review_id = source.review_id

WHEN MATCHED AND target.review_score != source.review_score THEN
  UPDATE SET
    review_score           = source.review_score,
    review_comment_message = source.review_comment_message,
    _load_timestamp        = CURRENT_TIMESTAMP(),
    _batch_id              = '{batch_id}'

WHEN NOT MATCHED THEN
  INSERT (
    review_id, order_id, review_score,
    review_comment_title, review_comment_message,
    review_creation_date, review_answer_timestamp,
    _source_file, _load_timestamp, _batch_id
  )
  VALUES (
    source.review_id, source.order_id, source.review_score,
    source.review_comment_title, source.review_comment_message,
    source.review_creation_date, source.review_answer_timestamp,
    'olist_raw.raw_order_reviews', CURRENT_TIMESTAMP(), '{batch_id}'
  )
