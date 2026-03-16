-- curated_order_reviews.sql
-- Reads from olist_raw.raw_order_reviews
-- Transformations:
--   - Normalize date fields to TIMESTAMP
--   - Trim text fields, replace empty strings with NULL
--   - Cast review_score to INT64, guard out-of-range (1–5)
--   - Deduplicate by review_id (keep most recent answer)

CREATE OR REPLACE TABLE `{project}.olist_curated.order_reviews`
AS
WITH deduped AS (
  SELECT
    review_id,
    order_id,
    CAST(review_score AS INT64)                                AS review_score,
    NULLIF(TRIM(review_comment_title),   '')                   AS review_comment_title,
    NULLIF(TRIM(review_comment_message), '')                   AS review_comment_message,
    CAST(review_creation_date    AS TIMESTAMP)                 AS review_creation_date,
    CAST(review_answer_timestamp AS TIMESTAMP)                 AS review_answer_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY review_id
      ORDER BY CAST(review_answer_timestamp AS TIMESTAMP) DESC
    ) AS _row_num
  FROM `{project}.olist_raw.raw_order_reviews`
  WHERE review_id IS NOT NULL
    AND order_id  IS NOT NULL
    AND CAST(review_score AS INT64) BETWEEN 1 AND 5
)

SELECT
  review_id,
  order_id,
  review_score,
  review_comment_title,
  review_comment_message,
  review_creation_date,
  review_answer_timestamp,

  -- Traceability
  'olist_raw.raw_order_reviews'  AS _source_file,
  CURRENT_TIMESTAMP()            AS _load_timestamp,
  '{batch_id}'                   AS _batch_id

FROM deduped
WHERE _row_num = 1
