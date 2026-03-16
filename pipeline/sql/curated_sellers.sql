-- curated_sellers.sql
-- Reads from olist_raw.raw_sellers
-- Transformations:
--   - Normalize city/state casing
--   - Deduplicate by seller_id

CREATE OR REPLACE TABLE `{project}.olist_curated.sellers`
AS
WITH deduped AS (
  SELECT
    seller_id,
    CAST(seller_zip_code_prefix AS STRING)  AS seller_zip_code_prefix,
    LOWER(TRIM(seller_city))                AS seller_city,
    UPPER(TRIM(seller_state))               AS seller_state,
    ROW_NUMBER() OVER (
      PARTITION BY seller_id
      ORDER BY seller_id
    ) AS _row_num
  FROM `{project}.olist_raw.raw_sellers`
  WHERE seller_id IS NOT NULL
)

SELECT
  seller_id,
  seller_zip_code_prefix,
  seller_city,
  seller_state,

  -- Traceability
  'olist_raw.raw_sellers'  AS _source_file,
  CURRENT_TIMESTAMP()      AS _load_timestamp,
  '{batch_id}'             AS _batch_id

FROM deduped
WHERE _row_num = 1
