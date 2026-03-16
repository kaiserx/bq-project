-- curated_products.sql
-- Reads from olist_raw.raw_products + raw_product_category_translation
-- Transformations:
--   - Join English category names
--   - Cast numeric fields, null → NULL (not 0, as missing dimensions are meaningful)
--   - Deduplicate by product_id

CREATE OR REPLACE TABLE `{project}.olist_curated.products`
AS
WITH deduped_products AS (
  SELECT
    product_id,
    LOWER(TRIM(product_category_name))          AS product_category_name_pt,
    CAST(product_name_lenght        AS INT64)   AS product_name_length,
    CAST(product_description_lenght AS INT64)   AS product_description_length,
    CAST(product_photos_qty         AS INT64)   AS product_photos_qty,
    CAST(product_weight_g           AS FLOAT64) AS product_weight_g,
    CAST(product_length_cm          AS FLOAT64) AS product_length_cm,
    CAST(product_height_cm          AS FLOAT64) AS product_height_cm,
    CAST(product_width_cm           AS FLOAT64) AS product_width_cm,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY product_id  -- deterministic tie-break
    ) AS _row_num
  FROM `{project}.olist_raw.raw_products`
  WHERE product_id IS NOT NULL
),

translations AS (
  SELECT
    LOWER(TRIM(product_category_name))         AS category_pt,
    LOWER(TRIM(product_category_name_english)) AS category_en
  FROM `{project}.olist_raw.raw_product_category_translation`
)

SELECT
  p.product_id,
  p.product_category_name_pt,
  COALESCE(t.category_en, 'unknown')           AS product_category_name_en,
  p.product_name_length,
  p.product_description_length,
  p.product_photos_qty,
  p.product_weight_g,
  p.product_length_cm,
  p.product_height_cm,
  p.product_width_cm,

  -- Derived: volume in cm³
  CASE
    WHEN p.product_length_cm IS NOT NULL
      AND p.product_height_cm IS NOT NULL
      AND p.product_width_cm  IS NOT NULL
    THEN p.product_length_cm * p.product_height_cm * p.product_width_cm
    ELSE NULL
  END AS product_volume_cm3,

  -- Traceability
  'olist_raw.raw_products'  AS _source_file,
  CURRENT_TIMESTAMP()       AS _load_timestamp,
  '{batch_id}'              AS _batch_id

FROM deduped_products p
LEFT JOIN translations t ON p.product_category_name_pt = t.category_pt
WHERE p._row_num = 1
