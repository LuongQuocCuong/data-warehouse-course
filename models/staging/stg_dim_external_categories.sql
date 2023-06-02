WITH external_categories__rename AS (
SELECT  
  category_id AS category_key
  , category_name
  , parent_category_id AS parent_category_key
  , category_level
FROM `vit-lam-data.wide_world_importers.external__categories` 
)

, external_categories__cast_data AS (
SELECT
  CAST(category_key AS INTEGER) AS category_key
  , CAST (category_name AS STRING) AS category_name
  , CAST (parent_category_key AS INTEGER) AS parent_category_key
  , CAST (category_level AS INTEGER) AS category_level
FROM external_categories__rename
)

, external_categories__null_handle AS (
SELECT
  COALESCE(category_key, 0) AS category_key
  , COALESCE(category_name, 'Invalid') AS category_name
  , COALESCE(parent_category_key, 0) AS parent_category_key
  , COALESCE(category_level, 0) AS category_level
FROM external_categories__cast_data
)

, external_categories__add_invalid_record AS(
SELECT 
  * 
FROM external_categories__cast_data
UNION ALL
  SELECT
    -1 AS category_key
    , 'Invalid' AS category_name
    , -1 AS parent_category_key
    , -1 AS category_level
UNION ALL
  SELECT
    0 AS category_key
    , 'Undefined' AS category_name
    , 0 AS parent_category_key
    , 0 AS category_level
)

SELECT
  COALESCE(dim_category.category_key, 0) AS category_key
  , COALESCE(dim_category.category_name, 'Invalid') AS category_name
  , COALESCE(dim_category.parent_category_key,0) AS parent_category_key
  , COALESCE(dim_parent_category.category_name, 'Invalied') AS parent_category_name
  , COALESCE(dim_category.category_level,0) AS category_level
FROM external_categories__add_invalid_record AS dim_category
LEFT JOIN external_categories__add_invalid_record AS dim_parent_category
  ON dim_category.parent_category_key = dim_parent_category.category_key
ORDER BY category_key