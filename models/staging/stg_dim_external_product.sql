WITH external_stock_item__rename AS (
SELECT  
  stock_item_id AS product_key
  ,category_id AS category_key
FROM `vit-lam-data.wide_world_importers.external__stock_item` 
)

, external_stock_item__cast_data AS (
SELECT
  CAST(product_key AS INTEGER) AS product_key
  , CAST (category_key AS INTEGER) AS category_key
FROM external_stock_item__rename
)

, external_stock_item__null_handle AS (
SELECT
  COALESCE(product_key, -1) AS product_key
  , COALESCE(category_key, -1) AS category_key
FROM external_stock_item__cast_data
)

SELECT
*
FROM external_stock_item__null_handle