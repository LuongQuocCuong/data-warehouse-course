WITH dim_supplier__source AS (
SELECT 
  *
FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)

, dim_supplier__rename_column AS (
SELECT 
   supplier_id AS supplier_key
  ,supplier_name	AS supplier_name
  ,supplier_category_id AS supplier_category_key
  ,delivery_city_id AS supplier_delivery_city_key
FROM dim_supplier__source
)

, dim_supplier__cast_data AS (
SELECT
  CAST (supplier_key AS integer) AS supplier_key
  , CAST (supplier_name AS string) AS supplier_name
  , CAST (supplier_category_key AS integer) AS supplier_category_key
  , CAST (supplier_delivery_city_key AS integer) AS supplier_delivery_city_key
FROM dim_supplier__rename_column
)

, dim_supplier__add_undefined_record AS (
SELECT
  supplier_key
  ,  supplier_name
  , supplier_category_key
  , supplier_delivery_city_key
FROM dim_supplier__cast_data

UNION ALL 
  SELECT
    0 AS supplier_key
    , 'Undefined' AS supplier_name
    , 0 AS supplier_category_key
    , 0 AS supplier_delivery_city_key

, UNION ALL 
  SELECT
    -1 AS supplier_key
    , 'Invalid' AS supplier_name
    , -1 AS supplier_category_key
    , -1 AS supplier_delivery_city_key
)

SELECT 
  dim_supllier.supplier_key
  , COALESCE (dim_supllier.supplier_name , 'Undefined') AS supplier_name
  , COALESCE (dim_supllier.supplier_category_key, 0 ) AS supplier_category_key
  , supplier_category.supplier_category_name
  , COALESCE (dim_supllier.supplier_delivery_city_key,0) AS supplier_delivery_city_key
  , stg_location.supplier_delivery_city_name
  , stg_location.supplier_delivery_province_name
  , stg_location.supplier_delivery_country_name
  , COALESCE (stg_location.supplier_delivery_province_key,0) AS supplier_delivery_province_key
  , COALESCE (stg_location.supplier_delivery_country_key,0) AS supplier_delivery_country_key
FROM dim_supplier__add_undefined_record AS dim_supllier
LEFT JOIN {{ref('stg_supplier_category')}} AS supplier_category
  ON dim_supllier.supplier_category_key = supplier_category.supplier_category_key
LEFT JOIN {{ref('stg_location')}} AS stg_location
  ON stg_location.supplier_delivery_city_key = dim_supllier.supplier_delivery_city_key