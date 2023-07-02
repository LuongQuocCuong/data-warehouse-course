WITH dim_product__source AS (
SELECT * 
FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

,dim_product__rename_column AS (
SELECT 
  stock_item_id AS product_key
  , stock_item_name AS product_name
  , brand 
  , size
  , lead_time_days
  , quantity_per_outer
  , is_chiller_stock
  , tax_rate
  , supplier_id AS supplier_key
  , unit_price
  ,	recommended_retail_price
  , unit_package_id AS unit_package_key
  , outer_package_id AS outer_package_key
  , color_id AS color_key
FROM dim_product__source
)

, dim_product__cast_type AS (
SELECT
  CAST(product_key AS INTEGER) AS product_key
  , CAST(product_name AS STRING) AS product_name
  , CAST(brand AS STRING) AS brand
  , CAST(size AS STRING) AS size
  , CAST(lead_time_days AS INTEGER) AS lead_time_days
  , CAST(color_key AS INTEGER) AS color_key
  , CAST(quantity_per_outer AS INTEGER) AS quantity_per_outer
  , CAST (is_chiller_stock AS boolean ) AS is_chiller_stock_boolean
  , CAST (tax_rate AS numeric ) AS tax_rate
  , CAST (unit_price AS numeric ) AS unit_price
  , CAST (recommended_retail_price AS numeric ) AS recommended_retail_price
  , CAST(supplier_key AS INTEGER) AS supplier_key
  , CAST(unit_package_key AS INTEGER) AS unit_package_key
  , CAST(outer_package_key AS INTEGER) AS outer_package_key
FROM dim_product__rename_column
)

, dim_product__convert_is_chiller_stock AS (
  SELECT 
    *,
    CASE
      WHEN is_chiller_stock_boolean IS true THEN 'Chiller Stock'
      WHEN is_chiller_stock_boolean IS false THEN 'Not Chiller Stock'
      WHEN is_chiller_stock_boolean IS NULL THEN 'Undefined'
      else 'invalid'
    END AS is_chiller_stock
  FROM dim_product__cast_type
)

, dim_product__handle_null AS(
SELECT  
  product_key
  , COALESCE (product_name , 'Undefined') AS product_name
  , COALESCE (color_key,0) AS color_key
  , COALESCE (outer_package_key,0) AS outer_package_key
  , COALESCE ( brand , 'Undefined') AS brand
  , COALESCE (supplier_key, 0) AS supplier_key
  , is_chiller_stock
  , COALESCE ( size , 'Undefined') AS size
  , COALESCE ( lead_time_days , 0) AS lead_time_days
  , COALESCE ( quantity_per_outer ,0) AS quantity_per_outer
  , tax_rate
  , unit_price
  , recommended_retail_price
  , COALESCE (unit_package_key,0) AS unit_package_key
FROM dim_product__convert_is_chiller_stock
)

, dim_product__add_undefined_record AS (
SELECT
  product_key
  , product_name
  , color_key
  , outer_package_key
  , brand
  , supplier_key
  , is_chiller_stock
  , size
  , lead_time_days
  , quantity_per_outer
  , tax_rate
  , unit_price
  , recommended_retail_price
  , unit_package_key
FROM dim_product__handle_null
UNION ALL 
  SELECT
    0 AS product_key
    , 'Undefined' AS product_name
    , 0 AS color_key
    , 0 AS outer_package_key
    , 'Undefined' AS brand
    , 0 AS supplier_key
    , 'Undefined' AS is_chiller_stock
    , 'Undefined' AS size
    , 0 AS lead_time_days
    , 0 AS quantity_per_outer
    , 0 AS tax_rate
    , 0 AS unit_price
    , 0 AS recommended_retail_price
    , 0 AS unit_package_key

, UNION ALL
  SELECT
    -1 AS product_key
    , 'Invalid' AS product_name
    , -1 AS color_key
    , -1 AS outer_package_key
    , 'Invalid' AS brand
    , -1 AS supplier_key
    , 'Invalid' AS is_chiller_stock
    , 'Invalid' AS size
    , -1 AS lead_time_days
    , -1 AS quantity_per_outer
    , -1 AS tax_rate
    , -1 AS unit_price
    , -1 AS recommended_retail_price
    , -1 AS unit_package_key
)
SELECT 
  dim_product.product_key 
  , dim_product.product_name 
  , dim_product.brand
  , dim_product.size 
  , dim_product.lead_time_days 
  , dim_product.quantity_per_outer 
  , dim_product.is_chiller_stock
  , COALESCE (external_product.category_key , -1) AS category_key
  , COALESCE(dim_categories.category_name, 'Invalid') AS category_name
  , COALESCE(dim_categories.parent_category_key, -1) AS parent_category_key
  , COALESCE(dim_categories.parent_category_name, 'Invalid') AS parent_category_name
  , COALESCE(dim_categories.category_level, -1) AS category_level
  , dim_categories.category_level_1_key
  , dim_categories.category_level_1_name
  , dim_categories.category_level_2_key
  , dim_categories.category_level_2_name
  , dim_categories.category_level_3_key
  , dim_categories.category_level_3_name
  , dim_categories.category_level_4_key
  , dim_categories.category_level_4_name
  , dim_product.tax_rate 
  , dim_product.unit_price 
  , dim_product.recommended_retail_price 
  , dim_product.color_key
  , COALESCE ( color.color_name ,'Invalid') AS color_name
  , dim_product.outer_package_key
  , COALESCE ( stg_outer_package_type.package_type_name, 'Invalid') AS outer_package_name
  , dim_product.unit_package_key
  , COALESCE ( stg_unit_package_type.package_type_name , 'Invalid') AS unit_package_name
  , COALESCE ( dim_supplier.supplier_name , 'Invalid') AS supplier_name
  , dim_product.supplier_key
  , dim_supplier.supplier_category_key
  , COALESCE ( dim_supplier.supplier_category_name , 'Invalid') AS supplier_category_name
  , dim_supplier.supplier_delivery_city_key 
  , COALESCE ( dim_supplier.supplier_delivery_city_name , 'Invalid') AS supplier_delivery_city_name
  , dim_supplier.supplier_delivery_province_key 
  , COALESCE ( dim_supplier.supplier_delivery_province_name ,'Invalid') AS supplier_delivery_province_name
  , dim_supplier.supplier_delivery_country_key 
  , COALESCE ( dim_supplier.supplier_delivery_country_name , 'Invalid') AS supplier_delivery_country_name
FROM dim_product__add_undefined_record AS dim_product
LEFT JOIN {{ref('dim_supplier')}} AS dim_supplier
  ON dim_product.supplier_key = dim_supplier.supplier_key
LEFT JOIN {{ref('stg_dim_color')}} AS color
  ON color.color_key = dim_product.color_key 
LEFT JOIN {{ref('stg_dim_package_type')}} AS stg_outer_package_type
  ON stg_outer_package_type.package_type_key = dim_product.outer_package_key
LEFT JOIN {{ref('stg_dim_package_type')}} AS stg_unit_package_type
  ON stg_unit_package_type.package_type_key = dim_product.unit_package_key
LEFT JOIN {{ref('stg_dim_external_product')}} AS external_product
  USING (product_key)
LEFT JOIN {{ref('dim_category')}} AS dim_categories
  USING (category_key)