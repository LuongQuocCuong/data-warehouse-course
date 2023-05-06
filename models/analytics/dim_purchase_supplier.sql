WITH dim_supplier__source AS (
SELECT 
  *
FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)

, dim_supplier__rename_column AS (
SELECT 
   supplier_id AS supplier_key
  , supplier_name	AS supplier_name
  , supplier_category_id AS supplier_category_key
  , primary_contact_person_id AS primary_contact_person_key
  , alternate_contact_person_id AS alternate_contact_person_key
  , delivery_method_id AS delivery_method_key
  , delivery_city_id AS delivery_city_key
  , supplier_reference
  , payment_days
FROM dim_supplier__source
)

, dim_supplier__cast_data AS (
SELECT
  CAST (supplier_key AS integer) AS supplier_key
  , CAST (supplier_name AS string) AS supplier_name
  , CAST (supplier_category_key AS integer) AS supplier_category_key
  , CAST (primary_contact_person_key AS integer) AS primary_contact_person_key
  , CAST (alternate_contact_person_key AS integer) AS alternate_contact_person_key
  , CAST (delivery_method_key AS integer) AS delivery_method_key
  , CAST (delivery_city_key AS integer) AS delivery_city_key
  , CAST (supplier_reference AS string) AS supplier_reference
  , CAST (payment_days AS integer) AS payment_days
FROM dim_supplier__rename_column
)

, dim_supplier__add_undefined_record AS (
SELECT
   supplier_key
  , supplier_name
  , supplier_category_key
  , primary_contact_person_key
  , alternate_contact_person_key
  , delivery_method_key
  , delivery_city_key
  , supplier_reference
  , payment_days
FROM dim_supplier__cast_data

UNION ALL 
  SELECT
    0 AS supplier_key
    , 'Undefined' AS supplier_name
    , 0 AS supplier_category_key
    , 0 AS primary_contact_person_key
    , 0 AS alternate_contact_person_key
    , 0 AS delivery_method_key
    , 0 AS delivery_city_key
    , 'Undefined' AS supplier_reference
    , 0 AS payment_days

, UNION ALL 
  SELECT
    -1 AS supplier_key
    , 'Invalid' AS supplier_name
    , -1 AS supplier_category_key
    , -1 AS primary_contact_person_key
    , -1 AS alternate_contact_person_key
    , -1 AS delivery_method_key
    , -1 AS delivery_city_key
    , 'Invalid' AS supplier_reference
    , -1 AS payment_days
)

SELECT
  dim_supplier.supplier_key
  , COALESCE(dim_supplier.supplier_name, 'Invalid') AS supplier_name
  , COALESCE(dim_supplier.supplier_category_key, 0) AS supplier_category_key
  , supplier_category_name
  , COALESCE(dim_supplier.primary_contact_person_key ,0) AS primary_contact_person_key
  , COALESCE(dim_supplier.alternate_contact_person_key, 0) AS alternate_contact_person_key
  , COALESCE(dim_supplier.delivery_method_key, 0) AS delivery_method_key
  , delivery_method.delivery_method_name
  , COALESCE(dim_supplier.delivery_city_key,0) AS supplier_city_key
  , location.supplier_delivery_city_name AS supplier_city_name
  , COALESCE(location.supplier_delivery_province_key,0) AS supplier_province_key
  , location.supplier_delivery_province_name AS supplier_province_name
  , COALESCE(location.supplier_delivery_country_key,0) AS supplier_country_key
  , location.supplier_delivery_country_name AS supplier_country_name
  , COALESCE(dim_supplier.supplier_reference, 'Invalid') AS supplier_reference
  , COALESCE(dim_supplier.payment_days,0) AS payment_days
FROM dim_supplier__add_undefined_record AS dim_supplier
LEFT JOIN {{ref('stg_supplier_category')}} AS supplier_category
  ON supplier_category.supplier_category_key = dim_supplier.supplier_category_key
LEFT JOIN {{ref('stg_delivery_method')}} AS delivery_method
  ON delivery_method.delivery_method_key = dim_supplier.delivery_method_key
LEFT JOIN {{ref('stg_location')}} AS location
  ON location.supplier_delivery_province_key = dim_supplier.delivery_city_key
