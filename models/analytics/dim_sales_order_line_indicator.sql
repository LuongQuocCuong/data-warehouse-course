WITH indicator_table__source as (
SELECT * FROM {{ref('stg_dim_undersupply_back_ordered')}}
  CROSS JOIN {{ref('stg_dim_package_type')}}
)

, indicator_table__add_column_key AS (
SELECT
  FARM_FINGERPRINT(CONCAT(is_undersupply_back_ordered_key,"-",package_type_key)) as sales_order_line_indicator_key
  , is_undersupply_back_ordered_name
  , package_type_key
  , package_type_name
FROM indicator_table__source
)

, indicator_table__add_undifined_record AS (
SELECT
  sales_order_line_indicator_key
  , is_undersupply_back_ordered_name
  , package_type_key
  , package_type_name
FROM indicator_table__add_column_key
UNION ALL
  SELECT
    0 AS sales_order_line_indicator_key
    , 'Undefined' AS is_undersupply_back_ordered_name
    , 0 AS package_type_key
    , 'Undefined' AS package_type_name
, UNION ALL
  SELECT
    -1 AS sales_order_line_indicator_key
    , 'Invalid' AS is_undersupply_back_ordered_name
    , -1 AS package_type_key
    , 'Invalid' AS package_type_name
)

SELECT
*
FROM indicator_table__add_undifined_record