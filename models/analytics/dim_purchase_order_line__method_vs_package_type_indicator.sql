SELECT 
FARM_FiNGERPRINT(CONCAT(delivery_method_key,'-',package_type_key)) AS purchase_order_line_indicator_key
, * 
FROM `data-warehouse-course-384003.wide_world_importers_dwh_staging.stg_delivery_method`
CROSS JOIN `data-warehouse-course-384003.wide_world_importers_dwh_staging.stg_package_type`
UNION ALL
  SELECT 
    0 AS order_line_indicator_key
    , 0 AS delivery_method_key
    , 'Undefined' AS delivery_method_name
    , 0 AS package_type_key
    , 'Undefined' AS package_type_name
,UNION ALL
  SELECT 
    -1 AS order_line_indicator_key
    , -1 AS delivery_method_key
    , 'Invalid' AS delivery_method_name
    , -1 AS package_type_key
    , 'Invalid' AS package_type_name