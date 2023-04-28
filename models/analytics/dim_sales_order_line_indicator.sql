WITH compile_table as (
SELECT * FROM `data-warehouse-course-384003.wide_world_importers_dwh_staging.stg_undersupply_back_ordered` 
  CROSS JOIN `data-warehouse-course-384003.wide_world_importers_dwh_staging.stg_package_type`
)
SELECT
  FARM_FINGERPRINT(CONCAT(is_undersupply_back_ordered_key,"-",package_type_key)) as sales_order_line_indicator_key
  ,*
FROM compile_table