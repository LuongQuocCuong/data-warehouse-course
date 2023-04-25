with stg_location__source as (
SELECT * 
FROM `vit-lam-data.wide_world_importers.application__cities`
)

, stg_location__rename_column as(
SELECT
  city_id as city_key
  , city_name
FROM stg_location__source
)

, stg_location__cast_data as (
SELECT
  cast (city_key as integer) as city_key
  , cast (city_name as string) as city_name
FROM stg_location__rename_column
)

, stg_location_add_undefined_record as (
SELECT 
  city_key
  ,city_name
FROM stg_location__cast_data
UNION ALL
  SELECT
    0 as city_key
    ,'Undefined' as city_name
, UNION ALL
  SELECT
    -1 as city_key
    ,'Invalid' as city_name
)

SELECT
  city_key
  ,city_name
FROM stg_location_add_undefined_record