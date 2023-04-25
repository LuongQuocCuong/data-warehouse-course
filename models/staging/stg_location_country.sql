with stg_location__source as (
SELECT * 
FROM `vit-lam-data.wide_world_importers.application__state_provinces`
)

, stg_location__rename_column as(
SELECT
  state_province_id as state_province_key
  , state_province_name 
FROM stg_location__source
)

, stg_location__cast_data as (
SELECT
  cast (state_province_key as integer) as state_province_key
  , cast (state_province_name as string) as state_province_name
FROM stg_location__rename_column
)

, stg_location_add_undefined_record as (
SELECT 
  state_province_key
  ,state_province_name
FROM stg_location__cast_data
UNION ALL
  SELECT
    0 as state_province_key
    ,'Undefined' as state_province_name
, UNION ALL
  SELECT
    -1 as state_province_key
    ,'Invalid' as state_province_name
)

SELECT 
  state_province_key
  ,state_province_name
FROM stg_location_add_undefined_record