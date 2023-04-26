with stg_location__source as (
SELECT * 
FROM `vit-lam-data.wide_world_importers.application__state_provinces`
)

, stg_location__rename_column as(
SELECT
  state_province_id as province_key
  , state_province_name as province_name
  , country_id as country_key
FROM stg_location__source
)

, stg_location__cast_data as (
SELECT
  cast (province_key as integer) as province_key
  , cast (province_name as string) as province_name
  , cast (country_key as integer) as country_key
FROM stg_location__rename_column
)

, stg_location_add_undefined_record as (
SELECT 
  province_key
  , province_name
  , country_key
FROM stg_location__cast_data
UNION ALL
  SELECT
    0 as province_key
    , 'Undefined' as province_name
    , 0 as country_key
, UNION ALL
  SELECT
    -1 as province_key
    , 'Invalid' as province_name
    , -1 as country_key
)

SELECT 
  province_key
  , province_name
  , country_key
FROM stg_location_add_undefined_record