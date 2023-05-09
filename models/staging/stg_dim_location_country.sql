with stg_location__source as (
SELECT * 
FROM `vit-lam-data.wide_world_importers.application__countries`
)

, stg_location__rename_column as(
SELECT
  country_id as country_key
  , country_name
FROM stg_location__source
)

, stg_location__cast_data as (
SELECT
  cast (country_key as integer) as country_key
  , cast (country_name as string) as country_name
FROM stg_location__rename_column
)

, stg_location_add_undefined_record as (
SELECT 
  country_key
  ,country_name
FROM stg_location__cast_data
UNION ALL
  SELECT
    0 as country_key
    ,'Undefined' as country_name
, UNION ALL
  SELECT
    -1 as country_key
    ,'Invalid' as country_name
)

SELECT
  country_key
  ,country_name
FROM stg_location_add_undefined_record
