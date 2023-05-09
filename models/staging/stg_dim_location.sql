with stg_location__source as (
SELECT * 
FROM `vit-lam-data.wide_world_importers.application__cities`
)

, stg_location__rename_column as(
SELECT
  city_id as city_key
  , city_name
  , state_province_id as state_province_key
FROM stg_location__source
)

, stg_location__cast_data as (
SELECT
  cast (city_key as integer) as city_key
  , cast (city_name as string) as city_name
  , cast (state_province_key as integer) as state_province_key

FROM stg_location__rename_column
)

, stg_location_add_undefined_record as (
SELECT 
  city_key
  , city_name
  , state_province_key
FROM stg_location__cast_data
UNION ALL
  SELECT
    0 as city_key
    , 'Undefined' as city_name
    , 0 as state_province_key
, UNION ALL
  SELECT
    -1 as city_key
    , 'Invalid' as city_name
    , -1 as state_province_key

)

SELECT
  city.city_key as supplier_delivery_city_key
  , city.city_name as supplier_delivery_city_name
  , coalesce(city.state_province_key,0) as supplier_delivery_province_key
  , province.province_name as supplier_delivery_province_name
  , coalesce(province.country_key,0) as supplier_delivery_country_key
  , country.country_name as supplier_delivery_country_name
FROM stg_location_add_undefined_record as city
LEFT JOIN {{ref('stg_dim_location_province')}} as province
  ON city.state_province_key = province.province_key
LEFT JOIN {{ref('stg_dim_location_country')}} as country
  ON province.country_key = country.country_key