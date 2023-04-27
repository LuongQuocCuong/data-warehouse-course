with dim_supplier__source as (
SELECT 
  *
FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)

, dim_supplier__rename_column as (
SELECT 
   supplier_id as supplier_key
  ,supplier_name	as supplier_name
  ,supplier_category_id as supplier_category_key
  ,delivery_city_id as supplier_delivery_city_key
FROM dim_supplier__source
)

, dim_supplier__cast_data as (
SELECT
  cast(supplier_key as integer) as supplier_key
  , cast (supplier_name as string) as supplier_name
  , cast (supplier_category_key as integer) as supplier_category_key
  , cast (supplier_delivery_city_key as integer) as supplier_delivery_city_key
FROM dim_supplier__rename_column
)

select 
  dim_supllier.supplier_key
  , dim_supllier.supplier_name
  , dim_supllier.supplier_category_key
  , supplier_category.supplier_category_name
  , dim_supllier.supplier_delivery_city_key
  , stg_location.supplier_delivery_city_name
  , stg_location.supplier_delivery_province_name
  , stg_location.supplier_delivery_country_name

FROM dim_supplier__cast_data as dim_supllier
LEFT JOIN {{ref('stg_supplier_category')}} as supplier_category
  ON dim_supllier.supplier_category_key = supplier_category.supplier_category_key
LEFT JOIN {{ref('stg_location')}} as stg_location
  ON stg_location.supplier_delivery_city_key = dim_supllier.supplier_delivery_city_key