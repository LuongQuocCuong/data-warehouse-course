with dim_product__source as (
SELECT * 
FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

,dim_product__rename_column as (
select 
  stock_item_id as product_key
  , stock_item_name as product_name
  , brand 
  , size
  , lead_time_days
  , quantity_per_outer
  , is_chiller_stock
  , tax_rate
  , supplier_id as supplier_key
  , unit_price
  ,	recommended_retail_price
  , unit_package_id as unit_package_key
  , outer_package_id as outer_package_key
  , color_id as color_key
from dim_product__source
)

, dim_product__cast_type as (
select
  CAST(product_key AS INTEGER) as product_key
  , CAST(product_name AS STRING) as product_name
  , CAST(brand AS STRING) as brand
  , CAST(size AS STRING) as size
  , CAST(lead_time_days AS INTEGER) as lead_time_days
  , CAST(color_key AS INTEGER) as color_key
  , CAST(quantity_per_outer AS INTEGER) as quantity_per_outer
  , CAST (is_chiller_stock as boolean ) as is_chiller_stock_boolean
  , CAST (tax_rate as numeric ) as tax_rate
  , CAST (unit_price as numeric ) as unit_price
  , CAST (recommended_retail_price as numeric ) as recommended_retail_price
  , CAST(supplier_key AS INTEGER) AS supplier_key
  , CAST(unit_package_key AS INTEGER) AS unit_package_key
  , CAST(outer_package_key AS INTEGER) AS outer_package_key
from dim_product__rename_column
)

, dim_product__convert_is_chiller_stock as (
  select 
    *,
    case
      when is_chiller_stock_boolean is true then 'Chiller Stock'
      when is_chiller_stock_boolean is false then 'Not Chiller Stock'
      when is_chiller_stock_boolean is null then 'Undefined'
      else 'invalid'
    end as is_chiller_stock
  from dim_product__cast_type
)

, dim_product__handle_null as(
select  
coalesce(color_key,0) as color_key
  , coalesce(product_key,0) as product_key
  , coalesce(outer_package_key,0) as outer_package_key
  , coalesce(product_name , 'Invalid') as product_name
  , coalesce( brand , 'Invalid') as brand
  , coalesce(supplier_key,0) as supplier_key
  , coalesce( is_chiller_stock, 'Invalid') as is_chiller_stock
  , coalesce( size , 'Invalid') as size
  , coalesce( lead_time_days , 0) as lead_time_days
  , coalesce( quantity_per_outer ,0) as quantity_per_outer
  , coalesce( tax_rate , 0) as tax_rate
  , coalesce( unit_price , 0) as unit_price
  , coalesce( recommended_retail_price , 0) as recommended_retail_price
  , coalesce(unit_package_key,0) as unit_package_key
from dim_product__convert_is_chiller_stock
)

SELECT 
  dim_product.product_key 
  , dim_product.product_name 
  , dim_product.brand
  , dim_product.size 
  , dim_product.lead_time_days 
  , dim_product.quantity_per_outer 
  , dim_product.is_chiller_stock 
  , dim_product.tax_rate 
  , dim_product.unit_price 
  , dim_product.recommended_retail_price 
  , dim_product.color_key
  , coalesce( color.color_name ,'Invalid') as color_name
  , dim_product.outer_package_key
  , coalesce( stg_outer_package_type.package_type_name, 'Invalid') as outer_package_name
  , dim_product.unit_package_key
  , coalesce( stg_unit_package_type.package_type_name , 'Invalid') as unit_package_name
  , coalesce( dim_supplier.supplier_name , 'Invalid') as supplier_name
  , dim_product.supplier_key
  , coalesce( dim_supplier.supplier_category_key , 0) as supplier_category_key
  , coalesce( dim_supplier.supplier_category_name , 'Invalid') as supplier_category_name
  , coalesce( dim_supplier.supplier_delivery_city_key , 0) as supplier_delivery_city_key
  , coalesce( dim_supplier.supplier_delivery_city_name , 'Invalid') as supplier_delivery_city_name
  , coalesce( dim_supplier.supplier_delivery_province_key , 0) as supplier_delivery_province_key
  , coalesce( dim_supplier.supplier_delivery_province_name ,'Invalid') as supplier_delivery_province_name
  , coalesce( dim_supplier.supplier_delivery_country_key , 0) as supplier_delivery_country_key
  , coalesce( dim_supplier.supplier_delivery_country_name , 'Invalid') as supplier_delivery_country_name
FROM dim_product__handle_null as dim_product
LEFT JOIN {{ref('dim_supplier')}} as dim_supplier
  ON dim_product.supplier_key = dim_supplier.supplier_key
LEFT JOIN {{ref("stg_color")}} as color
  ON color.color_key = dim_product.color_key 
LEFT JOIN {{ref('stg_package_type')}} as stg_outer_package_type
  ON stg_outer_package_type.package_type_key = dim_product.outer_package_key
LEFT JOIN {{ref('stg_package_type')}} as stg_unit_package_type
  ON stg_unit_package_type.package_type_key = dim_product.unit_package_key