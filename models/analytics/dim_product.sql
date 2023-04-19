with dim_product__source as (
SELECT * 
FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

,dim_product__rename_column as (
select 
  stock_item_id as product_key
  ,stock_item_name as product_name
  ,brand as brand_name
  ,supplier_id as supplier_key
  ,is_chiller_stock as is_chiller_stock
from dim_product__source
)

, dim_product__cast_type as (
select
  CAST(product_key AS INTEGER) as product_key
  , CAST(product_name AS STRING) as product_name
  , CAST(brand_name AS STRING) as brand_name
  , CAST(supplier_key AS INTEGER) AS supplier_key
  , CAST (is_chiller_stock as boolean ) as is_chiller_stock_boolean
from dim_product__rename_column
)

, dim_product__convert_is_chiller_stock as (
  select 
    *,
    case
      when is_chiller_stock_boolean is true then 'Chiller Stock'
      when is_chiller_stock_boolean is false then 'Not Chiller Stock'
      when is_chiller_stock_boolean is null then 'undefined'
      else 'invalid'
    end as is_chiller_stock
  from dim_product__cast_type
)

SELECT 
  dim_product.product_key
  , dim_product.product_name
  , coalesce(dim_product.brand_name , 'Undifined') as brand_name
  , dim_product.supplier_key
  , dim_supplier.supplier_name
  , dim_product.is_chiller_stock
FROM dim_product__convert_is_chiller_stock as dim_product
LEFT JOIN {{ref('dim_supplier')}} as dim_supplier
on dim_product.supplier_key = dim_supplier.supplier_key