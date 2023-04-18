with fact_sale__soure as (
select * 
from `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sale_rename_column as (
select
  stock_item_id as product_key
  ,order_line_id AS sales_order_line_key
  ,quantity as quantity
  ,unit_price as unit_price
  ,order_id as sale_order_key
from fact_sale__soure
)

, fact_sale_cast_data as (
select
  cast (product_key as integer) as product_key
  ,cast (sales_order_line_key as integer) as sales_order_line_key
  ,cast (quantity as integer) as quantity
  ,cast (unit_price as numeric) as unit_price
  ,cast (sale_order_key as integer) as sale_order_key
from fact_sale_rename_column
)

SELECT 
  product_key
, sales_order_line_key
, quantity
, unit_price
, sale_order_key
,quantity * unit_price as gross_amount
 FROM fact_sale_cast_data