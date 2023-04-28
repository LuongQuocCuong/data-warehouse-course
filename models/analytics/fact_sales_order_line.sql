with fact_sale__soure as (
select * 
from `vit-lam-data.wide_world_importers.sales__order_lines` 
)

, fact_sale_rename_column as (
SELECT
	order_line_id as order_line_key
  , order_id as order_key
  , stock_item_id as product_key
  , package_type_id as package_type_key
  , quantity as quantity
  , unit_price 
  , picked_quantity
  , tax_rate
  , picking_completed_when
FROM fact_sale__soure
)

, fact_sale_cast_data as (
select
  cast (order_line_key as integer) as order_line_key
  , cast (order_key as integer) as order_key
  , cast (product_key as integer) as product_key
  , cast (package_type_key as integer) as package_type_key
  , cast (quantity as integer) as quantity
  , cast (unit_price as numeric) as unit_price
  , cast (picked_quantity as integer) as picked_quantity
  , cast (tax_rate as numeric) as tax_rate
  , cast (picking_completed_when as date) as picking_completed_when
from fact_sale_rename_column
)

, fact_sale__undefined_handle as (
SELECT
  coalesce (order_line_key, 0) as order_line_key
  , coalesce (order_key, 0) as order_key
  , coalesce (product_key, 0) as product_key
  , coalesce (package_type_key, 0) as package_type_key
  , coalesce (quantity, 0) as quantity
  , coalesce (picked_quantity, 0) as picked_quantity
  , coalesce (tax_rate, 0) as tax_rate
  , coalesce (unit_price, 0) as unit_price
  , picking_completed_when
FROM fact_sale_cast_data
)

SELECT 
   fact_line.order_line_key
, fact_line.order_key
, fact_line.product_key
, fact_line.package_type_key
, coalesce( stg_sale_order.customer_key , 0) as customer_key
, coalesce( stg_sale_order.picked_by_person_key , 0) as picked_by_person_key
, coalesce( stg_sale_order.sales_person_key , 0) as sales_person_key
, coalesce( stg_sale_order.contact_person_key ,0) as contact_person_key
, fact_line.picking_completed_when as line_picking_completed_when
, stg_sale_order.picking_completed_when as order_picking_completed_when
, stg_sale_order.is_undersupply_back_ordered
, stg_sale_order.order_date
, stg_sale_order.expected_delivery_date
, fact_line.picked_quantity
, fact_line.quantity
, fact_line.unit_price
, fact_line.tax_rate/100 as tax_rate
, fact_line.quantity * fact_line.unit_price as gross_amount
, fact_line.quantity * fact_line.unit_price * fact_line.tax_rate/100 as tax_amount
, (fact_line.quantity * fact_line.unit_price) - (fact_line.quantity * fact_line.unit_price * fact_line.tax_rate/100) as net_amount
FROM fact_sale__undefined_handle as fact_line
LEFT JOIN {{ref('stg_sale_order')}} as stg_sale_order
  ON fact_line.order_key = stg_sale_order.order_key