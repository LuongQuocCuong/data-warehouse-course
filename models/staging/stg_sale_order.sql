with stg_sale_order__source as (
select *
from `vit-lam-data.wide_world_importers.sales__orders`
)

, stg_sale_order__rename as (
select
  customer_id as customer_key
  ,order_id as sale_order_key
from stg_sale_order__source
)

, stg_sale_order__cast_data as (
select
  cast(customer_key as integer) as customer_key
  , cast(sale_order_key as integer) as sale_order_key
from stg_sale_order__rename
)

SELECT 
  customer_key
  ,sale_order_key
FROM stg_sale_order__cast_data