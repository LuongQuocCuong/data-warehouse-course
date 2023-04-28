WITH stg_sale_order__source as (
SELECT *
FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, stg_sale_order__rename as (
SELECT
  order_id as order_key
  , customer_id as customer_key
  , salesperson_person_id AS sales_person_key
  , picked_by_person_id as picked_by_person_key
  , contact_person_id as contact_person_key
  , order_date
  , expected_delivery_date
  , picking_completed_when
  , is_undersupply_backordered
FROM stg_sale_order__source
)

, stg_sale_order__cast_data as (
SELECT
  cast(order_key as integer) as order_key
  , cast(customer_key as integer) as customer_key
  , cast (sales_person_key as integer) as sales_person_key
  , cast (picked_by_person_key as integer) as picked_by_person_key
  , cast (contact_person_key as integer) as contact_person_key
  , cast (order_date as date) as order_date
  , cast (expected_delivery_date as date) as expected_delivery_date
  , cast (picking_completed_when as date) as picking_completed_when
  , cast (is_undersupply_backordered as boolean) as is_undersupply_backordered_boolean
  , cast (is_undersupply_backordered as boolean) as is_undersupply_backordered_boolean_key
FROM stg_sale_order__rename
)

, stg_sale_order__convert_boolean as (
SELECT
  *
  , CASE
      WHEN is_undersupply_backordered_boolean is TRUE then 'Under Supply Back Ordered'
      WHEN is_undersupply_backordered_boolean is FALSE then 'Not Under Supply Back Ordered'
      ELSE 'Invalid'
    END AS is_undersupply_back_ordered
FROM stg_sale_order__cast_data
)

, stg_sale_order__Unifined_handle as (
SELECT
  coalesce (order_key , 0) as order_key
  , coalesce (customer_key , 0) as customer_key
  , coalesce (sales_person_key , 0) as sales_person_key
  , coalesce (picked_by_person_key , 0) as picked_by_person_key
  , coalesce (contact_person_key , 0) as contact_person_key
  , order_date
  , expected_delivery_date
  , picking_completed_when
  , is_undersupply_back_ordered
  , is_undersupply_backordered_boolean_key
FROM stg_sale_order__convert_boolean
) 

SELECT 
  order_key
  , customer_key
  , sales_person_key
  , picked_by_person_key
  , contact_person_key
  , order_date
  , expected_delivery_date
  , picking_completed_when
  , is_undersupply_back_ordered
  , is_undersupply_backordered_boolean_key
FROM stg_sale_order__Unifined_handle