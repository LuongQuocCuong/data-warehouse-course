WITH fact_purchase_order__source AS (
SELECT * 
FROM `vit-lam-data.wide_world_importers.purchasing__purchase_orders`
)

, fact_purchase_order__rename_column AS (
SELECT
  purchase_order_id as purchase_order_key
  , supplier_id as supplier_key
  , order_date
  , delivery_method_id as delivery_method_key
  , contact_person_id as contact_person_key 
  , expected_delivery_date as expected_delivery_date
  , is_order_finalized
FROM fact_purchase_order__source
)

, fact_purchase_order__cast_data AS(
SELECT
  CAST(purchase_order_key AS INTEGER) AS purchase_order_key
  , CAST(supplier_key AS INTEGER) AS supplier_key
  , CAST(order_date AS DATE) AS order_date
  , CAST(delivery_method_key AS INTEGER) AS delivery_method_key
  , CAST(contact_person_key AS INTEGER) AS contact_person_key
  , CAST(expected_delivery_date AS DATE) AS expected_delivery_date
  , CAST(is_order_finalized AS BOOLEAN) AS is_order_finalized_boolean
FROM fact_purchase_order__rename_column
)

, fact_purchase_order__handle_null AS (
SELECT 
  purchase_order_key
  , COALESCE(supplier_key , 0) AS supplier_key
  , COALESCE(delivery_method_key , 0) AS delivery_method_key
  , COALESCE(contact_person_key , 0) AS contact_person_key
  , order_date
  , expected_delivery_date
  , is_order_finalized_boolean
FROM fact_purchase_order__cast_data
)

, fact_purchase_order__convert_boolean AS (
SELECT
  *
  , CASE 
      WHEN is_order_finalized_boolean is TRUE THEN 'Order Finalized'
      WHEN is_order_finalized_boolean is FALSE THEN 'Order Not Finalized'
      ELSE 'Invalid'
    END AS is_order_finalized
FROM fact_purchase_order__handle_null
) 

SELECT *
FROM fact_purchase_order__convert_boolean