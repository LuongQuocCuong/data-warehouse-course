WITH fact_purchase_order_line__source AS (
SELECT * 
FROM `vit-lam-data.wide_world_importers.purchasing__purchase_order_lines`
)

, fact_purchase_order_line__rename_column AS (
SELECT
  purchase_order_line_id as purchase_order_line_key
  , purchase_order_id as purchase_order_key
  , stock_item_id as product_key
  , package_type_id as package_type_key
  , is_order_line_finalized 
  , last_receipt_date
  , received_outers
  , ordered_outers
  , expected_unit_price_per_outer
FROM fact_purchase_order_line__source
)

, fact_purchase_order_line__cast_data AS(
SELECT
  CAST(purchase_order_line_key AS INTEGER) AS purchase_order_line_key
  , CAST(purchase_order_key AS INTEGER) AS purchase_order_key
  , CAST(product_key AS INTEGER) AS product_key
  , CAST(package_type_key AS INTEGER) AS package_type_key
  , CAST(is_order_line_finalized AS BOOLEAN) AS is_order_line_finalized_boolean
  , CAST(last_receipt_date AS DATE) AS last_receipt_date
  , CAST(received_outers AS INTEGER) AS received_outers
  , CAST(ordered_outers AS INTEGER) AS ordered_outers
  , CAST(expected_unit_price_per_outer AS NUMERIC) AS expected_unit_price_per_outer
FROM fact_purchase_order_line__rename_column
)

, fact_purchase_order_line__convert_boolean AS (
SELECT
  *
  , CASE 
      WHEN is_order_line_finalized_boolean is TRUE THEN 'Order Line Finalized'
      WHEN is_order_line_finalized_boolean is FALSE THEN 'Order Line Not Finalized'
      ELSE 'Invalid'
    END AS is_order_line_finalized
FROM fact_purchase_order_line__cast_data
)

, fact_purchase_order_line__handle_null AS (
SELECT 
  purchase_order_line_key 
  , COALESCE(purchase_order_key , 0) AS purchase_order_key
  , COALESCE(product_key , 0) AS product_key
  , COALESCE(package_type_key , 0) AS package_type_key
  , COALESCE(is_order_line_finalized , 'Undefined') AS is_order_line_finalized
  , last_receipt_date
  , received_outers
  , ordered_outers
  , expected_unit_price_per_outer
FROM fact_purchase_order_line__convert_boolean
)


SELECT 
  purchase_fact_head.purchase_order_line_key
  , purchase_fact_head.purchase_order_key
FROM fact_purchase_order_line__handle_null AS purchase_fact_head
