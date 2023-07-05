WITH fact_sale__soure AS (
SELECT * 
from `vit-lam-data.wide_world_importers.sales__order_lines` 
)

, fact_sale_rename_column AS (
SELECT
	order_line_id AS order_line_key
  , order_id AS order_key
  , stock_item_id AS product_key
  , package_type_id AS package_type_key
  , quantity AS quantity
  , unit_price 
  , picked_quantity
  , tax_rate
  , picking_completed_when
FROM fact_sale__soure
)

, fact_sale_cast_data AS (
SELECT
  CAST (order_line_key AS integer) AS order_line_key
  , CAST (order_key AS integer) AS order_key
  , CAST (product_key AS integer) AS product_key
  , CAST (package_type_key AS integer) AS package_type_key
  , CAST (quantity AS integer) AS quantity
  , CAST (unit_price AS numeric) AS unit_price
  , CAST (picked_quantity AS integer) AS picked_quantity
  , CAST (tax_rate AS numeric) AS tax_rate
  , CAST (picking_completed_when AS date) AS picking_completed_when
from fact_sale_rename_column
)

, fact_sale__undefined_handle AS (
SELECT
  COALESCE (order_line_key, 0) AS order_line_key
  , COALESCE (order_key, 0) AS order_key
  , COALESCE (product_key, 0) AS product_key
  , COALESCE (package_type_key, 0) AS package_type_key
  , COALESCE (quantity, 0) AS quantity
  , COALESCE (picked_quantity, 0) AS picked_quantity
  , COALESCE (tax_rate, 0) AS tax_rate
  , COALESCE (unit_price, 0) AS unit_price
  , picking_completed_when
FROM fact_sale_cast_data
)

SELECT 
   fact_line.order_line_key
, COALESCE (fact_line.order_key,0) AS order_key
, COALESCE (fact_line.product_key,0) AS product_key
, COALESCE (fact_line.package_type_key,0) AS package_type_key
, COALESCE (stg_sale_order.customer_id , 0) AS customer_id
, COALESCE(FARM_FiNGERPRINT(CONCAT(dim_customer.customer_id,dim_customer.begin_effective_date)),0) AS customer_key
, COALESCE (stg_sale_order.picked_by_person_key , 0) AS picked_by_person_key
, COALESCE (stg_sale_order.sales_person_key , 0) AS sales_person_key
, COALESCE (stg_sale_order.contact_person_key ,0) AS contact_person_key
, fact_line.picking_completed_when AS line_picking_completed_when
, stg_sale_order.picking_completed_when AS order_picking_completed_when
, COALESCE (FARM_FINGERPRINT(CONCAT(stg_sale_order.is_undersupply_back_ordered_boolean,"-",fact_line.package_type_key)),0) AS sales_order_line_indicator_key
, stg_sale_order.order_date
, stg_sale_order.expected_delivery_date
, fact_line.picked_quantity
, fact_line.quantity
, fact_line.unit_price
, fact_line.tax_rate/100 AS tax_rate
, fact_line.quantity * fact_line.unit_price AS gross_amount
, fact_line.quantity * fact_line.unit_price * fact_line.tax_rate/100 AS tax_amount
, (fact_line.quantity * fact_line.unit_price) - (fact_line.quantity * fact_line.unit_price * fact_line.tax_rate/100) AS net_amount
FROM fact_sale__undefined_handle AS fact_line
LEFT JOIN {{ref('stg_fact_sale_order')}} AS stg_sale_order
  ON fact_line.order_key = stg_sale_order.order_key
LEFT JOIN {{ref('dim_customer')}} AS dim_customer
  ON dim_customer.customer_id = stg_sale_order.customer_id
  AND stg_sale_order.order_date BETWEEN dim_customer.begin_effective_date AND dim_customer.end_effective_date