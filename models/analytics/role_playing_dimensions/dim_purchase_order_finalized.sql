WITH finalized_indicator_true AS(
SELECT 
  'true' AS is_order_line_finalized_boolean
  ,'Order Finalized' AS is_order_finalized
)
, finalized_indicator_false AS(
SELECT * 
FROM finalized_indicator_true
UNION ALL
  SELECT  
    'false' AS is_order_line_finalized_boolean
    , 'Order Not Finalized' AS is_order_finalized
)

, finalized_indicator_cast_data AS (
SELECT
  CAST(FARM_FiNGERPRINT(is_order_line_finalized_boolean) AS INTEGER) AS order_finalized_key
  , is_order_finalized AS order_finalized_name
FROM finalized_indicator_false
)

, finalized_indicator_add_undefined_record AS (
SELECT
  order_finalized_key
  , order_finalized_name
FROM finalized_indicator_cast_data
UNION ALL
  SELECT
    0 AS order_finalized_key
    , 'Undefined' AS order_finalized_name
, UNION ALL
  SELECT
    -1 AS order_finalized_key
    , 'Invalid' AS order_finalized_name
)

SELECT
  order_finalized_key
  , order_finalized_name
FROM finalized_indicator_add_undefined_record