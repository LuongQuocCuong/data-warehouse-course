WITH finalized_indicator_true AS(
SELECT 
  'true' AS order_line_finalized_boolean
  ,'Order Line Finalized' AS order_line_finalized_name
)
, finalized_indicator_false AS(
SELECT * 
FROM finalized_indicator_true
UNION ALL
  SELECT  
    'false' AS order_line_finalized_boolean
    , 'Order Line Not Finalized' AS order_line_finalized_name
)

SELECT
  order_line_finalized_boolean
  , order_line_finalized_name
FROM finalized_indicator_false