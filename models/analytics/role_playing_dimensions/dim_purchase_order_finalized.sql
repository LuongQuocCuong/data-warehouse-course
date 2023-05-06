WITH finalized_indicator_true AS(
SELECT 
  'true' AS order_finalized_boolean
  ,'Order Finalized' AS order_finalized_name
)
, finalized_indicator_false AS(
SELECT * 
FROM finalized_indicator_true
UNION ALL
  SELECT  
    'false' AS order_finalized_boolean
    , 'Order Not Finalized' AS order_finalized_name
)

SELECT
  order_finalized_boolean
  , order_finalized_name
FROM finalized_indicator_false