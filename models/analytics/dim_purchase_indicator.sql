WITH order_finalized_indicator AS(
SELECT 
  'true' AS order_finalized_boolean
  ,'Order Finalized' AS order_finalized_name
UNION ALL
  SELECT  
    'false' AS order_finalized_boolean
    , 'Order Not Finalized' AS order_finalized_name
)

, order_line_finalized_indicator AS(
SELECT 
  'true' AS order_line_finalized_boolean
  ,'Order Line Finalized' AS order_line_finalized_name
UNION ALL
  SELECT  
    'false' AS order_line_finalized_boolean
    , 'Order Line Not Finalized' AS order_line_finalized_name
)
SELECT 
     FARM_FiNGERPRINT(CONCAT(order_line_finalized_boolean,'-',order_finalized_boolean,'-',delivery_method_key,'-',	
package_type_key)) AS purchase_indicator_key
    ,* 
FROM order_line_finalized_indicator
CROSS JOIN order_finalized_indicator
CROSS JOIN {{ref('stg_dim_delivery_method')}}
CROSS JOIN {{ref('stg_dim_package_type')}}
