SELECT
  FARM_FiNGERPRINT(CONCAT(order_finalized_boolean,'-',order_line_finalized_boolean)) AS finalized_key
  , *
FROM {{ref('dim_purchase_order_finalized')}} 
CROSS JOIN {{ref('dim_purchase_order_line_finalized')}}
UNION ALL
  SELECT
    0 AS finalized_key 
    , 'Undifined' AS order_finalized_boolean
    , 'Undifined' AS order_finalized_name
    , 'Undifined' AS order_line_finalized_boolean
    , 'Undifined' AS order_line_finalized_name
, UNION ALL
  SELECT
    -1 AS finalized_key 
    , 'Invallid' AS order_finalized_boolean
    , 'Invallid' AS order_finalized_name
    , 'Invallid' AS order_line_finalized_boolean
    , 'Invallid' AS order_line_finalized_name