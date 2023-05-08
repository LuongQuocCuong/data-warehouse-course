WITH Dim__undersupply_back_ordered AS (
 SELECT 
  TRUE AS is_undersupply_back_ordered_key
  ,'Under Supply Back Ordered' AS is_undersupply_back_ordered_name
)

SELECT 
    is_undersupply_back_ordered_key
    , is_undersupply_back_ordered_name
FROM Dim__undersupply_back_ordered
UNION ALL
  SELECT
    FALSE AS is_undersupply_back_ordered_key
    ,'Not Under Supply Back Ordered' AS is_undersupply_back_ordered_name