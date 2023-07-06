WITH dim_category AS (
  SELECT
    *
  FROM {{ref('dim_category')}}
)

SELECT 
   category_level_1_key AS parent_category_key
  , category_level_1_name AS parent_category_name
  ,  category_key AS child_category_key
  , category_name AS child_category_name
FROM dim_category 
UNION ALL
SELECT 
   category_level_2_key AS parent_category_key
  , category_level_2_name AS parent_category_name
  , category_key AS child_category_key
  , category_name AS child_category_name
FROM dim_category 
WHERE category_level_2_key <> 0
UNION ALL
SELECT 
   category_level_3_key AS parent_category_key
  , category_level_3_name AS parent_category_name
  , category_key AS child_category_key
  , category_name AS child_category_name
FROM dim_category 
WHERE category_level_3_key <> 0
UNION ALL
SELECT 
   category_level_4_key AS parent_category_key
  , category_level_4_name AS parent_category_name
  , category_key AS child_category_key
  , category_name AS child_category_name
FROM dim_category 
WHERE category_level_4_key <> 0
ORDER BY 1