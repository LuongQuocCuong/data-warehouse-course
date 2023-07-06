WITH map_bridge AS( 
SELECT 
  date 
  ,'Current year' AS comparing_type
  , date AS date_for_joining
FROM `data-warehouse-course-384003.wide_world_importers_dwh.dim_date`
UNION ALL
SELECT
   CAST((date + INTERVAL 1 YEAR) AS DATE) AS date
  , 'Last year' AS comparing_type
  , date  AS date_for_joining
FROM `data-warehouse-course-384003.wide_world_importers_dwh.dim_date`
UNION ALL
SELECT
    DATE_TRUNC(date , MONTH) 
    , 'Current month' AS comparing_type
    , DATE_TRUNC(date , MONTH) AS date_for_joining
FROM `data-warehouse-course-384003.wide_world_importers_dwh.dim_date`
UNION ALL
SELECT
    CAST(DATE_TRUNC(date , MONTH) + INTERVAL 1 MONTH AS DATE)
    , 'Last month' AS comparing_type
    , DATE_TRUNC(date , MONTH) AS date_for_joining
FROM `data-warehouse-course-384003.wide_world_importers_dwh.dim_date`
)
SELECT *
FROM map_bridge
WHERE date >= CAST('2013-01-01' AS DATE FORMAT 'YYYY-MM-DD')
