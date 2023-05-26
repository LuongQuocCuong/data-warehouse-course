WITH fact_target_gross_amount__source AS(
  SELECT 
    salesperson_person_key AS sales_person_key
    , year_month
    , target_gross_amount
  FROM {{ref('stg_fact_target_salesperson')}}
)
, fact_target_gross_amount__from_sale AS (
  SELECT
  sales_person_key
  , DATE_TRUNC (order_date, MONTH) AS year_month
  , SUM(gross_amount) AS gross_amount
FROM {{ref('fact_sales_order_line')}}
GROUP BY 1,2
)

, fact_target_gross_amount__join AS(
SELECT
    COALESCE (fact_sale.sales_person_key, fact_sale.sales_person_key) AS sales_person_key
  , COALESCE (fact_sale.year_month, fact_target.year_month) AS year_month
  , COALESCE(target_gross_amount,0) AS target_gross_amount
  , COALESCE(gross_amount,0) AS gross_amount
FROM fact_target_gross_amount__from_sale AS fact_sale
FULL OUTER JOIN fact_target_gross_amount__source AS fact_target
USING (sales_person_key, year_month)
)

, fact_target_gross_amount__calculate_achievement_percentage AS (
SELECT
  sales_person_key
  , year_month
  , target_gross_amount
  , gross_amount
  , (NULLIF(gross_amount,0)/NULLIF(target_gross_amount,0))*100 AS achievement_percentage
FROM fact_target_gross_amount__join
ORDER BY sales_person_key
)

, fact_target_gross_amount__achievement_status AS (
SELECT
  *
  ,CASE
    WHEN achievement_percentage >= 100 THEN 'Đạt Xuất Sắc'
    WHEN achievement_percentage >= 80 AND achievement_percentage < 100 THEN 'Đạt'
    WHEN achievement_percentage >= 60 AND achievement_percentage < 80 THEN 'Đạt Trung Bình'
    WHEN achievement_percentage < 60 THEN 'Chưa Đạt'
    ELSE 'Invalid'
  END AS achievement_status
FROM fact_target_gross_amount__calculate_achievement_percentage
)

SELECT
  *
FROM fact_target_gross_amount__achievement_status
