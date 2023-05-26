WITH fact_customer_anallytics__summarize AS(
SELECT
 customer_key
  , SUM(gross_amount) AS lifetime_sale_amount
  , COUNT(DISTINCT(order_key) )AS lifetime_sale_orders
  , SUM(CASE
      WHEN order_date BETWEEN (DATE_TRUNC('2016-05-31' , MONTH) - INTERVAl 12 MONTH) AND '2016-05-31' THEN gross_amount
    END) AS l2months_sale_amount 
  , COUNT(DISTINCT(CASE
      WHEN order_date BETWEEN (DATE_TRUNC('2016-05-31' , MONTH) - INTERVAl 12 MONTH) AND '2016-05-31' Then order_key
    END)) AS l2months_sale_orders
FROM `data-warehouse-course-384003.wide_world_importers_dwh.fact_sales_order_line`
GROUP BY 1
)

, fact_customer_anallytics__add_percentitle AS (
SELECT 
  *
  , PERCENT_RANK() OVER(ORDER BY lifetime_sale_amount) AS percenttile_lifetime_sale_amount
  , PERCENT_RANK() OVER(ORDER BY l2months_sale_amount) AS percenttile_l2months_sale_amount
FROM fact_customer_anallytics__summarize
)

, fact_customer_anallytics__add_segment AS (
SELECT
  *
  , CASE
      WHEN percenttile_lifetime_sale_amount BETWEEN 0.8 AND 1 THEN 'High'
      WHEN percenttile_lifetime_sale_amount BETWEEN 0.5 AND 0.8 THEN 'Medium'
      WHEN percenttile_lifetime_sale_amount BETWEEN 0 AND 0.5 THEN 'Low'
      ELSE 'Invalid' END AS lifetime_segment
  , CASE
      WHEN percenttile_l2months_sale_amount BETWEEN 0.8 AND 1 THEN 'High'
      WHEN percenttile_l2months_sale_amount BETWEEN 0.5 AND 0.8 THEN 'Medium'
      WHEN percenttile_l2months_sale_amount BETWEEN 0 AND 0.5 THEN 'Low'
      ELSE 'Invalid' END AS l2months_segment
FROM fact_customer_anallytics__add_percentitle
)

SELECT
*
FROM fact_customer_anallytics__add_segment