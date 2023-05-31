WITH dim_customer_attribute__summarize AS(
SELECT
 customer_key
  , SUM(gross_amount) AS lifetime_sale_amount
  , COUNT(DISTINCT(order_key) )AS lifetime_sale_orders
  , COALESCE(SUM(CASE
      WHEN order_date BETWEEN (DATE_TRUNC('2016-05-31' , MONTH) - INTERVAl 12 MONTH) AND '2016-05-31' THEN gross_amount
    END),0) AS l12m_sale_amount 
  , COUNT(DISTINCT(CASE
      WHEN order_date BETWEEN (DATE_TRUNC('2016-05-31' , MONTH) - INTERVAl 12 MONTH) AND '2016-05-31' Then order_key
    END)) AS l12m_sale_orders
  , DATE_TRUNC(MAX(order_date), MONTH) AS customer_purchase_end 
  , DATE_TRUNC(MIN(order_date), MONTH) AS customer_purchase_start 
FROM {{ref('fact_sales_order_line')}}
GROUP BY 1
ORDER BY 1
)

, dim_customer_attribute__percentitle_monetary AS (
SELECT 
  customer_key
  , lifetime_sale_amount
  , lifetime_sale_orders
  , l12m_sale_amount
  , l12m_sale_orders
  , PERCENT_RANK() OVER( ORDER BY lifetime_sale_amount) AS lifetime_monetary_percenttile
  , PERCENT_RANK() OVER( ORDER BY l12m_sale_amount) AS l12m_monetary_percenttile
  , customer_purchase_end
  , customer_purchase_start
FROM dim_customer_attribute__summarize
)

, dim_customer_attribute__segment AS (
SELECT
  customer_key
  , lifetime_sale_amount
  , lifetime_sale_orders
  , l12m_sale_amount
  , l12m_sale_orders
  , lifetime_monetary_percenttile
  , l12m_monetary_percenttile
  , CASE
      WHEN lifetime_monetary_percenttile BETWEEN 0.8 AND 1 THEN 'High'
      WHEN lifetime_monetary_percenttile BETWEEN 0.5 AND 0.8 THEN 'Medium'
      WHEN lifetime_monetary_percenttile BETWEEN 0 AND 0.5 THEN 'Low'
      ELSE 'Invalid' END AS lifetime_segment
  , CASE
      WHEN l12m_monetary_percenttile BETWEEN 0.8 AND 1 THEN 'High'
      WHEN l12m_monetary_percenttile BETWEEN 0.5 AND 0.8 THEN 'Medium'
      WHEN l12m_monetary_percenttile BETWEEN 0 AND 0.5 THEN 'Low'
      ELSE 'Invalid' END AS l12m_segment
  , customer_purchase_end
  , customer_purchase_start
FROM dim_customer_attribute__percentitle_monetary
)
SELECT * FROM dim_customer_attribute__segment
ORDER BY 1
