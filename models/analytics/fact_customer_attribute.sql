WITH fact_snapshot_customer_attribute__summarize AS(
SELECT 
	customer_key
	, DATE_TRUNC(order_date , MONTH) AS year_month 
	, SUM(gross_amount) AS month_sale_amount 
FROM {{ref('fact_sales_order_line')}}
GROUP BY 1,2
)

, sale_order_line__date AS (
SELECT 
	DISTINCT (order_date) AS year_month
FROM {{ref('fact_sales_order_line')}}
)

,  sale_order_line__unique_date AS(
	SELECT 
DISTINCT(DATE_TRUNC(year_month , MONTH)) AS year_month
FROM sale_order_line__date
order by 1
)

, sale_order_line__unique_customer_key AS(
SELECT
	DISTINCT(customer_key) AS customer_key
FROM {{ref('fact_sales_order_line')}}
)

, sale_order_line__join AS (
	SELECT
	*
	FROM sale_order_line__unique_date
		CROSS JOIN sale_order_line__unique_customer_key
)

, fact_snapshot_customer_attribute__dense AS(
  SELECT 
  customer_key
  , year_month
  , COALESCE(month_sale_amount, 0) AS month_sale_amount
  FROM sale_order_line__join
  LEFT JOIN fact_snapshot_customer_attribute__summarize
    USING (customer_key, year_month)
)

, fact_snapshot_customer_attribute__calculate_column AS (
SELECT
	customer_key
	, year_month
	, month_sale_amount
	, SUM(month_sale_amount) OVER (PARTITION BY customer_key ORDER BY year_month) AS lifetime_sale_amount
FROM fact_snapshot_customer_attribute__dense
)

, fact_snapshot_customer_attribute__percentitle_monetary AS(
SELECT 
	customer_key
	, year_month
	, month_sale_amount	
	, lifetime_sale_amount
	, PERCENT_RANK() OVER(PARTITION BY year_month ORDER BY month_sale_amount) AS month_percentitle_monetary
	, PERCENT_RANK() OVER(PARTITION BY year_month ORDER BY lifetime_sale_amount) AS lifetime_percentitle_monetary
FROM fact_snapshot_customer_attribute__calculate_column
)

, fact_snapshot_customer_attribute__percentitle_monetary_segment AS(
SELECT
		customer_key
	, year_month
	, month_sale_amount	
	, lifetime_sale_amount
	, CASE
			WHEN month_percentitle_monetary BETWEEN 0.8 AND 1 THEN 'High'
			WHEN month_percentitle_monetary BETWEEN 0.5 AND 0.8 THEN 'Medium'
			WHEN month_percentitle_monetary < 0.5 THEN 'Low'
		ELSE 'Invalid'
		END AS month_percentitle_monetary_segment
	, CASE
			WHEN lifetime_percentitle_monetary BETWEEN 0.8 AND 1 THEN 'High'
			WHEN lifetime_percentitle_monetary BETWEEN 0.5 AND 0.8 THEN 'Medium'
			WHEN lifetime_percentitle_monetary < 0.5 THEN 'Low'
		ELSE 'Invalid'
		END AS lifetime_percentitle_monetary_segment
FROM fact_snapshot_customer_attribute__percentitle_monetary
)


SELECT
		fact_customer_attribute.customer_key 
	, fact_customer_attribute.year_month
	, fact_customer_attribute.month_sale_amount	
	, fact_customer_attribute.lifetime_sale_amount
	, fact_customer_attribute.month_percentitle_monetary_segment
	, fact_customer_attribute.lifetime_percentitle_monetary_segment
FROM fact_snapshot_customer_attribute__percentitle_monetary_segment AS fact_customer_attribute
LEFT JOIN {{ref('dim_customer_attribute')}} AS dim_customer_attribute
USING (customer_key)
WHERE fact_customer_attribute.year_month BETWEEN dim_customer_attribute.customer_purchase_start AND dim_customer_attribute.customer_purchase_end
ORDER BY 1, 2