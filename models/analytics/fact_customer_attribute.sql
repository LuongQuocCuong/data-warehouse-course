WITH fact_snapshot_customer_attribute__summarize AS(
SELECT 
	customer_key
	, DATE_TRUNC(order_date , MONTH) AS year_month 
	, SUM(gross_amount) AS month_sale_amount 
FROM `data-warehouse-course-384003`.`wide_world_importers_dwh`.`fact_sales_order_line`
GROUP BY 1,2
)

, sale_order_line__date AS (
SELECT 
	DISTINCT (order_date) AS year_month
FROM `data-warehouse-course-384003`.`wide_world_importers_dwh`.`fact_sales_order_line`
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
FROM `data-warehouse-course-384003`.`wide_world_importers_dwh`.`fact_sales_order_line`
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
ORDER BY 1,2
)

, fact_snapshot_customer_attribute__add_last_month_sale_amount AS(
SELECT
	fact.customer_key
	, fact.year_month
	, fact.month_sale_amount
	, fact.lifetime_sale_amount
	, fact_last_month.month_sale_amount AS last_month_sale_amount
FROM fact_snapshot_customer_attribute__calculate_column AS fact
LEFT JOIN fact_snapshot_customer_attribute__calculate_column AS fact_last_month
	ON fact.customer_key = fact_last_month.customer_key
	AND fact.year_month = fact_last_month.year_month + INTERVAL 1 MONTH
)

, fact_snapshot_customer_attribute__percentitle_monetary AS(
SELECT 
	customer_key
	, year_month
	, month_sale_amount	
	, last_month_sale_amount
	, lifetime_sale_amount
	, PERCENT_RANK() OVER(PARTITION BY year_month ORDER BY month_sale_amount) AS month_percentitle_monetary
	, PERCENT_RANK() OVER(PARTITION BY year_month ORDER BY lifetime_sale_amount) AS lifetime_percentitle_monetary
FROM fact_snapshot_customer_attribute__add_last_month_sale_amount
)

, fact_snapshot_customer_attribute__percentitle_monetary_segment AS(
SELECT
		customer_key
	, year_month
	, month_sale_amount	
	, last_month_sale_amount
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
	, fact_customer_attribute.last_month_sale_amount	
	, fact_customer_attribute.lifetime_sale_amount
	, fact_customer_attribute.month_percentitle_monetary_segment
	, fact_customer_attribute.lifetime_percentitle_monetary_segment
FROM fact_snapshot_customer_attribute__percentitle_monetary_segment AS fact_customer_attribute
LEFT JOIN `data-warehouse-course-384003`.`wide_world_importers_dwh`.`dim_customer_attribute` AS dim_customer_attribute
USING (customer_key)
WHERE fact_customer_attribute.year_month BETWEEN dim_customer_attribute.customer_purchase_start AND dim_customer_attribute.customer_purchase_end
ORDER BY 1, 2