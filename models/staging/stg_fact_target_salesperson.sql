WITH fact_sale_target__source AS (
SELECT *
FROM `vit-lam-data.wide_world_importers.external__salesperson_target`
)

, fact_sale_target__rename AS (
SELECT 
  year_month
  , salesperson_person_id	AS salesperson_person_key
  , target_revenue AS target_gross_amount
FROM fact_sale_target__source
)

, fact_sale_target__cast_date AS (
SELECT
  CAST(salesperson_person_key AS INTEGER) AS salesperson_person_key
  , CAST(year_month AS DATE) AS year_month
  , CAST(target_gross_amount AS NUMERIC) AS target_gross_amount
FROM fact_sale_target__rename
)

SELECT
  salesperson_person_key
  , year_month
  , target_gross_amount
FROM fact_sale_target__cast_date
