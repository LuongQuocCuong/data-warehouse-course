WITH customer_relationship__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.external__customer_membership`
)

, customer_relationship__cast_data AS(
  SELECT
    CAST(customer_id AS INTEGER) AS customer_id
    , CAST(membership AS STRING) AS membership
    , CAST(begin_effective_date AS DATE) AS begin_effective_date
    , CAST(end_effective_date AS DATE) AS end_effective_date
  FROM customer_relationship__source
)

SELECT *
FROM customer_relationship__cast_data