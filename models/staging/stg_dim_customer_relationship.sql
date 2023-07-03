WITH customer_relationship__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.external__customer_membership`
)
, customer_relationship__rename AS(
  SELECT
    customer_id AS customer_key
    , membership
    , begin_effective_date
    , end_effective_date
  FROM customer_relationship__source
)

, customer_relationship__cast_data AS(
  SELECT
    CAST(customer_key AS INTEGER) AS customer_key
    , CAST(membership AS STRING) AS membership
    , CAST(begin_effective_date AS DATE) AS begin_effective_date
    , CAST(end_effective_date AS DATE) AS end_effective_date
  FROM customer_relationship__rename
)

SELECT *
FROM customer_relationship__cast_data