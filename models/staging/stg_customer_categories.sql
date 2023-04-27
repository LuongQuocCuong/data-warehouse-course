with customer_categories__source as (
SELECT 
*
FROM `vit-lam-data.wide_world_importers.sales__customer_categories` 
)

, customer_categories__rename_column as(
select
  customer_category_id as customer_category_key
  ,customer_category_name
from customer_categories__source
)

, customer_categories__cast_type as (
select
  cast(customer_category_key as integer) as customer_category_key
  , cast(customer_category_name as string ) as customer_category_name
from customer_categories__rename_column
)

, customer_categories__add_undefined_record as (
SELECT
  customer_category_key
  , customer_category_name
FROM customer_categories__cast_type
UNION ALL
  SELECT
    0 as customer_category_key
    ,'Undefined' as customer_category_name
,UNION ALL
  SELECT
    -1 as customer_category_key
    ,'Invalid' as customer_category_name
)

select
  customer_category_key
  ,customer_category_name
from customer_categories__add_undefined_record