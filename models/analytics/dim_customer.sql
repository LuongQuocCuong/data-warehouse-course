with dim_customer__source as(
SELECT *
 FROM `vit-lam-data.wide_world_importers.sales__customers` 
)

, dim_customer__rename_coulumn as (
select
  customer_id as customer_key
  ,customer_name as customer_name
  ,customer_category_id as customer_category_key
  ,buying_group_id as buying_group_key
from dim_customer__source
)

, dim_customer__cast_data as(
select  
  cast(customer_key as integer) as customer_key
  ,cast (customer_name as string) as customer_name
  ,cast(customer_category_key as integer ) as customer_category_key
  ,cast(buying_group_key as integer) as buying_group_key
from dim_customer__rename_coulumn
)

SELECT 
dim_customer.customer_key
,dim_customer.customer_name
,dim_customer.customer_category_key
,customer_categories.customer_category_name
,dim_customer.buying_group_key
,buying_group.buying_group_name
FROM dim_customer__cast_data as dim_customer
left join {{ref('stg_customer_categories')}} as customer_categories
on customer_categories.customer_category_key = dim_customer.customer_category_key
left join {{ref('buying_group')}} as buying_group
on dim_customer.buying_group_key = buying_group.buying_group_key