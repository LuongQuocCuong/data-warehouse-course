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
  ,is_on_credit_hold as is_on_credit_hold
from dim_customer__source
)

, dim_customer__cast_data as(
select  
  cast(customer_key as integer) as customer_key
  ,cast (customer_name as string) as customer_name
  ,cast(customer_category_key as integer ) as customer_category_key
  ,cast(buying_group_key as integer) as buying_group_key
  ,cast (is_on_credit_hold as boolean) as is_on_credit_hold_boolean
from dim_customer__rename_coulumn
)

, dim_customer__convert_is_on_credit_hold as(
SELECT
  *
  , case 
      when is_on_credit_hold_boolean is true then 'On Hold Credit'
      when is_on_credit_hold_boolean is false then 'Not On Hold Credit'
      when is_on_credit_hold_boolean is null then 'Undefined'
      else 'invalid'
    END AS is_on_credit_hold
FROM dim_customer__cast_data
)

, dim_customer__handle_null as(
select 
    customer_key
  , coalesce (customer_name, 'Undefined') AS customer_name
  , customer_category_key
  , buying_group_key
  , coalesce(is_on_credit_hold,'Undefined' ) AS is_on_credit_hold
FROM dim_customer__convert_is_on_credit_hold
)

SELECT 
dim_customer.customer_key
,dim_customer.customer_name
,dim_customer.customer_category_key
,customer_categories.customer_category_name
,dim_customer.buying_group_key
,buying_group.buying_group_name
,dim_customer.is_on_credit_hold
FROM dim_customer__handle_null as dim_customer
left join {{ref('stg_customer_categories')}} as customer_categories
on customer_categories.customer_category_key = dim_customer.customer_category_key
left join {{ref('buying_group')}} as buying_group
on dim_customer.buying_group_key = buying_group.buying_group_key