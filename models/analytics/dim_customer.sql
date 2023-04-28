with dim_customer__source as(
SELECT *
 FROM `vit-lam-data.wide_world_importers.sales__customers` 
)

, dim_customer__rename_coulumn as (
select
  customer_id as customer_key
  , customer_name as customer_name
  , is_statement_sent
  , is_on_credit_hold
  , credit_limit
  , standard_discount_percentage
  , payment_days
  , account_opened_date
  , customer_category_id as customer_category_key
  , buying_group_id as buying_group_key
  , delivery_method_id as delivery_method_key
  , delivery_city_id as delivery_city_key
  , postal_city_id as postal_city_key
  , primary_contact_person_id as primary_contact_person_key
  , alternate_contact_person_id as alternate_contact_person_key
  , bill_to_customer_id as bill_to_customer_key
from dim_customer__source
)

, dim_customer__cast_data as(
select  
  cast(customer_key as integer) as customer_key
  , cast (customer_name as string) as customer_name
  , cast (is_statement_sent as boolean) as is_statement_sent_boolean
  , cast (is_on_credit_hold as boolean) as is_on_credit_hold_boolean
  , cast(credit_limit as numeric) as credit_limit
  , cast(standard_discount_percentage as numeric) as standard_discount_percentage
  , cast(payment_days as integer) as payment_days
  , cast(account_opened_date as date) as account_opened_date
  , cast(customer_category_key as integer) as customer_category_key
  , cast(buying_group_key as integer) as buying_group_key
  , cast(delivery_method_key as integer) as delivery_method_key
  , cast(delivery_city_key as integer) as delivery_city_key
  , cast(postal_city_key as integer) as postal_city_key
  , cast(primary_contact_person_key as integer) as primary_contact_person_key
  , cast(alternate_contact_person_key as integer) as alternate_contact_person_key
  , cast(bill_to_customer_key as integer) as bill_to_customer_key
FROM dim_customer__rename_coulumn
)

, dim_customer__convert_data as(
SELECT
  *
  , case 
      when is_on_credit_hold_boolean is true then 'On Hold Credit'
      when is_on_credit_hold_boolean is false then 'Not On Hold Credit'
      when is_on_credit_hold_boolean is null then 'Undefined'
      else 'invalid'
    END AS is_on_credit_hold
    , case 
      when is_statement_sent_boolean is true then 'Statement Sent'
      when is_statement_sent_boolean is false then 'Not Statement Sent'
      when is_statement_sent_boolean is null then 'Undefined'
      else 'invalid'
    END AS is_statement_sent
FROM dim_customer__cast_data
)

, dim_customer__handle_null as(
select 
 coalesce( customer_key , 0) as customer_key
  , coalesce( customer_name , 'Invalid') as customer_name
  , coalesce( is_statement_sent , 'Invalid') as is_statement_sent
  , coalesce( is_on_credit_hold , 'Invalid') as is_on_credit_hold
  , coalesce( credit_limit , 0) as credit_limit
  , coalesce( standard_discount_percentage , 0) as standard_discount_percentage
  , coalesce( payment_days , 0) as payment_days
  , account_opened_date
  , coalesce( customer_category_key ,0) as customer_category_key
  , coalesce( delivery_city_key ,0) as delivery_city_key
  , coalesce( buying_group_key ,0) as buying_group_key
  , coalesce( delivery_method_key ,0) as delivery_method_key
  , coalesce( postal_city_key ,0) as postal_city_key
  , coalesce( primary_contact_person_key ,0) as primary_contact_person_key
  , coalesce( alternate_contact_person_key ,0) as alternate_contact_person_key
  , coalesce( bill_to_customer_key ,0) as bill_to_customer_key
FROM dim_customer__convert_data
)

SELECT 
  dim_customer.customer_key
  , dim_customer.customer_name
  , dim_customer.is_statement_sent
  , dim_customer.is_on_credit_hold
  , dim_customer.credit_limit
  , dim_customer.standard_discount_percentage
  , dim_customer.payment_days
  , dim_customer.account_opened_date
  , dim_customer.customer_category_key
  , coalesce( customer_category.customer_category_name , 'Invalid') as customer_category_name
  , dim_customer.buying_group_key
  , coalesce( buying_group.buying_group_name , 'Invalid') as buying_group_name
  , dim_customer.delivery_method_key
  , coalesce( delivery_method.delivery_method_name , 'Invalid') as delivery_method_name
  , dim_customer.delivery_city_key
  , coalesce( stg_delivery_location.supplier_delivery_city_name, 'Invalid') as delivery_city_name
  , coalesce( stg_delivery_location.supplier_delivery_province_key, 0) as delivery_state_province_key
  , coalesce( stg_delivery_location.supplier_delivery_province_name,'Invalid') as delivery_state_province_name
  , coalesce( stg_delivery_location.supplier_delivery_country_key , 0) as delivery_country_key
  , coalesce( stg_delivery_location.supplier_delivery_country_name,'Invalid') as delivery_country_name
  , dim_customer.postal_city_key
  , coalesce( stg_postal_location.supplier_delivery_city_name , 'Invalid') as postal_city_name
  , coalesce( stg_postal_location.supplier_delivery_province_key , 0 ) as postal_state_province_key
  , coalesce( stg_postal_location.supplier_delivery_province_name , 'Invalid') as postal_state_province_name
  , coalesce( stg_postal_location.supplier_delivery_country_key , 0 ) as postal_state_country_key
  , coalesce( stg_postal_location.supplier_delivery_country_name ,'Invalid') as postal_state_country_name
  , dim_customer.primary_contact_person_key
  , coalesce( dim_person__primary_contact.full_name , 'Invalid')as primary_contact_person_name
  , dim_customer.alternate_contact_person_key
  , coalesce( dim_person__alternate_contact.full_name ,'Invalid') as alternate_contact_person_name
  , dim_customer.customer_key as bill_to_customer_key
  , coalesce( bill_to_customer.customer_name, 'Invalid') as bill_to_customer_name
FROM dim_customer__handle_null as dim_customer
LEFT JOIN {{ref('stg_customer_categories')}} as customer_category
  ON customer_category.customer_category_key = dim_customer.customer_category_key
LEFT JOIN {{ref('stg_buying_group')}} as buying_group
  ON dim_customer.buying_group_key = buying_group.buying_group_key
LEFT JOIN {{ref('stg_delivery_method')}} as delivery_method
  ON delivery_method.delivery_method_key = dim_customer.delivery_method_key
LEFT JOIN {{ref("stg_location")}} as stg_delivery_location
  ON stg_delivery_location.supplier_delivery_city_key = dim_customer.delivery_city_key
LEFT JOIN {{ref("stg_location")}}  as stg_postal_location
  ON stg_postal_location.supplier_delivery_city_key = dim_customer.postal_city_key
LEFT JOIN {{ref('dim_person')}} as dim_person__primary_contact
  ON dim_person__primary_contact.person_key = dim_customer.primary_contact_person_key
LEFT JOIN {{ref('dim_person')}} as dim_person__alternate_contact
  ON dim_person__alternate_contact.person_key = dim_customer.alternate_contact_person_key
LEFT JOIN dim_customer__handle_null as bill_to_customer
  ON bill_to_customer.customer_key =  dim_customer.customer_key
