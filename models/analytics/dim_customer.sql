WITH dim_customer__source AS(
SELECT *
 FROM `vit-lam-data.wide_world_importers.sales__customers` 
)

, dim_customer__rename_coulumn AS (
SELECT
  customer_id AS customer_key
  , customer_name AS customer_name
  , is_statement_sent
  , is_on_credit_hold
  , credit_limit
  , standard_discount_percentage
  , payment_days
  , account_opened_date
  , customer_category_id AS customer_category_key
  , buying_group_id AS buying_group_key
  , delivery_method_id AS delivery_method_key
  , delivery_city_id AS delivery_city_key
  , postal_city_id AS postal_city_key
  , primary_contact_person_id AS primary_contact_person_key
  , alternate_contact_person_id AS alternate_contact_person_key
  , bill_to_customer_id AS bill_to_customer_key
from dim_customer__source
)

, dim_customer__cast_data AS(
SELECT  
 CAST (customer_key AS integer) AS customer_key
  , CAST (customer_name AS string) AS customer_name
  , CAST (is_statement_sent AS boolean) AS is_statement_sent_boolean
  , CAST (is_on_credit_hold AS boolean) AS is_on_credit_hold_boolean
  , CAST (credit_limit AS numeric) AS credit_limit
  , CAST (standard_discount_percentage AS numeric) AS standard_discount_percentage
  , CAST (payment_days AS integer) AS payment_days
  , CAST (account_opened_date AS date) AS account_opened_date
  , CAST (customer_category_key AS integer) AS customer_category_key
  , CAST (buying_group_key AS integer) AS buying_group_key
  , CAST (delivery_method_key AS integer) AS delivery_method_key
  , CAST (delivery_city_key AS integer) AS delivery_city_key
  , CAST (postal_city_key AS integer) AS postal_city_key
  , CAST (primary_contact_person_key AS integer) AS primary_contact_person_key
  , CAST (alternate_contact_person_key AS integer) AS alternate_contact_person_key
  , CAST (bill_to_customer_key AS integer) AS bill_to_customer_key
FROM dim_customer__rename_coulumn
)

, dim_customer__convert_data AS(
SELECT
  *
  , CASE 
      WHEN is_on_credit_hold_boolean IS true THEN 'On Hold Credit'
      WHEN is_on_credit_hold_boolean IS false THEN 'Not On Hold Credit'
      WHEN is_on_credit_hold_boolean IS null THEN 'Undefined'
    ELSE 'invalid'
    END AS is_on_credit_hold
    , CASE 
      WHEN is_statement_sent_boolean IS true THEN 'Statement Sent'
      WHEN is_statement_sent_boolean IS false THEN 'Not Statement Sent'
      WHEN is_statement_sent_boolean IS null THEN 'Undefined'
    ELSE 'invalid'
    END AS is_statement_sent
FROM dim_customer__cast_data
)

, dim_customer__handle_null AS(
SELECT 
  customer_key
  , coalesce( customer_name , 'Undefined') AS customer_name
  , is_statement_sent
  , is_on_credit_hold
  , coalesce( credit_limit , 0) AS credit_limit
  , coalesce( standard_discount_percentage , 0) AS standard_discount_percentage
  , coalesce( payment_days , 0) AS payment_days
  , account_opened_date
  , coalesce( customer_category_key ,0) AS customer_category_key
  , coalesce( delivery_city_key ,0) AS delivery_city_key
  , coalesce( buying_group_key ,0) AS buying_group_key
  , coalesce( delivery_method_key ,0) AS delivery_method_key
  , coalesce( postal_city_key ,0) AS postal_city_key
  , coalesce( primary_contact_person_key ,0) AS primary_contact_person_key
  , coalesce( alternate_contact_person_key ,0) AS alternate_contact_person_key
  , coalesce( bill_to_customer_key ,0) AS bill_to_customer_key
FROM dim_customer__convert_data
)

, dim_customer__add_undefined_record AS (
SELECT 
  customer_key
  , customer_name
  , is_statement_sent
  , is_on_credit_hold
  , credit_limit
  , standard_discount_percentage
  , payment_days
  , account_opened_date
  , customer_category_key
  , delivery_city_key
  , buying_group_key
  , delivery_method_key
  , postal_city_key
  , primary_contact_person_key
  , alternate_contact_person_key
  , bill_to_customer_key
FROM dim_customer__handle_null
UNION ALL 
  SELECT
    0 AS customer_key
    , 'Undefined' AS customer_name
    , 'Undefined' AS is_statement_sent
    , 'Undefined' AS is_on_credit_hold
    , 0 AS credit_limit
    , 0 AS standard_discount_percentage
    , 0 AS payment_days
    , CAST(NULL AS DATE) AS account_opened_date
    , 0 AS customer_category_key
    , 0 AS delivery_city_key
    , 0 AS buying_group_key
    , 0 AS delivery_method_key
    , 0 AS postal_city_key
    , 0 AS primary_contact_person_key
    , 0 AS alternate_contact_person_key
    , 0 AS bill_to_customer_key

, UNION ALL 
  SELECT
    -1 AS customer_key
    , 'Invalid' AS customer_name
    , 'Invalid' AS is_statement_sent
    , 'Invalid' AS is_on_credit_hold
    , -1 AS credit_limit
    , -1 AS standard_discount_percentage
    , -1 AS payment_days
    , CAST(NULL AS DATE) AS account_opened_date
    , -1 AS customer_category_key
    , -1 AS delivery_city_key
    , -1 AS buying_group_key
    , -1 AS delivery_method_key
    , -1 AS postal_city_key
    , -1 AS primary_contact_person_key
    , -1 AS alternate_contact_person_key
    , -1 AS bill_to_customer_key
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
  , COALESCE ( customer_category.customer_category_name , 'Invalid') AS customer_category_name
  , dim_customer.buying_group_key
  , COALESCE ( buying_group.buying_group_name , 'Invalid') AS buying_group_name
  , dim_customer.delivery_method_key
  , COALESCE ( delivery_method.delivery_method_name , 'Invalid') AS delivery_method_name
  , dim_customer.delivery_city_key
  , COALESCE ( stg_delivery_location.supplier_delivery_city_name, 'Invalid') AS delivery_city_name
  , COALESCE ( stg_delivery_location.supplier_delivery_province_key, 0) AS delivery_state_province_key
  , COALESCE ( stg_delivery_location.supplier_delivery_province_name,'Invalid') AS delivery_state_province_name
  , COALESCE ( stg_delivery_location.supplier_delivery_country_key , 0) AS delivery_country_key
  , COALESCE ( stg_delivery_location.supplier_delivery_country_name,'Invalid') AS delivery_country_name
  , dim_customer.postal_city_key
  , COALESCE ( stg_postal_location.supplier_delivery_city_name , 'Invalid') AS postal_city_name
  , COALESCE ( stg_postal_location.supplier_delivery_province_key , 0 ) AS postal_state_province_key
  , COALESCE ( stg_postal_location.supplier_delivery_province_name , 'Invalid') AS postal_state_province_name
  , COALESCE ( stg_postal_location.supplier_delivery_country_key , 0 ) AS postal_state_country_key
  , COALESCE ( stg_postal_location.supplier_delivery_country_name ,'Invalid') AS postal_state_country_name
  , dim_customer.primary_contact_person_key
  , COALESCE ( dim_person__primary_contact.full_name , 'Invalid')AS primary_contact_person_name
  , dim_customer.alternate_contact_person_key
  , COALESCE ( dim_person__alternate_contact.full_name ,'Invalid') AS alternate_contact_person_name
  , dim_customer.customer_key AS bill_to_customer_key
  , COALESCE ( bill_to_customer.customer_name, 'Invalid') AS bill_to_customer_name
FROM dim_customer__add_undefined_record AS dim_customer
LEFT JOIN {{ref('stg_dim_customer_categories')}} AS customer_category
  ON customer_category.customer_category_key = dim_customer.customer_category_key
LEFT JOIN {{ref('stg_dim_buying_group')}} AS buying_group
  ON dim_customer.buying_group_key = buying_group.buying_group_key
LEFT JOIN {{ref('stg_dim_delivery_method')}} AS delivery_method
  ON delivery_method.delivery_method_key = dim_customer.delivery_method_key
LEFT JOIN {{ref("stg_dim_location")}} AS stg_delivery_location
  ON stg_delivery_location.supplier_delivery_city_key = dim_customer.delivery_city_key
LEFT JOIN {{ref("stg_dim_location")}}  AS stg_postal_location
  ON stg_postal_location.supplier_delivery_city_key = dim_customer.postal_city_key
LEFT JOIN {{ref('dim_person')}} AS dim_person__primary_contact
  ON dim_person__primary_contact.person_key = dim_customer.primary_contact_person_key
LEFT JOIN {{ref('dim_person')}} AS dim_person__alternate_contact
  ON dim_person__alternate_contact.person_key = dim_customer.alternate_contact_person_key
LEFT JOIN dim_customer__handle_null AS bill_to_customer
  ON bill_to_customer.customer_key =  dim_customer.customer_key
