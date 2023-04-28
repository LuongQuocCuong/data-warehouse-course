SELECT
  person_key as picked_by_person_person_key
  , full_name as picked_by_person_person_name
  , is_employee	
  , is_sales_person
FROM {{ref('dim_person')}}
WHERE is_sales_person = 'Not Sales Person'
  OR person_key in (0,-1)