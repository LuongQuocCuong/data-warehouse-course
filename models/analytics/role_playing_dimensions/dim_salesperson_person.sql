SELECT
  person_key as salesperson_person_key
  , full_name as salesperson_person_name
  , is_employee	
  , is_sales_person
FROM {{ref('dim_person')}}
WHERE is_sales_person = 'Sales Person'
  OR person_key in (0,-1)