with dim_person__source as (
SELECT *
FROM `vit-lam-data.wide_world_importers.application__people`
)
, dim_person__rename as (
SELECT 
  person_id as person_key
  , full_name
  , is_employee
  , is_salesperson as is_sales_person
  , preferred_name
FROM dim_person__source
)

, dim_person__cast_data as(
SELECT
  cast (person_key as integer) as person_key
  , cast(full_name as string) as full_name
  , cast(is_employee as boolean) as is_employee_boolean
  , cast(is_sales_person as boolean) as is_sales_person_boolean
  , cast (preferred_name as string) as preferred_name
FROM dim_person__rename
)

,dim_person__change_boolean as (
SELECT
*
  , CASE
      WHEN is_employee_boolean IS TRUE THEN 'Employee'
      WHEN is_employee_boolean IS FALSE THEN 'Not Employee'
      ELSE 'Invalid'
    END AS is_employee
  
  ,  CASE
      WHEN is_sales_person_boolean IS TRUE THEN 'Sales Person'
      WHEN is_sales_person_boolean IS FALSE THEN 'Not Sales Person'
      ELSE 'Invalid'
    END AS is_sales_person
FROM dim_person__cast_data
)

, dim_person__add_undefined_record as(
SELECT
person_key
, full_name
, is_employee
, is_sales_person
FROM dim_person__change_boolean
  UNION ALL
    SELECT  
      0 AS person_key
      ,'Undefined' AS full_name
      ,'Undefined' AS is_employee
      ,'Undefined' AS is_sales_person
  ,UNION ALL
    SELECT
      -1 AS person_key
      ,'Invalid' AS full_name
      ,'Invalid' AS is_employee
      ,'Invalid' AS is_sales_person
)

SELECT
person_key
, coalesce( full_name , 'Invalid') as full_name
, coalesce( is_employee , 'Invalid') as is_employee
, coalesce( is_sales_person , 'Invalid') as is_sales_person
FROM dim_person__add_undefined_record
