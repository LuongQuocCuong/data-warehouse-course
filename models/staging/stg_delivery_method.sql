with delivery_method__source as (
SELECT 
*
FROM `vit-lam-data.wide_world_importers.application__delivery_methods`
)

, delivery_method__rename_column as(
select
  delivery_method_id as delivery_method_key
  , delivery_method_name
from delivery_method__source
)

, delivery_method__cast_type as (
select
  cast(delivery_method_key as integer) as delivery_method_key
  , cast(delivery_method_name as string ) as delivery_method_name
from delivery_method__rename_column
)

, delivery_method__add_undefined_record as (
SELECT
  delivery_method_key
  , delivery_method_name
FROM delivery_method__cast_type
UNION ALL
  SELECT
    0 as delivery_method_key
    ,'Undefined' as delivery_method_name
,UNION ALL
  SELECT
    -1 as delivery_method_key
    ,'Invalid' as delivery_method_name
)

select
  delivery_method_key
  ,delivery_method_name
from delivery_method__add_undefined_record
