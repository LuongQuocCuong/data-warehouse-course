with package_type__source as (
SELECT 
*
FROM `vit-lam-data.wide_world_importers.warehouse__package_types`
)

, package_type__rename_column as(
select
  package_type_id as package_type_key
  ,package_type_name
from package_type__source
)

, package_type__cast_type as (
select
  cast(package_type_key as integer) as package_type_key
  , cast(package_type_name as string ) as package_type_name
from package_type__rename_column
)

, package_type__add_undefined_record as (
SELECT
  package_type_key
  , package_type_name
FROM package_type__cast_type
UNION ALL
  SELECT
    0 as package_type_key
    ,'Undefined' as package_type_name
,UNION ALL
  SELECT
    -1 as package_type_key
    ,'Invalid' as package_type_name
)

select
  package_type_key
  ,coalesce( package_type_name ,'Invalid') as package_type_name
from package_type__add_undefined_record