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

select
  package_type_key
  ,package_type_name
from package_type__cast_type