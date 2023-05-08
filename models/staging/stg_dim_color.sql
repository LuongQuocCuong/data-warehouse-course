with color__source as (
SELECT 
*
FROM `vit-lam-data.wide_world_importers.warehouse__colors`
)

, color__source__rename_column as(
select
  color_id as color_key
  ,color_name
from color__source
)

, color__cast_type as (
select
  cast(color_key as integer) as color_key
  , cast(color_name as string ) as color_name
from color__source__rename_column
)

, color__add_undefined_record as (
SELECT
  color_key
  , color_name
FROM color__cast_type
UNION ALL
  SELECT
    0 as color_key
    ,'Undefined' as color_name
,UNION ALL
  SELECT
    -1 as color_key
    ,'Invalid' as color_name
)

select
  color_key
  ,color_name
from color__add_undefined_record
