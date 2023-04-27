with buying_group__source as (
SELECT 
*
FROM `vit-lam-data.wide_world_importers.sales__buying_groups`
)

, buying_group__source__rename_column as(
select
  buying_group_id as buying_group_key
  ,buying_group_name
from buying_group__source
)

, buying_group__cast_type as (
select
  cast(buying_group_key as integer) as buying_group_key
  , cast(buying_group_name as string ) as buying_group_name
from buying_group__source__rename_column
)

select
  buying_group_key
  ,buying_group_name
from buying_group__cast_type