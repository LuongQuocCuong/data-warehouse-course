with dim_person__source as (
SELECT *
FROM `vit-lam-data.wide_world_importers.application__people`
)

, dim_person__rename as (
SELECT 
  person_id as person_key
  , full_name
FROM dim_person__source
)

, dim_person__cast_data as(
SELECT
  cast (person_key as integer) as person_key
  ,cast(full_name as string) as full_name
FROM dim_person__rename
)

, dim_person__add_undifined_record as (
SELECT 
  person_id
  ,full_name
FROM dim_person__cast_data
union all 
SELECT
  '0' as person_id
  ,'Undifined' as full_name
)


SELECT 
  person_key
  ,coalesce (full_name, 'Undifined') as full_name
FROM dim_person__add_undifined_record