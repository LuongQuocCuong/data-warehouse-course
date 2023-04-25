with supplier_category__source as (
SELECT 
  *
FROM `vit-lam-data.wide_world_importers.purchasing__supplier_categories`
)

, supplier_category__rename_column as (
SELECT 
   supplier_category_id as supplier_category_key
  ,supplier_category_name	as supplier_category_name
FROM supplier_category__source
)

, supplier_category__cast_data as (
SELECT
  cast(supplier_category_key as integer) as supplier_category_key
  ,cast (supplier_category_name as string) as supplier_category_name
FROM supplier_category__rename_column
)

select 
  supplier_category_key
  , supplier_category_name
FROM supplier_category__cast_data