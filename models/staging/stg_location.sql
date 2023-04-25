SELECT 
  city.city_name as city_name
  , provinces.state_province_name as province_name
  , countries.country_name as country_name
  , city.city_id as city_key
  , provinces.state_province_id as province_key
  , provinces.country_id as country_key
FROM `vit-lam-data.wide_world_importers.application__cities` as city
LEFT JOIN `vit-lam-data.wide_world_importers.application__state_provinces` AS provinces
  ON city.state_province_id = provinces.state_province_id
LEFT JOIN `vit-lam-data.wide_world_importers.application__countries` AS countries
  ON provinces.country_id = countries.country_id