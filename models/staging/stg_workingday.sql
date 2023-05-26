 with dim_date__source as (
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2013-01-01', '2050-12-31', INTERVAL 1 DAY)) AS date 
  )

, dim_date__data_enrich as (
  select 
  * 
  ,FORMAT_DATE('%a', date) AS day_of_week_short
  ,DATE_TRUNC (date, month) as year_month

  from dim_date__source
) 

, dim_date__add_column AS (
select
  *
  , case 
    when day_of_week_short in ('Mon','Tue','Wed','Thu','Fri') then '1' 
    when day_of_week_short in ('Sun', 'Sat') then '0'
    else 'Invaid'
  end as is_weekday_or_weekend 
from dim_date__data_enrich
)

, dim_date__cast_data AS (
SELECT
  year_month
  , CAST (is_weekday_or_weekend AS INTEGER) AS is_weekday_or_weekend
FROM dim_date__add_column
)

SELECT
 year_month
, SUM(is_weekday_or_weekend) AS working_day
FROM dim_date__cast_data
GROUP BY 1