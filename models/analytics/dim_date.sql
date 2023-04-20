 with dim_date__source as (
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2010-01-01', '2050-12-31', INTERVAL 1 DAY)) AS date 
  )

, dim_date__data_enrich as (
  select 
  * 
  ,FORMAT_DATE('%A', date) AS day_of_week
  ,FORMAT_DATE('%a', date) AS day_of_week_short
  ,DATE_TRUNC (date, month) as year_month
  ,FORMAT_DATE('%B', date) as month
  ,DATE_TRUNC (date, year) as year
  from dim_date__source
) 

select
  *
  ,case 
    when day_of_week_short in ('Mon','Tue','Wed','Thu','Fri') then 'Weekday'
    when day_of_week_short in ('Sun', 'Sat') then 'Weekend'
    else 'Invaid'
  end as is_weekday_or_weekend
from dim_date__data_enrich