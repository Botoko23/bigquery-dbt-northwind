select
  d                     as full_date,
  extract(YEAR from d)  as year,
  extract(MONTH from d) as month,
  extract(WEEK from d)  as year_week,
  extract(DAY from d)   as year_day,
  format_date('%B', d)  as month_name,
  format_date('%w', d)  as week_day,
  format_date('%A', d)  as day_name,
  (case when format_date('%A', d) in ('Sunday', 'Saturday') then 0 else 1 end) as is_weekday
from (
  select
    *
  from
    unnest(generate_date_array('2006-01-01', '2025-01-01', interval 1 day)) as d )




