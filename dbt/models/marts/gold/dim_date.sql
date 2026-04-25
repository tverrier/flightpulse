{{
    config(
        materialized='table',
        tags=['gold', 'dim', 'critical'],
    )
}}

{# Date spine 1987-01-01 → today + 2 years. US federal holidays via static
   list — covers the 11 OPM-recognized holidays. Avoids a network dep. #}

with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('1987-01-01' as date)",
        end_date="dateadd(year, 2, current_date)"
    ) }}
),

dates as (
    select cast(date_day as date) as date_key from spine
),

holidays as (
    select column1::date as date_key, column2 as holiday_name from values
      ('2024-01-01', 'New Years Day'),
      ('2024-01-15', 'Martin Luther King Jr. Day'),
      ('2024-02-19', 'Washingtons Birthday'),
      ('2024-05-27', 'Memorial Day'),
      ('2024-06-19', 'Juneteenth'),
      ('2024-07-04', 'Independence Day'),
      ('2024-09-02', 'Labor Day'),
      ('2024-10-14', 'Columbus Day'),
      ('2024-11-11', 'Veterans Day'),
      ('2024-11-28', 'Thanksgiving'),
      ('2024-12-25', 'Christmas')
)

select
    d.date_key,
    year(d.date_key)                          as year,
    quarter(d.date_key)                       as quarter,
    month(d.date_key)                         as month,
    day(d.date_key)                           as day,
    dayofweekiso(d.date_key)                  as day_of_week,
    dayname(d.date_key)                       as day_name,
    dayofweekiso(d.date_key) in (6, 7)        as is_weekend,
    h.date_key is not null                    as is_us_federal_holiday,
    case when month(d.date_key) >= 10
         then year(d.date_key) + 1
         else year(d.date_key)
    end                                        as fiscal_year,
    case
        when month(d.date_key) in (10, 11, 12) then 1
        when month(d.date_key) in (1, 2, 3)    then 2
        when month(d.date_key) in (4, 5, 6)    then 3
        else 4
    end                                        as fiscal_quarter,
    weekiso(d.date_key)                       as iso_week
from dates d
left join holidays h using (date_key)
