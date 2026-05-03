{{
    config(materialized='table')
}}

with date_spine as (
    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('" ~ var('start_date') ~ "' as date)",
            end_date="dateadd(year, 3, current_date())"
        )
    }}
),

dates as (
    select
        cast(date_day as date)                                          as date_day
    from date_spine
)

select
    date_day                                                            as date_key,
    date_day,
    dayofweek(date_day)                                                 as day_of_week,
    dayname(date_day)                                                   as day_of_week_name,
    left(dayname(date_day), 3)                                          as day_of_week_name_short,
    day(date_day)                                                       as day_of_month,
    dayofyear(date_day)                                                 as day_of_year,
    weekofyear(date_day)                                                as week_of_year,
    month(date_day)                                                     as month_of_year,
    monthname(date_day)                                                 as month_name,
    left(monthname(date_day), 3)                                        as month_name_short,
    quarter(date_day)                                                   as quarter_of_year,
    year(date_day)                                                      as year_number,
    to_number(to_char(date_day, 'YYYYMM'))                              as year_month_key,
    to_number(to_char(date_day, 'YYYY') || quarter(date_day)::varchar)  as year_quarter_key,
    date_trunc('week', date_day)                                        as first_day_of_week,
    date_trunc('month', date_day)                                       as first_day_of_month,
    last_day(date_day, 'month')                                         as last_day_of_month,
    date_trunc('quarter', date_day)                                     as first_day_of_quarter,
    date_trunc('year', date_day)                                        as first_day_of_year,
    case when date_day = current_date() then true else false end         as is_today,
    case when date_day <= current_date() then true else false end        as is_past_or_today,
    case when date_trunc('month', date_day) = date_trunc('month', current_date())
        then true else false end                                        as is_current_month,
    case when date_trunc('quarter', date_day) = date_trunc('quarter', current_date())
        then true else false end                                        as is_current_quarter,
    case when year(date_day) = year(current_date())
        then true else false end                                        as is_current_year,
    case when dayofweek(date_day) not in (0, 6)
        then true else false end                                        as is_weekday
from dates
