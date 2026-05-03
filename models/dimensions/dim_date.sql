-- Dim Date: generated date spine using dbt_date
{{
    config(materialized='table')
}}

with date_spine as (
    {{
        dbt_date.get_date_dimension(
            start_date="var('start_date')",
            end_date="dateadd(year, 3, current_date())"
        )
    }}
)

select
    date_day                                                    as date_key,
    date_day,
    day_of_week,
    day_of_week_name,
    day_of_week_name_short,
    day_of_month,
    day_of_year,
    week_of_year,
    month_of_year,
    month_name,
    month_name_short,
    quarter_of_year,
    year_number,
    -- Composite keys for reporting
    to_char(date_day, 'YYYYMM')::integer                        as year_month_key,
    to_char(date_day, 'YYYYQ')::integer                         as year_quarter_key,
    -- Period flags
    date_trunc('week', date_day)                                as first_day_of_week,
    date_trunc('month', date_day)                               as first_day_of_month,
    last_day(date_day, 'month')                                 as last_day_of_month,
    date_trunc('quarter', date_day)                             as first_day_of_quarter,
    date_trunc('year', date_day)                                as first_day_of_year,
    -- Relative flags
    case when date_day = current_date() then true else false end as is_today,
    case when date_day <= current_date() then true else false end as is_past_or_today,
    case when date_day = date_trunc('month', current_date()) then true else false end as is_current_month,
    case when date_day = date_trunc('quarter', current_date()) then true else false end as is_current_quarter,
    case when year_number = year(current_date()) then true else false end as is_current_year,
    -- Business day flag (Mon-Fri)
    case when day_of_week not in (1, 7) then true else false end as is_weekday
from date_spine
